// ----------------------------------------------------------------------
// File: FileInspector.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include "common/Path.hh"
#include "common/FileId.hh"
#include "common/IntervalStopwatch.hh"
#include "common/LayoutId.hh"
#include "common/Timing.hh"
#include "common/ParseUtils.hh"
#include "mgm/Master.hh"
#include "mgm/FsView.hh"
#include "mgm/inspector/FileInspector.hh"
#include "mgm/proc/ProcCommand.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/XrdMgmOfsDirectory.hh"
#include "namespace/interface/IView.hh"
#include "namespace/ns_quarkdb/inspector/FileScanner.hh"
#include "namespace/ns_quarkdb/FileMD.hh"
#include "namespace/Resolver.hh"
#include "namespace/Prefetcher.hh"
#include <qclient/QClient.hh>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FileInspector::FileInspector() : timeCurrentScan(0), timeLastScan(0), nfiles(0),
  ndirs(0)
{
  mVid = eos::common::VirtualIdentity::Root();
  mThread.reset(&FileInspector::backgroundThread, this);
  scanned_percent.store(0, std::memory_order_seq_cst);
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
FileInspector::~FileInspector()
{
  mThread.join();
}

//------------------------------------------------------------------------------
// Retrieve current file inspector configuration options
//------------------------------------------------------------------------------
FileInspector::Options FileInspector::getOptions()
{
  eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
  FileInspector::Options opts;
  // Default options
  opts.enabled = false;
  opts.interval = std::chrono::minutes(4 * 60);

  if (FsView::gFsView.mSpaceView.count("default")) {
    if (FsView::gFsView.mSpaceView["default"]->GetConfigMember("inspector") ==
        "on") {
      opts.enabled = true;
    }

    int64_t intv = 0;
    std::string interval =
      FsView::gFsView.mSpaceView["default"]->GetConfigMember("inspector.interval");

    if (!interval.empty()) {
      common::ParseInt64(interval, intv);

      if (intv) {
        opts.interval = std::chrono::seconds(intv);
      }
    }
  }

  if (opts.enabled) {
    enable();
    eos_static_debug("file inspector is enabled - interval = %ld seconds",
                     opts.interval.count());
  } else {
    disable();
  }

  return opts;
}


//------------------------------------------------------------------------------
// Background Thread cleaning up left-over atomic uploads
//------------------------------------------------------------------------------
void
FileInspector::backgroundThread(ThreadAssistant& assistant) noexcept
{
  gOFS->WaitUntilNamespaceIsBooted(assistant);
  // set the initial state after boot
  Options opts = getOptions();

  if (opts.enabled) {
    enable();
  } else {
    disable();
  }

  assistant.wait_for(std::chrono::seconds(10));
  eos_static_info("msg=\"async thread started\"");

  while (!assistant.terminationRequested()) {
    // every now and then we wake up
    Options opts = getOptions();

    // Only a master needs to run a FileInspector
    if (opts.enabled) {
      enable();
    } else {
      disable();
    }

    common::IntervalStopwatch stopwatch(std::chrono::seconds(60));

    if (opts.enabled && gOFS->mMaster->IsMaster()) {
      eos_static_info("msg=\"scan started!\"");
      {
        std::lock_guard<std::mutex> sMutex(mutexScanStats);
        timeCurrentScan = time(NULL);
      }

      if (gOFS->eosView->inMemory()) {
        performCycleInMem(assistant);
      } else {
        performCycleQDB(assistant);
      }

      eos_static_info("msg=\"scan finished!\"");
    }

    assistant.wait_for(stopwatch.timeRemainingInCycle());
  }
}

//------------------------------------------------------------------------------
// Perform a single inspector cycle, in-memory namespace
//------------------------------------------------------------------------------

void FileInspector::performCycleInMem(ThreadAssistant& assistant) noexcept
{
  // Do a slow find
  unsigned long long nfiles_processed;
  nfiles = ndirs = nfiles_processed = 0;
  time_t s_time = time(NULL);
  {
    eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex, __FUNCTION__,
                                            __LINE__, __FILE__);
    nfiles = (unsigned long long) gOFS->eosFileService->getNumFiles();
    ndirs = (unsigned long long) gOFS->eosDirectoryService->getNumContainers();
  }
  time_t ms = 1;

  if (ndirs > 10000000) {
    ms = 0;
  }

  Options opts = getOptions();
  uint64_t interval = opts.interval.count();
  eos_static_info("msg=\"start inspector scan\" ndir=%llu nfiles=%llu ms=%u",
                  ndirs, nfiles, ms);

  if (!nfiles) {
    // nothing to scan
    return;
  }

  std::map<std::string, std::set<std::string> > inspectordirs;
  XrdOucString stdErr;

  if (!gOFS->_find("/", mError, stdErr, mVid, inspectordirs, 0,
                   "*", true, ms, false)) {
    eos_static_info("msg=\"finished inspector find\" inspector-dirs=%llu",
                    inspectordirs.size());
    time_t c_time = time(NULL);

    // scan backwards ...
    for (auto it = inspectordirs.rbegin(); it != inspectordirs.rend(); it++) {
      if (it->first.substr(0, gOFS->MgmProcPath.length()) ==
          gOFS->MgmProcPath.c_str()) {
        // skip over proc directory entries
        continue;
      }

      // Get the attributes
      eos_static_debug("inspector-dir=\"%s\"", it->first.c_str());
      // loop over all files
      XrdMgmOfsDirectory dir;
      int listrc = dir.open(it->first.c_str(), mVid, "ls.skip.directories=true");

      if (!listrc) {
        const char* item;

        while ((item = dir.nextEntry())) {
          nfiles_processed++;
          std::string filepath = it->first + item;
          Process(filepath);
        }
      }

      scanned_percent.store(100.0 * nfiles_processed / nfiles,
                            std::memory_order_seq_cst);
      time_t target_time = (1.0 * nfiles_processed / nfiles) * interval;
      time_t is_time = time(NULL) - s_time;

      if (target_time > is_time) {
        uint64_t p_time = target_time - is_time;

        if (p_time > 5) {
          p_time = 5;
        }

        eos_static_debug("is:%lu target:%lu is_t:%lu target_t:%lu interval:%lu - pausing for %lu seconds\n",
                         nfiles_processed, nfiles, is_time, target_time, interval, p_time);
        // pause for the diff ...
        std::this_thread::sleep_for(std::chrono::seconds(p_time));
      }

      if (assistant.terminationRequested()) {
        return;
      }

      if ((time(NULL) - c_time) > 60) {
        c_time = time(NULL);
        Options opts = getOptions();
        interval = opts.interval.count();

        if (!opts.enabled) {
          // interrupt the scan
          break;
        }

        if (!gOFS->mMaster->IsMaster()) {
          // interrupt the scan
          break;
        }
      }
    }
  }

  scanned_percent.store(100.0, std::memory_order_seq_cst);
  std::lock_guard<std::mutex> sMutex(mutexScanStats);
  lastScanStats = currentScanStats;
  lastFaultyFiles = currentFaultyFiles;
  timeLastScan = timeCurrentScan;
}

//------------------------------------------------------------------------------
// Perform a single inspector cycle, QDB namespace
//------------------------------------------------------------------------------
void FileInspector::performCycleQDB(ThreadAssistant& assistant) noexcept
{
  eos_static_info("msg=\"start FileInspector scan on QDB\"");

  //----------------------------------------------------------------------------
  // Initialize qclient..
  //----------------------------------------------------------------------------
  if (!mQcl) {
    mQcl.reset(new qclient::QClient(gOFS->mQdbContactDetails.members,
                                    gOFS->mQdbContactDetails.constructOptions()));
  }

  //this have been put in case of a test and will not be in the final version of the system
  {
    std::string member = gOFS->mQdbContactDetails.members.toString();
    eos_static_info(member.c_str());
    eos_static_info("member:=%s", member.c_str());
  }
  //----------------------------------------------------------------------------
  // Start scanning files
  //----------------------------------------------------------------------------
  unsigned long long nfiles_processed;
  nfiles = ndirs = nfiles_processed = 0;
  time_t s_time = time(NULL);
  {
    eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex, __FUNCTION__,
                                            __LINE__, __FILE__);
    nfiles = (unsigned long long) gOFS->eosFileService->getNumFiles();
    ndirs = (unsigned long long) gOFS->eosDirectoryService->getNumContainers();
  }
  Options opts = getOptions();
  uint64_t interval = opts.interval.count();
  FileScanner scanner(*(mQcl.get()));
  time_t c_time = s_time;

  while (scanner.valid()) {
    scanner.next();
    std::string err;
    eos::ns::FileMdProto item;

    if (scanner.getItem(item)) {
      std::shared_ptr<eos::QuarkFileMD> fmd = std::make_shared<eos::QuarkFileMD>();
      fmd->initialize(std::move(item));
      Process(fmd);
      nfiles_processed++;
      scanned_percent.store(100.0 * nfiles_processed / nfiles,
                            std::memory_order_seq_cst);
      time_t target_time = (1.0 * nfiles_processed / nfiles) * interval;
      time_t is_time = time(NULL) - s_time;

      //here it goes wrong
      if (target_time > is_time) {
        uint64_t p_time = target_time - is_time;

        if (p_time > 5) {
          p_time = 5;
        }

        eos_static_debug("is:%lu target:%lu is_t:%lu target_t:%lu interval:%lu - pausing for %lu seconds\n",
                         nfiles_processed, nfiles, is_time, target_time, interval, p_time);
        // pause for the diff ...
        std::this_thread::sleep_for(std::chrono::seconds(p_time));
      }

      if (assistant.terminationRequested()) {
        return;
      }

      if ((time(NULL) - c_time) > 60) {
        c_time = time(NULL);
        Options opts = getOptions();
        interval = opts.interval.count();

        if (!opts.enabled) {
          // interrupt the scan
          break;
        }

        if (!gOFS->mMaster->IsMaster()) {
          // interrupt the scan
          break;
        }
      }
    }

    if (scanner.hasError(err)) {
      eos_static_err("msg=\"QDB scanner error - interrupting scan\" error=\"%s\"",
                     err.c_str());
      break;
    }
  }

  //this differ as well
  scanned_percent.store(100.0, std::memory_order_seq_cst);
  std::lock_guard<std::mutex> sMutex(mutexScanStats);
  lastScanStats = currentScanStats;
  lastFaultyFiles = currentFaultyFiles;
  timeLastScan = timeCurrentScan;
}


//------------------------------------------------------------------------------
// Process a given fmd object
//------------------------------------------------------------------------------

void
FileInspector::Process(std::shared_ptr<eos::IFileMD> fmd)
{
  if (fmd->isLink()) {
    return;
  }

  uint64_t lid = fmd->getLayoutId();
  std::lock_guard<std::mutex> sMutex(mutexScanStats);

  // zero size files
  if (!fmd->getSize()) {
    currentScanStats[lid]["zerosize"]++;
  } else {
    currentScanStats[lid]["volume"] += fmd->getSize();
  }

  // no location files
  if (!fmd->getNumLocation()) {
    currentScanStats[lid]["nolocation"]++;
    currentFaultyFiles["nolocation"].insert(fmd->getId());
  }

  eos::IFileMD::LocationVector l = fmd->getLocations();
  eos::IFileMD::LocationVector u_l = fmd->getUnlinkedLocations();

  for (auto const& fs : l) {
    if (!FsView::gFsView.HasMapping(fs)) {
      // shadow filesystem
      currentScanStats[lid]["shadowlocation"]++;
      currentFaultyFiles["shadowlocation"].insert(fmd->getId());
    }
  }

  for (auto const& fs : u_l) {
    if (!FsView::gFsView.HasMapping(fs)) {
      // shadow filesystem
      currentScanStats[lid]["shadowdeletion"]++;
      currentFaultyFiles["shadowdeletion"].insert(fmd->getId());
    }
  }

  // unlinked locations
  currentScanStats[lid]["unlinkedlocations"] += fmd->getNumUnlinkedLocation();
  // linked locations
  currentScanStats[lid]["locations"] += fmd->getNumLocation();
  // stripe number
  size_t stripes = eos::common::LayoutId::GetStripeNumber(lid) + 1;
  std::string tag = "repdelta:";
  int64_t sdiff = fmd->getNumLocation() - stripes;

  if (sdiff == 0) {
    tag += "0";
  } else if (sdiff < 0) {
    tag += std::to_string(sdiff);
    currentFaultyFiles[tag].insert(fmd->getId());
  } else {
    tag += "+";
    tag += std::to_string(sdiff);
    currentFaultyFiles[tag].insert(fmd->getId());
  }

  currentScanStats[lid][tag]++;
}

//------------------------------------------------------------------------------
// Process a given path
//------------------------------------------------------------------------------

void
FileInspector::Process(std::string& filepath)
{
  eos_static_debug("inspector-file=\"%s\"", filepath.c_str());
  eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
  eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex, __FUNCTION__,
                                          __LINE__, __FILE__);
  std::shared_ptr<eos::IFileMD> fmd;

  try {
    fmd = gOFS->eosView->getFile(filepath, false);
    Process(fmd);
  } catch (eos::MDException& e) {
    std::lock_guard<std::mutex> sMutex(mutexScanStats);
    currentScanStats[999999999]["unfound"]++;
    eos_static_crit("path=%s not found", filepath.c_str());
  }
}

//------------------------------------------------------------------------------
// Dump current status
//------------------------------------------------------------------------------

void
FileInspector::Dump(std::string& out, std::string& options)
{
  char line[4096];
  time_t now = time(NULL);
  std::lock_guard<std::mutex> sMutex(mutexScanStats);

  if (options.find("m") != std::string::npos) {
    for (auto it = lastScanStats.begin(); it != lastScanStats.end(); ++it) {
      snprintf(line, sizeof(line),
               "key=last layout=%08lx type=%s checksum=%s blockchecksum=%s blocksize=%s",
               it->first,
               eos::common::LayoutId::GetLayoutTypeString(it->first),
               eos::common::LayoutId::GetChecksumStringReal(it->first),
               eos::common::LayoutId::GetBlockChecksumString(it->first),
               eos::common::LayoutId::GetBlockSizeString(it->first));
      out += line;

      for (auto mit = it->second.begin(); mit != it->second.end(); ++mit) {
        snprintf(line, sizeof(line), " %s=%lu",  mit->first.c_str(), mit->second);
        out += line;
      }

      out += "\n";
    }

    return;
  }

  out += "# ------------------------------------------------------------------------------------\n";
  out += "# ";
  out += eos::common::Timing::ltime(now);
  out += "\n";

  if (!enabled()) {
    out += "# inspector is disabled - use 'eos space config default space.inspector=on'\n";
  }

  Options opts = getOptions();
  out += "# ";
  out += std::to_string((int)(scanned_percent.load()));
  out += " % done - estimate to finish: ";
  out += std::to_string((int)(opts.interval.count() - (scanned_percent.load() *
                              opts.interval.count() / 100.0)));
  out += " seconds\n";

  if ((options.find("c") != std::string::npos)) {
    if (options.find("p") != std::string::npos) {
      for (auto& i : currentFaultyFiles) {
        for (auto& s : i.second) {
          out += "fxid:";
          out += eos::common::FileId::Fid2Hex(s);
          out += " ";
          out += i.first;
          out += "\n";
        }
      }
    } else if (options.find("e") != std::string::npos) {
      std::string exportname = "/var/log/eos/mgm/FileInspector.";
      exportname += std::to_string(now);
      exportname += ".list";
      std::ofstream exportfile(exportname);

      if (exportfile.is_open()) {
        for (auto& i : currentFaultyFiles) {
          for (auto& s : i.second) {
            exportfile << "fxid:" << eos::common::FileId::Fid2Hex(s) << " " << i.first <<
                       "\n";
          }
        }

        out += "# file list exported on MGM to '";
        out += exportname;
        out += "'\n";
        exportfile.close();
      } else {
        out += "# file list could not be written on MGM to '";
        out += exportname;
        out += "'\n";
      }
    } else {
      out += "# current scan: ";
      out += eos::common::Timing::ltime(timeCurrentScan).c_str();
      out += "\n";
      out += " not-found-during-scan            : ";
      out += std::to_string(currentScanStats[999999999]["unfound"]);
      out += "\n";

      for (auto it = currentScanStats.begin(); it != currentScanStats.end(); ++it) {
        if (it->first == 999999999) {
          continue;
        }

        snprintf(line, sizeof(line),
                 " layout=%08lx type=%-13s checksum=%-8s blockchecksum=%-8s blocksize=%-4s\n\n",
                 it->first,
                 eos::common::LayoutId::GetLayoutTypeString(it->first),
                 eos::common::LayoutId::GetChecksumStringReal(it->first),
                 eos::common::LayoutId::GetBlockChecksumString(it->first),
                 eos::common::LayoutId::GetBlockSizeString(it->first));
        out +=  "======================================================================================\n";
        out += line;

        for (auto mit = it->second.begin(); mit != it->second.end(); ++mit) {
          snprintf(line, sizeof(line), " %-32s : %lu\n",  mit->first.c_str(),
                   mit->second);
          out += line;
        }

        out += "\n";
      }
    }
  }

  if ((options.find("l") != std::string::npos)) {
    if (options.find("p") != std::string::npos) {
      for (auto& i : lastFaultyFiles) {
        for (auto& s : i.second) {
          out += "fxid:";
          out += eos::common::FileId::Fid2Hex(s);
          out += " ";
          out += i.first;
          out += "\n";
        }
      }
    } else if (options.find("e") != std::string::npos) {
      std::string exportname = "/var/log/eos/mgm/FileInspector.";
      exportname += std::to_string(now);
      exportname += ".list";
      std::ofstream exportfile(exportname);

      if (exportfile.is_open()) {
        for (auto& i : lastFaultyFiles) {
          for (auto& s : i.second) {
            exportfile << "fxid:" << eos::common::FileId::Fid2Hex(s) << " " << i.first <<
                       "\n";
          }
        }

        out += "# file list exported on MGM to '";
        out += exportname;
        out += "'\n";
        exportfile.close();
      } else {
        out += "# file list could not be written on MGM to '";
        out += exportname;
        out += "'\n";
      }
    } else {
      out += "# last scan: ";
      out += eos::common::Timing::ltime(timeLastScan).c_str();
      out += "\n";
      out += " not-found-during-scan            : ";
      out += std::to_string(lastScanStats[999999999]["unfound"]);
      out += "\n";

      for (auto it = lastScanStats.begin(); it != lastScanStats.end(); ++it) {
        if (it->first == 999999999) {
          continue;
        }

        snprintf(line, sizeof(line),
                 " layout=%08lx type=%-13s checksum=%-8s blockchecksum=%-8s blocksize=%-4s\n\n",
                 it->first,
                 eos::common::LayoutId::GetLayoutTypeString(it->first),
                 eos::common::LayoutId::GetChecksumStringReal(it->first),
                 eos::common::LayoutId::GetBlockChecksumString(it->first),
                 eos::common::LayoutId::GetBlockSizeString(it->first));
        out +=  "======================================================================================\n";
        out += line;

        for (auto mit = it->second.begin(); mit != it->second.end(); ++mit) {
          snprintf(line, sizeof(line), " %-32s : %lu\n",  mit->first.c_str(),
                   mit->second);
          out += line;
        }

        out += "\n";
      }
    }
  }

  out += "# ------------------------------------------------------------------------------------\n";
}

EOSMGMNAMESPACE_END

