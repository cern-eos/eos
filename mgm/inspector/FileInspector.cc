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
FileInspector::FileInspector(std::string_view space_name) :
  timeCurrentScan(0), timeLastScan(0), nfiles(0), ndirs(0),
  mSpaceName(space_name)
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
  FileInspector::Options opts;
  // Default options
  opts.enabled = false;
  opts.interval = std::chrono::minutes(4 * 60);
  eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);

  if (FsView::gFsView.mSpaceView.count(mSpaceName)) {
    if (FsView::gFsView.mSpaceView[mSpaceName]->GetConfigMember("inspector") ==
        "on") {
      opts.enabled = true;
    }

    int64_t intv = 0;
    std::string interval =
      FsView::gFsView.mSpaceView[mSpaceName]->GetConfigMember("inspector.interval");

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
        std::lock_guard<std::mutex> lock(mutexScanStats);
        timeCurrentScan = time(NULL);
      }
      performCycleQDB(assistant);
      eos_static_info("msg=\"scan finished!\"");
    }

    assistant.wait_for(stopwatch.timeRemainingInCycle());
  }
}

//------------------------------------------------------------------------------
// Perform a single inspector cycle, QDB namespace
//------------------------------------------------------------------------------
void FileInspector::performCycleQDB(ThreadAssistant& assistant) noexcept
{
  eos_static_info("msg=\"start FileInspector scan on QDB\"");

  // Initialize qclient..
  if (!mQcl) {
    mQcl.reset(new qclient::QClient(gOFS->mQdbContactDetails.members,
                                    gOFS->mQdbContactDetails.constructOptions()));
  }

  // Start scanning files
  unsigned long long nfiles_processed;
  nfiles = ndirs = nfiles_processed = 0;
  time_t s_time = time(NULL);
  {
    eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex);
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

      if (target_time > is_time) {
        uint64_t p_time = target_time - is_time;

        if (p_time > 5) {
          p_time = 5;
        }

        eos_static_debug("is:%lu target:%lu is_t:%lu target_t:%lu interval:%lu "
                         "- pausing for %lu seconds\n",
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

  scanned_percent.store(100.0, std::memory_order_seq_cst);
  std::lock_guard<std::mutex> lock(mutexScanStats);
  lastScanStats = currentScanStats;
  lastFaultyFiles = currentFaultyFiles;
  lastAccessTimeFiles = currentAccessTimeFiles;
  lastAccessTimeVolume = currentAccessTimeVolume;
  lastBirthTimeFiles = currentBirthTimeFiles;
  lastBirthTimeVolume = currentBirthTimeVolume;
  currentScanStats.clear();
  currentFaultyFiles.clear();
  currentAccessTimeFiles.clear();
  currentAccessTimeVolume.clear();
  currentBirthTimeFiles.clear();
  currentBirthTimeVolume.clear();
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
  std::lock_guard<std::mutex> lock(mutexScanStats);

  // zero size files
  if (!fmd->getSize()) {
    currentScanStats[lid]["zerosize"]++;
  } else {
    currentScanStats[lid]["volume"] += fmd->getSize();
    currentScanStats[lid]["physicalsize"] += (fmd->getSize() * eos::common::LayoutId::GetSizeFactor(lid));
  }

  // no location files
  if (!fmd->getNumLocation()) {
    currentScanStats[lid]["nolocation"]++;
    currentFaultyFiles["nolocation"].insert
    (std::make_pair(fmd->getId(), fmd->getLayoutId()));
  }

  eos::IFileMD::LocationVector l = fmd->getLocations();
  eos::IFileMD::LocationVector u_l = fmd->getUnlinkedLocations();

  for (auto const& fs : l) {
    if (!FsView::gFsView.HasMapping(fs)) {
      // shadow filesystem
      currentScanStats[lid]["shadowlocation"]++;
      currentFaultyFiles["shadowlocation"].insert
      (std::make_pair(fmd->getId(), fmd->getLayoutId()));
    }
  }

  for (auto const& fs : u_l) {
    if (!FsView::gFsView.HasMapping(fs)) {
      // shadow filesystem
      currentScanStats[lid]["shadowdeletion"]++;
      currentFaultyFiles["shadowdeletion"].insert
      (std::make_pair(fmd->getId(), fmd->getLayoutId()));
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
    currentFaultyFiles[tag].insert(std::make_pair(fmd->getId(),
                                   fmd->getLayoutId()));
  } else {
    tag += "+";
    tag += std::to_string(sdiff);
    currentFaultyFiles[tag].insert(std::make_pair(fmd->getId(),
                                   fmd->getLayoutId()));
  }

  currentScanStats[lid][tag]++;
  static std::set<double> time_bin {0, 86400ll, 7 * 86400ll, 30 * 86400ll, 90 * 86400ll,
                                    182.5 * 86400ll, 365 * 86400ll, 2 * 365 * 86400ll, 5 * 365 * 86400ll};
  {
    // create access time distributions
    set<int>::reverse_iterator rev_it;
    eos::IFileMD::ctime_t atime;
    fmd->getATime(atime);

    // future access time goes to bin 0
    if (atime.tv_sec > timeCurrentScan) {
      currentAccessTimeFiles[0]++;
      currentAccessTimeVolume[0] += fmd->getSize();
    } else {
      for (auto rev_it = time_bin.rbegin(); rev_it != time_bin.rend(); rev_it++) {
	if ((timeCurrentScan - (int64_t)atime.tv_sec) >= (int64_t) *rev_it) {
	  currentAccessTimeFiles[(uint64_t)*rev_it]++;
	  currentAccessTimeVolume[*rev_it] += fmd->getSize();
	  break;
	}
      }
    }
  }
  {
    // create birth time distributions
    set<int>::reverse_iterator rev_it;

    eos::IFileMD::ctime_t btime {0, 0};
    eos::IFileMD::XAttrMap xattrs = fmd->getAttributes();
    if (xattrs.count("sys.eos.btime")) {
      eos::common::Timing::Timespec_from_TimespecStr(xattrs["sys.eos.btime"], btime);
    }
    
    // future birth time goes to bin 0
    if (btime.tv_sec > timeCurrentScan) {
      currentBirthTimeFiles[0]++;
      currentBirthTimeVolume[0] += fmd->getSize();
    } else {
      for (auto rev_it = time_bin.rbegin(); rev_it != time_bin.rend(); rev_it++) {
	if ((timeCurrentScan - (int64_t)btime.tv_sec) >= (int64_t) *rev_it) {
	  currentBirthTimeFiles[(uint64_t)*rev_it]++;
	  currentBirthTimeVolume[*rev_it] += fmd->getSize();
	  break;
	}
      }
    }
  }
}

//------------------------------------------------------------------------------
// Dump current status
//------------------------------------------------------------------------------
void
FileInspector::Dump(std::string& out, std::string_view options)
{
  char line[4096];
  time_t now = time(NULL);
  const bool is_monitoring = (options.find('m') != std::string::npos);

  if (!is_monitoring) {
    out += "# ------------------------------------------------------------------------------------\n";
    out += "# ";
    out += eos::common::Timing::ltime(now);
    out += "\n";
  }

  if (!enabled()) {
    if (is_monitoring) {
      out = "key=error space=" + mSpaceName + " msg=\"inspector disabled\"";
    } else {
      out += "# inspector is disabled - use 'eos space config default space.inspector=on'\n";
    }

    return;
  }

  std::lock_guard<std::mutex> lock(mutexScanStats);

  if (options.find("m") != std::string::npos) {
    for (auto it = lastScanStats.begin(); it != lastScanStats.end(); ++it) {
      snprintf(line, sizeof(line),
               "key=last layout=%08lx type=%s nominal_stripes=%s checksum=%s "
               "blockchecksum=%s blocksize=%s",
               it->first,
               eos::common::LayoutId::GetLayoutTypeString(it->first),
               eos::common::LayoutId::GetStripeNumberString(it->first).c_str(),
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

    if (lastAccessTimeFiles.size()) {
      std::string afiles = "kay=last tag=accesstime::files ";

      for (auto it = lastAccessTimeFiles.begin(); it != lastAccessTimeFiles.end();
           ++it) {
        afiles += std::to_string(it->first);
        afiles += "=";
        afiles += std::to_string(it->second);
        afiles += " ";
      }

      out += afiles;
      out += "\n";
    }

    if (lastAccessTimeVolume.size()) {
      std::string avolume = "key=last tag=accesstime::volume ";

      for (auto it = lastAccessTimeVolume.begin(); it != lastAccessTimeVolume.end();
           ++it) {
        avolume += std::to_string(it->first);
        avolume += "=";
        avolume += std::to_string(it->second);
        avolume += " ";
      }

      out += avolume;
      out += "\n";
    }

    if (lastBirthTimeFiles.size()) {
      std::string bfiles = "kay=last tag=birthtime::files ";

      for (auto it = lastBirthTimeFiles.begin(); it != lastBirthTimeFiles.end();
           ++it) {
        bfiles += std::to_string(it->first);
        bfiles += "=";
        bfiles += std::to_string(it->second);
        bfiles += " ";
      }

      out += bfiles;
      out += "\n";
    }

    if (lastBirthTimeVolume.size()) {
      std::string bvolume = "key=last tag=birthtime::volume ";

      for (auto it = lastBirthTimeVolume.begin(); it != lastBirthTimeVolume.end();
           ++it) {
        bvolume += std::to_string(it->first);
        bvolume += "=";
        bvolume += std::to_string(it->second);
        bvolume += " ";
      }

      out += bvolume;
      out += "\n";
    }

    return;
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
        for (auto& pair : i.second) {
          out += "fxid:";
          out += eos::common::FileId::Fid2Hex(pair.first);
          out += " layoutid:";
          out += eos::common::StringConversion::integral_to_hex(pair.second).c_str();
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
          for (auto& pair : i.second) {
            exportfile << "fxid:" << eos::common::FileId::Fid2Hex(pair.first)
                       << " layoutid:"
                       << eos::common::StringConversion::integral_to_hex(pair.second)
                       << " " << i.first << "\n";
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
                 " layout=%08lx type=%-13s nominal_stripes=%s checksum=%-8s "
                 "blockchecksum=%-8s blocksize=%-4s\n\n",
                 it->first,
                 eos::common::LayoutId::GetLayoutTypeString(it->first),
                 eos::common::LayoutId::GetStripeNumberString(it->first).c_str(),
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
        for (auto& pair : i.second) {
          out += "fxid:";
          out += eos::common::FileId::Fid2Hex(pair.first);
          out += " layoutid:";
          out += eos::common::StringConversion::integral_to_hex(pair.second).c_str();
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
          for (auto& pair : i.second) {
            exportfile << "fxid:" << eos::common::FileId::Fid2Hex(pair.first)
                       << " layoutid:"
                       << eos::common::StringConversion::integral_to_hex(pair.second)
                       << " " << i.first << "\n";
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
                 " layout=%08lx type=%-13s nominal_stripes=%s checksum=%-8s "
                 "blockchecksum=%-8s blocksize=%-4s\n\n",
                 it->first,
                 eos::common::LayoutId::GetLayoutTypeString(it->first),
                 eos::common::LayoutId::GetStripeNumberString(it->first).c_str(),
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

      if (lastAccessTimeFiles.size()) {
        out +=  "======================================================================================\n";
        out +=  " Access time distribution of files\n";
        uint64_t totalfiles = 0;

        for (auto it = lastAccessTimeFiles.begin(); it != lastAccessTimeFiles.end();
             ++it) {
          totalfiles += it->second;
        }

        for (auto it = lastAccessTimeFiles.begin(); it != lastAccessTimeFiles.end();
             ++it) {
          double fraction = totalfiles ? (100.0 * it->second / totalfiles) : 0;
          XrdOucString age;
          snprintf(line, sizeof(line), " %-32s : %s (%.02f%%)\n",
                   eos::common::StringConversion::GetReadableAgeString(age, it->first),
                   eos::common::StringConversion::GetReadableSizeString(it->second, "").c_str(),
                   fraction);
          out += line;
        }
      }

      if (lastAccessTimeVolume.size()) {
        out +=  "======================================================================================\n";
        out +=  " Access time volume distribution of files\n";
        uint64_t totalvolume = 0;

        for (auto it = lastAccessTimeVolume.begin(); it != lastAccessTimeVolume.end();
             ++it) {
          totalvolume += it->second;
        }

        for (auto it = lastAccessTimeVolume.begin(); it != lastAccessTimeVolume.end();
             ++it) {
          double fraction = totalvolume ? (100.0 * it->second / totalvolume) : 0;
          XrdOucString age;
          snprintf(line, sizeof(line), " %-32s : %s (%.02f%%)\n",
                   eos::common::StringConversion::GetReadableAgeString(age, it->first),
                   eos::common::StringConversion::GetReadableSizeString(it->second, "B").c_str(),
                   fraction);
          out += line;
        }
      }

      if (lastBirthTimeFiles.size()) {
        out +=  "======================================================================================\n";
        out +=  " Birth time distribution of files\n";
        uint64_t totalfiles = 0;

        for (auto it = lastBirthTimeFiles.begin(); it != lastBirthTimeFiles.end();
             ++it) {
          totalfiles += it->second;
        }

        for (auto it = lastBirthTimeFiles.begin(); it != lastBirthTimeFiles.end();
             ++it) {
          double fraction = totalfiles ? (100.0 * it->second / totalfiles) : 0;
          XrdOucString age;
          snprintf(line, sizeof(line), " %-32s : %s (%.02f%%)\n",
                   eos::common::StringConversion::GetReadableAgeString(age, it->first),
                   eos::common::StringConversion::GetReadableSizeString(it->second, "").c_str(),
                   fraction);
          out += line;
        }
      }

      if (lastBirthTimeVolume.size()) {
        out +=  "======================================================================================\n";
        out +=  " Birth time volume distribution of files\n";
        uint64_t totalvolume = 0;

        for (auto it = lastBirthTimeVolume.begin(); it != lastBirthTimeVolume.end();
             ++it) {
          totalvolume += it->second;
        }

        for (auto it = lastBirthTimeVolume.begin(); it != lastBirthTimeVolume.end();
             ++it) {
          double fraction = totalvolume ? (100.0 * it->second / totalvolume) : 0;
          XrdOucString age;
          snprintf(line, sizeof(line), " %-32s : %s (%.02f%%)\n",
                   eos::common::StringConversion::GetReadableAgeString(age, it->first),
                   eos::common::StringConversion::GetReadableSizeString(it->second, "B").c_str(),
                   fraction);
          out += line;
        }
      }
    }
  }

  out += "# ------------------------------------------------------------------------------------\n";
}

EOSMGMNAMESPACE_END
