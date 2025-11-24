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

#include "Logging.hh"
#include "common/Path.hh"
#include "common/FileId.hh"
#include "common/IntervalStopwatch.hh"
#include "common/LayoutId.hh"
#include "common/Timing.hh"
#include "common/json/Json.hh"
#include "common/ParseUtils.hh"
#include "mgm/FsView.hh"
#include "mgm/inspector/FileInspector.hh"
#include "mgm/proc/ProcCommand.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/XrdMgmOfsDirectory.hh"
#include "namespace/interface/IView.hh"
#include "namespace/ns_quarkdb/inspector/FileScanner.hh"
#include "namespace/ns_quarkdb/FileMD.hh"
#include "namespace/utils/Stat.hh"
#include "namespace/Resolver.hh"
#include "namespace/Prefetcher.hh"
#include <qclient/QClient.hh>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FileInspector::FileInspector(std::string_view space_name,
                             const eos::QdbContactDetails& qdb_details) :
  nfiles(0), ndirs(0), mSpaceName(space_name),
  mQdbHelper(qdb_details)
{
  mVid = eos::common::VirtualIdentity::Root();
  mThread.reset(&FileInspector::backgroundThread, this);
  scanned_percent.store(0, std::memory_order_seq_cst);
  PriceTbPerYearDisk = 20;
  PriceTbPerYearTape = 10;
  currency = currencies[0];
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
FileInspector::Options FileInspector::getOptions(const LockFsView lockfsview)
{
  FileInspector::Options opts;
  // Default options
  opts.enabled = false;
  opts.interval = std::chrono::minutes(4 * 60);
  eos::common::RWMutexReadLock lock;

  if (lockfsview == LockFsView::On) {
    lock.Grab(FsView::gFsView.ViewMutex);
  }

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

    std::string tbprice =
      FsView::gFsView.mSpaceView[mSpaceName]->GetConfigMember("inspector.price.disk.tbyear");
    double price = 0;

    if (!tbprice.empty()) {
      price = common::ParseDouble(tbprice);

      if (price) {
        PriceTbPerYearDisk = price;
      }
    }

    tbprice =
      FsView::gFsView.mSpaceView[mSpaceName]->GetConfigMember("inspector.price.tape.tbyear");
    price = 0;

    if (!tbprice.empty()) {
      price = common::ParseDouble(tbprice);

      if (price) {
        PriceTbPerYearTape = price;
      }
    }

    std::string scurrency =
      FsView::gFsView.mSpaceView[mSpaceName]->GetConfigMember("inspector.price.currency");

    if (!scurrency.empty()) {
      int64_t index = 0;
      common::ParseInt64(scurrency, index);

      if (index < 6) {
        currency = currencies[index];
      }
    }
  }

  if (opts.enabled) {
    enable();
    eos_static_debug("msg=\"file inspector is enabled\"  interval=%lds",
                     opts.interval.count());
  } else {
    disable();
  }

  return opts;
}


//------------------------------------------------------------------------------
// Background Thread to compute the stats
//------------------------------------------------------------------------------
void
FileInspector::backgroundThread(ThreadAssistant& assistant) noexcept
{
  ThreadAssistant::setSelfThreadName("FileInspector");
  gOFS->WaitUntilNamespaceIsBooted(assistant);
  // set the initial state after boot
  Options opts = getOptions(LockFsView::On);

  if (opts.enabled) {
    enable();
  } else {
    disable();
  }

  assistant.wait_for(std::chrono::seconds(10));
  eos_static_info("%s", "msg=\"async thread started\"");

  if (mQdbHelper.HasStats()) {
    mQdbHelper.Load(mLastStats);
  }

  while (!assistant.terminationRequested()) {
    Options opts = getOptions(LockFsView::On);

    // Only a master needs to run a FileInspector
    if (opts.enabled) {
      enable();
    } else {
      disable();
    }

    common::IntervalStopwatch stopwatch(std::chrono::seconds(
                                          opts.interval.count()));

    if (opts.enabled && gOFS->mMaster->IsMaster()) {
      eos_static_info("%s", "msg=\"scan started\"");
      mCurrentStats.TimeScan = time(NULL);
      performCycleQDB(assistant);
      eos_static_info("%s", "msg=\"scan finished\"");
    }

    assistant.wait_for(stopwatch.timeRemainingInCycle());
  }
}

//------------------------------------------------------------------------------
// Perform a single inspector cycle, QDB namespace
//------------------------------------------------------------------------------
void FileInspector::performCycleQDB(ThreadAssistant& assistant) noexcept
{
  eos_static_info("%s", "msg=\"start FileInspector scan on QDB\"");

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
  Options opts = getOptions(LockFsView::On);
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
                         nfiles_processed, nfiles.load(), is_time, target_time, interval, p_time);
        // pause for the diff ...
        std::this_thread::sleep_for(std::chrono::seconds(p_time));
      }

      if (assistant.terminationRequested()) {
        return;
      }

      if ((time(NULL) - c_time) > 60) {
        c_time = time(NULL);
        Options opts = getOptions(LockFsView::On);
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
      eos_static_err("msg=\"QDB scanner error, interrupting scan\" error=\"%s\"",
                     err.c_str());
      break;
    }
  }

  scanned_percent.store(100.0, std::memory_order_seq_cst);
  std::lock_guard<std::mutex> lock(mutexScanStats);
  mLastStats = std::move(mCurrentStats);
  mQdbHelper.Store(mLastStats);
  mCurrentStats = FileInspectorStats{}; // reset current stats
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
  // Totals
  mCurrentStats.TotalFileCount++;
  mCurrentStats.TotalLogicalBytes += fmd->getSize();
  double disksize = 1.0 * fmd->getSize() * eos::common::LayoutId::GetSizeFactor(
                      lid);
  bool ontape = eos::modeFromMetadataEntry(fmd) & EOS_TAPE_MODE_T;
  double tapesize = ontape ? (1.0 * fmd->getSize()) : 0;

  // zero size files
  if (!fmd->getSize()) {
    mCurrentStats.ScanStats[lid]["zerosize"]++;
  } else {
    mCurrentStats.ScanStats[lid]["volume"] += fmd->getSize();
    mCurrentStats.ScanStats[lid]["physicalsize"] += disksize;
  }

  // no location files
  if (!fmd->getNumLocation()) {
    mCurrentStats.ScanStats[lid]["nolocation"]++;

    if (mCurrentStats.NumFaultyFiles < maxfaulty) {
      mCurrentStats.FaultyFiles["nolocation"][fmd->getId()] = fmd->getLayoutId();
    }

    mCurrentStats.NumFaultyFiles++;
  }

  eos::IFileMD::LocationVector l = fmd->getLocations();
  eos::IFileMD::LocationVector u_l = fmd->getUnlinkedLocations();

  for (auto const& fs : l) {
    if (!FsView::gFsView.HasMapping(fs)) {
      // shadow filesystem
      mCurrentStats.ScanStats[lid]["shadowlocation"]++;

      if (mCurrentStats.NumFaultyFiles < maxfaulty) {
        mCurrentStats.FaultyFiles["shadowlocation"][fmd->getId()] = fmd->getLayoutId();
      }

      mCurrentStats.NumFaultyFiles++;
    }
  }

  for (auto const& fs : u_l) {
    if (!FsView::gFsView.HasMapping(fs)) {
      // shadow filesystem
      mCurrentStats.ScanStats[lid]["shadowdeletion"]++;

      if (mCurrentStats.NumFaultyFiles < maxfaulty) {
        mCurrentStats.FaultyFiles["shadowdeletion"][fmd->getId()] = fmd->getLayoutId();
      }

      mCurrentStats.NumFaultyFiles++;
    }
  }

  // unlinked locations
  mCurrentStats.ScanStats[lid]["unlinkedlocations"] +=
    fmd->getNumUnlinkedLocation();
  // linked locations
  mCurrentStats.ScanStats[lid]["locations"] += fmd->getNumLocation();
  // stripe number
  size_t stripes = eos::common::LayoutId::GetStripeNumber(lid) + 1;
  std::string tag = "repdelta:";
  int64_t sdiff = fmd->getNumLocation() - stripes;

  if (sdiff == 0) {
    tag += "0";
  } else {
    if (sdiff > 0) {
      tag += "+";
    }

    tag += std::to_string(sdiff);

    if (mCurrentStats.NumFaultyFiles < maxfaulty) {
      mCurrentStats.FaultyFiles[tag][fmd->getId()] = fmd->getLayoutId();
    }

    mCurrentStats.NumFaultyFiles++;
  }

#define UNDEFINED_BIN (100 *365 * 86400.0)
  mCurrentStats.ScanStats[lid][tag]++;
  static std::set<double> time_bin {0, 86400ll, 7 * 86400ll, 30 * 86400ll, 90 * 86400ll,
                                    182.5 * 86400ll, 365 * 86400ll, 2 * 365 * 86400ll, 5 * 365 * 86400ll, UNDEFINED_BIN};
  size_t atime_bin = 0;
  {
    // create access time distributions
    eos::IFileMD::ctime_t atime;
    fmd->getATime(atime);

    if (!atime.tv_sec) {
      mCurrentStats.AccessTimeFiles[UNDEFINED_BIN]++;
      mCurrentStats.AccessTimeVolume[UNDEFINED_BIN] += fmd->getSize();
      atime_bin = UNDEFINED_BIN;
    } else {
      // future access time goes to bin 0
      if (atime.tv_sec > mCurrentStats.TimeScan) {
        mCurrentStats.AccessTimeFiles[0]++;
        mCurrentStats.AccessTimeVolume[0] += fmd->getSize();
        atime_bin = 0;
      } else {
        for (auto rev_it = time_bin.rbegin(); rev_it != time_bin.rend(); rev_it++) {
          if ((mCurrentStats.TimeScan - (int64_t)atime.tv_sec) >= (int64_t) *rev_it) {
            mCurrentStats.AccessTimeFiles[(uint64_t)*rev_it]++;
            mCurrentStats.AccessTimeVolume[*rev_it] += fmd->getSize();
            atime_bin = *rev_it;
            break;
          }
        }
      }
    }
  }
  {
    // create birth time distributions
    double ageInYears = 0; // stores the ages of a file in years as a double
    eos::IFileMD::ctime_t btime {0, 0};
    eos::IFileMD::XAttrMap xattrs = fmd->getAttributes();
    uint64_t sizeBytes = fmd->getSize();
    // size bins upper bounds in bytes; 0 => >= 1TB
    static const uint64_t KB = 1024ull;
    static const uint64_t MB = KB * 1024ull;
    static const uint64_t GB = MB * 1024ull;
    static const uint64_t TB = GB * 1024ull;
    static const std::vector<uint64_t> size_bins {
      4 * KB, 1 * MB, 16 * MB, 64 * MB, 128 * MB, 256 * MB,
      1 * GB, 4 * GB, 16 * GB, 128 * GB, 512 * GB, 1 * TB
    };
    uint64_t size_bin_key = 0; // default for >= 1TB
    for (auto ub : size_bins) {
      if (sizeBytes < ub) { size_bin_key = ub; break; }
    }

    if (xattrs.count("sys.eos.btime")) {
      eos::common::Timing::Timespec_from_TimespecStr(xattrs["sys.eos.btime"], btime);

      if (btime.tv_sec > mCurrentStats.TimeScan) {
        ageInYears = 0;
      } else {
        ageInYears = (mCurrentStats.TimeScan - btime.tv_sec) / (86400 * 365.0);
      }
    } else {
      eos::IFileMD::ctime_t ctime;
      fmd->getCTime(ctime);

      if (ctime.tv_sec > mCurrentStats.TimeScan) {
        ageInYears = 0;
      } else {
        ageInYears = (mCurrentStats.TimeScan - ctime.tv_sec) / (86400 * 365.0);
      }
    }

    // future birth time goes to bin 0
    if (btime.tv_sec > mCurrentStats.TimeScan) {
      mCurrentStats.BirthTimeFiles[0]++;
      mCurrentStats.BirthTimeVolume[0] += fmd->getSize();
      mCurrentStats.BirthVsAccessTimeFiles[0][atime_bin]++;
      mCurrentStats.BirthVsAccessTimeVolume[0][atime_bin] += fmd->getSize();
      // size distributions
      mCurrentStats.SizeBinsFiles[size_bin_key]++;
      mCurrentStats.SizeBinsVolume[size_bin_key] += sizeBytes;
      mCurrentStats.BirthVsSizeFiles[0][size_bin_key]++;
      mCurrentStats.BirthVsSizeVolume[0][size_bin_key] += sizeBytes;
    } else {
      for (auto rev_it = time_bin.rbegin(); rev_it != time_bin.rend(); rev_it++) {
        if ((mCurrentStats.TimeScan - (int64_t)btime.tv_sec) >= (int64_t) *rev_it) {
          mCurrentStats.BirthTimeFiles[(uint64_t)*rev_it]++;
          mCurrentStats.BirthTimeVolume[*rev_it] += fmd->getSize();
          mCurrentStats.BirthVsAccessTimeFiles[*rev_it][atime_bin]++;
          mCurrentStats.BirthVsAccessTimeVolume[*rev_it][atime_bin] += fmd->getSize();
          // size distributions
          mCurrentStats.SizeBinsFiles[size_bin_key]++;
          mCurrentStats.SizeBinsVolume[size_bin_key] += sizeBytes;
          mCurrentStats.BirthVsSizeFiles[*rev_it][size_bin_key]++;
          mCurrentStats.BirthVsSizeVolume[*rev_it][size_bin_key] += sizeBytes;
          break;
        }
      }
    }

    double costdisk = disksize * PriceTbPerYearDisk * ageInYears;
    double costtape = tapesize * PriceTbPerYearTape * ageInYears;

    if (costdisk) {
      // create costs disk
      mCurrentStats.UserCosts[0][fmd->getCUid()]  += costdisk;
      mCurrentStats.GroupCosts[0][fmd->getCGid()] += costdisk;
      mCurrentStats.TotalCosts[0] += costdisk;
    }

    if (costtape) {
      // create costs tape
      mCurrentStats.UserCosts[1][fmd->getCUid()]  += costtape;
      mCurrentStats.GroupCosts[1][fmd->getCGid()] += costtape;
      mCurrentStats.TotalCosts[1] += costtape;
    }

    if (disksize) {
      // create costs disk
      mCurrentStats.UserBytes[0][fmd->getCUid()]  += disksize;
      mCurrentStats.GroupBytes[0][fmd->getCGid()] += disksize;
      mCurrentStats.TotalBytes[0] += disksize;
    }

    if (tapesize) {
      // create costs tape
      mCurrentStats.UserBytes[1][fmd->getCUid()]  += tapesize;
      mCurrentStats.GroupBytes[1][fmd->getCGid()] += tapesize;
      mCurrentStats.TotalBytes[1] += tapesize;
    }
  }
}

//------------------------------------------------------------------------------
// Dump current status
//------------------------------------------------------------------------------
void
FileInspector::Dump(std::string& out, std::string_view options,
                    const LockFsView lockfsview)
{
  char line[4096];
  time_t now = time(NULL);
  const bool is_monitoring = (options.find('m') != std::string::npos);
  bool printall = false; // normally we only print top 10!
  bool printlayouts = true;
  bool printcosts = true;
  bool printusage = true;
  bool printaccesstime = true;
  bool printbirthtime = true;
  bool printbirthvsaccesstime = true;
  bool printmoney = false;

  if (options.find("Z") != std::string::npos) {
    printall = true;
  }

  if (options.find('M') != std::string::npos) {
    printmoney = true;
  }

  if (options.find('L') != std::string::npos  ||
      options.find('C') != std::string::npos ||
      options.find('U') != std::string::npos ||
      options.find('A') != std::string::npos ||
      options.find('B') != std::string::npos ||
      options.find('V') != std::string::npos) {
    printlayouts = printcosts = printusage = printaccesstime = printbirthtime =
                                  printbirthvsaccesstime = false;

    if (options.find('L') != std::string::npos) {
      printlayouts = true;
    }

    if (options.find('C') != std::string::npos) {
      printcosts = true;
    }

    if (options.find('A') != std::string::npos) {
      printaccesstime = true;
    }

    if (options.find('U') != std::string::npos) {
      printusage = true;
    }

    if (options.find('B') != std::string::npos) {
      printbirthtime = true;
    }

    if (options.find('V') != std::string::npos) {
      printbirthvsaccesstime = true;
    }
  }

  if (!is_monitoring) {
    out += "# ------------------------------------------------------------------------------------\n";
    out += "# ";
    out += eos::common::Timing::ltime(now);
    out += "\n";
    // Summary at top: total files and average filesize
    if (mLastStats.TotalFileCount > 0) {
      char sum[256];
      double avg = static_cast<double>(mLastStats.TotalLogicalBytes) /
                   static_cast<double>(mLastStats.TotalFileCount);
      std::string totals =
        eos::common::StringConversion::GetReadableSizeString(
          mLastStats.TotalLogicalBytes, "B");
      snprintf(sum, sizeof(sum), "# total_files: %lu\n# total_size: %s\n# average_filesize_bytes: %.0f\n",
               (unsigned long)mLastStats.TotalFileCount, avg);
      // Insert total size string between the tokens
      {
        // Reconstruct with totals
        char buf[512];
        snprintf(buf, sizeof(buf), "# total_files: %lu\n# total_size: %s\n# average_filesize_bytes: %.0f\n",
                 (unsigned long)mLastStats.TotalFileCount, totals.c_str(), avg);
        out += buf;
      }
    } else {
      out += "# total_files: 0\n# total_size: 0B\n# average_filesize_bytes: 0\n";
    }
    // Size histogram (files) using predefined bins
    {
      out += "# Size histogram (files)\n";
      // Define bins in the desired order (upper bound in bytes; 0 => >= 1TB)
      static const uint64_t KB = 1024ull;
      static const uint64_t MB = KB * 1024ull;
      static const uint64_t GB = MB * 1024ull;
      static const uint64_t TB = GB * 1024ull;
      static const std::vector<uint64_t> bins {
        4 * KB, 1 * MB, 16 * MB, 64 * MB, 128 * MB, 256 * MB,
        1 * GB, 4 * GB, 16 * GB, 128 * GB, 512 * GB, 1 * TB, 0ull
      };
      static const std::vector<std::string> labels {
        "<4K", "<1M", "<16M", "<64M", "<128M", "<256M",
        "<1G", "<4G", "<16G", "<128G", "<512G", "<1T", ">=1T"
      };
      std::vector<uint64_t> counts;
      counts.reserve(bins.size());
      uint64_t maxc = 0;
      for (auto ub : bins) {
        uint64_t c = 0;
        auto it = mLastStats.SizeBinsFiles.find(ub);
        if (it != mLastStats.SizeBinsFiles.end()) c = it->second;
        counts.push_back(c);
        if (c > maxc) maxc = c;
      }
      // Render vertical columns with a maximum height and axes
      const int colWidth = 6;       // width per bin column
      const int maxHeight = 20;
      uint64_t scale = (maxc > (uint64_t)maxHeight) ? ((maxc + maxHeight - 1) / maxHeight) : 1;
      std::vector<uint64_t> heights;
      heights.reserve(counts.size());
      for (auto c : counts) {
        uint64_t h = (c + scale - 1) / scale;
        heights.push_back(h);
      }
      uint64_t hmax = 0;
      for (auto h : heights) if (h > hmax) hmax = h;
      // Top arrow for Y axis
      out += "# \xE2\x86\x91\n"; // "↑"
      for (uint64_t row = hmax; row >= 1; --row) {
        std::string line = "# ";
        line += "\xE2\x94\x82"; // "│"
        for (size_t i = 0; i < heights.size(); ++i) {
          if (heights[i] >= row) {
            line += "  *   ";
          } else {
            line += "      ";
          }
        }
        line += "\n";
        out += line;
        if (row == 1) break; // avoid unsigned wrap
      }
      // X-axis base with arrow
      {
        std::string line = "# ";
        line += "\xE2\x94\x94"; // "└"
        for (size_t i = 0; i < labels.size(); ++i) {
          for (int k = 0; k < colWidth; ++k) line += "\xE2\x94\x80"; // "─"
        }
        line += "\xE2\x86\x92"; // "→"
        line += "\n";
        out += line;
      }
      // Scale note
      {
        char buf[128];
        snprintf(buf, sizeof(buf), "# (each * ~ %lu files)\n", (unsigned long)scale);
        out += buf;
      }
    }
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
    // Monitoring: emit summary as two lines
    {
      std::string l;
      l = "key=last tag=summary::total_files value=";
      l += std::to_string(mLastStats.TotalFileCount);
      out += l; out += "\n";
      l = "key=last tag=summary::avg_filesize value=";
      if (mLastStats.TotalFileCount > 0) {
        l += std::to_string(mLastStats.TotalLogicalBytes / mLastStats.TotalFileCount);
      } else {
        l += "0";
      }
      out += l; out += "\n";
    }
    for (auto it = mLastStats.ScanStats.begin(); it != mLastStats.ScanStats.end();
         ++it) {
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

    if (mLastStats.AccessTimeFiles.size()) {
      for (auto it = mLastStats.AccessTimeFiles.begin();
           it != mLastStats.AccessTimeFiles.end();
           ++it) {
        std::string afiles = "key=last tag=accesstime::files bin=";
        afiles += std::to_string(it->first);
        afiles += " value=";
        afiles += std::to_string(it->second);
        out += afiles;
        out += "\n";
      }
    }

    if (mLastStats.AccessTimeVolume.size()) {
      for (auto it = mLastStats.AccessTimeVolume.begin();
           it != mLastStats.AccessTimeVolume.end();
           ++it) {
        std::string avolume = "key=last tag=accesstime::volume bin=";
        avolume += std::to_string(it->first);
        avolume += " value=";
        avolume += std::to_string(it->second);
        out += avolume;
        out += "\n";
      }
    }

    if (mLastStats.BirthTimeFiles.size()) {
      for (auto it = mLastStats.BirthTimeFiles.begin();
           it != mLastStats.BirthTimeFiles.end();
           ++it) {
        std::string bfiles = "key=last tag=birthtime::files bin=";
        bfiles += std::to_string(it->first);
        bfiles += " value=";
        bfiles += std::to_string(it->second);
        bfiles += " ";
        out += bfiles;
        out += "\n";
      }
    }

    if (mLastStats.BirthTimeVolume.size()) {
      for (auto it = mLastStats.BirthTimeVolume.begin();
           it != mLastStats.BirthTimeVolume.end();
           ++it) {
        std::string bvolume = "key=last tag=birthtime::volume bin=";
        bvolume += std::to_string(it->first);
        bvolume += " value=";
        bvolume += std::to_string(it->second);
        bvolume += " ";
        out += bvolume;
        out += "\n";
      }
    }

    if (mLastStats.BirthVsAccessTimeFiles.size()) {
      for (auto it = mLastStats.BirthVsAccessTimeFiles.begin();
           it != mLastStats.BirthVsAccessTimeFiles.end();
           ++it) {
        for (auto iit = it->second.begin(); iit != it->second.end(); ++iit) {
          std::string bfiles = "key=last tag=birthvsaccesstime::files xbin=";
          bfiles += std::to_string(it->first);
          bfiles += " ybin=";
          bfiles += std::to_string(iit->first);
          bfiles += " value=";
          bfiles += std::to_string(iit->second);
          out += bfiles;
          out += "\n";
        }
      }
    }

    if (mLastStats.BirthTimeVolume.size()) {
      for (auto it = mLastStats.BirthTimeVolume.begin();
           it != mLastStats.BirthTimeVolume.end();
           ++it) {
        std::string bvolume = "key=last tag=birthtime::volume bin=";
        bvolume += std::to_string(it->first);
        bvolume += " value=";
        bvolume += std::to_string(it->second);
        out += bvolume;
        out += "\n";
      }
    }

    // Size distributions (files/volume)
    if (mLastStats.SizeBinsFiles.size()) {
      for (auto it = mLastStats.SizeBinsFiles.begin();
           it != mLastStats.SizeBinsFiles.end(); ++it) {
        std::string sfiles = "key=last tag=size::files bin=";
        sfiles += std::to_string(it->first);
        sfiles += " value=";
        sfiles += std::to_string(it->second);
        out += sfiles;
        out += "\n";
      }
    }
    if (mLastStats.SizeBinsVolume.size()) {
      for (auto it = mLastStats.SizeBinsVolume.begin();
           it != mLastStats.SizeBinsVolume.end(); ++it) {
        std::string svol = "key=last tag=size::volume bin=";
        svol += std::to_string(it->first);
        svol += " value=";
        svol += std::to_string(it->second);
        out += svol;
        out += "\n";
      }
    }

    // Birth vs Size (files/volume)
    if (mLastStats.BirthVsSizeFiles.size()) {
      for (auto it = mLastStats.BirthVsSizeFiles.begin();
           it != mLastStats.BirthVsSizeFiles.end(); ++it) {
        for (auto jt = it->second.begin(); jt != it->second.end(); ++jt) {
          std::string bs = "key=last tag=birthvssize::files xbin=";
          bs += std::to_string(it->first);
          bs += " ybin=";
          bs += std::to_string(jt->first);
          bs += " value=";
          bs += std::to_string(jt->second);
          out += bs;
          out += "\n";
        }
      }
    }
    if (mLastStats.BirthVsSizeVolume.size()) {
      for (auto it = mLastStats.BirthVsSizeVolume.begin();
           it != mLastStats.BirthVsSizeVolume.end(); ++it) {
        for (auto jt = it->second.begin(); jt != it->second.end(); ++jt) {
          std::string bsv = "key=last tag=birthvssize::volume xbin=";
          bsv += std::to_string(it->first);
          bsv += " ybin=";
          bsv += std::to_string(jt->first);
          bsv += " value=";
          bsv += std::to_string(jt->second);
          out += bsv;
          out += "\n";
        }
      }
    }

    for (auto n = 0; n < 2; ++n) {
      std::string media = "disk";
      double price = PriceTbPerYearDisk;

      if (n == 1) {
        media = "tape";
        price = PriceTbPerYearTape;
      }

      if (mLastStats.UserCosts[n].size()) {
        for (auto it = mLastStats.UserCosts[n].begin();
             it != mLastStats.UserCosts[n].end();
             ++it) {
          std::string ucost = "key=last tag=user::cost::";
          ucost += media;
          ucost += " ";
          int terrc = 0;
          std::string username = eos::common::Mapping::UidToUserName(it->first, terrc);

          if (terrc) {
            username = std::to_string(it->first);
          }

          ucost += "username=";
          ucost += username;
          ucost += " uid=";
          ucost += std::to_string(it->first);
          ucost += " cost=";
          ucost += std::to_string(it->second / 1000000000000.0);
          ucost += " price=";
          ucost += std::to_string(price);
          ucost += " tbyears=";

          if (price) {
            ucost += std::to_string(it->second / 1000000000000.0 / price);
          }

          out += ucost;
          out += "\n";
        }
      }

      if (mLastStats.GroupCosts[n].size()) {
        for (auto it = mLastStats.GroupCosts[n].begin();
             it != mLastStats.GroupCosts[n].end();
             ++it) {
          std::string gcost = "key=last tag=group::cost::";
          gcost += media;
          gcost += " ";
          int terrc = 0;
          std::string groupname = eos::common::Mapping::GidToGroupName(it->first, terrc);

          if (terrc) {
            groupname = std::to_string(it->first);
          }

          gcost += "groupname=";
          gcost += groupname;
          gcost += " gid=";
          gcost += std::to_string(it->first);
          gcost += " cost=";
          gcost += std::to_string(it->second / 1000000000000.0);
          gcost += " price=";
          gcost += std::to_string(price);
          gcost += " tbyears=";

          if (price) {
            gcost += std::to_string(it->second / 1000000000000.0 / price);
          }

          out += gcost;
          out += "\n";
        }
      }

      if (mLastStats.UserBytes[n].size()) {
        for (auto it = mLastStats.UserBytes[n].begin();
             it != mLastStats.UserBytes[n].end();
             ++it) {
          std::string ubytes = "key=last tag=user::bytes::";
          ubytes += media;
          ubytes += " ";
          int terrc = 0;
          std::string username = eos::common::Mapping::UidToUserName(it->first, terrc);

          if (terrc) {
            username = std::to_string(it->first);
          }

          ubytes += "username=";
          ubytes += username;
          ubytes += " uid=";
          ubytes += std::to_string(it->first);
          ubytes += " bytes=";
          ubytes += std::to_string(it->second);
          out += ubytes;
          out += "\n";
        }
      }

      if (mLastStats.GroupBytes[n].size()) {
        for (auto it = mLastStats.GroupBytes[n].begin();
             it != mLastStats.GroupBytes[n].end();
             ++it) {
          std::string gbytes = "key=last tag=group::bytes::";
          gbytes += media;
          gbytes += " ";
          int terrc = 0;
          std::string groupname = eos::common::Mapping::GidToGroupName(it->first, terrc);

          if (terrc) {
            groupname = std::to_string(it->first);
          }

          gbytes += "groupname=";
          gbytes += groupname;
          gbytes += " gid=";
          gbytes += std::to_string(it->first);
          gbytes += " bytes=";
          gbytes += std::to_string(it->second);
          out += gbytes;
          out += "\n";
        }
      }
    }

    return;
  }

  Options opts = getOptions(lockfsview);
  out += "# ";
  out += std::to_string((int)(scanned_percent.load()));
  out += " % done - estimate to finish: ";
  out += std::to_string((int)(opts.interval.count() - (scanned_percent.load() *
                              opts.interval.count() / 100.0)));
  out += " seconds\n";

  if ((options.find("c") != std::string::npos)) {
    if (options.find("p") != std::string::npos) {
      for (const auto& i : mCurrentStats.FaultyFiles) {
        for (const auto& pair : i.second) {
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
        for (const auto& i : mCurrentStats.FaultyFiles) {
          for (const auto& pair : i.second) {
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
      out += "# current scan          : ";
      out += eos::common::Timing::ltime(mCurrentStats.TimeScan).c_str();
      out += "\n";
      out += "# not-found-during-scan : ";
      out += std::to_string(mCurrentStats.ScanStats[999999999]["unfound"]);
      out += "\n";

      for (auto it = mCurrentStats.ScanStats.begin();
           it != mCurrentStats.ScanStats.end(); ++it) {
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
      for (auto& i : mLastStats.FaultyFiles) {
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
        for (auto& i : mLastStats.FaultyFiles) {
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
      if (printlayouts) {
        out += "# last scan             : ";
        out += eos::common::Timing::ltime(mLastStats.TimeScan).c_str();
        out += "\n";
        out += "# not-found-during-scan : ";
        out += std::to_string(mLastStats.ScanStats[999999999]["unfound"]);
        out += "\n";

        for (auto it = mLastStats.ScanStats.begin(); it != mLastStats.ScanStats.end();
             ++it) {
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

      if (printaccesstime && mLastStats.AccessTimeFiles.size()) {
        out +=  "======================================================================================\n";
        out +=  " Access time distribution of files\n";
        uint64_t totalfiles = 0;

        for (auto it = mLastStats.AccessTimeFiles.begin();
             it != mLastStats.AccessTimeFiles.end();
             ++it) {
          totalfiles += it->second;
        }

        for (auto it = mLastStats.AccessTimeFiles.begin();
             it != mLastStats.AccessTimeFiles.end();
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

      if (printaccesstime && mLastStats.AccessTimeVolume.size()) {
        out +=  "======================================================================================\n";
        out +=  " Access time volume distribution of files\n";
        uint64_t totalvolume = 0;

        for (auto it = mLastStats.AccessTimeVolume.begin();
             it != mLastStats.AccessTimeVolume.end();
             ++it) {
          totalvolume += it->second;
        }

        for (auto it = mLastStats.AccessTimeVolume.begin();
             it != mLastStats.AccessTimeVolume.end();
             ++it) {
          double fraction = totalvolume ? (100.0 * it->second / totalvolume) : 0;
          XrdOucString age;
          snprintf(line, sizeof(line), " %-32s : %16s (%.02f%%)\n",
                   eos::common::StringConversion::GetReadableAgeString(age, it->first),
                   eos::common::StringConversion::GetReadableSizeString(it->second, "B").c_str(),
                   fraction);
          out += line;
        }
      }

      if (printbirthtime && mLastStats.BirthTimeFiles.size()) {
        out +=  "======================================================================================\n";
        out +=  " Birth time distribution of files\n";
        uint64_t totalfiles = 0;

        for (auto it = mLastStats.BirthTimeFiles.begin();
             it != mLastStats.BirthTimeFiles.end();
             ++it) {
          totalfiles += it->second;
        }

        for (auto it = mLastStats.BirthTimeFiles.begin();
             it != mLastStats.BirthTimeFiles.end();
             ++it) {
          double fraction = totalfiles ? (100.0 * it->second / totalfiles) : 0;
          XrdOucString age;
          snprintf(line, sizeof(line), " %-32s : %16s (%.02f%%)\n",
                   eos::common::StringConversion::GetReadableAgeString(age, it->first),
                   eos::common::StringConversion::GetReadableSizeString(it->second, "").c_str(),
                   fraction);
          out += line;
        }
      }

      if (printbirthtime && mLastStats.BirthTimeVolume.size()) {
        out +=  "======================================================================================\n";
        out +=  " Birth time volume distribution of files\n";
        uint64_t totalvolume = 0;

        for (auto it = mLastStats.BirthTimeVolume.begin();
             it != mLastStats.BirthTimeVolume.end();
             ++it) {
          totalvolume += it->second;
        }

        for (auto it = mLastStats.BirthTimeVolume.begin();
             it != mLastStats.BirthTimeVolume.end();
             ++it) {
          double fraction = totalvolume ? (100.0 * it->second / totalvolume) : 0;
          XrdOucString age;
          snprintf(line, sizeof(line), " %-32s : %16s (%.02f%%)\n",
                   eos::common::StringConversion::GetReadableAgeString(age, it->first),
                   eos::common::StringConversion::GetReadableSizeString(it->second, "B").c_str(),
                   fraction);
          out += line;
        }
      }

      if (printbirthvsaccesstime && mLastStats.BirthVsAccessTimeFiles.size()) {
        out +=  "======================================================================================\n";
        out +=  " Birth vs Access time distribution of files\n";
        std::map<time_t, uint64_t> totalfiles;

        for (auto it = mLastStats.BirthVsAccessTimeFiles.begin();
             it != mLastStats.BirthVsAccessTimeFiles.end();
             ++it) {
          for (auto iit = it->second.begin(); iit != it->second.end(); iit++) {
            totalfiles[it->first] += iit->second;
          }
        }

        for (auto it = mLastStats.BirthVsAccessTimeFiles.begin();
             it != mLastStats.BirthVsAccessTimeFiles.end();
             ++it) {
          XrdOucString age;
          snprintf(line, sizeof(line), " %-8s : [ \n",
                   eos::common::StringConversion::GetReadableAgeString(age, it->first));
          out += line;

          for (auto iit = it->second.begin(); iit != it->second.end(); ++iit) {
            double fraction = totalfiles[it->first] ? (100.0 * iit->second /
                              totalfiles[it->first]) : 0;
            snprintf(line, sizeof(line), " %-8s     %-32s %16s (%.02f%%)\n",
                     "",
                     eos::common::StringConversion::GetReadableAgeString(age, iit->first),
                     eos::common::StringConversion::GetReadableSizeString(iit->second, "").c_str(),
                     fraction);
            out += line;
          }

          snprintf(line, sizeof(line), " %-8s   ] \n",
                   "");
          out += line;
        }
      }

      if (printbirthvsaccesstime && mLastStats.BirthVsAccessTimeVolume.size()) {
        out +=  "======================================================================================\n";
        out +=  " Birth vs Access time volume distribution of files\n";
        std::map<time_t, uint64_t> totalfiles;

        for (auto it = mLastStats.BirthVsAccessTimeVolume.begin();
             it != mLastStats.BirthVsAccessTimeVolume.end();
             ++it) {
          for (auto iit = it->second.begin(); iit != it->second.end(); iit++) {
            totalfiles[it->first] += iit->second;
          }
        }

        for (auto it = mLastStats.BirthVsAccessTimeVolume.begin();
             it != mLastStats.BirthVsAccessTimeVolume.end();
             ++it) {
          XrdOucString age;
          snprintf(line, sizeof(line), " %-8s : [ \n",
                   eos::common::StringConversion::GetReadableAgeString(age, it->first));
          out += line;

          for (auto iit = it->second.begin(); iit != it->second.end(); ++iit) {
            double fraction = totalfiles[it->first] ? (100.0 * iit->second /
                              totalfiles[it->first]) : 0;
            snprintf(line, sizeof(line), " %-8s     %-32s %16s (%.02f%%)\n",
                     "",
                     eos::common::StringConversion::GetReadableAgeString(age, iit->first),
                     eos::common::StringConversion::GetReadableSizeString(iit->second, "B").c_str(),
                     fraction);
            out += line;
          }

          snprintf(line, sizeof(line), " %-8s   ] \n",
                   "");
          out += line;
        }
      }

      for (auto n = 0; n < 2; n++) {
        std::string media = "disk";

        if (n == 1) {
          media = "tape";
        }

        std::string unit = "[tb*years]";
        double rescale = 1.0;

        if (printmoney) {
          unit = "[";
          unit += currency;
          unit += "]";
        } else {
          if (n == 1) {
            // tape price
            rescale = PriceTbPerYearTape;
          } else {
            // disk price
            rescale = PriceTbPerYearDisk;
          }
        }

        if (printcosts && mLastStats.UserCosts[n].size()) {
          out +=  "======================================================================================\n";
          out +=  " Storage Costs - User View [ ";
          out += media;
          out += " ]\n";
          out +=  " -------------------------------------------------------------------------------------\n";
          out +=  " Total Costs : ";
          out += eos::common::StringConversion::GetReadableSizeString(
                   mLastStats.TotalCosts[n] / 1000000000000.0 / rescale, unit.c_str()).c_str();
          out += "\n";
          out +=  " -------------------------------------------------------------------------------------\n";
          size_t cnt = 0;
          size_t top_cnt = 10;

          if (printall) {
            top_cnt = 1000000;
          }

          for (auto it = mLastStats.UserCosts[n].rbegin();
               it != mLastStats.UserCosts[n].rend();
               ++it) {
            int terrc = 0;
            std::string username = eos::common::Mapping::UidToUserName(it->first, terrc);

            if (terrc) {
              username = std::to_string(it->first);
            }

            if (it->first < 1) {
              continue;
            }

            snprintf(line, sizeof(line), " %02ld. %-28s : %s\n",
                     ++cnt,
                     username.c_str(),
                     eos::common::StringConversion::GetReadableSizeString(it->second /
                         1000000000000.0
                         / rescale, unit.c_str()).c_str());
            out += line;

            if (cnt >= top_cnt) {
              break;
            }
          }
        }

        if (printcosts && mLastStats.GroupCosts[n].size()) {
          out +=  "======================================================================================\n";
          out +=  " Storage Costs - Group View [ ";
          out += media;
          out += " ]\n";
          out +=  " -------------------------------------------------------------------------------------\n";
          out +=  " Total Costs : ";
          out += eos::common::StringConversion::GetReadableSizeString(
                   mLastStats.TotalCosts[n] / 1000000000000.0 / rescale,
                   unit.c_str()).c_str();
          out += "\n";
          out +=  " -------------------------------------------------------------------------------------\n";
          size_t cnt = 0;
          size_t top_cnt = 10;

          if (printall) {
            top_cnt = 1000000;
          }

          for (auto it = mLastStats.GroupCosts[n].rbegin();
               it != mLastStats.GroupCosts[n].rend();
               ++it) {
            int terrc = 0;
            std::string groupname = eos::common::Mapping::GidToGroupName(it->first, terrc);

            if (terrc) {
              groupname = std::to_string(it->first);
            }

            if (it->first < 1) {
              continue;
            }

            snprintf(line, sizeof(line), " %02ld. %-28s : %s\n",
                     ++cnt,
                     groupname.c_str(),
                     eos::common::StringConversion::GetReadableSizeString(it->second /
                         1000000000000.0
                         / rescale, unit.c_str()).c_str());
            out += line;

            if (cnt >= top_cnt) {
              break;
            }
          }
        }

        if (printusage && mLastStats.UserBytes[n].size()) {
          out +=  "======================================================================================\n";
          out +=  " Storage Bytes - User View [ ";
          out += media;
          out += " ]\n";
          out +=  " -------------------------------------------------------------------------------------\n";
          out +=  " Total Bytes : ";
          out += eos::common::StringConversion::GetReadableSizeString(
                   mLastStats.TotalBytes[n], "B").c_str();
          out += "\n";
          out +=  " -------------------------------------------------------------------------------------\n";
          size_t cnt = 0;
          size_t top_cnt = 10;

          if (printall) {
            top_cnt = 1000000;
          }

          for (auto it = mLastStats.UserBytes[n].rbegin();
               it != mLastStats.UserBytes[n].rend();
               ++it) {
            int terrc = 0;
            std::string username = eos::common::Mapping::UidToUserName(it->first, terrc);

            if (terrc) {
              username = std::to_string(it->first);
            }

            if (it->first < 1) {
              continue;
            }

            snprintf(line, sizeof(line), " %02ld. %-28s : %s\n",
                     ++cnt,
                     username.c_str(),
                     eos::common::StringConversion::GetReadableSizeString(it->second, "B").c_str());
            out += line;

            if (cnt >= top_cnt) {
              break;
            }
          }
        }

        if (printusage && mLastStats.GroupBytes[n].size()) {
          out +=  "======================================================================================\n";
          out +=  " Storage Bytes - Group View [ ";
          out += media;
          out += " ]\n";
          out +=  " -------------------------------------------------------------------------------------\n";
          out +=  " Total Bytes : ";
          out += eos::common::StringConversion::GetReadableSizeString(
                   mLastStats.TotalBytes[n], "B").c_str();
          out += "\n";
          out +=  " -------------------------------------------------------------------------------------\n";
          size_t cnt = 0;
          size_t top_cnt = 10;

          if (printall) {
            top_cnt = 1000000;
          }

          for (auto it = mLastStats.GroupBytes[n].rbegin();
               it != mLastStats.GroupBytes[n].rend();
               ++it) {
            int terrc = 0;
            std::string groupname = eos::common::Mapping::GidToGroupName(it->first, terrc);

            if (terrc) {
              groupname = std::to_string(it->first);
            }

            if (it->first < 1) {
              continue;
            }

            snprintf(line, sizeof(line), " %02ld. %-28s : %s\n",
                     ++cnt,
                     groupname.c_str(),
                     eos::common::StringConversion::GetReadableSizeString(it->second, "B").c_str());
            out += line;

            if (cnt >= top_cnt) {
              break;
            }
          }
        }
      }
    }
  }

  out += "# ------------------------------------------------------------------------------------\n";
}

void FileInspector::QdbHelper::Store(const FileInspectorStats& stats)
{
  mQHashStats.hmset({
    SCAN_STATS_KEY, Marshal(stats.ScanStats),
    FAULTY_FILES_KEY, Marshal(stats.FaultyFiles),
    ACCESS_TIME_FILES_KEY, Marshal(stats.AccessTimeFiles),
    ACCESS_TIME_VOLUME_KEY, Marshal(stats.AccessTimeVolume),
    BIRTH_TIME_FILES_KEY, Marshal(stats.BirthTimeFiles),
    BIRTH_TIME_VOLUME_KEY, Marshal(stats.BirthTimeVolume),
    BIRTH_VS_ACCESS_TIME_FILES_KEY, Marshal(stats.BirthVsAccessTimeFiles),
    BIRTH_VS_ACCESS_TIME_VOLUME_KEY, Marshal(stats.BirthVsAccessTimeVolume),
    USER_COSTS_KEY, Marshal(stats.UserCosts),
    GROUP_COSTS_KEY, Marshal(stats.GroupCosts),
    TOTAL_COSTS_KEY, Marshal(stats.TotalCosts),
    USER_BYTES_KEY, Marshal(stats.UserBytes),
    GROUP_BYTES_KEY, Marshal(stats.GroupBytes),
    TOTAL_BYTES_KEY, Marshal(stats.TotalBytes),
    NUM_FAULTY_FILES_KEY, Marshal(stats.NumFaultyFiles),
    TIME_SCAN_KEY, Marshal(stats.TimeScan)
  });
}

void FileInspector::QdbHelper::Load(FileInspectorStats& stats)
{
  std::vector<std::string> members = mQHashStats.hgetall();

  try {
    for (int i = 0; i < members.size() - 1; i += 2) {
      std::string key = members[i];
      std::string value = members[i + 1];

      if (key == SCAN_STATS_KEY) {
        Unmarshal(value, stats.ScanStats);
      } else if (key == FAULTY_FILES_KEY) {
        Unmarshal(value, stats.FaultyFiles);
      } else if (key == ACCESS_TIME_FILES_KEY) {
        Unmarshal(value, stats.AccessTimeFiles);
      } else if (key == ACCESS_TIME_VOLUME_KEY) {
        Unmarshal(value, stats.AccessTimeVolume);
      } else if (key == BIRTH_TIME_FILES_KEY) {
        Unmarshal(value, stats.BirthTimeFiles);
      } else if (key == BIRTH_TIME_VOLUME_KEY) {
        Unmarshal(value, stats.BirthTimeVolume);
      } else if (key == BIRTH_VS_ACCESS_TIME_FILES_KEY) {
        Unmarshal(value, stats.BirthVsAccessTimeFiles);
      } else if (key == BIRTH_VS_ACCESS_TIME_VOLUME_KEY) {
        Unmarshal(value, stats.BirthVsAccessTimeVolume);
      } else if (key == USER_COSTS_KEY) {
        Unmarshal(value, stats.UserCosts);
      } else if (key == GROUP_COSTS_KEY) {
        Unmarshal(value, stats.GroupCosts);
      } else if (key == TOTAL_COSTS_KEY) {
        Unmarshal(value, stats.TotalCosts);
      } else if (key == USER_BYTES_KEY) {
        Unmarshal(value, stats.UserBytes);
      } else if (key == GROUP_BYTES_KEY) {
        Unmarshal(value, stats.GroupBytes);
      } else if (key == TOTAL_BYTES_KEY) {
        Unmarshal(value, stats.TotalBytes);
      } else if (key == NUM_FAULTY_FILES_KEY) {
        Unmarshal(value, stats.NumFaultyFiles);
      } else if (key == TIME_SCAN_KEY) {
        Unmarshal(value, stats.TimeScan);
      }
    }
  } catch (...) {
    eos_static_warning("msg=\"error unmarshalling FileInspector stats from QDB\"");
    stats = FileInspectorStats(); // Reset stats on error
  }
}


EOSMGMNAMESPACE_END
