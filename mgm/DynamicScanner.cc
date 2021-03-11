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
#include "mgm/DynamicScanner.hh"
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

//-------------------------------------------------------------------------------
// Constructor
//-------------------------------------------------------------------------------

DynamicScanner::DynamicScanner()
{
  statusFiles.clear();
  timeCurrentScan = 0;
  timeLastScan = 0;
  mThread.reset(&DynamicScanner::performCycleQDB, this);
  ndirs = 0;
  nfiles = 0;
}

void
DynamicScanner::Stop()
{
  eos_static_info("stop");
  mThread.join();
}

DynamicScanner::~DynamicScanner()
{
  Stop();
}

//------------------------------------------------------------------------------
// Retrieve current file inspector configuration options
//------------------------------------------------------------------------------
DynamicScanner::Options DynamicScanner::getOptions()
{
  eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
  DynamicScanner::Options opts;
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

/////This is for compilng this will have  to be build up later on.

void
DynamicScanner::Process(std::string& filepath)
{
}

void
DynamicScanner::Process(std::shared_ptr<eos::IFileMD> fmd)
{
}



//------------------------------------------------------------------------------
// Perform a single inspector cycle, QDB namespace
//------------------------------------------------------------------------------
void DynamicScanner::performCycleQDB(ThreadAssistant& assistant) noexcept
{
  eos_static_info("msg=\"start FileInspector scan on QDB\"");

  //----------------------------------------------------------------------------
  // Initialize qclient..
  //----------------------------------------------------------------------------
  if (!mQcl) {
    mQcl.reset(new qclient::QClient(gOFS->mQdbContactDetails.members,
                                    gOFS->mQdbContactDetails.constructOptions()));
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
      //fmd->getSize();
      scanned_percent.store(100.0 * nfiles_processed / nfiles,
                            std::memory_order_seq_cst);
      time_t target_time = (1.0 * nfiles_processed / nfiles) * interval;
      time_t is_time = time(NULL) - s_time;

      if (eos::common::LayoutId::GetLayoutType(fmd->getLayoutId()) ==
          eos::common::LayoutId::kQrain) {
        statusFiles[fmd->getId()] = fmd;
      }

      /*
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
      */

      //Get something in for this on how to terminate in the middle.

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
          // interrupt the scan f
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

  //This is currently not used
  /*
  scanned_percent.store(100.0, std::memory_order_seq_cst);
  std::lock_guard<std::mutex> sMutex(mutexScanStats);
  lastScanStats = currentScanStats;
  lastFaultyFiles = currentFaultyFiles;
  timeLastScan = timeCurrentScan;
  */
}




EOSMGMNAMESPACE_END
