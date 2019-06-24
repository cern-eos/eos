// ----------------------------------------------------------------------
// File: DrainJob.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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

/*----------------------------------------------------------------------------*/
#include "mgm/DrainJob.hh"
#include "mgm/FileSystem.hh"
#include "mgm/FsView.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Quota.hh"
#include "mgm/Master.hh"
#include "namespace/interface/IFsView.hh"
#include "namespace/Prefetcher.hh"
#include "common/FileId.hh"
#include "common/LayoutId.hh"
#include "common/Logging.hh"
#include "common/TransferQueue.hh"
#include "common/TransferJob.hh"

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN


DrainJob::~DrainJob()
/*----------------------------------------------------------------------------*/
/**
 * @brief Destructor
 *
 * Cancels and joins the drain thread.
 */
/*----------------------------------------------------------------------------*/
{
  eos_static_info("waiting for join ...");

  if (mThread) {
    XrdSysThread::Cancel(mThread);

    if (!gOFS->Shutdown) {
      XrdSysThread::Join(mThread, NULL);
    }

    mThread = 0;
  }

  ResetCounter();
  eos_static_notice("Stopping Drain Job for fs=%u", mFsId);
}

/*----------------------------------------------------------------------------*/
void
/*----------------------------------------------------------------------------*/
/**
 * @brief resets all drain relevant counters to 0
 *
 */
/*----------------------------------------------------------------------------*/
DrainJob::ResetCounter()
{
  FileSystem* fs = FsView::gFsView.mIdView.lookupByID(mFsId);

  if (fs) {
    common::FileSystemUpdateBatch batch;
    batch.setLongLongTransient("stat.drainbytesleft", 0);
    batch.setLongLongTransient("stat.drainfiles", 0);
    batch.setLongLongTransient("stat.timeleft", 0);
    batch.setLongLongTransient("stat.drainprogress", 0);
    batch.setLongLongTransient("stat.drainretry", 0);
    batch.setDrainStatus(eos::common::DrainStatus::kNoDrain);
    fs->applyBatch(batch);

    SetDrainer();
  }
}

/*----------------------------------------------------------------------------*/
void*
DrainJob::StaticThreadProc(void* arg)
/*----------------------------------------------------------------------------*/
/**
 * @brief static thread start function
 *
 */
/*----------------------------------------------------------------------------*/
{
  return reinterpret_cast<DrainJob*>(arg)->Drain();
}

/*----------------------------------------------------------------------------*/
void
DrainJob::SetDrainer()
/*----------------------------------------------------------------------------*/
/**
 * @brief en-/disable the drain pull in all nodes participating in draining
 *
 */
/*----------------------------------------------------------------------------*/
{
  FileSystem* fs = FsView::gFsView.mIdView.lookupByID(mFsId);
  if (!fs) {
    return;
  }

  FsGroup::const_iterator git;
  bool setactive = false;

  if (FsView::gFsView.mGroupView.count(mGroup)) {
    for (git = FsView::gFsView.mGroupView[mGroup]->begin();
         git != FsView::gFsView.mGroupView[mGroup]->end(); git++) {

      FileSystem* checkDrain = FsView::gFsView.mIdView.lookupByID(*git);
      if(checkDrain) {
        eos::common::DrainStatus drainstatus =
          (eos::common::FileSystem::GetDrainStatusFromString(
             checkDrain->GetString("stat.drain").c_str())
          );

        if ((drainstatus == eos::common::DrainStatus::kDraining) ||
            (drainstatus == eos::common::DrainStatus::kDrainStalling)) {
          // if any mGroup filesystem is draining, all the others have
          // to enable the pull for draining!
          setactive = true;
        }
      }
    }

    // if the mGroup get's disabled we stop the draining
    if (FsView::gFsView.mGroupView[mGroup]->GetConfigMember("status") != "on") {
      setactive = false;
    }

    for (git = FsView::gFsView.mGroupView[mGroup]->begin();
         git != FsView::gFsView.mGroupView[mGroup]->end(); git++) {
      fs = FsView::gFsView.mIdView.lookupByID(*git);

      if (fs) {
        if (setactive) {
          if (fs->GetString("stat.drainer") != "on") {
            fs->SetString("stat.drainer", "on");
          }
        } else {
          if (fs->GetString("stat.drainer") != "off") {
            fs->SetString("stat.drainer", "off");
          }
        }
      }
    }
  }
}

//------------------------------------------------------------------------------
// @brief Set number of transfers and rate in all participating nodes
//------------------------------------------------------------------------------
void
DrainJob::SetSpaceNode()

{
  std::string SpaceNodeTransfers = "";
  std::string SpaceNodeTransferRate = "";
  eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);

  if (FsView::gFsView.mSpaceView.count(mSpace)) {
    SpaceNodeTransfers =
      FsView::gFsView.mSpaceView[mSpace]->GetConfigMember("drainer.node.ntx");
    SpaceNodeTransferRate =
      FsView::gFsView.mSpaceView[mSpace]->GetConfigMember("drainer.node.rate");
  }

  FsGroup::const_iterator git;

  if (FsView::gFsView.mGroupView.count(mGroup)) {
    for (git = FsView::gFsView.mGroupView[mGroup]->begin();
         git != FsView::gFsView.mGroupView[mGroup]->end(); git++) {

      FileSystem* fs = FsView::gFsView.mIdView.lookupByID(*git);
      if (fs) {
        if (FsView::gFsView.mNodeView.count(fs->GetQueue())) {
          FsNode* node = FsView::gFsView.mNodeView[fs->GetQueue()];

          if (node) {
            // broadcast the rate & stream configuration if changed
            if (node->GetConfigMember("stat.drain.ntx") != SpaceNodeTransfers) {
              node->SetConfigMember("stat.drain.ntx",
                                    SpaceNodeTransfers, false, "", true);
            }

            if (node->GetConfigMember("stat.drain.rate") != SpaceNodeTransferRate) {
              node->SetConfigMember("stat.drain.rate",
                                    SpaceNodeTransferRate, false, "", true);
            }
          }
        }
      }
    }
  }
}

//------------------------------------------------------------------------------
// @brief Thread function running the drain supervision
//------------------------------------------------------------------------------
void*
DrainJob::Drain(void)
{
  XrdSysThread::SetCancelDeferred();
  // the retry is currently hardcoded to 1
  // e.g. the maximum time for a drain operation is 1 x <drainperiod>
  int maxtry = 1;
  int ntried = 0;
  long long filesleft = 0;
retry:
  ntried++;
  eos_static_notice("Starting Drain Job for fs=%u onopserror=%d try=%d", mFsId,
                    mOnOpsError, ntried);
  FileSystem* fs = 0;
  {
    eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
    ResetCounter();
  }
  time_t drainstart = time(NULL);
  time_t drainperiod = 0;
  time_t drainendtime = 0;
  eos::common::FileSystem::fs_snapshot_t drain_snapshot;
  {
    // set status to 'prepare'
    eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
    fs = FsView::gFsView.mIdView.lookupByID(mFsId);

    if (!fs) {
      eos_static_notice("Filesystem fsid=%u has been removed during drain "
                        "operation", mFsId);
      XrdSysThread::SetCancelOn();
      return 0;
    }

    fs->SetDrainStatus(eos::common::DrainStatus::kDrainPrepare);
    fs->SetLongLong("stat.drainretry", ntried - 1);
    mGroup = fs->GetString("schedgroup");
    fs->SnapShotFileSystem(drain_snapshot, false);
    drainperiod = fs->GetLongLong("drainperiod");
    drainendtime = drainstart + drainperiod;
    mSpace = drain_snapshot.mSpace;
    mGroup = drain_snapshot.mGroup;
  }
  XrdSysThread::CancelPoint();
  // now we wait 60 seconds or the service delay time indicated by Master
  size_t kLoop = gOFS->mMaster->GetServiceDelay();

  if (!kLoop) {
    kLoop = 60;
  }

  for (size_t k = 0; k < kLoop; k++) {
    fs->SetLongLong("stat.timeleft", kLoop - 1 - k);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    XrdSysThread::CancelPoint();
  }

  fs->SetDrainStatus(eos::common::DrainStatus::kDrainWait);
  gOFS->WaitUntilNamespaceIsBooted();
  // build the list of files to migrate
  long long totalfiles = 0;
  long long wopenfiles = 0;
  {
    eos::Prefetcher::prefetchFilesystemFileListAndWait(gOFS->eosView,
        gOFS->eosFsView, mFsId);
    eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
    eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex);

    try {
      totalfiles = gOFS->eosFsView->getNumFilesOnFs(mFsId);

      if (fs->GetConfigStatus() == eos::common::ConfigStatus::kDrain) {
        // if we are still an alive file system, we cannot finish a drain
        // as a long as we see some open files
        wopenfiles = fs->GetLongLong("stat.wopen");
      }
    } catch (eos::MDException& e) {
      // there are no files in that view
    }
  }
  XrdSysThread::CancelPoint();

  if ((!wopenfiles) && (!totalfiles)) {
    goto nofilestodrain;
  }

  // set the shared object counter
  {
    eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
    fs = FsView::gFsView.mIdView.lookupByID(mFsId);

    if (!fs) {
      eos_static_notice("Filesystem fsid=%u has been removed during drain "
                        "operation", mFsId);
      XrdSysThread::SetCancelOn();
      return 0;
    }

    fs->SetLongLong("stat.drainbytesleft",
                    fs->GetLongLong("stat.statfs.usedbytes"));
    fs->SetLongLong("stat.drainfiles", totalfiles);
  }

  if (mOnOpsError) {
    time_t waitendtime;
    time_t waitreporttime;
    time_t now;
    {
      // Set status to 'waiting'
      eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
      fs = FsView::gFsView.mIdView.lookupByID(mFsId);

      if (!fs) {
        eos_static_notice("Filesystem fsid=%u has been removed during drain "
                          "operation", mFsId);
        XrdSysThread::SetCancelOn();
        return 0;
      }

      fs->SetDrainStatus(eos::common::DrainStatus::kDrainWait);
      waitendtime = time(NULL) + (time_t) fs->GetLongLong("graceperiod");
    }
    waitreporttime = time(NULL) + 10; // we report every 10 seconds

    while ((now = time(NULL)) < waitendtime) {
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
      // Check if we should abort
      XrdSysThread::CancelPoint();

      if (now > waitreporttime) {
        // update stat.timeleft
        eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
        fs = FsView::gFsView.mIdView.lookupByID(mFsId);

        if (!fs) {
          eos_static_notice("Filesystem fsid=%u has been removed during drain "
                            "operation", mFsId);
          XrdSysThread::SetCancelOn();
          return 0;
        }

        fs->SetLongLong("stat.timeleft", waitendtime - now);
        waitreporttime = now + 10;
      }
    }

    // Set the new drain times
    drainstart = now;
    drainendtime = drainstart + drainperiod;
  }

  XrdSysThread::CancelPoint();
  // Extract all fids to drain -make statistics of files to be lost if we are
  //in draindead
  {
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
    fs = FsView::gFsView.mIdView.lookupByID(mFsId);

    if (!fs) {
      eos_static_notice("Filesystem fsid=%u has been removed during drain "
                        "operation", mFsId);
      XrdSysThread::SetCancelOn();
      return 0;
    }

    fs->SetDrainStatus(eos::common::DrainStatus::kDraining);
    // Enable the pull functionality on FST
    SetDrainer();
  }
  time_t last_filesleft_change;
  last_filesleft_change = time(NULL);
  long long last_filesleft;
  last_filesleft = 0;
  filesleft = 0;

  // Enable draining
  do {
    bool stalled = ((time(NULL) - last_filesleft_change) > 600);
    SetSpaceNode();
    {
      // TODO(gbitzes): It's a shame to prefetch the whole thing just to get its
      // size.. make getNumFilesOnFs not need to load the whole thing, or at
      // least introduce an async version.
      eos::Prefetcher::prefetchFilesystemFileListAndWait(gOFS->eosView,
          gOFS->eosFsView, mFsId);
      eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
      last_filesleft = filesleft;
      filesleft = gOFS->eosFsView->getNumFilesOnFs(mFsId);
    }

    if (!last_filesleft) {
      last_filesleft = filesleft;
    }

    if (filesleft != last_filesleft) {
      last_filesleft_change = time(NULL);
    }

    eos_static_debug(
      "stalled=%d now=%llu last_filesleft_change=%llu filesleft=%llu last_filesleft=%llu",
      stalled,
      time(NULL),
      last_filesleft_change,
      filesleft,
      last_filesleft);

    // update drain display variables
    if ((filesleft != last_filesleft) || stalled) {
      // -----------------------------------------------------------------------
      // get a rough estimate about the drain progress
      // --------------------------------------------- -------------------------
      {
        eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
        fs = FsView::gFsView.mIdView.lookupByID(mFsId);

        if (!fs) {
          eos_static_notice(
            "Filesystem fsid=%u has been removed during drain operation", mFsId);
          return 0;
        }

        fs->SetLongLong("stat.drainbytesleft",
                        fs->GetLongLong("stat.statfs.usedbytes"));
        fs->SetLongLong("stat.drainfiles",
                        filesleft);

        if (stalled) {
          fs->SetDrainStatus(eos::common::DrainStatus::kDrainStalling);
        } else {
          fs->SetDrainStatus(eos::common::DrainStatus::kDraining);
        }
      }
      int progress = (int)(totalfiles) ? (100.0 * (totalfiles - filesleft) /
                                          totalfiles) : 100;
      {
        eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
        fs->SetLongLong("stat.drainprogress", progress, false);

        if ((drainendtime - time(NULL)) > 0) {
          fs->SetLongLong("stat.timeleft", drainendtime - time(NULL), false);
        } else {
          fs->SetLongLong("stat.timeleft", 99999999999LL, false);
        }
      }

      if (!filesleft) {
        break;
      }
    }

    if (!filesleft) {
      break;
    }

    if (!filesleft) {
      break;
    }

    //--------------------------------------------------------------------------
    // check how long we do already draining
    //--------------------------------------------------------------------------
    drainperiod = fs->GetLongLong("drainperiod");
    drainendtime = drainstart + drainperiod;
    //--------------------------------------------------------------------------
    // set timeleft
    //--------------------------------------------------------------------------
    {
      eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
      int progress = (int)(totalfiles) ? (100.0 * (totalfiles - filesleft) /
                                          totalfiles) : 100;
      fs = FsView::gFsView.mIdView.lookupByID(mFsId);

      if (!fs) {
        eos_static_notice("Filesystem fsid=%u has been removed during drain "
                          "operation", mFsId);
        XrdSysThread::SetCancelOn();
        return 0;
      }

      fs->SetLongLong("stat.drainprogress", progress, false);

      if ((drainendtime - time(NULL)) > 0) {
        fs->SetLongLong("stat.timeleft", drainendtime - time(NULL), false);
      } else {
        fs->SetLongLong("stat.timeleft", 99999999999LL, false);
      }
    }

    if ((drainperiod) && (drainendtime < time(NULL))) {
      eos_static_notice(
        "Terminating drain operation after drainperiod of %lld seconds has been exhausted",
        drainperiod);
      //------------------------------------------------------------------------
      // set status to 'drainexpired'
      //------------------------------------------------------------------------
      {
        eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
        fs = FsView::gFsView.mIdView.lookupByID(mFsId);

        if (!fs) {
          eos_static_notice("Filesystem fsid=%u has been removed during drain "
                            "operation", mFsId);
          XrdSysThread::SetCancelOn();
          return 0;
        }

        fs->SetLongLong("stat.drainfiles", filesleft);
        fs->SetDrainStatus(eos::common::DrainStatus::kDrainExpired);
        SetDrainer();

        // Retry logic
        if (ntried < maxtry) {
          // Trigger retry
        } else {
          XrdSysThread::SetCancelOn();
          return 0;
        }
      }
      goto retry;
    }

    for (int k = 0; k < 10; k++) {
      XrdSysThread::CancelPoint();
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  } while (true);

nofilestodrain:
  //----------------------------------------------------------------------------
  // set status to 'drained'
  //----------------------------------------------------------------------------
  {
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
    fs = FsView::gFsView.mIdView.lookupByID(mFsId);

    if (!fs) {
      eos_static_notice("Filesystem fsid=%u has been removed during drain "
                        "operation", mFsId);
      XrdSysThread::SetCancelOn();
      return 0;
    }

    fs->SetLongLong("stat.drainfiles", filesleft);
    fs->SetDrainStatus(eos::common::DrainStatus::kDrained);
    fs->SetLongLong("stat.drainbytesleft", 0);
    fs->SetLongLong("stat.timeleft", 0);
    SetDrainer();

    if (!gOFS->Shutdown) {
      //--------------------------------------------------------------------------
      // we automatically switch this filesystem to the 'empty' state -
      // if the system is not shutting down
      //--------------------------------------------------------------------------
      static_cast<eos::common::FileSystem*>(fs)->SetString("configstatus", "empty");
      // don't store anymore the 'empty' state into the configuration file
      // 'empty' is only set at the end of a drain
      // !!! FsView::gFsView.StoreFsConfig(fs);
      fs->SetLongLong("stat.drainprogress", 100);
    }
  }
  XrdSysThread::SetCancelOn();
  return 0;
}

EOSMGMNAMESPACE_END
