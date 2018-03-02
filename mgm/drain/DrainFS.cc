//------------------------------------------------------------------------------
// @file DrainFS.cc
// @author Andrea Manzi - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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

#include "mgm/drain/DrainFS.hh"
#include "mgm/drain/DrainTransferJob.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Master.hh"
#include "mgm/FsView.hh"
#include "namespace/interface/IFsView.hh"
#include <sstream>

EOSMGMNAMESPACE_BEGIN

using namespace std::chrono;

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
DrainFS::~DrainFS()
{
  eos_notice("msg=\"fsid=%u stop draining", mFsId);

  if (mThread.joinable()) {
    mDrainStop = true;
    mThread.join();
  }

  ResetCounters();
}

//------------------------------------------------------------------------------
// Reset drain counters and status
//------------------------------------------------------------------------------
void
DrainFS::ResetCounters()
{
  eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);

  if (FsView::gFsView.mIdView.count(mFsId)) {
    FileSystem* fs = FsView::gFsView.mIdView[mFsId];

    if (fs) {
      fs->OpenTransaction();
      fs->SetLongLong("stat.drainbytesleft", 0);
      fs->SetLongLong("stat.drainfiles", 0);
      fs->SetLongLong("stat.timeleft", 0);
      fs->SetLongLong("stat.drainprogress", 0);
      fs->SetLongLong("stat.drainretry", 0);
      fs->SetDrainStatus(eos::common::FileSystem::kNoDrain);
      fs->CloseTransaction();
      FsView::gFsView.StoreFsConfig(fs);
    }
  }

  mDrainStatus = eos::common::FileSystem::kNoDrain;
}

//------------------------------------------------------------------------------
// Get space defined drain variables
//------------------------------------------------------------------------------
void
DrainFS::GetSpaceConfiguration(const std::string& space_name)
{
  if (FsView::gFsView.mSpaceView.count(space_name)) {
    auto space = FsView::gFsView.mSpaceView[space_name];

    if (space->GetConfigMember("drainer.retries") != "") {
      mMaxRetries = std::stoi(space->GetConfigMember("drainer.retries"));
      eos_static_debug("msg=\"drain retries=%u\"", mMaxRetries);
    }

    if (space->GetConfigMember("drainer.fs.ntx") != "") {
      maxParallelJobs =  std::stoi(space->GetConfigMember("drainer.fs.ntx"));
      eos_static_debug("msg=\"per fs max parallel jobs=%u\"", maxParallelJobs);
    }
  }
}

//------------------------------------------------------------------------------
// Method draining the file system
//------------------------------------------------------------------------------
void
DrainFS::DoIt()
{
  int ntried = 0;
  eos_notice("msg=\"fsid=%u start draining\"", mFsId);

  do { // Loop for retries
    ntried++;

    if (!PrepareFs()) {
      return;
    }

    if (CollectDrainJobs() == 0) {
      CompleteDrain();
      return;
    }

    if (!MarkFsDraining()) {
      return;
    }

    do { // Loop to drain the files
      auto job = mJobsPending.begin();
      // @todo (esindril) Use a thread pool for all the draining jobs

      while ((mJobsRunning.size() <= maxParallelJobs) &&
             (job != mJobsPending.end())) {
        // @todo (esindril) this is a hack for getting different TPC keys in
        // xrootd. Should be fixed in XRootD code.
        std::this_thread::sleep_for(milliseconds(200));
        (*job)->Start();
        mJobsRunning.push_back(*job);
        job = mJobsPending.erase(job);
      }

      for (auto it = mJobsRunning.begin(); it !=  mJobsRunning.end();) {
        if ((*it)->GetStatus() == DrainTransferJob::OK) {
          it = mJobsRunning.erase(it);
        } else if ((*it)->GetStatus() == DrainTransferJob::Failed) {
          mJobsFailed.push_back(*it);
          it = mJobsRunning.erase(it);
        } else {
          ++it;
        }
      }

      State state = UpdateProgress();

      if (state == State::EXPIRED) {
        break;
      } else if ((state == State::DONE) || (state == State::FAILED)) {
        return;
      }
    } while (!mDrainStop);
  } while ((ntried < mMaxRetries) && !mDrainStop);
}

//----------------------------------------------------------------------------
// Clean up when draining is completed
//----------------------------------------------------------------------------
void
DrainFS::CompleteDrain()
{
  eos_notice("msg=\"fsid=%u is drained\"", mFsId);
  eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);

  if (FsView::gFsView.mIdView.count(mFsId)) {
    FileSystem* fs = FsView::gFsView.mIdView[mFsId];

    if (fs) {
      mDrainStatus = eos::common::FileSystem::kDrained;
      fs->OpenTransaction();
      fs->SetDrainStatus(eos::common::FileSystem::kDrained);
      fs->SetLongLong("stat.drainbytesleft", 0);
      fs->SetLongLong("stat.timeleft", 0);

      if (!gOFS->Shutdown) {
        // If drain done and the system is not shutting down then set the
        // file system in "empty" state
        fs->SetString("configstatus", "empty");
        fs->SetLongLong("stat.drainprogress", 100);
        FsView::gFsView.StoreFsConfig(fs);
      }

      fs->CloseTransaction();
    }
  }
}

//------------------------------------------------------------------------------
// Stop draining the file system
//------------------------------------------------------------------------------
void
DrainFS::Stop()
{
  mDrainStop = true;

  if (mThread.joinable()) {
    mThread.join();
  }

  eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);

  if (FsView::gFsView.mIdView.count(mFsId)) {
    FileSystem* fs  = FsView::gFsView.mIdView[mFsId];

    if (fs) {
      mDrainStatus = eos::common::FileSystem::kNoDrain;
      fs->OpenTransaction();
      fs->SetConfigStatus(eos::common::FileSystem::kRW, true);
      fs->SetDrainStatus(eos::common::FileSystem::kNoDrain);
      fs->CloseTransaction();
      FsView::gFsView.StoreFsConfig(fs);
      return;
    }
  }

  eos_notice("fsid=%u has been removed during drain", mFsId);
}

//------------------------------------------------------------------------------
// Prepare the file system for drain i.e. delay the start by the configured
// amount of time, set the status etc.
//------------------------------------------------------------------------------
bool
DrainFS::PrepareFs()
{
  ResetCounters();
  {
    FileSystem* fs = nullptr;
    eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);

    if (FsView::gFsView.mIdView.count(mFsId)) {
      fs = FsView::gFsView.mIdView[mFsId];
    }

    if (!fs) {
      eos_notice("msg=\"fsid=%u removed during drain prepare\"", mFsId);
      return false;
    }

    fs->OpenTransaction();
    fs->SetDrainStatus(eos::common::FileSystem::kDrainPrepare);
    mDrainStatus = eos::common::FileSystem::kDrainPrepare;
    //    fs->SetLongLong("stat.drainretry", ntried - 1);
    fs->CloseTransaction();
    mDrainPeriod = seconds(fs->GetLongLong("drainperiod"));
    eos::common::FileSystem::fs_snapshot_t drain_snapshot;
    fs->SnapShotFileSystem(drain_snapshot, false);
    GetSpaceConfiguration(drain_snapshot.mSpace);
  }
  mDrainStart = steady_clock::now();
  mDrainEnd = mDrainStart + mDrainPeriod;
  // Now we wait 60 seconds or the service delay time indicated by Master
  size_t kLoop = gOFS->MgmMaster.GetServiceDelay();

  if (!kLoop) {
    kLoop = 60;
  }

  for (size_t k = 0; k < kLoop; ++k) {
    {
      FileSystem* fs = nullptr;
      eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);

      if (FsView::gFsView.mIdView.count(mFsId)) {
        fs = FsView::gFsView.mIdView[mFsId];
      }

      if (!fs) {
        eos_err("msg=\"fsid=%u removed during drain prepare\"", mFsId);
        return false;
      }

      fs->SetLongLong("stat.timeleft", kLoop - 1 - k, false);
    }
    std::this_thread::sleep_for(seconds(1));

    if (mDrainStop) {
      ResetCounters();
      return false;
    }
  }

  return true;
}

//------------------------------------------------------------------------------
// Collect and prepare all the drain jobs
//------------------------------------------------------------------------------
uint64_t
DrainFS::CollectDrainJobs()
{
  eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex);

  for (auto it_fid = gOFS->eosFsView->getFileList(mFsId);
       (it_fid && it_fid->valid()); it_fid->next()) {
    mJobsPending.emplace_back(new DrainTransferJob(it_fid->getElement(),
                              mFsId, mTargetFsId));
    ++mTotalFiles;
  }

  return mTotalFiles;
}

//-----------------------------------------------------------------------------
// Mark the file system as draining
//-----------------------------------------------------------------------------
bool
DrainFS::MarkFsDraining()
{
  FileSystem* fs = nullptr;
  eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);

  if (FsView::gFsView.mIdView.count(mFsId)) {
    fs = FsView::gFsView.mIdView[mFsId];
  }

  if (!fs) {
    eos_notice("msg=\"fsid=%u removed during drain\"", mFsId);
    return false;
  }

  mDrainStatus = eos::common::FileSystem::kDraining;
  fs->OpenTransaction();
  fs->SetDrainStatus(eos::common::FileSystem::kDraining);
  fs->SetLongLong("stat.drainbytesleft",
                  fs->GetLongLong("stat.statfs.usedbytes"));
  fs->SetLongLong("stat.drainfiles", mTotalFiles);
  fs->SetConfigStatus(eos::common::FileSystem::kRO, true);
  fs->CloseTransaction();
  FsView::gFsView.StoreFsConfig(fs);
  return true;
}

//------------------------------------------------------------------------------
// Update progress of the drain
//-----------------------------------------------------------------------------
DrainFS::State
DrainFS::UpdateProgress()
{
  static bool first_run = true;
  static seconds stall_timeout(600);
  static uint64_t old_num_to_drain = 0;
  static time_point<steady_clock> last_change = steady_clock::now();
  uint64_t num_to_drain = mJobsPending.size() + mJobsFailed.size();
  bool expired = false;
  auto now = steady_clock::now();

  if (old_num_to_drain == 0) {
    old_num_to_drain = num_to_drain;
  }

  if (old_num_to_drain != num_to_drain) {
    last_change = now;
  }

  auto duration = now - last_change;
  bool is_stalled = (duration_cast<seconds>(duration).count() >
                     stall_timeout.count());
  eos_static_debug("msg=\"timestamp=%llu, last_change=%llu, is_stalled=%i,"
                   "num_to_drain=%llu, old_num_to_drain=%llu\"",
                   duration_cast<milliseconds>(now.time_since_epoch()).count(),
                   duration_cast<milliseconds>(last_change.time_since_epoch()).count(),
                   is_stalled, num_to_drain, old_num_to_drain);

  // Check if drain expired
  if (mDrainPeriod.count() && (mDrainEnd < now)) {
    eos_warning("msg=\"fsid=%u drain expired\"", mFsId);
    expired = true;
  }

  // Update drain display variables
  if (first_run || is_stalled || expired || (num_to_drain != old_num_to_drain)) {
    first_run = false;
    FileSystem* fs = nullptr;
    eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);

    if (FsView::gFsView.mIdView.count(mFsId)) {
      fs = FsView::gFsView.mIdView[mFsId];
    }

    if (!fs) {
      eos_static_notice("msg=\"fsid=%u removed during drain\"", mFsId);
      return State::FAILED;
    }

    if (expired) {
      mDrainStatus = eos::common::FileSystem::kDrainExpired;
      fs->OpenTransaction();
      fs->SetLongLong("stat.drainfiles", num_to_drain);
      fs->SetDrainStatus(eos::common::FileSystem::kDrainExpired);
      fs->CloseTransaction();
      FsView::gFsView.StoreFsConfig(fs);
      return State::EXPIRED;
    }

    fs->OpenTransaction();
    fs->SetLongLong("stat.drainbytesleft",
                    fs->GetLongLong("stat.statfs.usedbytes"));
    fs->SetLongLong("stat.drainfiles", num_to_drain);

    if (is_stalled) {
      if (mDrainStatus != eos::common::FileSystem::kDrainStalling) {
        mDrainStatus = eos::common::FileSystem::kDrainStalling;
        fs->SetDrainStatus(eos::common::FileSystem::kDrainStalling);
      }
    } else {
      if (mDrainStatus != eos::common::FileSystem::kDraining) {
        mDrainStatus = eos::common::FileSystem::kDraining;
        fs->SetDrainStatus(eos::common::FileSystem::kDraining);
      }
    }

    uint64_t progress = 100u;

    if (mTotalFiles) {
      progress = 100.0 * (mTotalFiles - num_to_drain) / mTotalFiles;
    }

    fs->SetLongLong("stat.drainprogress", progress, false);

    if (mDrainEnd > steady_clock::now()) {
      auto duration = mDrainEnd - steady_clock::now();
      fs->SetLongLong("stat.timeleft", duration_cast<seconds>(duration).count(),
                      false);
    } else {
      fs->SetLongLong("stat.timeleft", 99999999999LL, false);
    }

    fs->CloseTransaction();
    FsView::gFsView.StoreFsConfig(fs);
  }

  if (!num_to_drain) {
    CompleteDrain();
    return State::DONE;
  }

  return State::CONTINUE;
}

EOSMGMNAMESPACE_END
