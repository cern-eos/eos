//------------------------------------------------------------------------------
// @file DrainFs.cc
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

#include "mgm/drain/DrainFs.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Master.hh"
#include "mgm/FsView.hh"
#include "common/ThreadPool.hh"
#include "namespace/interface/IView.hh"
#include <sstream>

EOSMGMNAMESPACE_BEGIN

using namespace std::chrono;

constexpr std::chrono::seconds DrainFs::sRefreshTimeout;
constexpr std::chrono::seconds DrainFs::sStallTimeout;

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
DrainFs::DrainFs(eos::common::ThreadPool& thread_pool, eos::IFsView* fs_view,
                 eos::common::FileSystem::fsid_t src_fsid,
                 eos::common::FileSystem::fsid_t dst_fsid):
  mNsFsView(fs_view), mFsId(src_fsid), mTargetFsId(dst_fsid),
  mStatus(eos::common::FileSystem::kNoDrain),
  mDrainStop(false), mMaxRetries(1), mMaxJobs(10),
  mDrainPeriod(0), mThreadPool(thread_pool), mTotalFiles(0ull),
  mPending(0ull), mLastPending(0ull),
  mLastProgressTime(steady_clock::now()),
  mLastUpdateTime(steady_clock::now()), mSpace()
{}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
DrainFs::~DrainFs()
{
  eos_debug("msg=\"fsid=%u destroying fs drain object", mFsId);
  ResetCounters();
}

//------------------------------------------------------------------------------
// Get space defined drain variables
//------------------------------------------------------------------------------
void
DrainFs::GetSpaceConfiguration(const std::string& space_name)
{
  if (!space_name.empty() && FsView::gFsView.mSpaceView.count(space_name)) {
    auto space = FsView::gFsView.mSpaceView[space_name];

    if (space) {
      if (space->GetConfigMember("drainer.retries") != "") {
        mMaxRetries.store(std::stoul(space->GetConfigMember("drainer.retries")));
        eos_debug("msg=\"drain retries=%u\"", mMaxRetries.load());
      }

      if (space->GetConfigMember("drainer.fs.ntx") != "") {
        mMaxJobs.store(std::stoul(space->GetConfigMember("drainer.fs.ntx")));
        eos_debug("msg=\"per fs max parallel jobs=%u\"", mMaxJobs.load());
      }
    } else {
      eos_warning("msg=\"space %s not yet initialized\"", space_name.c_str());
    }
  } else {
    // Use some sensible default values for testing
    mMaxRetries = 2;
    mMaxJobs = 2;
  }
}

//------------------------------------------------------------------------------
// Method draining the file system
//------------------------------------------------------------------------------
DrainFs::State
DrainFs::DoIt()
{
  uint32_t ntried = 0;
  State state = State::Running;
  eos_notice("msg=\"start draining\" fsid=%d", mFsId);

  if (!PrepareFs()) {
    return State::Failed;
  }

  mTotalFiles = mNsFsView->getNumFilesOnFs(mFsId);
  mPending = mTotalFiles;

  if (mTotalFiles == 0) {
    SuccessfulDrain();
    return State::Done;
  }

  if (!MarkFsDraining()) {
    return State::Failed;
  }

  do { // Loop to drain the files
    ntried++;
    eos_debug("msg=\"drain attempt %i\\%i\" fsid=%llu", ntried,
              mMaxRetries.load(), mFsId);

    for (auto it_fids = mNsFsView->getStreamingFileList(mFsId);
         it_fids && it_fids->valid(); /* no progress */) {
      if (mJobsRunning.size() <= mMaxJobs) {
        std::shared_ptr<DrainTransferJob> job {
          new DrainTransferJob(it_fids->getElement(), mFsId, mTargetFsId)};
        mJobsRunning.push_back(job);
        mThreadPool.PushTask<void>([job] {return job->DoIt();});
        // Advance to the next file id to be drained
        it_fids->next();
        --mPending;
      }

      HandleRunningJobs();
      state = UpdateProgress();

      if (mDrainStop || (state != State::Running)) {
        break;
      }
    }

    do {
      HandleRunningJobs();
      state = UpdateProgress();
    } while (!mDrainStop && (state == State::Running));

    // If new files where added to the fs under drain then we run again
    if (state == State::Rerun) {
      continue;
    }
  } while (!mDrainStop && (ntried < mMaxRetries));

  if (mDrainStop) {
    Stop();
    state = State::Stopped;
  }

  eos_notice("msg=\"finished draining\" fsid=%d state=%i", mFsId, state);
  return state;
}

//----------------------------------------------------------------------------
// Handle running jobs
//----------------------------------------------------------------------------
void
DrainFs::HandleRunningJobs()
{
  for (auto it = mJobsRunning.begin();
       it !=  mJobsRunning.end(); /* no progress */) {
    if ((*it)->GetStatus() == DrainTransferJob::Status::OK) {
      it = mJobsRunning.erase(it);
    } else if ((*it)->GetStatus() == DrainTransferJob::Status::Failed) {
      mJobsFailed.push_back(*it);
      it = mJobsRunning.erase(it);
    } else {
      ++it;
    }
  }

  if (mJobsRunning.size() > mMaxJobs.load()) {
    std::this_thread::sleep_for(seconds(1));
  }
}

//----------------------------------------------------------------------------
// Signal the stop of the file system drain
//---------------------------------------------------------------------------
void
DrainFs::SignalStop()
{
  mDrainStop.store(true);
}

//----------------------------------------------------------------------------
// Mark file system drain as successful
//----------------------------------------------------------------------------
void
DrainFs::SuccessfulDrain()
{
  eos_notice("msg=\"complete drain\" fsid=%d", mFsId);
  eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);

  if (FsView::gFsView.mIdView.count(mFsId)) {
    FileSystem* fs = FsView::gFsView.mIdView[mFsId];

    if (fs) {
      mStatus = eos::common::FileSystem::kDrained;
      fs->OpenTransaction();
      fs->SetDrainStatus(mStatus, false);
      fs->SetLongLong("stat.drainbytesleft", 0, false);
      fs->SetLongLong("stat.timeleft", 0, false);

      if (!gOFS->Shutdown) {
        // If drain done and the system is not shutting down then set the
        // file system to "empty" state
        fs->SetLongLong("stat.drainprogress", 100, false);
        static_cast<eos::common::FileSystem*>(fs)->SetString("configstatus",
            "empty");
        fs->CloseTransaction();

	// we don't store anymore an 'empty' configuration state 
	// 'empty' is only set at the end of a drain job
        // !!! FsView::gFsView.StoreFsConfig(fs);
      } else {
        fs->CloseTransaction();
      }
    }
  }
}

//------------------------------------------------------------------------------
// Mark file system drain as failed
//------------------------------------------------------------------------------
void
DrainFs::FailedDrain()
{
  eos_notice("msg=\"failed drain\" fsid=%d", mFsId);
  eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);

  if (FsView::gFsView.mIdView.count(mFsId)) {
    FileSystem* fs = FsView::gFsView.mIdView[mFsId];

    if (fs) {
      mStatus = eos::common::FileSystem::kDrainFailed;
      fs->OpenTransaction();
      fs->SetDrainStatus(mStatus, false);
      fs->SetLongLong("stat.timeleft", 0, false);
      fs->SetLongLong("stat.drainprogress", 100, false);
      fs->SetLongLong("stat.drain.failed", mJobsFailed.size(), false);
      fs->CloseTransaction();
      FsView::gFsView.StoreFsConfig(fs);
    }
  }
}

//------------------------------------------------------------------------------
// Stop draining the file system
//------------------------------------------------------------------------------
void
DrainFs::Stop()
{
  // Wait for any ongoing transfers
  while (!mJobsRunning.empty()) {
    auto sz_begin = mJobsRunning.size();
    auto it = mJobsRunning.begin();

    if ((*it)->GetStatus() != DrainTransferJob::Status::Running) {
      mJobsRunning.erase(it);
    }

    auto sz_end = mJobsRunning.size();

    // If no progress then wait one second
    if ((sz_end != 0) && (sz_begin == sz_end)) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }

  // eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
  // if (FsView::gFsView.mIdView.count(mFsId)) {
  //   FileSystem* fs  = FsView::gFsView.mIdView[mFsId];
  //   if (fs) {
  //     mStatus = eos::common::FileSystem::kNoDrain;
  //     fs->OpenTransaction();
  //     fs->SetDrainStatus(eos::common::FileSystem::kNoDrain, false);
  //     fs->CloseTransaction();
  //     FsView::gFsView.StoreFsConfig(fs);
  //   }
  // }
}

//------------------------------------------------------------------------------
// Prepare the file system for drain i.e. delay the start by the configured
// amount of time, set the status etc.
//------------------------------------------------------------------------------
bool
DrainFs::PrepareFs()
{
  ResetCounters();
  {
    FileSystem* fs = nullptr;
    eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);

    if (FsView::gFsView.mIdView.count(mFsId)) {
      fs = FsView::gFsView.mIdView[mFsId];
    }

    if (!fs) {
      eos_notice("msg=\"removed during drain prepare\" fsid=%d", mFsId);
      return false;
    }

    mStatus = eos::common::FileSystem::kDrainPrepare;
    fs->SetDrainStatus(mStatus, false);
    fs->SetLongLong("stat.drain.failed", 0, false);
    mDrainPeriod = seconds(fs->GetLongLong("drainperiod"));
    eos::common::FileSystem::fs_snapshot_t drain_snapshot;
    fs->SnapShotFileSystem(drain_snapshot, false);
    mSpace = drain_snapshot.mSpace;
  }
  mDrainStart = steady_clock::now();
  mDrainEnd = mDrainStart + mDrainPeriod;
  // Now we wait 60 seconds or the service delay time indicated by Master
  size_t kLoop = gOFS->mMaster->GetServiceDelay();

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
        eos_err("msg=\"removed during drain prepare\" fsid=%d", mFsId);
        return false;
      }

      fs->SetLongLong("stat.timeleft", kLoop - 1 - k, false);
    }
    std::this_thread::sleep_for(seconds(1));

    if (mDrainStop.load()) {
      ResetCounters();
      return false;
    }
  }

  return true;
}

//-----------------------------------------------------------------------------
// Mark the file system as draining
//-----------------------------------------------------------------------------
bool
DrainFs::MarkFsDraining()
{
  FileSystem* fs = nullptr;
  eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
  GetSpaceConfiguration(mSpace);

  if (FsView::gFsView.mIdView.count(mFsId)) {
    fs = FsView::gFsView.mIdView[mFsId];
  }

  if (!fs) {
    eos_notice("msg=\"removed during drain\" fsid=%d", mFsId);
    return false;
  }

  mStatus = eos::common::FileSystem::kDraining;
  fs->SetDrainStatus(eos::common::FileSystem::kDraining);
  fs->SetLongLong("stat.drainbytesleft",
                  fs->GetLongLong("stat.statfs.usedbytes"), false);
  fs->SetLongLong("stat.drainfiles", mTotalFiles, false);
  fs->SetLongLong("stat.drain.failed", 0, false);
  fs->SetLongLong("stat.drainretry", mMaxRetries);
  FsView::gFsView.StoreFsConfig(fs);
  return true;
}

//------------------------------------------------------------------------------
// Update progress of the drain
//------------------------------------------------------------------------------
DrainFs::State
DrainFs::UpdateProgress()
{
  bool is_expired = false;
  auto now = steady_clock::now();

  if (mLastPending != mPending) {
    mLastPending = mPending;
    mLastProgressTime = now;
  } else {
    std::this_thread::sleep_for(seconds(1));
  }

  auto duration = now - mLastProgressTime;
  bool is_stalled = (duration_cast<seconds>(duration).count() >
                     sStallTimeout.count());
  eos_debug("msg=\"fsid=%d, timestamp=%llu, last_progress=%llu, is_stalled=%i, "
            "total_files=%llu, last_pending=%llu, pending=%llu, running=%llu, "
            "failed=%llu\"", mFsId,
            duration_cast<milliseconds>(now.time_since_epoch()).count(),
            duration_cast<milliseconds>(mLastProgressTime.time_since_epoch()).count(),
            is_stalled, mTotalFiles, mLastPending, mPending, mJobsRunning.size(),
            mJobsFailed.size());

  // Check if drain expired
  if (mDrainPeriod.count() && (mDrainEnd < now)) {
    eos_warning("msg=\"drain expired\" fsid=%d", mFsId);
    is_expired = true;
  }

  // Update drain display variables
  if (is_stalled || is_expired || (mLastProgressTime == now)) {
    FileSystem* fs = nullptr;
    eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);

    if (FsView::gFsView.mIdView.count(mFsId)) {
      fs = FsView::gFsView.mIdView[mFsId];
    }

    if (!fs) {
      eos_err("msg=\"removed during drain\" fsid=%d", mFsId);
      return State::Failed;
    }

    if (is_expired) {
      fs->SetLongLong("stat.timeleft", 0, false);
      fs->SetLongLong("stat.drainfiles", mPending, false);
      mStatus = eos::common::FileSystem::kDrainExpired;
      fs->SetDrainStatus(eos::common::FileSystem::kDrainExpired);
      FsView::gFsView.StoreFsConfig(fs);
      return State::Expired;
    }

    if (is_stalled) {
      if (mStatus != eos::common::FileSystem::kDrainStalling) {
        mStatus = eos::common::FileSystem::kDrainStalling;
        fs->SetDrainStatus(eos::common::FileSystem::kDrainStalling);
        FsView::gFsView.StoreFsConfig(fs);
      }
    } else {
      if (mStatus != eos::common::FileSystem::kDraining) {
        mStatus = eos::common::FileSystem::kDraining;
        fs->SetDrainStatus(eos::common::FileSystem::kDraining);
        FsView::gFsView.StoreFsConfig(fs);
      }
    }

    uint64_t progress = 100ull;

    if (mTotalFiles) {
      progress = 100.0 * (mTotalFiles - mPending) / mTotalFiles;
    }

    uint64_t time_left = 99999999999ull;

    if (mDrainEnd > steady_clock::now()) {
      auto duration = mDrainEnd - steady_clock::now();
      time_left = duration_cast<seconds>(duration).count();
    }

    fs->SetLongLong("stat.drain.failed", mJobsFailed.size(), false);
    fs->SetLongLong("stat.drainfiles", mPending, false);
    fs->SetLongLong("stat.drainprogress", progress, false);
    fs->SetLongLong("stat.timeleft", time_left, false);
    fs->SetLongLong("stat.drainbytesleft",
                    fs->GetLongLong("stat.statfs.usedbytes"), false);
    eos_debug("msg=\"fsid=%d, update progress", mFsId);
  }

  // Sleep for a longer period since nothing moved in the last 10 min
  if (is_stalled) {
    std::this_thread::sleep_for(std::chrono::seconds(30));
  }

  // If we have only failed jobs check if the files still exist. It could also
  // be that there were new files written while draining was started.
  if ((mPending == 0) && (mJobsRunning.size() == 0)) {
    uint64_t total_files = mNsFsView->getNumFilesOnFs(mFsId);

    if (total_files == 0) {
      SuccessfulDrain();
      return State::Done;
    } else {
      if (total_files == mJobsFailed.size()) {
        FailedDrain();
        return State::Failed;
      } else {
        eos_info("msg=\"still %llu files to drain before declaring the file "
                 "system empty\" fsid=%lu", mTotalFiles, mFsId);
        mTotalFiles = total_files;
        mPending = total_files;
        return State::Rerun;
      }
    }
  }

  return State::Running;
}

//------------------------------------------------------------------------------
// Reset drain counters and status
//------------------------------------------------------------------------------
void
DrainFs::ResetCounters()
{
  eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);

  if (FsView::gFsView.mIdView.count(mFsId)) {
    FileSystem* fs = FsView::gFsView.mIdView[mFsId];

    if (fs) {
      fs->SetLongLong("stat.drainbytesleft", 0, false);
      fs->SetLongLong("stat.drainfiles", 0, false);
      fs->SetLongLong("stat.timeleft", 0, false);
      fs->SetLongLong("stat.drainprogress", 0, false);
      fs->SetLongLong("stat.drainretry", 0, false);
      fs->SetDrainStatus(eos::common::FileSystem::kNoDrain);
      FsView::gFsView.StoreFsConfig(fs);
    }
  }

  mStatus = eos::common::FileSystem::kNoDrain;
}

EOSMGMNAMESPACE_END
