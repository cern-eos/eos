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
#include "namespace/interface/IFsView.hh"
#include <sstream>

EOSMGMNAMESPACE_BEGIN

using namespace std::chrono;

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
DrainFs::DrainFs(eos::common::ThreadPool& thread_pool,
                 eos::common::FileSystem::fsid_t fs_id,
                 eos::common::FileSystem::fsid_t target_fs_id):
  mFsId(fs_id), mTargetFsId(target_fs_id),
  mStatus(eos::common::FileSystem::kNoDrain),
  mDrainStop(false), mForceRetry(false), mMaxRetries(1), mMaxJobs(10),
  mTotalFiles(0), mDrainPeriod(0), mThreadPool(thread_pool)
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
  if (FsView::gFsView.mSpaceView.count(space_name)) {
    auto space = FsView::gFsView.mSpaceView[space_name];

    if (space->GetConfigMember("drainer.retries") != "") {
      mMaxRetries.store(std::stoul(space->GetConfigMember("drainer.retries")));
      eos_debug("msg=\"drain retries=%u\"", mMaxRetries.load());
    }

    if (space->GetConfigMember("drainer.fs.ntx") != "") {
      mMaxJobs.store(std::stoul(space->GetConfigMember("drainer.fs.ntx")));
      eos_debug("msg=\"per fs max parallel jobs=%u\"", mMaxJobs.load());
    }
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

  do { // Loop for retries
    ntried++;

    if (!PrepareFs()) {
      return State::Failed;
    }

    if (CollectDrainJobs() == 0) {
      CompleteDrain();
      return State::Done;
    }

    if (!MarkFsDraining()) {
      return State::Failed;
    }

    do { // Loop to drain the files
      if (mForceRetry.load()) {
        mForceRetry.store(false);

        for (const auto job : mJobsFailed) {
          job->SetForce();
          mJobsPending.push_back(job);
        }

        mJobsFailed.clear();
      }

      auto it_job = mJobsPending.begin();

      while ((mJobsRunning.size() <= mMaxJobs.load()) &&
             (it_job != mJobsPending.end())) {
        // @todo (esindril) this is a hack for getting different TPC keys in
        // xrootd. Should be fixed in XRootD code.
        std::this_thread::sleep_for(milliseconds(200));
        auto job = *it_job;
        mThreadPool.PushTask<void>([job] {return job->DoIt();});
        mJobsRunning.push_back(*it_job);
        it_job = mJobsPending.erase(it_job);
      }

      for (auto it = mJobsRunning.begin(); it !=  mJobsRunning.end();) {
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

      state = UpdateProgress();

      if (state == State::Expired) {
        break;
      } else if ((state == State::Done) || (state == State::Failed)) {
        return state;
      }
    } while (!mDrainStop.load());
  } while ((ntried < mMaxRetries.load()) && !mDrainStop.load());

  if (mDrainStop.load()) {
    Stop();
    state = State::Stopped;
  }

  return state;
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
// Clean up when draining is completed
//----------------------------------------------------------------------------
void
DrainFs::CompleteDrain()
{
  eos_notice("msg=\"completely drained\" fsid=%d", mFsId);
  eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);

  if (FsView::gFsView.mIdView.count(mFsId)) {
    FileSystem* fs = FsView::gFsView.mIdView[mFsId];

    if (fs) {
      mStatus = eos::common::FileSystem::kDrained;
      fs->OpenTransaction();
      fs->SetDrainStatus(eos::common::FileSystem::kNoDrain);
      fs->SetLongLong("stat.drainbytesleft", 0);
      fs->SetLongLong("stat.timeleft", 0);

      if (!gOFS->Shutdown) {
        // If drain done and the system is not shutting down then set the
        // file system to "empty" state
        fs->SetConfigStatus(eos::common::FileSystem::kEmpty);
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
DrainFs::Stop()
{
  // Wait for any ongoing transfers
  while (!mJobsRunning.empty()) {
    auto it = mJobsRunning.begin();

    if ((*it)->GetStatus() != DrainTransferJob::Status::OK) {
      mJobsFailed.push_back(*it);
    }

    mJobsRunning.erase(it);
  }

  eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);

  if (FsView::gFsView.mIdView.count(mFsId)) {
    FileSystem* fs  = FsView::gFsView.mIdView[mFsId];

    if (fs) {
      mStatus = eos::common::FileSystem::kNoDrain;
      fs->OpenTransaction();
      fs->SetConfigStatus(eos::common::FileSystem::kRW);
      fs->SetDrainStatus(eos::common::FileSystem::kNoDrain);
      fs->CloseTransaction();
      FsView::gFsView.StoreFsConfig(fs);
      return;
    }
  }
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

    fs->OpenTransaction();
    fs->SetDrainStatus(eos::common::FileSystem::kDrainPrepare);
    mStatus = eos::common::FileSystem::kDrainPrepare;
    // fs->SetLongLong("stat.drainretry", ntried - 1);
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

//------------------------------------------------------------------------------
// Collect and prepare all the drain jobs
//------------------------------------------------------------------------------
uint64_t
DrainFs::CollectDrainJobs()
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
DrainFs::MarkFsDraining()
{
  FileSystem* fs = nullptr;
  eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);

  if (FsView::gFsView.mIdView.count(mFsId)) {
    fs = FsView::gFsView.mIdView[mFsId];
  }

  if (!fs) {
    eos_notice("msg=\"removed during drain\" fsid=%d", mFsId);
    return false;
  }

  mStatus = eos::common::FileSystem::kDraining;
  fs->OpenTransaction();
  fs->SetDrainStatus(eos::common::FileSystem::kDraining);
  fs->SetLongLong("stat.drainbytesleft",
                  fs->GetLongLong("stat.statfs.usedbytes"));
  fs->SetLongLong("stat.drainfiles", mTotalFiles);
  fs->CloseTransaction();
  FsView::gFsView.StoreFsConfig(fs);
  return true;
}

//------------------------------------------------------------------------------
// Update progress of the drain
//-----------------------------------------------------------------------------
DrainFs::State
DrainFs::UpdateProgress()
{
  static bool first_run = true;
  static seconds refresh_timeout(60);
  static seconds stall_timeout(600);
  static uint64_t old_num_to_drain = 0;
  static time_point<steady_clock> last_change = steady_clock::now();
  uint64_t num_to_drain = mJobsPending.size() + mJobsFailed.size() +
                          mJobsRunning.size();
  bool expired = false;
  auto now = steady_clock::now();

  if (old_num_to_drain != num_to_drain) {
    old_num_to_drain = num_to_drain;
    last_change = now;
  } else {
    std::this_thread::sleep_for(seconds(1));
  }

  auto duration = now - last_change;
  bool is_stalled = (duration_cast<seconds>(duration).count() >
                     stall_timeout.count());
  eos_static_debug("msg=\"timestamp=%llu, last_change=%llu, is_stalled=%i, "
                   "num_to_drain=%llu, old_num_to_drain=%llu, running=%llu,"
                   "pending=%llu, failed=%llu\"",
                   duration_cast<milliseconds>(now.time_since_epoch()).count(),
                   duration_cast<milliseconds>(last_change.time_since_epoch()).count(),
                   is_stalled, num_to_drain, old_num_to_drain, mJobsRunning.size(),
                   mJobsPending.size(), mJobsFailed.size());

  // Check if drain expired
  if (mDrainPeriod.count() && (mDrainEnd < now)) {
    eos_warning("msg=\"drain expired\" fsid=%d", mFsId);
    expired = true;
  }

  // Update drain display variables
  if (first_run || is_stalled || expired || (last_change == now)) {
    first_run = false;
    FileSystem* fs = nullptr;
    eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);

    if (FsView::gFsView.mIdView.count(mFsId)) {
      fs = FsView::gFsView.mIdView[mFsId];
    }

    if (!fs) {
      eos_static_err("msg=\"removed during drain\" fsid=%d", mFsId);
      return State::Failed;
    }

    if (expired) {
      mStatus = eos::common::FileSystem::kDrainExpired;
      fs->OpenTransaction();
      fs->SetLongLong("stat.drainfiles", num_to_drain);
      fs->SetDrainStatus(eos::common::FileSystem::kDrainExpired);
      fs->CloseTransaction();
      FsView::gFsView.StoreFsConfig(fs);
      return State::Expired;
    }

    fs->OpenTransaction();
    fs->SetLongLong("stat.drainbytesleft",
                    fs->GetLongLong("stat.statfs.usedbytes"));
    fs->SetLongLong("stat.drainfiles", num_to_drain);

    if (is_stalled) {
      if (mStatus != eos::common::FileSystem::kDrainStalling) {
        mStatus = eos::common::FileSystem::kDrainStalling;
        fs->SetDrainStatus(eos::common::FileSystem::kDrainStalling);
      }
    } else {
      if (mStatus != eos::common::FileSystem::kDraining) {
        mStatus = eos::common::FileSystem::kDraining;
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

  // If we have only failed jobs check if files still exist
  if ((mJobsRunning.size() == 0) &&
      (mJobsPending.size() == 0) &&
      (mJobsFailed.size())) {
    static auto last_tstamp = steady_clock::now();
    auto now_tstamp = steady_clock::now();
    auto dur = now_tstamp - last_tstamp;
    bool do_refresh = (duration_cast<seconds>(dur).count() >
                       refresh_timeout.count());

    if (do_refresh) {
      last_tstamp = now_tstamp;
      eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex);

      for (auto it = mJobsFailed.begin(); it != mJobsFailed.end();) {
        if (gOFS->eosFsView->hasFileId((*it)->GetFileId(), (*it)->GetSourceFS())) {
          ++it;
        } else {
          it = mJobsFailed.erase(it);
        }
      }
    }
  }

  if (num_to_drain == 0) {
    CompleteDrain();
    return State::Done;
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

  mStatus = eos::common::FileSystem::kNoDrain;
}

EOSMGMNAMESPACE_END
