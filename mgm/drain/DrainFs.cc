//------------------------------------------------------------------------------
// @file DrainFs.cc
//------------------------------------------------------------------------------

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

#include "mgm/drain/DrainFs.hh"
#include "mgm/drain/DrainTransferJob.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Master.hh"
#include "mgm/FsView.hh"
#include "common/table_formatter/TableFormatterBase.hh"
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
  mStatus(eos::common::DrainStatus::kNoDrain), mDidRerun(false),
  mDrainStop(false), mMaxJobs(10), mDrainPeriod(0), mThreadPool(thread_pool),
  mTotalFiles(0ull), mPending(0ull), mLastPending(0ull),
  mLastProgressTime(steady_clock::now()),
  mLastUpdateTime(steady_clock::now())
{}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
DrainFs::~DrainFs()
{
  eos_debug_lite("msg=\"fsid=%u destroying fs drain object", mFsId);
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
      if (space->GetConfigMember("drainer.fs.ntx") != "") {
        mMaxJobs.store(std::stoul(space->GetConfigMember("drainer.fs.ntx")));
        eos_debug_lite("msg=\"per fs max parallel jobs=%u\"", mMaxJobs.load());
      }
    } else {
      eos_warning("msg=\"space %s not yet initialized\"", space_name.c_str());
    }
  } else {
    // Use some sensible default values for testing
    mMaxJobs = 2;
  }
}

//------------------------------------------------------------------------------
// Method draining the file system
//------------------------------------------------------------------------------
DrainFs::State
DrainFs::DoIt()
{
  eos_notice("msg=\"start draining\" fsid=%d", mFsId);
  WaitUntilNamespaceIsBooted();

  if (mDrainStop) {
    eos_err("msg=\"drain stopped while waiting for the namespace boot\" "
            "fsid=%lu", mFsId);
    return State::Failed;
  }

  mTotalFiles = mNsFsView->getNumFilesOnFs(mFsId);

  if (mTotalFiles == 0) {
    SuccessfulDrain();
    return State::Done;
  }

  if (!PrepareFs()) {
    return State::Failed;
  }

  State state = State::Running;

  // Loop to drain the files
  while (!mDrainStop && (state != State::Done) && (state != State::Failed)) {
    mTotalFiles = mNsFsView->getNumFilesOnFs(mFsId);
    mPending = mTotalFiles;

    for (auto it_fid = mNsFsView->getStreamingFileList(mFsId);
         it_fid && it_fid->valid(); /* no progress */) {
      if (NumRunningJobs() <= mMaxJobs) {
        std::shared_ptr<DrainTransferJob> job {
          new DrainTransferJob(it_fid->getElement(), mFsId, mTargetFsId)};

        if (!gOFS->mFidTracker.AddEntry(it_fid->getElement(), TrackerType::Drain)) {
          job->ReportError(SSTR("msg=\"skip currently scheduled drain\" "
                                "fxid=" << std::hex << it_fid->getElement()));
          eos::common::RWMutexWriteLock wr_lock(mJobsMutex);
          mJobsFailed.insert(job);
        } else {
          mThreadPool.PushTask<void>([job] {return job->DoIt();});
          eos::common::RWMutexWriteLock wr_lock(mJobsMutex);
          mJobsRunning.push_back(job);
        }

        // Advance to the next file id to be drained
        it_fid->next();
        --mPending;
      } else {
        std::this_thread::sleep_for(seconds(1));
      }

      HandleRunningJobs();
      state = UpdateProgress();

      if (mDrainStop || (state != State::Running)) {
        break;
      }
    }

    if (mDrainStop) {
      break;
    }

    while (!mDrainStop && (state == State::Running)) {
      HandleRunningJobs();
      state = UpdateProgress();
    }
  }

  if (mDrainStop) {
    StopJobs();
    ResetCounters();
    state = State::Failed;
  } else {
    if (state == State::Rerun) {
      FailedDrain();
      state = State::Failed;
    }
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
  eos::common::RWMutexWriteLock wr_lock(mJobsMutex);

  for (auto it = mJobsRunning.begin();
       it !=  mJobsRunning.end(); /* no progress */) {
    std::string sfxid = (*it)->GetInfo({"fxid"}).front();
    eos::IFileMD::id_t fxid = eos::common::FileId::Hex2Fid(sfxid.c_str());

    if ((*it)->GetStatus() == DrainTransferJob::Status::OK) {
      gOFS->mFidTracker.RemoveEntry(fxid);
      it = mJobsRunning.erase(it);
    } else if ((*it)->GetStatus() == DrainTransferJob::Status::Failed) {
      gOFS->mFidTracker.RemoveEntry(fxid);
      mJobsFailed.insert(*it);
      it = mJobsRunning.erase(it);
    } else {
      ++it;
    }
  }
}

//----------------------------------------------------------------------------
// Mark file system drain as successful
//----------------------------------------------------------------------------
void
DrainFs::SuccessfulDrain()
{
  eos_notice("msg=\"complete drain\" fsid=%d", mFsId);
  eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
  FileSystem* fs = FsView::gFsView.mIdView.lookupByID(mFsId);

  if (fs) {
    mStatus = eos::common::DrainStatus::kDrained;
    eos::common::FileSystemUpdateBatch batch;
    batch.setDrainStatusLocal(mStatus);
    batch.setLongLongLocal("stat.drainbytesleft", 0);
    batch.setLongLongLocal("stat.timeleft", 0);
    batch.setLongLongLocal("stat.drain.failed", 0);
    batch.setLongLongLocal("stat.drainfiles", 0);

    if (!gOFS->Shutdown) {
      // If drain done and the system is not shutting down then set the
      // file system to "empty" state
      batch.setLongLongLocal("stat.drainprogress", 100);
      batch.setLongLongLocal("stat.drain.failed", 0);
      batch.setStringDurable("configstatus", "empty");
      FsView::gFsView.StoreFsConfig(fs);
    }
    
    fs->applyBatch(batch);
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
  FileSystem* fs = FsView::gFsView.mIdView.lookupByID(mFsId);

  if (fs) {
    mStatus = eos::common::DrainStatus::kDrainFailed;
    eos::common::FileSystemUpdateBatch batch;
    batch.setDrainStatusLocal(mStatus);
    batch.setLongLongLocal("stat.timeleft", 0);
    batch.setLongLongLocal("stat.drainprogress", 100);
    batch.setLongLongLocal("stat.drain.failed", NumFailedJobs());
    fs->applyBatch(batch);
  }
}

//------------------------------------------------------------------------------
// Stop ongoing drain jobs
//------------------------------------------------------------------------------
void
DrainFs::StopJobs()
{
  {
    eos::common::RWMutexReadLock rd_lock(mJobsMutex);

    // Signal all drain jobs to stop/cancel
    for (auto& job : mJobsRunning) {
      if (job->GetStatus() == DrainTransferJob::Status::Running) {
        job->Cancel();
      }
    }

    // Wait for drain jobs to cancel
    for (auto& job : mJobsRunning) {
      while ((job->GetStatus() == DrainTransferJob::Status::Running) ||
             (job->GetStatus() == DrainTransferJob::Status::Ready)) {
        std::this_thread::sleep_for(milliseconds(10));
      }
    }
  }
  eos::common::RWMutexWriteLock wr_lock(mJobsMutex);
  mJobsRunning.clear();
}

//------------------------------------------------------------------------------
// Prepare the file system for drain i.e. delay the start by the configured
// amount of time, set the status etc.
//------------------------------------------------------------------------------
bool
DrainFs::PrepareFs()
{
  std::string space_name;
  {
    eos_info("msg=\"setting the drain prepare status\" fsid=%i", mFsId);
    eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
    FileSystem* fs = FsView::gFsView.mIdView.lookupByID(mFsId);

    if (!fs) {
      eos_notice("msg=\"removed during prepare\" fsid=%d", mFsId);
      return false;
    }

    mStatus = eos::common::DrainStatus::kDrainPrepare;
    eos::common::FileSystemUpdateBatch batch;
    batch.setLongLongLocal("stat.drainbytesleft", 0);
    batch.setLongLongLocal("stat.drainfiles", 0);
    batch.setLongLongLocal("stat.drain.failed", 0);
    batch.setLongLongLocal("stat.timeleft", 0);
    batch.setLongLongLocal("stat.drainprogress", 0);
    batch.setDrainStatusLocal(mStatus);
    fs->applyBatch(batch);
    mDrainPeriod = seconds(fs->GetLongLong("drainperiod"));
    eos::common::FileSystem::fs_snapshot_t drain_snapshot;
    fs->SnapShotFileSystem(drain_snapshot, false);
    space_name = drain_snapshot.mSpace;
  }
  mDrainStart = steady_clock::now();
  mDrainEnd = mDrainStart + mDrainPeriod;
  // Wait 60 seconds or the service delay time indicated by Master
  size_t kLoop = gOFS->mMaster->GetServiceDelay();

  if (!kLoop) {
    kLoop = 60;
  }

  for (size_t k = 0; k < kLoop; ++k) {
    std::this_thread::sleep_for(seconds(1));
    {
      eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
      FileSystem* entry = FsView::gFsView.mIdView.lookupByID(mFsId);

      if (!entry) {
        eos_err("msg=\"removed during drain prepare\" fsid=%d", mFsId);
        return false;
      }

      entry->setLongLongLocal("stat.timeleft", kLoop - 1 - k);
    }

    if (mDrainStop) {
      ResetCounters();
      return false;
    }
  }

  // Mark file system as draining
  eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
  FileSystem* fs = FsView::gFsView.mIdView.lookupByID(mFsId);

  if (!fs) {
    eos_notice("msg=\"removed during drain\" fsid=%d", mFsId);
    return false;
  }

  GetSpaceConfiguration(space_name);
  mStatus = eos::common::DrainStatus::kDraining;
  eos::common::FileSystemUpdateBatch batch;
  batch.setDrainStatusLocal(mStatus);
  batch.setLongLongLocal("stat.drainfiles", mTotalFiles);
  batch.setLongLongLocal("stat.drain.failed", 0);
  batch.setLongLongLocal("stat.drainbytesleft",
                         fs->GetLongLong("stat.statfs.usedbytes"));
  fs->applyBatch(batch);
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
  eos_debug_lite("msg=\"fsid=%d, timestamp=%llu, last_progress=%llu, is_stalled=%i, "
                 "total_files=%llu, last_pending=%llu, pending=%llu, running=%llu, "
                 "failed=%llu\"", mFsId,
                 duration_cast<milliseconds>(now.time_since_epoch()).count(),
                 duration_cast<milliseconds>(mLastProgressTime.time_since_epoch()).count(),
                 is_stalled, mTotalFiles, mLastPending, mPending, NumRunningJobs(),
                 NumFailedJobs());

  // Check if drain expired
  if (mDrainPeriod.count() && (mDrainEnd < now)) {
    eos_warning("msg=\"drain expired\" fsid=%d", mFsId);
    is_expired = true;
  }

  // Update drain display variables
  if (is_stalled || is_expired || (mLastProgressTime == now)) {
    eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
    FileSystem* fs = FsView::gFsView.mIdView.lookupByID(mFsId);

    if (!fs) {
      eos_err("msg=\"removed during drain\" fsid=%d", mFsId);
      return State::Failed;
    }

    if (is_expired) {
      mStatus = eos::common::DrainStatus::kDrainExpired;
      common::FileSystemUpdateBatch batch;
      batch.setLongLongLocal("stat.timeleft", 0);
      batch.setLongLongLocal("stat.drainfiles", mPending);
      batch.setDrainStatusLocal(mStatus);
      fs->applyBatch(batch);
      return State::Failed;
    }

    common::FileSystemUpdateBatch batch;

    if (is_stalled) {
      if (mStatus != eos::common::DrainStatus::kDrainStalling) {
        mStatus = eos::common::DrainStatus::kDrainStalling;
        batch.setDrainStatusLocal(mStatus);
      }
    } else {
      if (mStatus != eos::common::DrainStatus::kDraining) {
        mStatus = eos::common::DrainStatus::kDraining;
        batch.setDrainStatusLocal(mStatus);
      }
    }

    uint64_t progress = 100ull;

    if (mTotalFiles) {
      progress = 100.0 * (mTotalFiles - mPending) / mTotalFiles;
    }

    uint64_t time_left = 99999999999ull;

    if (mDrainEnd > now) {
      time_left = duration_cast<seconds>(mDrainEnd - now).count();
    }

    batch.setLongLongLocal("stat.drain.failed", NumFailedJobs());
    batch.setLongLongLocal("stat.drainfiles", mPending);
    batch.setLongLongLocal("stat.drainprogress", progress);
    batch.setLongLongLocal("stat.timeleft", time_left);
    batch.setLongLongLocal("stat.drainbytesleft",
                           fs->GetLongLong("stat.statfs.usedbytes"));
    fs->applyBatch(batch);
    eos_debug_lite("msg=\"fsid=%d, update progress", mFsId);
  }

  // Sleep for a longer period since nothing moved in the last 10 min
  if (is_stalled) {
    std::this_thread::sleep_for(seconds(30));
  }

  // If we have only failed jobs check if the files still exist. It could also
  // be that there were new files written while draining was started.
  if ((mPending == 0) && (NumRunningJobs() == 0)) {
    uint64_t total_files = mNsFsView->getNumFilesOnFs(mFsId);

    if (total_files == 0) {
      SuccessfulDrain();
      return State::Done;
    } else {
      if (total_files == NumFailedJobs()) {
        FailedDrain();
        return State::Failed;
      } else {
        if (mDidRerun) {
          // If we already did a rerun then we just fail since there might be
          // ghost entries on the file system i.e. fids registered in the
          // FileSystem view but without any existing FileMD object.
          FailedDrain();
          return State::Failed;
        } else {
          mDidRerun = true;
          eos_info("msg=\"still %llu files to drain before declaring the file "
                   "system empty\" fsid=%lu", total_files, mFsId);
          mTotalFiles = total_files;
          mPending = mTotalFiles;
          eos::common::RWMutexWriteLock wr_lock(mJobsMutex);
          mJobsFailed.clear();
          return State::Rerun;
        }
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
  FileSystem* fs = FsView::gFsView.mIdView.lookupByID(mFsId);

  if (fs) {
    common::FileSystemUpdateBatch batch;
    batch.setLongLongLocal("stat.drainbytesleft", 0);
    batch.setLongLongLocal("stat.drainfiles", 0);
    batch.setLongLongLocal("stat.timeleft", 0);
    batch.setLongLongLocal("stat.drainprogress", 0);
    batch.setDrainStatusLocal(eos::common::DrainStatus::kNoDrain);
    fs->applyBatch(batch);
  }

  mStatus = eos::common::DrainStatus::kNoDrain;
}

//------------------------------------------------------------------------------
// Populate table with drain jobs info corresponding to the current fs
//------------------------------------------------------------------------------
void
DrainFs::PrintJobsTable(TableFormatterBase& table, bool show_errors,
                        const std::list<std::string>& itags) const
{
  TableData table_data;
  eos::common::RWMutexReadLock rd_lock(mJobsMutex);

  if (show_errors) {
    for (const auto& job : mJobsFailed) {
      table_data.emplace_back();
      std::list<string> data = job->GetInfo(itags);

      for (const auto& elem : data) {
        table_data.back().push_back(TableCell(elem, "s"));
      }
    }
  } else {
    for (const auto& job : mJobsRunning) {
      table_data.emplace_back();

      for (const auto& elem : job->GetInfo(itags)) {
        table_data.back().push_back(TableCell(elem, "s"));
      }
    }
  }

  table.AddRows(table_data);
}

//------------------------------------------------------------------------------
// Wait until namespace is booted or drain stop is requested
//------------------------------------------------------------------------------
void
DrainFs::WaitUntilNamespaceIsBooted() const
{
  while ((gOFS->mNamespaceState != NamespaceState::kBooted) && (!mDrainStop)) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    eos_debug_lite("msg=\"delay drain start until namespace is booted\" fsid=%u",
                   mFsId);
  }
}

EOSMGMNAMESPACE_END
