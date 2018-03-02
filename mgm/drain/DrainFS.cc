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
#include "mgm/GeoTreeEngine.hh"
#include "mgm/Master.hh"
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
  eos_notice("msg=\"fsid=%u start draining\"", mFsId);
  int ntried = 0;

  do {
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

    // Loop to drain the files
    do {
      auto job = mJobsPending.begin();
      eos::common::FileSystem::fsid_t fsIdTarget;

      while ((mJobsRunning.size() <= maxParallelJobs) &&
             (job != mJobsPending.end())) {
        if (!(*job)->GetTargetFS()) {
          if ((fsIdTarget = SelectTargetFS(&(*job->get()))) != 0) {
            (*job)->SetTargetFS(fsIdTarget);
          } else {
            std::string error = "Failed to find a suitable Target filesystem for draining";
            (*job)->ReportError(error);
            mJobsFailed.push_back(*job);
            job = mJobsPending.erase(job);
            continue;
          }
        }

        XrdSysTimer sleep;
        sleep.Wait(200);
        (*job)->SetStatus(DrainTransferJob::Ready);
        (*job)->Start();
        mJobsRunning.push_back(*job);
        job = mJobsPending.erase(job);
      }

      for (auto it_jobs = mJobsRunning.begin() ; it_jobs !=  mJobsRunning.end();) {
        if ((*it_jobs)->GetStatus() == DrainTransferJob::OK) {
          it_jobs = mJobsRunning.erase(it_jobs);
        } else if ((*it_jobs)->GetStatus() == DrainTransferJob::Failed) {
          mJobsFailed.push_back(*it_jobs);
          it_jobs = mJobsRunning.erase(it_jobs);
        } else {
          it_jobs++;
        }
      }

      State state = UpdateProgress();

      if ((state == State::DONE) || (state == State::FAILED)) {
        return;
      } else if (state == State::EXPIRED) {
        break;
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
  eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);

  if (FsView::gFsView.mIdView.count(mFsId)) {
    FileSystem* fs  = FsView::gFsView.mIdView[mFsId];

    if (fs) {
      fs->OpenTransaction();
      fs->SetConfigStatus(eos::common::FileSystem::kRW, true);
      fs->SetDrainStatus(eos::common::FileSystem::kNoDrain);
      fs->CloseTransaction();
      mDrainStatus = eos::common::FileSystem::kNoDrain;
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

//------------------------------------------------------------------------------
// Select target file system using the GeoTreeEngine
// @todo (esindril) this should be moved inside the drain job
//------------------------------------------------------------------------------
eos::common::FileSystem::fsid_t
DrainFS::SelectTargetFS(DrainTransferJob* job)
{
  unsigned int nfilesystems = 1;
  unsigned int ncollocatedfs = 0;
  std::vector<FileSystem::fsid_t> new_repl;
  eos::common::FileSystem::fs_snapshot source_snapshot;
  // Take locks
  eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
  eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex);
  auto fmd = gOFS->eosFileService->getFileMD(job->GetFileId());
  eos::common::FileSystem* source_fs =
    FsView::gFsView.mIdView[job->GetSourceFS()];
  source_fs->SnapShotFileSystem(source_snapshot);
  FsGroup* group = FsView::gFsView.mGroupView[source_snapshot.mGroup];
  // Check other replicas for the file
  std::vector<std::string> fsid_geotags;
  std::vector<FileSystem::fsid_t> existing_repl
    = static_cast<std::vector<FileSystem::fsid_t>>(fmd->getLocations());

  if (!gGeoTreeEngine.getInfosFromFsIds(existing_repl, &fsid_geotags, 0, 0)) {
    eos_notice("could not retrieve info for all avoid fsids");
    return 0;
  }

  for (auto repl : existing_repl) {
    eos_static_debug("existing replicas: %d", (unsigned long)repl);
  }

  for (auto geo : fsid_geotags) {
    eos_static_debug("geotags: %s", geo.c_str());
  }

  bool res = gGeoTreeEngine.placeNewReplicasOneGroup(
               group, nfilesystems,
               &new_repl,
               (ino64_t) fmd->getId(),
               NULL, // entrypoints
               NULL, // firewall
               GeoTreeEngine::draining,
               &existing_repl,
               &fsid_geotags,
               fmd->getSize(),
               "",// start from geotag
               "",// client geo tag
               ncollocatedfs,
               NULL, // excludeFS
               &fsid_geotags, // excludeGeoTags
               NULL);

  if (res) {
    std::ostringstream oss;

    for (auto elem : new_repl) {
      oss << " " << (unsigned long)(elem);
    }

    eos_static_debug("GeoTree Draining Placement returned %d with fs id's -> %s",
                     (int)res, oss.str().c_str());
    // Return only one FS now
    eos::common::FileSystem::fsid_t targetFS = *new_repl.begin();
    return targetFS;
  } else {
    eos_notice("fid=%llu could not place new replica", job->GetFileId());
    return 0;
  }
}

EOSMGMNAMESPACE_END
