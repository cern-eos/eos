//------------------------------------------------------------------------------
//! @file Drainer.cc
//! @author Andrea Manzi - CERN
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

#include "mgm/drain/Drainer.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/drain/DrainFs.hh"
#include "mgm/drain/DrainTransferJob.hh"
#include "mgm/FsView.hh"
#include "mgm/TableFormatter/TableFormatterBase.hh"

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
Drainer::Drainer():
  mThreadPool(std::thread::hardware_concurrency(), 400, 10, 6, 5)
{}

//------------------------------------------------------------------------------
// Start running thread
//------------------------------------------------------------------------------
void Drainer::Start()
{
  XrdSysThread::Run(&mThread, Drainer::StaticDrainer, static_cast<void*>(this),
                    XRDSYSTHREAD_HOLD, "Drainer Thread");
}

//------------------------------------------------------------------------------
// Stop running thread
//------------------------------------------------------------------------------
void
Drainer::Stop()
{
  XrdSysThread::Cancel(mThread);
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
Drainer::~Drainer()
{
  Stop();

  if (!gOFS->Shutdown) {
    XrdSysThread::Join(mThread, NULL);
  }
}

//------------------------------------------------------------------------------
// Start draining of a given file system
//------------------------------------------------------------------------------
bool
Drainer::StartFsDrain(eos::mgm::FileSystem* fs, unsigned int dst_fsid,
                      XrdOucString& err, bool force)
{
  uint32_t src_fsid = fs->GetId();
  eos_info("fsid=%d start drain", src_fsid);
  eos::common::FileSystem::fs_snapshot_t src_snapshot;
  fs->SnapShotFileSystem(src_snapshot);

  // Check that the destination fs, if specified, is in the same space and group
  // as the source
  if (dst_fsid) {
    auto it_fs = FsView::gFsView.mIdView.find(dst_fsid);
    eos::common::FileSystem::fs_snapshot_t  dst_snapshot;

    if (it_fs == FsView::gFsView.mIdView.end()) {
      err = "error: the destination FS does not exist";
      return false;
    }

    it_fs->second->SnapShotFileSystem(dst_snapshot, false);

    if ((src_snapshot.mSpace != dst_snapshot.mSpace) ||
        (src_snapshot.mGroup != dst_snapshot.mGroup)) {
      err = "error: destination fs does not belong to the same space and "
            "scheduling group as the source";
      return false;
    }
  }

  XrdSysMutexHelper scop_lock(mDrainMutex);
  auto it_drainfs = mDrainFs.find(src_snapshot.mHostPort);

  if (it_drainfs != mDrainFs.end()) {
    // Check if the fs is already draining for this node
    auto it = std::find_if(it_drainfs->second.begin(), it_drainfs->second.end(),
    [src_fsid](const shared_ptr<DrainFs>& elem) {
      return (elem->GetFsId() == src_fsid);
    });

    if (it != it_drainfs->second.end()) {
      if (force) {
        (*it)->ForceRetry();
        return true;
      } else {
        err = "error: drain has already started for the given fs";
        return false;
      }
    } else {
      // Check if we have reached the max fs per node for this node
      if (it_drainfs->second.size() >= GetSpaceConf(src_snapshot.mSpace)) {
        err = "error: reached maximum number of draining fs for the node";
        return false;
      }
    }
  }

  // Start the drain
  std::shared_ptr<DrainFs> dfs(new DrainFs(mThreadPool, src_fsid, dst_fsid));
  auto future = std::async(std::launch::async, &DrainFs::DoIt, dfs);
  dfs->SetFuture(std::move(future));
  mDrainFs[src_snapshot.mHostPort].emplace(dfs);
  return true;
}

//------------------------------------------------------------------------------
// Stop draining of a given file system
//------------------------------------------------------------------------------
bool
Drainer::StopFsDrain(eos::mgm::FileSystem* fs, XrdOucString& err)
{
  uint32_t fsid = fs->GetId();
  eos_notice("fs to stop draining=%d ", fsid);
  eos::common::FileSystem::fs_snapshot_t drain_snapshot;
  fs->SnapShotFileSystem(drain_snapshot);
  auto it_drainfs = mDrainFs.find(drain_snapshot.mHostPort);

  if (it_drainfs == mDrainFs.end()) {
    err = "error: no drain started for the given fs";
    return false;
  }

  // Check if the fs is draining
  auto it = std::find_if(it_drainfs->second.begin(), it_drainfs->second.end(),
  [fsid](const shared_ptr<DrainFs>& elem) {
    return (elem->GetFsId() == fsid);
  });

  if (it == it_drainfs->second.end()) {
    err = "error: no drain started for the givne fs";
    return false;
  }

  (*it)->SignalStop();
  return true;
}

//------------------------------------------------------------------------------
// Get draining status
//------------------------------------------------------------------------------
bool
Drainer::GetDrainStatus(unsigned int fsid, XrdOucString& out, XrdOucString& err)
{
  if (mDrainFs.size() == 0) {
    out += "info: there are no ongoing drain activities";
    return true;
  }

  if (fsid == 0) {
    TableFormatterBase table;
    std::vector<std::string> selections;

    for (const auto& pair : mDrainFs) {
      std::string node = pair.first;

      for (const auto& job : pair.second) {
        PrintTable(table, node, job.get());
      }
    }

    TableHeader table_header;
    table_header.push_back(std::make_tuple("node", 30, "s"));
    table_header.push_back(std::make_tuple("fs id", 10, "s"));
    table_header.push_back(std::make_tuple("drain status", 30, "s"));
    table.SetHeader(table_header);
    out =  table.GenerateTable(HEADER, selections).c_str();
  } else {
    eos::common::FileSystem::fs_snapshot_t drain_snapshot;
    {
      eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
      auto it_fs = FsView::gFsView.mIdView.find(fsid);

      if (it_fs == FsView::gFsView.mIdView.end()) {
        err = "error: the given FS does not exist";
        return false;
      }

      it_fs->second->SnapShotFileSystem(drain_snapshot, false);
    }
    auto it_drainfs = mDrainFs.find(drain_snapshot.mHostPort);

    if (it_drainfs == mDrainFs.end()) {
      err = "error: file system is not draining";
      return false;
    }

    auto it = std::find_if(it_drainfs->second.begin(), it_drainfs->second.end(),
    [fsid](const shared_ptr<DrainFs>& elem) {
      return (elem->GetFsId() == fsid);
    });

    if (it == it_drainfs->second.end()) {
      err = "error: file system is not draining";
      return false;
    }

    TableFormatterBase table;
    std::vector<std::string> selections;
    TableHeader table_header;
    table_header.push_back(std::make_tuple("node", 30, "s"));
    table_header.push_back(std::make_tuple("fs id", 10, "s"));
    table_header.push_back(std::make_tuple("drain status", 30, "s"));
    PrintTable(table, drain_snapshot.mHostPort, (*it).get());
    table.SetHeader(table_header);
    out += table.GenerateTable(HEADER, selections).c_str();
    // Second table with status of the failed jobs
    TableFormatterBase table_jobs;
    TableHeader table_header_jobs;
    table_header_jobs.push_back(std::make_tuple("file id", 30, "s"));
    table_header_jobs.push_back(std::make_tuple("source fs", 30, "s"));
    table_header_jobs.push_back(std::make_tuple("destination fs", 30, "s"));
    table_header_jobs.push_back(std::make_tuple("error", 100, "s"));
    table_jobs.SetHeader(table_header_jobs);
    auto job_vect_it = (*it)->GetFailedJobs().cbegin();

    if (job_vect_it != (*it)->GetFailedJobs().cend()) {
      out += "List of Files failed to be drained:\n\n";

      while (job_vect_it != (*it)->GetFailedJobs().cend()) {
        PrintJobsTable(table_jobs, (*job_vect_it).get());
        job_vect_it++;
      }

      out +=  table_jobs.GenerateTable(HEADER, selections).c_str();
    }
  }

  return true;
}

//------------------------------------------------------------------------------
// Print table with draining status
//------------------------------------------------------------------------------
void
Drainer::PrintTable(TableFormatterBase& table, std::string node, DrainFs* fs)
{
  TableData table_data;
  table_data.emplace_back();
  table_data.back().push_back(TableCell(node, "s"));
  table_data.back().push_back(TableCell(fs->GetFsId(), "s"));
  table_data.back().push_back(TableCell(FileSystem::GetDrainStatusAsString(
                                          fs->GetDrainStatus()), "s"));
  table.AddRows(table_data);
}

//------------------------------------------------------------------------------
// Print table of the drain jobs
//------------------------------------------------------------------------------
void
Drainer::PrintJobsTable(TableFormatterBase& table, DrainTransferJob* job)
{
  TableData table_data;
  table_data.emplace_back();
  table_data.back().push_back(TableCell(job->GetFileId(), "l"));
  table_data.back().push_back(TableCell(job->GetSourceFS(), "l"));
  table_data.back().push_back(TableCell(job->GetTargetFS(), "l"));
  table_data.back().push_back(TableCell(job->GetErrorString(), "s"));
  table.AddRows(table_data);
}

//------------------------------------------------------------------------------
// Static thread startup function
//------------------------------------------------------------------------------
void*
Drainer::StaticDrainer(void* arg)
{
  return reinterpret_cast<Drainer*>(arg)->Drain();
}

//------------------------------------------------------------------------------
// Method doing the drain monitoring
//------------------------------------------------------------------------------
void*
Drainer::Drain()
{
  XrdSysThread::SetCancelOn();
  eos_static_debug("Starting centralized drainer");

  // Wait that the namespace is initialized
  while (true) {
    if (gOFS->IsNsBooted()) {
      break;
    }

    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  while (true) {
    uint64_t timeout_ms = 100;

    while (FsView::gFsView.ViewMutex.TimedRdLock(timeout_ms)) {
      XrdSysThread::CancelPoint();
    }

    XrdSysThread::SetCancelOff();

    for (const auto& space : FsView::gFsView.mSpaceView) {
      int max_drain_fs = 5;

      if (space.second->GetConfigMember("drainer.node.nfs") != "") {
        max_drain_fs = atoi(space.second->GetConfigMember("drainer.node.nfs").c_str());
      } else {
        space.second->SetConfigMember("drainer.node.nfs", "5", true, "/eos/*/mgm");
      }

      // Set the space configuration
      XrdSysMutexHelper scope_lock(mCfgMutex);
      mCfgMap[space.first] = max_drain_fs;
    }

    // Clean up finished or stopped file system drains
    for (auto& pair : mDrainFs) {
      auto& set_fs = pair.second;

      for (auto it = set_fs.begin(); it != set_fs.end(); /*empty*/) {
        if (!(*it)->IsRunning()) {
          it = set_fs.erase(it);
        } else {
          ++it;
        }
      }
    }

    // Execute only once at boot time
    static bool run_once = true;

    if (run_once) {
      for (auto it_fs = FsView::gFsView.mIdView.begin();
           it_fs != FsView::gFsView.mIdView.end(); it_fs++) {
        eos::common::FileSystem::fs_snapshot_t drain_snapshot;
        it_fs->second->SnapShotFileSystem(drain_snapshot, false);
        FileSystem::fsstatus_t confstatus = it_fs->second->GetConfigStatus();
        FileSystem::fsstatus_t drainstatus = it_fs->second->GetDrainStatus();

        if (confstatus == eos::common::FileSystem::kRO) {
          if (drainstatus != eos::common::FileSystem::kNoDrain &&
              drainstatus !=  eos::common::FileSystem::kDrained) {
            // Start the drain
            XrdOucString err;

            if (!StartFsDrain(it_fs->second, 0, err)) {
              eos_notice("Failed to start the drain for fs %d: %s", it_fs->first,
                         err.c_str());
            }
          }
        }
      }

      run_once = false;
    }

    FsView::gFsView.ViewMutex.UnLockRead();
    XrdSysThread::SetCancelOn();
    std::this_thread::sleep_for(std::chrono::seconds(10));
  }

  return 0;
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
unsigned int
Drainer::GetSpaceConf(const std::string& space)
{
  XrdSysMutexHelper scope_lock(mCfgMutex);

  if (mCfgMap.count(space)) {
    return mCfgMap[space];
  } else {
    return 0;
  }
}

EOSMGMNAMESPACE_END
