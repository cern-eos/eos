//------------------------------------------------------------------------------
//! @file Drainer.cc
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

#include "mgm/drain/Drainer.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/drain/DrainFs.hh"
#include "mgm/drain/DrainTransferJob.hh"
#include "mgm/FsView.hh"
#include "common/table_formatter/TableFormatterBase.hh"

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
Drainer::Drainer():
  mIsRunning(false),
  mThreadPool(10, 100, 10, 6, 5, "central_drain")
{}

//------------------------------------------------------------------------------
// Start running thread
//------------------------------------------------------------------------------
void Drainer::Start()
{
  if (!mIsRunning)  {
    mIsRunning = true;
    mThread.reset(&Drainer::Drain, this);
  }
}

//------------------------------------------------------------------------------
// Stop running thread and implicitly all running drain jobs
//------------------------------------------------------------------------------
void
Drainer::Stop()
{
  if (mIsRunning) {
    mIsRunning = false;
    mThread.join();
    gOFS->mFidTracker.Clear(TrackerType::Drain);
  }
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
Drainer::~Drainer()
{
  Stop();
}

//------------------------------------------------------------------------------
// Start draining of a given file system
//------------------------------------------------------------------------------
bool
Drainer::StartFsDrain(eos::mgm::FileSystem* fs,
                      eos::common::FileSystem::fsid_t dst_fsid,
                      std::string& err)
{
  using eos::common::FileSystem;
  FileSystem::fsid_t src_fsid = fs->GetId();
  eos_info("msg=\"start draining\" fsid=%d", src_fsid);
  FileSystem::fs_snapshot_t src_snapshot;
  fs->SnapShotFileSystem(src_snapshot);

  // Check that the destination fs, if specified, is in the same space and
  // group as the source
  if (dst_fsid) {
    FileSystem* dst = FsView::gFsView.mIdView.lookupByID(dst_fsid);
    FileSystem::fs_snapshot_t  dst_snapshot;

    if (!dst) {
      err = SSTR("error: destination file system " << dst_fsid
                 << " does not exist");
      return false;
    }

    dst->SnapShotFileSystem(dst_snapshot, false);

    if ((src_snapshot.mSpace != dst_snapshot.mSpace) ||
        (src_snapshot.mGroup != dst_snapshot.mGroup)) {
      err = SSTR("error: destination file system " << dst_fsid << " does not "
                 << "belong to the same space and scheduling group as the "
                 << "source");
      return false;
    }
  }

  eos::common::RWMutexWriteLock wr_lock(mDrainMutex);
  auto it_drainfs = mDrainFs.find(src_snapshot.mHostPort);

  if (it_drainfs != mDrainFs.end()) {
    // Check if the fs is already draining for this node
    auto it = std::find_if(it_drainfs->second.begin(), it_drainfs->second.end(),
    [src_fsid](const shared_ptr<DrainFs>& elem) -> bool {
      return (elem->GetFsId() == src_fsid);
    });

    if (it != it_drainfs->second.end()) {
      err = SSTR("error: drain has already started for the given fsid="
                 << src_fsid);
      return false;
    } else {
      // Check if drain request is not already pending
      auto it_pending = std::find_if(mPending.begin(), mPending.end(),
                                     [src_fsid](const std::pair<FileSystem::fsid_t,
      FileSystem::fsid_t>& elem) -> bool {
        return (elem.first == src_fsid);
      });

      if (it_pending != mPending.end()) {
        err = SSTR("error: drain jobs is already pending for fsid="
                   << src_fsid);
        return false;
      }

      // Check if we have reached the max fs per node for this node
      if (it_drainfs->second.size() >=
          MaxDrainFsInParallel(src_snapshot.mSpace)) {
        fs->SetDrainStatus(eos::common::DrainStatus::kDrainWait);
        mPending.push_back(std::make_pair(src_fsid, dst_fsid));
        return true;
      }
    }
  }

  // Start the drain
  std::shared_ptr<DrainFs> dfs(new DrainFs(mThreadPool, gOFS->eosFsView,
                               src_fsid, dst_fsid));
  auto future = std::async(std::launch::async, &DrainFs::DoIt, dfs);
  dfs->SetFuture(std::move(future));
  mDrainFs[src_snapshot.mHostPort].emplace(dfs);
  return true;
}

//------------------------------------------------------------------------------
// Stop draining of a given file system
//------------------------------------------------------------------------------
bool
Drainer::StopFsDrain(eos::mgm::FileSystem* fs, std::string& err)
{
  eos::common::FileSystem::fsid_t fsid = fs->GetId();
  eos_notice("msg=\"stop draining\" fsid=%d ", fsid);
  eos::common::FileSystem::fs_snapshot_t drain_snapshot;
  fs->SnapShotFileSystem(drain_snapshot);
  eos::common::RWMutexWriteLock wr_lock(mDrainMutex);
  auto it_drainfs = mDrainFs.find(drain_snapshot.mHostPort);

  if (it_drainfs == mDrainFs.end()) {
    err = SSTR("error: no drain started for fsid=" << fsid);
    return false;
  }

  // Check if the fs is draining
  auto it = std::find_if(it_drainfs->second.begin(), it_drainfs->second.end(),
  [fsid](const shared_ptr<DrainFs>& elem) -> bool {
    return (elem->GetFsId() == fsid);
  });

  if (it == it_drainfs->second.end()) {
    // Check if there is a request pending
    auto it_pending = std::find_if(mPending.begin(), mPending.end(),
                                   [fsid](const std::pair<FileSystem::fsid_t,
    FileSystem::fsid_t>& elem) -> bool {
      return (elem.first == fsid);
    });

    if (it_pending != mPending.end()) {
      (void) mPending.erase(it_pending);
    }

    fs->SetDrainStatus(eos::common::DrainStatus::kNoDrain);
  } else {
    (*it)->SignalStop();
  }

  return true;
}

//------------------------------------------------------------------------------
// Get drain jobs info
//------------------------------------------------------------------------------
bool
Drainer::GetJobsInfo(std::string& out, const DrainHdrInfo& hdr_info,
                     unsigned int fsid, bool only_failed, bool monitor_fmt) const
{
  if (hdr_info.empty()) {
    eos_err("msg=\"drain info header is empty\"");
    return false;
  }

  // Collect list of internal tags for display
  std::list<std::string> itags;

  for (const auto& elem : hdr_info) {
    itags.push_back(elem.second);
  }

  TableFormatterBase table;
  TableHeader table_header;

  for (const auto& elem : hdr_info) {
    if (monitor_fmt) {
      table_header.push_back(std::make_tuple(elem.first, 10, "s"));
    } else {
      table_header.push_back(std::make_tuple(elem.first, 0, "s"));
    }
  }

  table.SetHeader(table_header);
  std::vector<std::string> selections;
  bool found {false};
  {
    // Loop through all drain jobs and collect status information
    eos::common::RWMutexReadLock rd_lock(mDrainMutex);

    if (mDrainFs.size() == 0) {
      out += "info: there is no ongoing drain activity";
      return true;
    }

    for (const auto& pair : mDrainFs) {
      for (const auto& drain_fs : pair.second) {
        if (fsid == 0) {
          drain_fs->PrintJobsTable(table, only_failed, itags);
        } else {
          if (fsid == drain_fs->GetFsId()) {
            drain_fs->PrintJobsTable(table, only_failed, itags);
            found = true;
            break;
          }
        }
      }

      if (found) {
        break;
      }
    }
  }
  out = table.GenerateTable(HEADER, selections).c_str();
  return true;
}

//------------------------------------------------------------------------------
// Method doing the drain monitoring
//------------------------------------------------------------------------------
void
Drainer::Drain(ThreadAssistant& assistant) noexcept
{
  eos_static_debug("%s", "msg=\"starting central drainer\"");
  gOFS->WaitUntilNamespaceIsBooted(assistant);

  while (!assistant.terminationRequested()) {
    UpdateFromSpaceConfig();
    HandleQueued();
    gOFS->mFidTracker.DoCleanup(TrackerType::Drain);
    assistant.wait_for(std::chrono::seconds(5));
    // Clean up finished or stopped file system drains
    eos::common::RWMutexWriteLock wr_lock(mDrainMutex);

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
  }

  WaitForAllDrainToStop();
}

//------------------------------------------------------------------------------
// Signal all drain file systems to stop and wait for them
//------------------------------------------------------------------------------
void
Drainer::WaitForAllDrainToStop()
{
  eos_notice("%s", "msg=\"stop all ongoing drain\"");
  {
    eos::common::RWMutexReadLock rd_lock(mDrainMutex);

    for (auto& node_elem : mDrainFs) {
      for (const auto& fs_elem : node_elem.second) {
        fs_elem->SignalStop();
      }
    }
  }
  bool all_stopped {false};

  while (!all_stopped) {
    {
      all_stopped = true;
      eos::common::RWMutexReadLock rd_lock(mDrainMutex);

      for (auto& node_elem : mDrainFs) {
        for (const auto& fs_elem : node_elem.second) {
          if (fs_elem->IsRunning()) {
            all_stopped = false;
            break;
          }
        }

        if (!all_stopped) {
          break;
        }
      }
    }

    if (!all_stopped) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  }

  eos::common::RWMutexWriteLock wr_lock(mDrainMutex);
  mDrainFs.clear();
  mPending.clear();
}

//------------------------------------------------------------------------------
// Update drain relevant configuration from the space view
//------------------------------------------------------------------------------
void
Drainer::UpdateFromSpaceConfig()
{
  using namespace std::chrono;
  static auto last_update = steady_clock::now();

  // Update every minute
  if (duration_cast<seconds>(steady_clock::now() - last_update).count() > 60) {
    last_update = std::chrono::steady_clock::now();
    eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);

    for (const auto& space : FsView::gFsView.mSpaceView) {
      int max_drain_fs = 5;

      if (space.second->GetConfigMember("drainer.node.nfs") != "") {
        max_drain_fs = atoi(space.second->GetConfigMember("drainer.node.nfs").c_str());
      } else {
        space.second->SetConfigMember("drainer.node.nfs", "5");
      }

      // Set the space configuration
      XrdSysMutexHelper scope_lock(mCfgMutex);
      mCfgMap[space.first] = max_drain_fs;
    }
  }
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
unsigned int
Drainer::MaxDrainFsInParallel(const std::string& space) const
{
  XrdSysMutexHelper scope_lock(mCfgMutex);
  const auto it = mCfgMap.find(space);

  if (it != mCfgMap.end()) {
    return it->second;
  }

  return 0u;
}

//------------------------------------------------------------------------------
// Handle queued draining requests
//------------------------------------------------------------------------------
void
Drainer::HandleQueued()
{
  std::string msg;
  ListPendingT lst;
  {
    eos::common::RWMutexWriteLock wr_lock(mDrainMutex);
    std::swap(lst, mPending);
  }

  while (!lst.empty()) {
    auto pair = lst.front();
    lst.pop_front();
    eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
    FileSystem* fs = FsView::gFsView.mIdView.lookupByID(pair.first);

    if (fs && !StartFsDrain(fs, pair.second, msg)) {
      eos_err("msg=\"failed to start pending drain src_fsid=%lu\""
              " msg=\"%s\"", pair.first, msg.c_str());
    }
  }
}

EOSMGMNAMESPACE_END
