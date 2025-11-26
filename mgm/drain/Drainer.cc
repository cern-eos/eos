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
#include "mgm/fsview/FsView.hh"
#include "mgm/imaster/IMaster.hh"
#include "common/StringTokenizer.hh"
#include "common/StacktraceHere.hh"
#include "common/table_formatter/TableFormatterBase.hh"

namespace
{
//! Drainer configuration key in the global map
std::string kDrainerCfg = "drainer";
//! Max number of file systems that can be drained in parallel per node
std::string kDrainerMaxFs      = "max-fs-per-node";
//! Max number of threads that the drainer can spawn
std::string kDrainerMaxThreads = "max-thread-pool-size";
}

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
Drainer::Drainer():
  mThreadPool(10, 100, 10, 6, 5, "drain"),
  mMaxFsInParallel(5)
{}

//------------------------------------------------------------------------------
// Start running thread
//------------------------------------------------------------------------------
void Drainer::Start()
{
  mThread.reset(&Drainer::Drain, this);
}

//------------------------------------------------------------------------------
// Stop running thread and implicitly all running drain jobs
//------------------------------------------------------------------------------
void
Drainer::Stop()
{
  mThread.join();
  gOFS->mFidTracker.Clear(TrackerType::Drain);
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
  eos_static_info("msg=\"start draining\" fsid=%d", src_fsid);

  if (src_fsid == 0) {
    std::ostringstream ss;
    ss << "Debug stacktrace: " << eos::common::getStacktrace();
    eos_static_crit("msg=\"%s\"", ss.str().c_str());
  }

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
    [src_fsid](const std::shared_ptr<DrainFs>& elem) -> bool {
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
      if (it_drainfs->second.size() >= mMaxFsInParallel) {
        fs->SetDrainStatus(eos::common::DrainStatus::kDrainWait);
        mPending.push_back(std::make_pair(src_fsid, dst_fsid));
        return true;
      }
    }
  }

  // Start the drain
  std::shared_ptr<DrainFs> dfs(new DrainFs(mThreadPool, gOFS->eosFsView,
                               src_fsid, dst_fsid));

  try {
    auto future = std::async(std::launch::async, &DrainFs::DoIt, dfs.get());
    dfs->SetFuture(std::move(future));
    mDrainFs[src_snapshot.mHostPort].emplace(dfs);
  } catch (const std::exception& e) {
    err = SSTR("Starting drain thread failure, std::async exception"
               << e.what());
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Stop draining of a given file system
//------------------------------------------------------------------------------
bool
Drainer::StopFsDrain(eos::mgm::FileSystem* fs, std::string& err)
{
  eos::common::FileSystem::fsid_t fsid = fs->GetId();
  eos_static_notice("msg=\"stop draining\" fsid=%d ", fsid);

  if (fsid == 0) {
    std::ostringstream ss;
    ss << "Debug stacktrace: " << eos::common::getStacktrace();
    eos_static_crit("msg=\"%s\"", ss.str().c_str());
  }

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
  [fsid](const std::shared_ptr<DrainFs>& elem) -> bool {
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
    eos_static_err("%s", "msg=\"drain info header is empty\"");
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
  ThreadAssistant::setSelfThreadName("DrainerMT");
  eos_static_notice("%s", "msg=\"starting central drainer\"");
  gOFS->WaitUntilNamespaceIsBooted(assistant);

  // Wait that current MGM becomes a master
  do {
    eos_static_debug("%s", "msg=\"drain waiting for master MGM\"");
    assistant.wait_for(std::chrono::seconds(10));
  } while (!assistant.terminationRequested() && !gOFS->mMaster->IsMaster());

  // Reapply the drain status for file systems in drain mode
  FsView::gFsView.ReapplyDrainStatus();

  while (!assistant.terminationRequested()) {
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
  eos_static_notice("%s", "msg=\"stopped central drainer\"");
}

//------------------------------------------------------------------------------
// Signal all drain file systems to stop and wait for them
//------------------------------------------------------------------------------
void
Drainer::WaitForAllDrainToStop()
{
  eos_static_notice("%s", "msg=\"stop all ongoing drain\"");
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
// Apply global configuration relevant for the drainer
//------------------------------------------------------------------------------
void
Drainer::ApplyConfig()
{
  using eos::common::StringTokenizer;
  std::string config = FsView::gFsView.GetGlobalConfig(kDrainerCfg);
  // Parse config of the form: key1=val1 key2=val2 etc.
  eos_static_info("msg=\"apply drainer configuration\" data=\"%s\"",
                  config.c_str());
  std::map<std::string, std::string> kv_map;
  auto pairs = StringTokenizer::split<std::list<std::string>>(config, ' ');

  for (const auto& pair : pairs) {
    auto kv = StringTokenizer::split<std::vector<std::string>>(pair, '=');

    if (kv.empty()) {
      eos_static_err("msg=\"unknown drainer config data\" data=\"%s\"",
                     config.c_str());
      continue;
    }

    // There is no use-case yet for keys without values!
    if (kv.size() == 1) {
      continue;
    }

    kv_map.emplace(kv[0], kv[1]);
  }

  for (const auto& [key, val] : kv_map) {
    SetConfig(key, val);
  }
}

//------------------------------------------------------------------------------
// Serialize drainer configuration
//------------------------------------------------------------------------------
std::string
Drainer::SerializeConfig() const
{
  std::ostringstream oss;
  oss << kDrainerMaxThreads << "=" << mThreadPool.GetMaxThreads() << " "
      << kDrainerMaxFs << "=" << mMaxFsInParallel;
  return oss.str();
}

//------------------------------------------------------------------------------
// Store configuration
//------------------------------------------------------------------------------
bool
Drainer::StoreConfig()
{
  return FsView::gFsView.SetGlobalConfig(kDrainerCfg, SerializeConfig());
}

//------------------------------------------------------------------------------
// Make configuration change
//------------------------------------------------------------------------------
bool
Drainer::SetConfig(const std::string& key, const std::string& val)
{
  bool config_change = false;

  if (key == kDrainerMaxThreads) {
    int max_threads = 100;

    try {
      max_threads = std::stoi(val);
    } catch (...) {
      eos_static_err("msg=\"failed parsing drainer max threads configuration\" "
                     "data=\"%s\"", val.c_str());
      return false;
    }

    if ((max_threads >= 5) &&
        (max_threads != mThreadPool.GetMaxThreads())) {
      mThreadPool.SetMaxThreads(max_threads);
      config_change = true;
    }
  } else if (key == kDrainerMaxFs) {
    unsigned int max_fs_parallel = 5;

    try {
      max_fs_parallel = std::stoi(val);
    } catch (...) {
      eos_static_err("msg=\"failed parsing drainer max fs in parallel\" "
                     "data=\"%s\"", val.c_str());
      return false;
    }

    if (max_fs_parallel && (max_fs_parallel != mMaxFsInParallel.load())) {
      mMaxFsInParallel.store(max_fs_parallel);
      config_change = true;
    }
  } else {
    return false;
  }

  if (config_change) {
    if (!StoreConfig()) {
      eos_static_err("%s", "msg=\"failed to save drainer configuration\"");
    }
  }

  return true;
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
      eos_static_err("msg=\"failed to start pending drain src_fsid=%lu\""
                     " msg=\"%s\"", pair.first, msg.c_str());
    }
  }
}

EOSMGMNAMESPACE_END
