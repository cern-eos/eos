//------------------------------------------------------------------------------
// File: Fsck.cc
// Author: Andreas-Joachim Peters/Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

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

#include "mgm/fsck/Fsck.hh"
#include "mgm/fsck/FsckEntry.hh"
#include "common/LayoutId.hh"
#include "common/Path.hh"
#include "common/StringConversion.hh"
#include "common/Mapping.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Master.hh"
#include "mgm/Messaging.hh"
#include "mgm/FsView.hh"
#include "namespace/interface/IView.hh"
#include "namespace/interface/IFsView.hh"
#include "namespace/Prefetcher.cc"

EOSMGMNAMESPACE_BEGIN

const char* Fsck::gFsckEnabled = "fsck";
const char* Fsck::gFsckInterval = "fsckinterval";

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
Fsck::Fsck():
  mShowOffline(false), mShowDarkFiles(false), mStartProcessing(false),
  mRunRepairThread(false), mEnabled(false),
  mRunInterval(std::chrono::minutes(30)),
  mRunning(false), eTimeStamp(0),
  mThreadPool(std::thread::hardware_concurrency(), 20, 10, 6, 5, "fsck"),
  mIdTracker(std::chrono::minutes(10), std::chrono::hours(2)), mQcl(nullptr)
{}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
Fsck::~Fsck()
{
  (void) Stop(false);
}

//------------------------------------------------------------------------------
// Start FSCK trhread
//------------------------------------------------------------------------------
bool
Fsck::Start(int interval_min)
{
  if (interval_min) {
    mRunInterval = std::chrono::minutes(interval_min);
  }

  if (!mRunning) {
    if (!gOFS->mQdbCluster.empty()) {
      mQcl = std::make_shared<qclient::QClient>
             (gOFS->mQdbContactDetails.members,
              gOFS->mQdbContactDetails.constructOptions());
    }

    mCollectorThread.reset(&Fsck::CollectErrs, this);

    if (mRunRepairThread) {
      mRepairThread.reset(&Fsck::RepairErrs, this);
    }

    mRunning = true;
    mEnabled = "true";
    return StoreFsckConfig();
  } else {
    return false;
  }
}

//------------------------------------------------------------------------------
// Stop FSCK thread
//------------------------------------------------------------------------------
bool
Fsck::Stop(bool store)
{
  if (mRunning) {
    eos_info("%s", "msg=\"join FSCK threads\"");
    mCollectorThread.join();
    mRepairThread.join();
    mRunning = false;
    mEnabled = false;
    Log("disabled check");

    if (store) {
      return StoreFsckConfig();
    } else {
      return true;
    }
  } else {
    return false;
  }
}

//------------------------------------------------------------------------------
// Apply the FSCK configuration stored in the configuration engine
//------------------------------------------------------------------------------
void
Fsck::ApplyFsckConfig()
{
  std::string enabled = FsView::gFsView.GetGlobalConfig(gFsckEnabled);

  if (enabled.length()) {
    mEnabled = enabled.c_str();
  }

  std::string sinterval = FsView::gFsView.GetGlobalConfig(gFsckInterval);

  if (sinterval.length()) {
    int interval = std::stoi(sinterval);

    if (interval < 0) {
      interval = 30;
    }

    mRunInterval = std::chrono::minutes(interval);
  }

  Log("enabled=%s", mEnabled.c_str());
  Log("check interval=%d minutes",
      std::chrono::duration_cast<std::chrono::minutes>(mRunInterval).count());

  if (mEnabled == "true") {
    Start();
  } else {
    Stop();
  }
}

//------------------------------------------------------------------------------
// Store the current running FSCK configuration in the config engine
//------------------------------------------------------------------------------
bool
Fsck::StoreFsckConfig()
{
  bool ok = true;
  std::string interval_min = std::to_string
                             (std::chrono::duration_cast<std::chrono::minutes>(mRunInterval).count());
  ok &= FsView::gFsView.SetGlobalConfig(gFsckEnabled, mEnabled.c_str());
  ok &= FsView::gFsView.SetGlobalConfig(gFsckInterval, interval_min.c_str());
  return ok;
}

//------------------------------------------------------------------------------
// Apply configuration options to the fsck mechanism
//------------------------------------------------------------------------------
bool
Fsck::Config(const std::string& key, const std::string& value)
{
  if (key == "show-dark-files") {
    mShowDarkFiles = (value == "yes");
  } else if (key == "show-offline") {
    mShowOffline = (value == "yes");
  } else if (key == "max-queued-jobs") {
    try {
      mMaxQueuedJobs = std::stoull(value);
    } catch (...) {
      eos_err("msg=\"failed to convert max-queued-jobs\" value=%s",
              value.c_str());
    }
  } else if (key == "toggle-repair") {
    if (mRunRepairThread == true) {
      mRepairThread.join();
    } else {
      mRepairThread.reset(&Fsck::RepairErrs, this);
    }

    mRunRepairThread = !mRunRepairThread;
  } else {
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Looping thread function collecting FSCK results
//------------------------------------------------------------------------------
void
Fsck::CollectErrs(ThreadAssistant& assistant) noexcept
{
  int bccount = 0;
  ClearLog();
  gOFS->WaitUntilNamespaceIsBooted();
  eos_info("%s", "msg=\"started fsck collector thread\"");

  while (!assistant.terminationRequested()) {
    ClearLog();
    Log("started check");

    // Don't run fsck if we are not a master
    while (!gOFS->mMaster->IsMaster()) {
      assistant.wait_for(std::chrono::seconds(60));

      if (assistant.terminationRequested()) {
        eos_info("%s", "msg=\"stopped fsck collector thread\"");
        return;
      }
    }

    Log("Filesystems to check: %lu", FsView::gFsView.GetNumFileSystems());
    // Broadcast fsck request and collect responses
    XrdOucString broadcastresponsequeue = gOFS->MgmOfsBrokerUrl;
    broadcastresponsequeue += "-fsck-";
    broadcastresponsequeue += bccount;
    XrdOucString broadcasttargetqueue = gOFS->MgmDefaultReceiverQueue;
    XrdOucString msgbody;
    // mgm.fsck.tags no longer necessary for newer versions, but keeping for
    // compatibility
    msgbody = "mgm.cmd=fsck&mgm.fsck.tags=*";
    XrdOucString stdOut = "";
    XrdOucString stdErr = "";

    if (!gOFS->MgmOfsMessaging->BroadCastAndCollect(broadcastresponsequeue,
        broadcasttargetqueue, msgbody, stdOut, 10, &assistant)) {
      eos_err("msg=\"failed to broadcast and collect fsck from [%s]:[%s]\"",
              broadcastresponsequeue.c_str(), broadcasttargetqueue.c_str());
      stdErr = "error: broadcast failed\n";
    }

    eos_info("msg=\"fsck reply size=%llu\" data=%s", stdOut.length(),
             stdOut.c_str());
    ResetErrorMaps();
    std::vector<std::string> lines;
    // Convert into a line-wise seperated array
    eos::common::StringConversion::StringToLineVector((char*) stdOut.c_str(),
        lines);

    for (size_t nlines = 0; nlines < lines.size(); ++nlines) {
      std::set<unsigned long long> fids;
      unsigned long fsid = 0;
      std::string err_tag;

      if (eos::common::StringConversion::ParseStringIdSet((char*)
          lines[nlines].c_str(), err_tag, fsid, fids)) {
        if (fsid) {
          eos::common::RWMutexWriteLock wr_lock(mErrMutex);

          // Add the fids into the error maps
          for (auto it = fids.cbegin(); it != fids.cend(); ++it) {
            eFsMap[err_tag][fsid].insert(*it);
          }
        }
      } else {
        eos_err("msg=\"cannot parse fsck response\" msg=\"%s\"",
                lines[nlines].c_str());
      }
    }

    // @note accounting the offline replicas/files is a heavy ns op.
    if (mShowOffline) {
      AccountOfflineReplicas();
      PrintOfflineReplicas();
      AccountOfflineFiles();
    }

    AccountNoReplicaFiles();
    PrintErrorsSummary();

    // @note the following operation is heavy for the qdb ns
    if (mShowDarkFiles) {
      AccountDarkFiles();
    }

    Log("stopping check");
    Log("=> next run in %d minutes",
        std::chrono::duration_cast<std::chrono::minutes>(mRunInterval).count());
    // Notify the repair thread that it can run now
    mStartProcessing = true;
    // Wait for next FSCK round ...
    assistant.wait_for(mRunInterval);
  }
}

//------------------------------------------------------------------------------
// Method submitting fsck repair jobs to the thread pool
//------------------------------------------------------------------------------
void
Fsck::RepairErrs(ThreadAssistant& assistant) noexcept
{
  gOFS->WaitUntilNamespaceIsBooted();
  eos_info("%s", "msg=\"started fsck repair thread\"");

  while (!assistant.terminationRequested()) {
    // Don't run if we are not a master
    while (!gOFS->mMaster->IsMaster()) {
      assistant.wait_for(std::chrono::seconds(1));

      if (assistant.terminationRequested()) {
        eos_info("%s", "msg=\"stopped fsck repair thread\"");
        return;
      }
    }

    // Wait for the collector thread to signal us
    while (!mStartProcessing) {
      assistant.wait_for(std::chrono::seconds(1));

      if (assistant.terminationRequested()) {
        eos_info("%s", "msg=\"stopped fsck repair thread\"");
        return;
      }
    }

    // Create local struct for errors so that we avoid the iterator invalidation
    // and the long locks
    std::map<std::string,
        std::map<eos::common::FileSystem::fsid_t,
        std::set <eos::common::FileId::fileid_t> > > local_emap;
    {
      eos::common::RWMutexReadLock rd_lock(mErrMutex);
      local_emap.insert(eFsMap.begin(), eFsMap.end());
    }

    for (const auto& err_fs : local_emap) {
      for (const auto& elem : err_fs.second) {
        for (const auto& fid : elem.second) {
          if (mIdTracker.HasEntry(fid)) {
            eos_debug("msg=\"skip already scheduled repair\" fid=%08llx", fid);
            continue;
          }

          mIdTracker.AddEntry(fid);
          std::shared_ptr<FsckEntry> job {
            new FsckEntry(fid, elem.first, err_fs.first, mQcl)};
          mThreadPool.PushTask<void>([job]() {
            return job->Repair();
          });
        }

        while (mThreadPool.GetQueueSize() > mMaxQueuedJobs) {
          assistant.wait_for(std::chrono::seconds(1));

          if (assistant.terminationRequested()) {
            // Wait that there are not more jobs in the queue
            while (mThreadPool.GetQueueSize()) {
              assistant.wait_for(std::chrono::seconds(1));
            }

            eos_info("%s", "msg=\"stopped fsck repair thread\"");
            return;
          }
        }
      }
    }

    mStartProcessing = false;
    eos_info("%s", "msg=\"loop in fsck repair thread\"");
  }

  // Wait that there are no more jobs in the queue
  while (mThreadPool.GetQueueSize()) {
    assistant.wait_for(std::chrono::seconds(1));
  }

  eos_info("%s", "msg=\"stopped fsck repair thread\"");
}

//------------------------------------------------------------------------------
// Try to repair a given entry
//------------------------------------------------------------------------------
bool
Fsck::RepairEntry(eos::IFileMD::id_t fid, bool async, std::string& out_msg)
{
  if (fid == 0ull) {
    eos_err("%s", "msg=\"not such file id 0\"");
    return false;
  }

  std::shared_ptr<FsckEntry> job {
    new FsckEntry(fid, 0, "none", mQcl)};

  if (async) {
    out_msg = "msg=\"repair job submitted\"";
    mThreadPool.PushTask<void>([job]() {
      return job->Repair();
    });
  } else {
    if (!job->Repair()) {
      out_msg = "msg=\"repair job failed\"";
      return false;
    } else {
      out_msg = "msg=\"repair successful\"";
    }
  }

  return true;
}

//------------------------------------------------------------------------------
// Print the current log output
//------------------------------------------------------------------------------
void
Fsck::PrintOut(std::string& out) const
{
  out = "Repair thread status: ";
  out += (mRunRepairThread ? "running\n" : "stopped\n");
  XrdSysMutexHelper lock(mLogMutex);
  out += mLog.c_str();
}

//------------------------------------------------------------------------------
// Return the current FSCK report
//------------------------------------------------------------------------------
bool
Fsck::Report(std::string& out, const std::set<std::string> tags,
             bool display_per_fs, bool display_fxid, bool display_lfn,
             bool display_json)
{
  std::ostringstream oss;
  eos::common::RWMutexReadLock rd_lock(mErrMutex);

  if (display_json) {
    ReportJsonFormat(oss, tags, display_per_fs, display_fxid, display_lfn);
  } else {
    ReportMonitorFormat(oss, tags, display_per_fs, display_fxid, display_lfn);
  }

  out = oss.str();
  return true;
}

//------------------------------------------------------------------------------
// Get the require format for the given file identifier. Empty if no format
// requested.
//------------------------------------------------------------------------------
std::string
Fsck::GetFidFormat(eos::IFileMD::id_t fid, bool display_fxid, bool
                   display_lfn) const
{
  if (display_fxid) {
    return eos::common::FileId::Fid2Hex(fid);
  } else if (display_lfn) {
    eos::Prefetcher::prefetchFileMDWithParentsAndWait(gOFS->eosView, fid);
    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);

    try {
      auto fmd = gOFS->eosFileService->getFileMD(fid);
      return gOFS->eosView->getUri(fmd.get());
    } catch (eos::MDException& e) {
      return "undefined";
    }
  }

  return "";
}

//------------------------------------------------------------------------------
// Create report in JSON format
//------------------------------------------------------------------------------
void
Fsck::ReportJsonFormat(std::ostringstream& oss,
                       const std::set<std::string> tags,
                       bool display_per_fs, bool display_fxid,
                       bool display_lfn) const
{
  Json::Value json;

  if (display_per_fs) {
    // Display per file system
    std::map<eos::common::FileSystem::fsid_t,
        std::map<std::string, std::set<unsigned long long>>> fs_fxid;

    for (const auto& elem_map : eFsMap) {
      if (!tags.empty() && (tags.find(elem_map.first) == tags.end())) {
        continue;  // skip unselected
      }

      for (const auto& elem : elem_map.second) {
        for (const auto& fxid : elem.second) {
          fs_fxid[elem.first][elem_map.first].insert(fxid);
        }
      }
    }

    for (const auto& elem : fs_fxid) {
      for (auto it = elem.second.begin(); it != elem.second.end(); ++it) {
        Json::Value json_entry;
        json_entry["timestamp"] = (Json::Value::UInt64)eTimeStamp;
        json_entry["fsid"] = (int) elem.first;
        json_entry["tag"] = it->first.c_str();
        json_entry["count"] = (int) it->second.size();

        if (!display_fxid && !display_lfn) {
          json.append(json_entry);
          continue;
        }

        Json::Value json_ids;

        for (auto it_fid = it->second.begin();
             it_fid != it->second.end(); ++it_fid) {
          json_ids.append(GetFidFormat(*it_fid, display_fxid, display_lfn));
        }

        if (display_fxid) {
          json_entry["fxid"] = json_ids;
        } else if (display_lfn) {
          json_entry["lfn"] = json_ids;
        }

        json.append(json_entry);
      }
    }
  } else {
    for (const auto& elem_map : eFsMap) {
      if (!tags.empty() && (tags.find(elem_map.first) == tags.end())) {
        continue;  // skip unselected
      }

      Json::Value json_entry;
      json_entry["timestamp"] = (Json::Value::UInt64) eTimeStamp;
      json_entry["tag"] = elem_map.first;
      Json::Value json_ids;
      std::set<unsigned long long> fids;

      for (const auto& elem : elem_map.second) {
        for (auto it = elem.second.begin(); it != elem.second.end(); ++it) {
          fids.insert(*it);
        }
      }

      for (auto it = fids.begin(); it != fids.end(); ++it) {
        json_ids.append((Json::Value::UInt64)(*it));
      }

      json_entry["count"] = (Json::Value::UInt64)fids.size();

      if (display_fxid) {
        json_entry["fxid"] = json_ids;
      } else if (display_lfn) {
        json_entry["lfn"] = json_ids;
      }

      json.append(json_entry);
    }
  }

  // List shadow filesystems
  if (!eFsDark.empty()) {
    for (auto fsit = eFsDark.begin(); fsit != eFsDark.end(); fsit++) {
      Json::Value json_entry;
      json_entry["timestamp"] = (Json::Value::UInt64) eTimeStamp;
      json_entry["tag"] = "shadow_fsid";
      json_entry["fsid"] = (Json::Value::UInt64)fsit->first;
      json_entry["count"] = (Json::Value::UInt64)fsit->second;
      json.append(json_entry);
    }
  }

  oss << json;
}

//------------------------------------------------------------------------------
// Create report in monitor format
//------------------------------------------------------------------------------
void
Fsck::ReportMonitorFormat(std::ostringstream& oss,
                          const std::set<std::string> tags,
                          bool display_per_fs, bool display_fxid,
                          bool display_lfn) const
{
  if (display_per_fs) {
    // Display per file system
    std::map<eos::common::FileSystem::fsid_t,
        std::map<std::string, std::set<unsigned long long>>> fs_fxid;

    for (const auto& elem_map : eFsMap) {
      if (!tags.empty() && (tags.find(elem_map.first) == tags.end())) {
        continue;  // skip unselected
      }

      for (const auto& elem : elem_map.second) {
        for (const auto& fxid : elem.second) {
          fs_fxid[elem.first][elem_map.first].insert(fxid);
        }
      }
    }

    for (const auto& elem : fs_fxid) {
      for (auto it = elem.second.begin(); it != elem.second.end(); ++it) {
        oss << "timestamp=" << eTimeStamp << " fsid=" << elem.first
            << " tag=\"" << it->first << "\" count=" << it->second.size();

        if (display_fxid) {
          oss << " fxid=";
        } else if (display_lfn) {
          oss << " lfn=";
        } else {
          oss << std::endl;
          continue;
        }

        for (auto it_fid = it->second.begin();
             it_fid != it->second.end(); ++it_fid) {
          oss << GetFidFormat(*it_fid, display_fxid, display_lfn);

          if (it_fid != std::prev(it->second.end())) {
            oss << ", ";
          }
        }

        oss << std::endl;
      }
    }
  } else {
    for (const auto& elem_map : eFsMap) {
      if (!tags.empty() && (tags.find(elem_map.first) == tags.end())) {
        continue;  // skip unselected
      }

      oss << "timestamp=" << eTimeStamp << " tag=\"" << elem_map.first << "\"";
      std::set<unsigned long long> fids;

      for (const auto& elem : elem_map.second) {
        for (auto it = elem.second.begin(); it != elem.second.end(); ++it) {
          fids.insert(*it);
        }
      }

      oss << " count=" << fids.size();

      if (display_fxid) {
        oss << " fxid=";
      } else if (display_lfn) {
        oss << " lfn=";
      } else {
        oss << std::endl;
        continue;
      }

      for (auto it = fids.begin(); it != fids.end(); ++it) {
        oss << GetFidFormat(*it, display_fxid, display_lfn);

        if (it != std::prev(fids.end())) {
          oss << ", ";
        }
      }

      oss << std::endl;
    }
  }

  // List shadow filesystems
  if (!eFsDark.empty()) {
    for (auto fsit = eFsDark.begin(); fsit != eFsDark.end(); ++fsit) {
      oss << "timestamp=" << eTimeStamp << " tag=\"shadow_fsid\""
          << " fsid=" << fsit->first << " count=" << fsit->second;
    }
  }
}

//------------------------------------------------------------------------------
// Clear the current FSCK log
//------------------------------------------------------------------------------
void
Fsck::ClearLog()
{
  XrdSysMutexHelper lock(mLogMutex);
  mLog = "";
}

//------------------------------------------------------------------------------
// Write log message to the current in-memory log
//------------------------------------------------------------------------------
void
Fsck::Log(const char* msg, ...) const
{
  static time_t current_time;
  static struct timeval tv;
  static struct timezone tz;
  static struct tm* tm;
  va_list args;
  va_start(args, msg);
  char buffer[16384];
  char* ptr;
  time(&current_time);
  gettimeofday(&tv, &tz);
  tm = localtime(&current_time);
  sprintf(buffer, "%02d%02d%02d %02d:%02d:%02d %lu.%06lu ", tm->tm_year - 100,
          tm->tm_mon + 1, tm->tm_mday, tm->tm_hour, tm->tm_min, tm->tm_sec, current_time,
          (unsigned long) tv.tv_usec);
  ptr = buffer + strlen(buffer);
  vsprintf(ptr, msg, args);
  XrdSysMutexHelper lock(mLogMutex);
  mLog += buffer;
  mLog += "\n";
  va_end(args);
}

//------------------------------------------------------------------------------
// Reset all collected errors in the error map
//------------------------------------------------------------------------------
void
Fsck::ResetErrorMaps()
{
  eos::common::RWMutexWriteLock wr_lock(mErrMutex);
  eFsMap.clear();
  eFsUnavail.clear();
  eFsDark.clear();
  eTimeStamp = time(NULL);
}

//------------------------------------------------------------------------------
// Account for offline replicas due to unavailable file systems
//------------------------------------------------------------------------------
void
Fsck::AccountOfflineReplicas()
{
  // Grab all files which are damaged because filesystems are down
  eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);

  for (auto it = FsView::gFsView.mIdView.cbegin();
       it != FsView::gFsView.mIdView.cend(); ++it) {
    // protect against illegal 0 filesystem pointer
    if (!it->second) {
      eos_crit("msg=\"found illegal pointer in filesystem view\" fsid=%lu",
               it->first);
      continue;
    }

    eos::common::FileSystem::fsid_t fsid = it->first;
    eos::common::ActiveStatus fsactive = it->second->GetActiveStatus();
    eos::common::ConfigStatus fsconfig = it->second->GetConfigStatus();
    eos::common::BootStatus fsstatus = it->second->GetStatus();

    if ((fsstatus == eos::common::BootStatus::kBooted) &&
        (fsconfig >= eos::common::ConfigStatus::kDrain) &&
        (fsactive == eos::common::ActiveStatus::kOnline)) {
      // Healthy, don't need to do anything
      continue;
    } else {
      // Not ok and contributes to replica offline errors
      try {
        eos::Prefetcher::prefetchFilesystemFileListAndWait(gOFS->eosView,
            gOFS->eosFsView, fsid);
        eos::common::RWMutexWriteLock wr_lock(mErrMutex);
        // Only need the view lock if we're in-memory
        eos::common::RWMutexReadLock nslock;

        if (gOFS->eosView->inMemory()) {
          nslock.Grab(gOFS->eosViewRWMutex);
        }

        for (auto it_fid = gOFS->eosFsView->getFileList(fsid);
             (it_fid && it_fid->valid()); it_fid->next()) {
          eFsUnavail[fsid]++;
          eFsMap["rep_offline"][fsid].insert(it_fid->getElement());
        }
      } catch (eos::MDException& e) {
        errno = e.getErrno();
        eos_debug("caught exception %d %s\n", e.getErrno(),
                  e.getMessage().str().c_str());
      }
    }
  }
}

//------------------------------------------------------------------------------
// Account for file with no replicas
//------------------------------------------------------------------------------
void
Fsck::AccountNoReplicaFiles()
{
  // Grab all files which have no replicas at all
  try {
    eos::common::RWMutexWriteLock wr_lock(mErrMutex);
    eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex);
    // it_fid not invalidated when items are added or removed for QDB
    // namespace, safe to release lock after each item.
    bool needLockThroughout = !gOFS->NsInQDB;

    for (auto it_fid = gOFS->eosFsView->getStreamingNoReplicasFileList();
         (it_fid && it_fid->valid()); it_fid->next()) {
      if (!needLockThroughout) {
        ns_rd_lock.Release();
        eos::Prefetcher::prefetchFileMDWithParentsAndWait(gOFS->eosView,
            it_fid->getElement());
        ns_rd_lock.Grab(gOFS->eosViewRWMutex);
      }

      auto fmd = gOFS->eosFileService->getFileMD(it_fid->getElement());
      std::string path = gOFS->eosView->getUri(fmd.get());
      XrdOucString fullpath = path.c_str();

      if (fullpath.beginswith(gOFS->MgmProcPath)) {
        // Don't report eos /proc files
        continue;
      }

      if (fmd && (!fmd->isLink())) {
        eFsMap["zero_replica"][0].insert(it_fid->getElement());
      }

      if (!needLockThroughout) {
        ns_rd_lock.Release();
        ns_rd_lock.Grab(gOFS->eosViewRWMutex);
      }
    }
  } catch (eos::MDException& e) {
    errno = e.getErrno();
    eos_debug("msg=\"caught exception\" errno=d%d msg=\"%s\"", e.getErrno(),
              e.getMessage().str().c_str());
  }
}

//------------------------------------------------------------------------------
// Print offline replicas summary
//------------------------------------------------------------------------------
void
Fsck::PrintOfflineReplicas() const
{
  eos::common::RWMutexReadLock rd_lock(mErrMutex);

  // Loop over unavailable filesystems
  for (auto ua_it = eFsUnavail.cbegin(); ua_it != eFsUnavail.cend();
       ++ua_it) {
    std::string host = "not configured";
    eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
    FileSystem* fs = FsView::gFsView.mIdView.lookupByID(ua_it->first);

    if (fs) {
      host = fs->GetString("hostport");
    }

    Log("host=%s fsid=%lu replica_offline=%llu", host.c_str(),
        ua_it->first, ua_it->second);
  }
}

//------------------------------------------------------------------------------
// Account for offline files or files that require replica adjustments
// i.e. file_offline and adjust_replica
//------------------------------------------------------------------------------
void
Fsck::AccountOfflineFiles()
{
  using eos::common::LayoutId;
  // Loop over all replica_offline and layout error files to assemble a
  // file offline list
  std::set <eos::common::FileId::fileid_t> fid2check;
  {
    eos::common::RWMutexReadLock rd_lock(mErrMutex);
    auto it_offline = eFsMap.find("rep_offline");

    if (it_offline != eFsMap.end()) {
      for (const auto& elem : it_offline->second) {
        fid2check.insert(elem.second.begin(), elem.second.end());
      }
    }

    auto it_diff_n = eFsMap.find("rep_diff_n");

    if (it_diff_n != eFsMap.end()) {
      for (const auto& elem : it_diff_n->second) {
        fid2check.insert(elem.second.begin(), elem.second.end());
      }
    }
  }

  for (auto it = fid2check.begin(); it != fid2check.end(); ++it) {
    std::shared_ptr<eos::IFileMD> fmd;
    eos::IFileMD::LocationVector loc_vect;
    eos::IFileMD::layoutId_t lid {0ul};
    size_t nlocations {0};

    try { // Check if locations are online
      eos::Prefetcher::prefetchFileMDAndWait(gOFS->eosView, *it);
      eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex);
      fmd = gOFS->eosFileService->getFileMD(*it);
      lid = fmd->getLayoutId();
      nlocations = fmd->getNumLocation();
      loc_vect = fmd->getLocations();
    } catch (eos::MDException& e) {
      continue;
    }

    size_t offlinelocations = 0;
    eos::common::RWMutexWriteLock wr_lock(mErrMutex);
    eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);

    for (const auto& loc : loc_vect) {
      if (loc) {
        FileSystem* fs = FsView::gFsView.mIdView.lookupByID(loc);

        if (fs) {
          eos::common::BootStatus bootstatus = fs->GetStatus(true);
          eos::common::ConfigStatus configstatus = fs->GetConfigStatus();
          bool conda = (fs->GetActiveStatus(true) == eos::common::ActiveStatus::kOffline);
          bool condb = (bootstatus != eos::common::BootStatus::kBooted);
          bool condc = (configstatus == eos::common::ConfigStatus::kDrainDead);

          if (conda || condb || condc) {
            ++offlinelocations;
          }
        }
      }
    }

    unsigned long layout_type = LayoutId::GetLayoutType(lid);

    if (layout_type == LayoutId::kReplica) {
      if (offlinelocations == nlocations) {
        eFsMap["file_offline"][0].insert(*it);
      }
    } else if (layout_type >= LayoutId::kArchive) {
      // Proper condition for RAIN layout
      if (offlinelocations > LayoutId::GetRedundancyStripeNumber(lid)) {
        eFsMap["file_offline"][0].insert(*it);
      }
    }

    if (offlinelocations && (offlinelocations != nlocations)) {
      eFsMap["adjust_replica"][0].insert(*it);
    }
  }
}

//------------------------------------------------------------------------------
// Print summary of the different type of errors collected so far and their
// corresponding counters
//------------------------------------------------------------------------------
void
Fsck::PrintErrorsSummary() const
{
  eos::common::RWMutexReadLock rd_lock(mErrMutex);

  for (const auto& elem_type : eFsMap) {
    uint64_t count {0ull};

    for (const auto& elem_errs : elem_type.second) {
      count += elem_errs.second.size();
    }

    Log("%-30s : %llu", elem_type.first.c_str(), count);
  }
}

//------------------------------------------------------------------------------
// Account for "dark" file entries i.e. file system ids which have file
// entries in the namespace view but have no configured file system in the
// FsView.
//------------------------------------------------------------------------------
void
Fsck::AccountDarkFiles()
{
  eos::common::RWMutexWriteLock wr_lock(mErrMutex);
  eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
  eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex);

  for (auto it = gOFS->eosFsView->getFileSystemIterator();
       it->valid(); it->next()) {
    IFileMD::location_t nfsid = it->getElement();

    try {
      // @todo(gbitzes): Urgent fix for QDB namespace needed.. This loop
      // will need to load all filesystems in memory, just to get a couple
      // of silly counters.
      uint64_t num_files = gOFS->eosFsView->getNumFilesOnFs(nfsid);

      if (num_files) {
        // Check if this exists in the gFsView
        if (!FsView::gFsView.mIdView.exists(nfsid)) {
          eFsDark[nfsid] += num_files;
          Log("shadow fsid=%lu shadow_entries=%llu ", nfsid, num_files);
        }
      }
    } catch (const eos::MDException& e) {
      // ignore
    }
  }
}

EOSMGMNAMESPACE_END
