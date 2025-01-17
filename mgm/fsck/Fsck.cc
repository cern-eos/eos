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
#include "common/Timing.hh"
#include "common/LayoutId.hh"
#include "common/Path.hh"
#include "common/Mapping.hh"
#include "common/StringConversion.hh"
#include "common/StringTokenizer.hh"
#include "common/StringUtils.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Messaging.hh"
#include "mgm/FsView.hh"
#include "namespace/interface/IView.hh"
#include "namespace/interface/IFsView.hh"
#include "namespace/Prefetcher.cc"
#include "qclient/structures/QSet.hh"
#include "json/json.h"

EOSMGMNAMESPACE_BEGIN

const std::string Fsck::sFsckKey {
  "fsck"
};
const std::string Fsck::sCollectKey {"toggle-collect"};
const std::string Fsck::sCollectIntervalKey {"collect-interval-min"};
const std::string Fsck::sRepairKey {"toggle-repair"};
const std::string Fsck::sRepairCategory {"repair-category"};
const std::string Fsck::sBestEffort {"toggle-best-effort"};

using eos::common::FsckErr;
using eos::common::ConvertToFsckErr;
//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
Fsck::Fsck():
  mShowOffline(false), mShowNoReplica(false), mShowDarkFiles(false),
  mStartProcessing(false), mCollectEnabled(false), mRepairEnabled(false),
  mCollectRunning(false), mRepairRunning(false), mDoBestEffort(false),
  mRepairCategory(FsckErr::None),
  mCollectInterval(std::chrono::seconds(30 * 60)),
  eTimeStamp(0),
  mThreadPool(2, mMaxThreadPoolSize, 10, 6, 5, "fsck")
{}

//------------------------------------------------------------------------------
// Stop all fsck related threads and activities
//------------------------------------------------------------------------------
void Fsck::Stop()
{
  mRepairThread.join();
  mCollectorThread.join();
}

//------------------------------------------------------------------------------
// Apply the FSCK configuration stored in the configuration engine
//------------------------------------------------------------------------------
void
Fsck::ApplyFsckConfig()
{
  using eos::common::StringTokenizer;
  // Parse config of the form: key1=val1 key2=val2 etc.
  std::string config = FsView::gFsView.GetGlobalConfig(sFsckKey);
  eos_info("msg=\"apply fsck configuration\" data=\"%s\"", config.c_str());
  std::map<std::string, std::string> kv_map;
  auto pairs = StringTokenizer::split<std::list<std::string>>(config, ' ');

  for (const auto& pair : pairs) {
    auto kv = StringTokenizer::split<std::vector<std::string>>(pair, '=');

    if (kv.empty()) {
      eos_err("msg=\"unknown fsck config data\" data=\"%s\"", config.c_str());
      continue;
    }

    if (kv.size() == 1) {
      kv.emplace_back("");
    }

    kv_map.emplace(kv[0], kv[1]);
  }

  // Apply the configuration to the fsck engine
  std::string msg;

  if (kv_map.count(sCollectKey) && kv_map.count(sCollectIntervalKey)) {
    bool enable_collect = (kv_map[sCollectKey] == "1");

    // Make fsck config enforcement idempotent. Note the value in the Config
    // call is overloaded with the "off" string marking the fact that fsck
    // collection should be disabled.
    if (enable_collect != mCollectEnabled) {
      Config(sCollectKey, (enable_collect ? kv_map[sCollectIntervalKey] : "off"),
             msg);
    }
  }

  if (kv_map.count(sRepairKey)) {
    Config(sRepairKey, kv_map[sRepairKey], msg);
  }

  if (kv_map.count(sBestEffort)) {
    Config(sBestEffort, kv_map[sBestEffort], msg);
  }

  if (kv_map.count(sRepairCategory)) {
    Config(sRepairCategory, kv_map[sRepairCategory], msg);
  }
}

//------------------------------------------------------------------------------
// Store the current running FSCK configuration in the config engine
//------------------------------------------------------------------------------
bool
Fsck::StoreFsckConfig()
{
  using namespace std::chrono;
  uint32_t collect_interval_min = duration_cast<minutes>
                                  (mCollectInterval).count();

  // Make sure the collection internval is at least one minute
  if (collect_interval_min == 0) {
    collect_interval_min = 1;
  }

  std::ostringstream oss;
  oss << sCollectKey << "=" << mCollectEnabled << " "
      << sCollectIntervalKey << "=" << collect_interval_min << " "
      << sRepairKey << "=" << mRepairEnabled << " "
      << sBestEffort << "=" << mDoBestEffort << " "
      << sRepairCategory  << "=" << eos::common::FsckErrToString(mRepairCategory);
  return FsView::gFsView.SetGlobalConfig(sFsckKey, oss.str());
}

//------------------------------------------------------------------------------
// Apply configuration options to the fsck mechanism
//------------------------------------------------------------------------------
bool
Fsck::Config(const std::string& key, const std::string& value, std::string& msg)
{
  // Make sure only one config runs at a time
  static std::mutex mutex;
  std::unique_lock<std::mutex> serialize_lock(mutex);

  if (mQcl == nullptr) {
    if (!gOFS->mQdbCluster.empty()) {
      mQcl = std::make_shared<qclient::QClient>
             (gOFS->mQdbContactDetails.members,
              gOFS->mQdbContactDetails.constructOptions());
    } else {
      msg = "error: no qclient configuration for fsck";
      eos_err("%s", msg.c_str());
      return false;
    }
  }

  if (key == sCollectKey) {
    if (value == "off") {
      mCollectEnabled = false;
    } else {
      mCollectEnabled = !mCollectEnabled;
    }

    if (mCollectEnabled == false) {
      mRepairEnabled = false;

      // Stop the collection and repair
      if (mRepairRunning) {
        mRepairThread.join();
      }

      if (mCollectRunning) {
        mCollectorThread.join();
      }
    } else {
      // If value is present then it represents the collection interval
      if (!value.empty()) {
        try {
          float fval = std::stof(value);

          if (fval == 0) {
            mCollectInterval = std::chrono::seconds(60);
          } else if (fval < 1) {
            mCollectInterval = std::chrono::seconds((long)std::ceil(fval * 60));
          } else {
            mCollectInterval = std::chrono::seconds((long)fval * 60);
          }
        } catch (...) {
          mCollectInterval = std::chrono::seconds(30 * 60);
        }
      }

      if (!mCollectRunning) {
        mCollectorThread.reset(&Fsck::CollectErrs, this);
      }
    }

    if (!StoreFsckConfig()) {
      msg = "error: failed to store fsck configuration changes";
      return false;
    }
  } else if (key == sRepairKey) {
    if (value.empty()) {
      // User triggered repair toggle
      mRepairEnabled = !mRepairRunning;
    } else {
      // Mandatory config coming from the stored configuration
      mRepairEnabled = (value == "1");
    }

    if (mRepairEnabled == false) {
      if (mRepairRunning) {
        mRepairThread.join();
      }
    } else {
      if (!mCollectEnabled) {
        msg = "error: repair can not be enabled without error collection";
        return false;
      }

      mRepairThread.reset(&Fsck::RepairErrs, this);
    }

    if (!StoreFsckConfig()) {
      msg = "error: failed to store fsck configuration changes";
      return false;
    }
  } else if (key == sBestEffort) {
    if (value.empty()) {
      // User triggered best-effort toggle
      mDoBestEffort = !mDoBestEffort;
    } else {
      // Mandatory config coming from the stored configuration
      mDoBestEffort = (value == "1");
    }
  } else if (key == sRepairCategory) {
    if (value == "all") {
      mRepairCategory = FsckErr::None;
    } else {
      const auto tmp_cat = ConvertToFsckErr(value);

      if (tmp_cat == FsckErr::None) {
        msg = "error: unknown repair category";
        return false;
      } else {
        mRepairCategory = tmp_cat;
      }
    }

    if (!StoreFsckConfig()) {
      msg = "error: failed to store fsck configuration changes";
      return false;
    }
  } else if (key == "show-dark-files") {
    mShowDarkFiles = (value == "yes");
  } else if (key == "show-offline") {
    mShowOffline = (value == "yes");
  } else if (key == "show-no-replica") {
    mShowNoReplica = (value == "yes");
  } else if (key == "max-queued-jobs") {
    try {
      mMaxQueuedJobs = std::stoull(value);
    } catch (...) {
      eos_err("msg=\"failed to convert max-queued-jobs\" value=%s",
              value.c_str());
    }
  } else if (key == "max-thread-pool-size") {
    try {
      mMaxThreadPoolSize = std::stoul(value);
      mThreadPool.SetMaxThreads(mMaxThreadPoolSize);
    } catch (...) {
      eos_err("msg=\"failed to convert max-thread-pool-size\" value=%s",
              value.c_str());
    }
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
  mCollectRunning = true;
  eos_info("%s", "msg=\"started fsck collector thread\"");

  if (mQcl == nullptr) {
    eos_err("%s", "msg=\"failed to fsck repair thread without a qclient\"");
    Log("Fsck error collection disabled, missing QuarkDB configuration");
    mCollectRunning = false;
    return;
  }

  gOFS->WaitUntilNamespaceIsBooted();

  // Wait that current MGM becomes a master
  do {
    eos_debug("%s", "msg=\"fsck waiting for master MGM\"");
    assistant.wait_for(std::chrono::seconds(10));
  } while (!assistant.terminationRequested() && !gOFS->mMaster->IsMaster());

  while (!assistant.terminationRequested()) {
    Log("Start error collection");
    Log("Filesystems to check: %lu", FsView::gFsView.GetNumFileSystems());
    decltype(eFsMap) tmp_err_map;
    QueryQdb(tmp_err_map);
    {
      // Swap in the new list of errors and clear the rest
      eos::common::RWMutexWriteLock wr_lock(mErrMutex);
      std::swap(tmp_err_map, eFsMap);
      eFsUnavail.clear();
      eFsDark.clear();
      eTimeStamp = time(NULL);
    }

    // @note accounting the offline replicas/files is a heavy ns op.
    if (mShowOffline) {
      AccountOfflineReplicas();
      PrintOfflineReplicas();
      AccountOfflineFiles();
    }

    // @note no replicas can be a really long list (e.g. PPS)
    if (mShowNoReplica) {
      AccountNoReplicaFiles();
    }

    PrintErrorsSummary();

    // @note the following operation is a heavy ns op.
    if (mShowDarkFiles) {
      AccountDarkFiles();
    }

    Log("Finished error collection");
    Log("Next run in %d minutes",
        std::chrono::duration_cast<std::chrono::minutes>(mCollectInterval).count());
    // Notify the repair thread that it can run now
    mStartProcessing = true;
    PublishLogs();
    // Wait for next FSCK round ...
    assistant.wait_for(mCollectInterval);
  }

  ResetErrorMaps();
  Log("Stop error collection");
  PublishLogs();
  eos_info("%s", "msg=\"stopped fsck collector thread\"");
  mCollectRunning = false;
}

//------------------------------------------------------------------------------
// Method submitting fsck repair jobs to the thread pool
//------------------------------------------------------------------------------
void
Fsck::RepairErrs(ThreadAssistant& assistant) noexcept
{
  mRepairRunning = true;
  eos_info("%s", "msg=\"started fsck repair thread\"");

  if (mQcl == nullptr) {
    eos_err("%s", "msg=\"failed to fsck repair thread without a qclient\"");
    Log("Fsck error repair disabled, missing QuarkDB configuration");
    mRepairRunning = false;
    return;
  }

  gOFS->WaitUntilNamespaceIsBooted();

  while (!assistant.terminationRequested()) {
    // Don't run if we are not a master
    while (!gOFS->mMaster->IsMaster()) {
      assistant.wait_for(std::chrono::seconds(1));

      if (assistant.terminationRequested()) {
        eos_info("%s", "msg=\"stopped fsck repair thread\"");
        mRepairRunning = false;
        return;
      }
    }

    // Wait for the collector thread to signal us
    while (!mStartProcessing) {
      assistant.wait_for(std::chrono::seconds(1));

      if (assistant.terminationRequested()) {
        eos_info("%s", "msg=\"stopped fsck repair thread\"");
        mRepairRunning = false;
        return;
      }
    }

    // Create local struct for errors so that we avoid the iterator invalidation
    // and the long locks
    std::map<std::string,
        std::map<eos::common::FileId::fileid_t,
        std::set <eos::common::FileSystem::fsid_t> > > local_emap;
    {
      eos::common::RWMutexReadLock rd_lock(mErrMutex);
      local_emap.insert(eFsMap.begin(), eFsMap.end());
    }
    uint64_t count = 0ull;
    uint64_t msg_delay = 0;
    static constexpr std::array err_priority{
      eos::common::FSCK_STRIPE_ERR, eos::common::FSCK_BLOCKXS_ERR,   eos::common::FSCK_UNREG_N,
      eos::common::FSCK_REP_DIFF_N, eos::common::FSCK_REP_MISSING_N, eos::common::FSCK_D_MEM_SZ_DIFF,
      eos::common::FSCK_M_CX_DIFF,  eos::common::FSCK_D_MEM_SZ_DIFF, eos::common::FSCK_D_CX_DIFF};

    for (const auto& err_type : err_priority) {
      eos::common::FsckErr err = ConvertToFsckErr(err_type);

      // Repair only targeted categories if this option is set
      if ((mRepairCategory != FsckErr::None) &&
          (mRepairCategory != err)) {
        continue;
      }

      for (const auto& [fid, fsids] : local_emap[err_type]) {
        if (!gOFS->mFidTracker.AddEntry(fid, TrackerType::Fsck)) {
          eos_debug("msg=\"skip already scheduled transfer\" fxid=%08llx", fid);
          continue;
        }

        std::shared_ptr<FsckEntry> job{
          new FsckEntry(fid, fsids, err, mDoBestEffort, mQcl)};
        mThreadPool.PushTask<void>([this, job, fid, err]() {
          if (job->Repair()) {
            eos::common::RWMutexWriteLock wr_lock(mErrMutex);
            mFailedRepair[err].erase(fid);
          } else {
            eos::common::RWMutexWriteLock wr_lock(mErrMutex);
            mFailedRepair[err].insert(fid);
          }
        });

        // Check regularly if we should exit and sleep if queue is full
        while ((mThreadPool.GetQueueSize() > mMaxQueuedJobs) ||
               (++count % 100 == 0)) {
          if (assistant.terminationRequested()) {
            // Wait that there are not more jobs in the queue - this can
            // take a while depending on the queue size
            while (mThreadPool.GetQueueSize()) {
              std::this_thread::sleep_for(std::chrono::seconds(1));

              if (++msg_delay % 5 == 0) {
                eos_info("%s", "msg=\"stopping fsck repair waiting for thread "
                         "pool queue to be consummed\"");
                msg_delay = 0;
              }
            }

            eos_info("%s", "msg=\"stopped fsck repair thread\"");
            mRepairRunning = false;
            return;
          } else {
            if (mThreadPool.GetQueueSize() > mMaxQueuedJobs) {
              assistant.wait_for(std::chrono::seconds(1));
            } else {
              break;
            }
          }
        }
      }
    }

    // Remove orphans from unavailable filesystems
    for (const auto& [fid, fsids] : local_emap[eos::common::FSCK_ORPHANS_N]) {
      for (const auto& fsid : fsids) {
        eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
        FileSystem* fs = FsView::gFsView.mIdView.lookupByID(fsid);

        if (!fs) {
          eos_info("msg=\"dropping orphans for missing filesystem\" "
                   "fxid=%08llx fsid=%d", fid, fsid);
          NotifyFixedErr(fid, fsid, eos::common::FSCK_ORPHANS_N);
        }
      }
    }

    // Force flush any collected notifications
    NotifyFixedErr(0ull, 0ul, "", true);
    mStartProcessing = false;
    eos_info("%s", "msg=\"loop in fsck repair thread\"");
  }

  // Wait that there are no more jobs in the queue
  while (mThreadPool.GetQueueSize()) {
    assistant.wait_for(std::chrono::seconds(1));
  }

  gOFS->mFidTracker.Clear(TrackerType::Fsck);
  eos_info("%s", "msg=\"stopped fsck repair thread\"");
  mRepairRunning = false;
}


void printFids(std::ostringstream& oss,
               const std::set<eos::common::FileId::fileid_t> fids,
               const eos::common::FsckErr err)
{
  for (const auto fid : fids) {
    oss << "fxid=" << eos::common::FileId::Fid2Hex(fid) << " err=" <<
        eos::common::FsckErrToString(err) << "\n";
  }
}

//------------------------------------------------------------------------------
// List failed repaired files
//------------------------------------------------------------------------------
bool Fsck::ListFailed(const std::string& err_type, std::string& out_msg) const
{
  eos::common::FsckErr err = eos::common::FsckErr::None;

  if (!err_type.empty()) {
    err = eos::common::ConvertToFsckErr(err_type);
  }

  std::ostringstream oss;
  eos::common::RWMutexReadLock rd_lock(mErrMutex);

  if (err == eos::common::FsckErr::None) {
    for (const auto& [error, fids] : mFailedRepair) {
      printFids(oss, fids, error);
    }
  } else {
    try {
      auto fids = mFailedRepair.at(err);
      printFids(oss, fids, err);
    } catch (const std::out_of_range& e) {}
  }

  out_msg = oss.str();
}

//------------------------------------------------------------------------------
// Try to repair a given entry
//------------------------------------------------------------------------------
bool
Fsck::RepairEntry(eos::IFileMD::id_t fid,
                  const std::set<eos::common::FileSystem::fsid_t>& fsid_err,
                  const std::string& err_type, bool async, std::string& out_msg)
{
  if (fid == 0ull) {
    eos_err("%s", "msg=\"not such file id 0\"");
    return false;
  }

  eos::common::FsckErr err = eos::common::ConvertToFsckErr(err_type);
  std::shared_ptr<FsckEntry> job {
    new FsckEntry(fid, fsid_err, err, false, mQcl)};

  if (async) {
    out_msg = "msg=\"repair job submitted\"";
    mThreadPool.PushTask<void>([this, job, fid, err]() {
      if (job->Repair()) {
        eos::common::RWMutexWriteLock wr_lock(mErrMutex);
        mFailedRepair[err].erase(fid);
      } else {
        eos::common::RWMutexWriteLock wr_lock(mErrMutex);
        mFailedRepair[err].insert(fid);
      }
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
Fsck::PrintOut(std::string& out, bool monitor_fmt) const
{
  std::ostringstream oss;

  if (monitor_fmt) {
    static time_t current_time;
    time(&current_time);
    oss << "timestamp=" << current_time << std::endl
        << "collection_thread=" << (mCollectEnabled ? "enabled" : "disabled")
        << std::endl
        << "repair_thread=" << (mRepairEnabled ? "enabled" : "disabled")
        << std::endl
        << "repair_category=" <<
        ((mRepairCategory == FsckErr::None) ?
         "all" : eos::common::FsckErrToString(mRepairCategory)) << std::endl
        << "best_effort=" << (mDoBestEffort ? "true" : "false") << std::endl;
  } else {
    oss << "Info: collection thread status -> "
        << (mCollectEnabled ? "enabled" : "disabled") << std::endl
        << "Info: repair thread status     -> "
        << (mRepairEnabled ? "enabled" : "disabled") << std::endl
        << "Info: repair category          -> "
        << ((mRepairCategory == FsckErr::None) ?
            "all" : eos::common::FsckErrToString(mRepairCategory)) << std::endl
        << "Info: best effort              -> "
        << (mDoBestEffort ? "true" : "false") << std::endl;
  }

  {
    XrdSysMutexHelper lock(mLogMutex);
    oss << (monitor_fmt ? mLogMonitor : mLog);
  }

  out = oss.str();
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
        for (const auto& fsid : elem.second) {
          fs_fxid[fsid][elem_map.first].insert(elem.first);
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
        fids.insert(elem.first);
      }

      for (auto it = fids.begin(); it != fids.end(); ++it) {
        json_ids.append(GetFidFormat(*it, display_fxid, display_lfn));
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
        for (const auto& fsid : elem.second) {
          fs_fxid[fsid][elem_map.first].insert(elem.first);
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
      uint64_t count = 0ull;

      for (const auto& elem : elem_map.second) {
        count += elem.second.size();
      }

      oss << " count=" << count;

      if (display_fxid) {
        oss << " fxid=";
      } else if (display_lfn) {
        oss << " lfn=";
      } else {
        oss << std::endl;
        continue;
      }

      std::set<unsigned long long> fids;

      for (const auto& elem : elem_map.second) {
        fids.insert(elem.first);
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
Fsck::PublishLogs()
{
  XrdSysMutexHelper lock(mLogMutex);
  mLog = mTmpLog;
  mTmpLog.clear();
  mLogMonitor = mTmpLogMonitor;
  mTmpLogMonitor.clear();
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
  mTmpLog += buffer;
  mTmpLog += "\n";
  va_end(args);
}

//------------------------------------------------------------------------------
// Write log message to the current in-memory log in monitoring format
//------------------------------------------------------------------------------
void
Fsck::LogMonitor(const char* msg, ...) const
{
  va_list args;
  va_start(args, msg);
  char buffer[16384];
  vsprintf(buffer, msg, args);
  XrdSysMutexHelper lock(mLogMutex);
  mTmpLogMonitor += buffer;
  mTmpLogMonitor += "\n";
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
  eos::common::RWMutexWriteLock wr_lock(mErrMutex);
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
        // Only need the view lock if we're in-memory
        eos::common::RWMutexReadLock nslock;

        if (gOFS->eosView->inMemory()) {
          nslock.Grab(gOFS->eosViewRWMutex);
        }

        for (auto it_fid = gOFS->eosFsView->getFileList(fsid);
             (it_fid && it_fid->valid()); it_fid->next()) {
          eFsUnavail[fsid]++;
          eFsMap["rep_offline"][it_fid->getElement()].insert(fsid);
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

    for (auto it_fid = gOFS->eosFsView->getStreamingNoReplicasFileList();
         (it_fid && it_fid->valid()); it_fid->next()) {
      ns_rd_lock.Release();
      eos::Prefetcher::prefetchFileMDWithParentsAndWait(gOFS->eosView,
          it_fid->getElement());
      ns_rd_lock.Grab(gOFS->eosViewRWMutex);
      auto fmd = gOFS->eosFileService->getFileMD(it_fid->getElement());
      std::string path = gOFS->eosView->getUri(fmd.get());
      XrdOucString fullpath = path.c_str();

      if (fullpath.beginswith(gOFS->MgmProcPath)) {
        // Don't report eos /proc files
        continue;
      }

      if (fmd && (!fmd->isLink())) {
        eFsMap["zero_replica"][it_fid->getElement()].insert(0);
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
        fid2check.insert(elem.first);
      }
    }

    auto it_diff_n = eFsMap.find("rep_diff_n");

    if (it_diff_n != eFsMap.end()) {
      for (const auto& elem : it_diff_n->second) {
        fid2check.insert(elem.first);
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
          bool conda = (fs->GetActiveStatus() == eos::common::ActiveStatus::kOffline);
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
        eFsMap["file_offline"][*it].insert(0);
      }
    } else if (layout_type >= LayoutId::kArchive) {
      // Proper condition for RAIN layout
      if (offlinelocations > LayoutId::GetRedundancyStripeNumber(lid)) {
        eFsMap["file_offline"][*it].insert(0);
      }
    }

    if (offlinelocations && (offlinelocations != nlocations)) {
      eFsMap["adjust_replica"][*it].insert(0);
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
    LogMonitor("%s=%llu", elem_type.first.c_str(), count);
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

//------------------------------------------------------------------------------
// Query QDB for fsck errors
//------------------------------------------------------------------------------
void
Fsck::QueryQdb(ErrMapT& err_map)
{
  static std::set<std::string> known_errs = eos::common::GetKnownFsckErrs();
  qclient::QSet set_errs(*mQcl.get(), "");
  // Helper function to parse fsck info stored in QDB
  auto parse_fsck =
    [](const std::string & data) ->
  std::pair<eos::IFileMD::id_t, eos::common::FileSystem::fsid_t> {
    const size_t pos = data.find(':');

    if ((pos == std::string::npos) || (pos == data.length()))
    {
      eos_static_err("msg=\"failed to parse fsck element\" data=\"%s\"",
                     data.c_str());
      return {0ull, 0ul};
    }

    eos::IFileMD::id_t fid;
    eos::common::FileSystem::fsid_t fsid;

    if (!eos::common::StringToNumeric(data.substr(0, pos), fid) ||
        !eos::common::StringToNumeric(data.substr(pos + 1), fsid))
    {
      eos_static_err("msg=\"failed to convert fsck info\" data=\"%s\"",
                     data.c_str());
      return {0ull, 0ul};
    }

    return {fid, fsid};
  };
  eos_static_info("%s", "msg=\"check for fsck errors\"");

  for (const auto& err_type : known_errs) {
    set_errs.setKey(SSTR("fsck:" << err_type));

    for (auto it = set_errs.getIterator(); it.valid(); it.next()) {
      // Set elements are in the form: fid:fsid
      auto pair_info = parse_fsck(it.getElement());
      err_map[err_type][pair_info.first].insert(pair_info.second);
    }
  }

  return;
}

//------------------------------------------------------------------------------
// Update the backend given the successful outcome of the repair
//------------------------------------------------------------------------------
void
Fsck::NotifyFixedErr(eos::IFileMD::id_t fid,
                     eos::common::FileSystem::fsid_t fsid_err,
                     const std::string& err_type,
                     bool force, uint32_t count_flush)
{
  eos_static_debug("msg=\"fsck notification\" fxid=%08llx fsid=%lu "
                   "err=%s", fid, fsid_err, err_type.c_str());
  static std::mutex mutex;
  static uint64_t num_updates = 0ull;
  static std::map<std::string, std::set<std::string>> updates;
  std::unique_lock<std::mutex> scope_lock(mutex);

  if ((fid || fsid_err) && !err_type.empty() && (err_type != "none")) {
    auto it = updates.find(err_type);

    if (it == updates.end()) {
      auto resp = updates.emplace(err_type, std::set<std::string>());
      it = resp.first;
    }

    const std::string value = SSTR(fid << ':' << fsid_err);
    auto resp = it->second.insert(value);

    if (resp.second == true) {
      ++num_updates;
    }
  }

  // Decide if time based flushing is needed
  using namespace std::chrono;
  static uint32_t s_flush_timeout_sec {60};
  static std::atomic<uint64_t> s_timestamp = duration_cast<seconds>
      (system_clock::now().time_since_epoch()).count();
  bool do_flush = false;
  uint64_t current_timestamp = duration_cast<seconds>
                               (system_clock::now().time_since_epoch()).count();

  if (current_timestamp - s_timestamp > s_flush_timeout_sec) {
    s_timestamp = current_timestamp;
    do_flush = true;
  }

  // Eventually flush the contents to the QDB backend if sentinel fid present
  // or if enough updates accumulated
  if (force || do_flush || (num_updates >= count_flush)) {
    qclient::QSet qset(*mQcl.get(), "");

    for (const auto& elem : updates) {
      qset.setKey(SSTR("fsck:" << elem.first));
      std::list<std::string> values(elem.second.begin(), elem.second.end());

      if (qset.srem(values) != (long long int)values.size()) {
        eos_static_err("msg=\"failed to delete some fsck errors\" err_type=%s",
                       elem.first.c_str());
      }
    }

    num_updates = 0ull;
    updates.clear();
  }
}

//------------------------------------------------------------------------------
// Force clean-up the orphans from QuarkDB
//------------------------------------------------------------------------------
void
Fsck::ForceCleanQdbOrphans() const
{
  static std::string key_orphans = "fsck:orphans_n";

  try {
    (void) mQcl->del(key_orphans);
  } catch (const std::exception& e) {
    eos_static_err("%s", "msg=\"failed while doing qdb orphan clean-up\"");
  }
}

EOSMGMNAMESPACE_END
