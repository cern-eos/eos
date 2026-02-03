// ----------------------------------------------------------------------
// File: Communicator.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

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

#include "fst/storage/Storage.hh"
#include "fst/XrdFstOfs.hh"
#include "fst/Config.hh"
#include "fst/storage/FileSystem.hh"
#include "common/SymKeys.hh"
#include "common/Assert.hh"
#include "common/Constants.hh"
#include "common/StringTokenizer.hh"
#include "mq/SharedHashWrapper.hh"
#include "qclient/structures/QScanner.hh"
#include "qclient/shared/SharedHashSubscription.hh"

EOSFSTNAMESPACE_BEGIN

// Set of keys updates to be tracked at the node level
std::set<std::string> Storage::sNodeUpdateKeys {
  "stat.refresh_fs", "manager", "symkey", "publish.interval",
  "debug.level", "error.simulation", "stripexs", "stat.scaler.xyz"};

//------------------------------------------------------------------------------
// Get configuration value from global FST config
//------------------------------------------------------------------------------
bool
Storage::GetFstConfigValue(const std::string& key, std::string& value) const
{
  common::SharedHashLocator locator =
    gConfig.getNodeHashLocator("getConfigValue", false);

  if (locator.empty()) {
    return false;
  }

  mq::SharedHashWrapper hash(gOFS.mMessagingRealm.get(), locator, true, false);
  return hash.get(key, value);
}

//------------------------------------------------------------------------------
// Get configuration value from global FST config
//------------------------------------------------------------------------------
bool
Storage::GetFstConfigValue(const std::string& key,
                           unsigned long long& value) const
{
  std::string strVal;

  if (!GetFstConfigValue(key, strVal)) {
    return false;
  }

  value = atoi(strVal.c_str());
  return true;
}

//------------------------------------------------------------------------------
// Unregister file system given a queue path
//------------------------------------------------------------------------------
void
Storage::UnregisterFileSystem(const std::string& queuepath)
{
  while (mFsMutex.TryLockWrite() != 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  auto it = std::find_if(mFsVect.begin(), mFsVect.end(), [&](FileSystem * fs) {
    return (fs->GetQueuePath() == queuepath);
  });

  if (it == mFsVect.end()) {
    eos_static_warning("msg=\"file system is already removed\" qpath=%s",
                       queuepath.c_str());
    mFsMutex.UnLockWrite();
    return;
  }

  auto fs = *it;
  mFsVect.erase(it);
  auto it_map = std::find_if(mFsMap.begin(),
  mFsMap.end(), [&](const auto & pair) {
    return (pair.second->GetQueuePath() == queuepath);
  });

  if (it_map == mFsMap.end()) {
    eos_static_warning("msg=\"file system missing from map\" qpath=%s",
                       queuepath.c_str());
  } else {
    mFsMap.erase(it_map);
  }

  mFsMutex.UnLockWrite();
  eos_static_info("msg=\"deleting file system\" qpath=%s",
                  fs->GetQueuePath().c_str());
  delete fs;
}

//------------------------------------------------------------------------------
// Register file system
//------------------------------------------------------------------------------
Storage::FsRegisterStatus
Storage::RegisterFileSystem(const std::string& queuepath)
{
  while (mFsMutex.TryLockWrite() != 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  auto it = std::find_if(mFsVect.begin(), mFsVect.end(),
  [&](FileSystem * fs) {
    return (fs->GetQueuePath() == queuepath);
  });

  if (it != mFsVect.end()) {
    eos_static_warning("msg=\"file system is already registered\" qpath=%s",
                       queuepath.c_str());
    mFsMutex.UnLockWrite();
    return FsRegisterStatus::kNoAction;
  }

  common::FileSystemLocator locator;

  if (!common::FileSystemLocator::fromQueuePath(queuepath, locator)) {
    eos_static_crit("msg=\"failed to parse locator\" qpath=%s",
                    queuepath.c_str());
    mFsMutex.UnLockWrite();
    return FsRegisterStatus::kNoAction;
  }

  fst::FileSystem* fs = new fst::FileSystem(locator, gOFS.mMessagingRealm.get());
  fs->SetLocalId();
  fs->SetLocalUuid();
  mFsVect.push_back(fs);
  eos_static_info("msg=\"attempt file system registration\" qpath=\"%s\" "
                  "fsid=%u uuid=\"%s\"", queuepath.c_str(),
                  fs->GetLocalId(), fs->GetLocalUuid().c_str());

  if ((fs->GetLocalId() == 0ul) || fs->GetLocalUuid().empty()) {
    eos_static_info("msg=\"partially register file system\" qpath=\"%s\"",
                    queuepath.c_str());
    mFsMutex.UnLockWrite();
    return FsRegisterStatus::kPartial;
  }

  if (mFsMap.find(fs->GetLocalId()) != mFsMap.end()) {
    eos_static_crit("msg=\"trying to register an already existing file system\" "
                    "fsid=%u uuid=\"%s\"", fs->GetLocalId(),
                    fs->GetLocalUuid().c_str());
    std::abort();
  }

  mFsMap[fs->GetLocalId()] = fs;

  if (gConfig.autoBoot &&
      (fs->GetConfigStatus() > eos::common::ConfigStatus::kOff)) {
    RunBootThread(fs, "");
  }

  mFsMutex.UnLockWrite();
  return FsRegisterStatus::kRegistered;
}

//------------------------------------------------------------------------------
// Manage scaler for IoAggregateMap
//------------------------------------------------------------------------------
void Storage::ScalerCmd(const std::string &data){
	google::protobuf::util::JsonParseOptions option;
	Shaping::Scaler scaler;

	auto absel = google::protobuf::json::JsonStringToMessage(data, &scaler, option);
	if (!absel.ok()){
	  eos_static_err("msg=\"Failed to convert scaler value to variable\"");
	} else{
		for (auto it : mScaler.windows()){
			if (std::find(scaler.windows().begin(), scaler.windows().end(), it) == scaler.windows().end())
				gOFS.ioMap.rm(it);
		}
		for (auto it : scaler.windows()){
			if (std::find(mScaler.windows().begin(), mScaler.windows().end(), it) == mScaler.windows().end())
				gOFS.ioMap.addWindow(it);
		}
		mScaler = scaler;
	}
}

//------------------------------------------------------------------------------
// Process incoming configuration change
//------------------------------------------------------------------------------
void
Storage::ProcessFstConfigChange(const std::string& key,
                                const std::string& value)
{
  static std::string last_refresh_ts;
  eos_static_debug("msg=\"FST node configuration change\" key=\"%s\" "
                  "value=\"%s\"", key.c_str(), value.c_str());

  // if key not in list, warning and return
  if (sNodeUpdateKeys.find(key) == sNodeUpdateKeys.end()) {
    eos_static_warning("msg=\"unhandled FST node configuration change due to invalid key\" "
                       "key=\"%s\" value=\"%s\"", key.c_str(), value.c_str());
    return;
  }

  if (key == "stat.refresh_fs") {
    // Refresh the list of filesystems registered from QDB shared hashes
    if (last_refresh_ts != value) {
      eos_static_info("msg=\"refreshing file system list\" "
                      "last_refresh_ts=\"%s\" new_refresh_ts=\"%s\"",
                      last_refresh_ts.c_str(), value.c_str());
      last_refresh_ts = value;
      SignalRegisterThread();
    }
  } else if (key == "manager") {
    eos_static_info("msg=\"manager changed\" new_manager=\"%s\"",
                    value.c_str());
    XrdSysMutexHelper lock(gConfig.Mutex);
    gConfig.Manager = value.c_str();
  } else if (key == "symkey") {
    eos_static_info("msg=\"symkey changed\"");
    eos::common::gSymKeyStore.SetKey64(value.c_str(), 0);
  } else if (key == "publish.interval") {
    eos_static_info("msg=\"publish interval changed\" new_interval=\"%s\"", value.c_str());
    XrdSysMutexHelper lock(gConfig.Mutex);
    try {
      gConfig.PublishInterval = std::stoi(value);
    } catch (const std::exception& e) {
      eos_static_warning("msg=\"invalid PublishInterval value\" value=\"%s\" error=\"%s\"",
                         value.c_str(), e.what());
    }
  } else if (key == "debug.level") {
    const std::string& debugLevel = value;
    eos_static_info("msg=\"debug level changed\" new_level=\"%s\"", debugLevel.c_str());
    eos::common::Logging& g_logging = eos::common::Logging::GetInstance();
    if (const int debugValue = g_logging.GetPriorityByString(debugLevel.c_str()); debugValue < 0) {
      eos_static_err("msg=\"unknown debug level\" level=\"%s\"",
                     debugLevel.c_str());
    } else {
      g_logging.SetLogPriority(debugValue);
    }
  } else if (key == "error.simulation") {
    eos_static_info("msg=\"error simulation changed\" new_value=\"%s\"", value.c_str());
    gOFS.SetSimulationError(value);
  } else if (key == "stripexs") {
    // value can be "on" or "off"
    mComputeStripeChecksum = (value == "on");
    eos_static_info("msg=\"stripe checksum calculation changed\" new_value=\"%s\" mComputeStripeChecksum=%s",
                    value.c_str(), mComputeStripeChecksum ? "enabled" : "disabled");
  } else if (key == "stat.scaler.xyz"){
    eos_static_debug("msg=\"stat.scaler.xyz changed\" new_value=\"%s\"", value.c_str());
	  ScalerCmd(value);
  } else {
    eos_static_err("msg=\"unhandled FST node configuration change because "
                   "of missing implementation\" key=\"%s\" value=\"%s\". "
                   "This should never happen!",
                   key.c_str(), value.c_str());
  }
}

//------------------------------------------------------------------------------
// Process incoming filesystem-level configuration change
//------------------------------------------------------------------------------
void
Storage::ProcessFsConfigChange(fst::FileSystem* fs, const std::string& key,
                               const std::string& value)
{
  if ((key == "id") || (key == "uuid") || (key == "bootsenttime")) {
    RunBootThread(fs, key);
  } else {
    mFsUpdQueue.emplace(fs->GetLocalId(), key, value);
  }
}

//------------------------------------------------------------------------------
// Process incoming filesystem-level configuration change
//------------------------------------------------------------------------------
void
Storage::ProcessFsConfigChange(const std::string& queuepath,
                               const std::string& key)
{
  eos::common::RWMutexReadLock fs_rd_lock(mFsMutex);
  auto it = std::find_if(mFsMap.begin(), mFsMap.end(), [&](const auto & pair) {
    return (pair.second->GetQueuePath() == queuepath);
  });

  if (it == mFsMap.end()) {
    // If file system does not exist in the map and this an "id" info then
    // it could be that we have a partially registered file system
    if ((key == "id") || (key == "uuid")) {
      // Switch to a write lock as we might add the new fs to the map
      fs_rd_lock.Release();

      // Mutex write lock non-blocking
      while (mFsMutex.TryLockWrite() != 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }

      auto itv = std::find_if(mFsVect.begin(), mFsVect.end(),
      [&](fst::FileSystem * fs) {
        return (fs->GetQueuePath() == queuepath);
      });

      if (itv == mFsVect.end()) {
        eos_static_err("msg=\"no file system for id modification\" "
                       "qpath=\"%s\" key=\"%s\"", queuepath.c_str(),
                       key.c_str());
        mFsMutex.UnLockWrite();
        return;
      }

      fst::FileSystem* fs = *itv;
      fs->SetLocalId();
      fs->SetLocalUuid();
      eos_static_info("msg=\"attempt file system registration\" qpath=\"%s\" "
                      "fsid=%u uuid=\"%s\"", queuepath.c_str(), fs->GetLocalId(),
                      fs->GetLocalUuid().c_str());

      if ((fs->GetLocalId() == 0ul) || fs->GetLocalUuid().empty()) {
        eos_static_info("msg=\"defer file system registration\" qpath=\"%s\"",
                        queuepath.c_str());
        mFsMutex.UnLockWrite();
        return;
      }

      eos::common::FileSystem::fsid_t fsid = fs->GetLocalId();
      it = mFsMap.emplace(fsid, fs).first;
      eos_static_info("msg=\"fully register file system\" qpath=%s fsid=%u "
                      "uuid=\"%s\"", queuepath.c_str(), fs->GetLocalId(),
                      fs->GetLocalUuid().c_str());
      // Switch back to read lock and update the iterator
      mFsMutex.UnLockWrite();
      fs_rd_lock.Grab(mFsMutex);
      it = mFsMap.find(fsid);
    } else {
      eos_static_err("msg=\"no file system for modification\" qpath=\"%s\" "
                     "key=\"%s\"", queuepath.c_str(), key.c_str());
      return;
    }
  }

  eos_static_info("msg=\"process modification\" qpath=\"%s\" key=\"%s\"",
                  queuepath.c_str(), key.c_str());
  fst::FileSystem* fs = it->second;
  mq::SharedHashWrapper hash(gOFS.mMessagingRealm.get(), fs->getHashLocator());
  std::string value;

  if (!hash.get(key, value)) {
    eos_static_err("msg=\"no such key in hash\" qpath=\"%s\" key=\"%s\"",
                   queuepath.c_str(), key.c_str());
    return;
  }

  return ProcessFsConfigChange(fs, key, value);
}

//------------------------------------------------------------------------------
// Extract filesystem path from QDB hash key - helper function
//------------------------------------------------------------------------------
static std::string ExtractFsPath(const std::string& key)
{
  std::vector<std::string> parts =
    common::StringTokenizer::split<std::vector<std::string>>(key, '|');
  return parts[parts.size() - 1];
}

//------------------------------------------------------------------------------
// Handle FS configuration updates in a separate thread to avoid deadlocks
// in the QClient callback mechanism.
//------------------------------------------------------------------------------
void
Storage::FsConfigUpdate(ThreadAssistant& assistant) noexcept
{
  eos_static_info("%s", "msg=\"starting fs config update thread\"");
  FsCfgUpdate upd;

  while (!assistant.terminationRequested()) {
    mFsUpdQueue.wait_pop(upd);

    // If sentinel object then exit
    if ((upd.fsid == 0) &&
        (upd.key == "ACTION") &&
        (upd.value == "EXIT")) {
      eos_static_notice("%s", "msg=\"fs config update thread got a "
                        "sentinel object exiting\"");
      break;
    }

    if ((upd.key == eos::common::SCAN_IO_RATE_NAME) ||
        (upd.key == eos::common::SCAN_ENTRY_INTERVAL_NAME) ||
        (upd.key == eos::common::SCAN_RAIN_ENTRY_INTERVAL_NAME) ||
        (upd.key == eos::common::SCAN_DISK_INTERVAL_NAME) ||
        (upd.key == eos::common::SCAN_NS_INTERVAL_NAME) ||
        (upd.key == eos::common::SCAN_NS_RATE_NAME) ||
        (upd.key == eos::common::SCAN_ALTXS_INTERVAL_NAME) ||
        (upd.key == eos::common::ALTXS_SYNC) ||
        (upd.key == eos::common::ALTXS_SYNC_INTERVAL)) {
      try {
        long long val = std::stoll(upd.value);

        if (val >= 0) {
          eos::common::RWMutexReadLock fs_rd_lock(mFsMutex);
          auto it = mFsMap.find(upd.fsid);

          if (it != mFsMap.end()) {
            it->second->ConfigScanner(&mFstLoad, upd.key.c_str(), val);
          }
        }
      } catch (...) {
        eos_static_err("msg=\"failed to convert value\" key=\"%s\" val=\"%s\"",
                       upd.key.c_str(), upd.value.c_str());
      }
    }
  }

  eos_static_info("%s", "msg=\"stopped fs config update thread\"");
}

//------------------------------------------------------------------------------
// Update file system list given the QDB shared hash configuration -this
// update is done in a separate thread handling the trigger event otherwise
// we deadlock in the QClient code.
//------------------------------------------------------------------------------
void Storage::UpdateRegisteredFs(ThreadAssistant& assistant) noexcept
{
  eos_static_info("%s", "msg=\"starting register file system thread\"");

  while (!assistant.terminationRequested()) {
    {
      // Reduce scope of mutex to avoid coupling the the SignalRegisterThread
      // which is called from the QClient event loop with other QClient requests
      // like the QScanner listing below - this will lead to a deadlock!!!
      std::unique_lock lock(mMutexRegisterFs);
      mCvRegisterFs.wait(lock, [&] {return mTriggerRegisterFs;});
      eos_static_info("%s", "msg=\"update registered file systems\"");
      mTriggerRegisterFs = false;
    }
    qclient::QScanner scanner(*gOFS.mMessagingRealm->getQSom()->getQClient(),
                              SSTR("eos-hash||fs||" << gConfig.FstHostPort << "||*"));
    std::set<std::string> new_filesystems;

    for (; scanner.valid(); scanner.next()) {
      std::string queuePath = SSTR("/eos/" << gConfig.FstHostPort <<
                                   "/fst" <<  ExtractFsPath(scanner.getValue()));
      new_filesystems.insert(queuePath);
    }

    // Filesystems added?
    std::set<std::string> partial_filesystems;

    for (auto it = new_filesystems.begin(); it != new_filesystems.end(); ++it) {
      if (mLastRoundFilesystems.find(*it) == mLastRoundFilesystems.end()) {
        if (RegisterFileSystem(*it) == FsRegisterStatus::kPartial) {
          partial_filesystems.insert(*it);
        }
      }
    }

    // Filesystems removed?
    for (auto it = mLastRoundFilesystems.begin();
         it != mLastRoundFilesystems.end(); ++it) {
      if (new_filesystems.find(*it) == new_filesystems.end()) {
        eos_static_info("msg=\"unregister file system\" queuepath=\"%s\"",
                        it->c_str());
        UnregisterFileSystem(*it);
      }
    }

    if (!partial_filesystems.empty()) {
      // Reset register trigger flag and remove partial file systems so
      // that we properly register them in them next loop.
      {
        std::unique_lock lock(mMutexRegisterFs);
        mTriggerRegisterFs = true;
      }

      for (const auto& elem : partial_filesystems) {
        UnregisterFileSystem(elem);
        auto it_del  = new_filesystems.find(elem);
        new_filesystems.erase(it_del);
      }

      eos_static_info("%s", "msg=\"re-trigger file system registration "
                      "in 5 seconds\"");
      assistant.wait_for(std::chrono::seconds(5));
    }

    mLastRoundFilesystems = std::move(new_filesystems);
  }

  eos_static_info("%s", "msg=\"stopped register file system thread\"");
}

//------------------------------------------------------------------------------
// FST node update callback - this is triggered whenever the underlying
// qclient::SharedHash corresponding to the node is modified.
//------------------------------------------------------------------------------
void
Storage::NodeUpdateCb(qclient::SharedHashUpdate&& upd)
{
  if (sNodeUpdateKeys.find(upd.key) != sNodeUpdateKeys.end()) {
    ProcessFstConfigChange(upd.key, upd.value);
  }
}

//------------------------------------------------------------------------------
// QdbCommunicator
//------------------------------------------------------------------------------
void
Storage::QdbCommunicator(ThreadAssistant& assistant) noexcept
{
  using namespace std::placeholders;
  eos_static_info("%s", "msg=\"starting QDB communicator thread\"");
  // Process initial FST configuration ... discover instance name
  std::string instance_name;

  for (size_t i = 0; i < 10; i++) {
    if (gOFS.mMessagingRealm->getInstanceName(instance_name)) {
      break;
    }

    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  if (instance_name.empty()) {
    eos_static_crit("%s", "msg=\"unable to obtain instance name from QDB\"");
    exit(1);
  }

  std::string cfg_queue = SSTR("/config/" << instance_name << "/node/" <<
                               gConfig.FstHostPort);
  gConfig.setFstNodeConfigQueue(cfg_queue);
  // Discover node-specific configuration
  mq::SharedHashWrapper node_hash(gOFS.mMessagingRealm.get(),
                                  gConfig.getNodeHashLocator(),
                                  false, false);
  // Discover MGM name
  std::string mgm_host;

  for (size_t i = 0; i < 10; i++) {
    if (node_hash.get("manager", mgm_host)) {
      break;
    }

    std::this_thread::sleep_for(std::chrono::seconds(5));
  }

  if (mgm_host.empty()) {
    eos_static_crit("%s", "msg=\"unable to obtain manager info for node\"");
    exit(1);
  }

  ProcessFstConfigChange("manager", mgm_host);

  // Discover the rest of the FST node configuration options
  for (const auto& node_key : sNodeUpdateKeys) {
    std::string value;

    if (node_hash.get(node_key, value)) {
      ProcessFstConfigChange(node_key, value);
    }
  }

  // One-off collect all configured file systems for this node
  SignalRegisterThread();
  // Attach callback for node configuration updates
  std::unique_ptr<qclient::SharedHashSubscription>
  node_subscription = node_hash.subscribe();
  node_subscription->attachCallback(std::bind(&Storage::NodeUpdateCb,
                                    this, _1));

  // Broadcast FST node hearbeat
  while (!assistant.terminationRequested()) {
    node_hash.set(eos::common::FST_HEARTBEAT_KEY, std::to_string(time(0)));
    assistant.wait_for(std::chrono::seconds(1));
  }

  node_subscription->detachCallback();
  node_subscription.reset(nullptr);
  mq::SharedHashWrapper::deleteHash(gOFS.mMessagingRealm.get(),
                                    gConfig.getNodeHashLocator(), false);
  eos_static_info("%s", "msg=\"stopped QDB communicator thread\"");
}

//------------------------------------------------------------------------------
// Signal the thread responsible with registered file systems
//------------------------------------------------------------------------------
void
Storage::SignalRegisterThread()
{
  {
    std::unique_lock lock(mMutexRegisterFs);
    mTriggerRegisterFs = true;
  }
  mCvRegisterFs.notify_one();
}

EOSFSTNAMESPACE_END
