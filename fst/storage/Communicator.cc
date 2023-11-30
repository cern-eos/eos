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
  "debug.level", "error.simulation" };

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
  eos::common::RWMutexWriteLock fs_wr_lock(mFsMutex);
  auto it = std::find_if(mFsVect.begin(), mFsVect.end(), [&](FileSystem * fs) {
    return (fs->GetQueuePath() == queuepath);
  });

  if (it == mFsVect.end()) {
    eos_static_warning("msg=\"file system is already removed\" qpath=%s",
                       queuepath.c_str());
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
  eos::common::RWMutexWriteLock fs_wr_lock(mFsMutex);
  auto it = std::find_if(mFsVect.begin(), mFsVect.end(),
  [&](FileSystem * fs) {
    return (fs->GetQueuePath() == queuepath);
  });

  if (it != mFsVect.end()) {
    eos_static_warning("msg=\"file system is already registered\" qpath=%s",
                       queuepath.c_str());
    return FsRegisterStatus::kNoAction;
  }

  common::FileSystemLocator locator;

  if (!common::FileSystemLocator::fromQueuePath(queuepath, locator)) {
    eos_static_crit("msg=\"failed to parse locator\" qpath=%s",
                    queuepath.c_str());
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
    return FsRegisterStatus::kPartial;
  }

  if (mFsMap.find(fs->GetLocalId()) != mFsMap.end()) {
    eos_static_crit("msg=\"trying to register an already existing file system\" "
                    "fsid=%u uuid=\"%s\"", fs->GetLocalId(),
                    fs->GetLocalUuid().c_str());
    std::abort();
  }

  mFsMap[fs->GetLocalId()] = fs;

  if (gOFS.mMessagingRealm->haveQDB()) {
    if (gConfig.autoBoot &&
        (fs->GetConfigStatus() > eos::common::ConfigStatus::kOff)) {
      RunBootThread(fs, "");
    }
  }

  return FsRegisterStatus::kRegistered;
}

//------------------------------------------------------------------------------
// Process incoming configuration change
//------------------------------------------------------------------------------
void
Storage::ProcessFstConfigChange(const std::string& key,
                                const std::string& value)
{
  static std::string last_refresh_ts = "";
  eos_static_info("msg=\"FST node configuration change\" key=\"%s\" "
                  "value=\"%s\"", key.c_str(), value.c_str());

  // Refresh the list of FS'es registered from QDB shared hashes
  if (key == "stat.refresh_fs") {
    if (last_refresh_ts != value) {
      last_refresh_ts = value;
      SignalRegisterThread();
    }

    return;
  }

  if (key == "manager") {
    XrdSysMutexHelper lock(gConfig.Mutex);
    gConfig.Manager = value.c_str();
    return;
  }

  if (key == "symkey") {
    eos::common::gSymKeyStore.SetKey64(value.c_str(), 0);
    return;
  }

  if (key == "publish.interval") {
    eos_static_info("publish.interval=%s", value.c_str());
    XrdSysMutexHelper lock(gConfig.Mutex);
    gConfig.PublishInterval = atoi(value.c_str());
    return;
  }

  if (key == "debug.level") {
    std::string debuglevel = value;
    eos::common::Logging& g_logging = eos::common::Logging::GetInstance();
    int debugval = g_logging.GetPriorityByString(debuglevel.c_str());

    if (debugval < 0) {
      eos_static_err("msg=\"unknown debug level\" level=\"%s\"",
                     debuglevel.c_str());
    } else {
      // we set the shared hash debug for the lowest 'debug' level
      if (debuglevel == "debug") {
        gOFS.ObjectManager.SetDebug(true);
      } else {
        gOFS.ObjectManager.SetDebug(false);
      }

      g_logging.SetLogPriority(debugval);
    }

    return;
  }

  if (key == "error.simulation") {
    gOFS.SetSimulationError(value.c_str());
    return;
  }
}

//------------------------------------------------------------------------------
// Process incoming filesystem-level configuration change
//------------------------------------------------------------------------------
void
Storage::ProcessFsConfigChange(fst::FileSystem* targetFs,
                               const std::string& key, const std::string& value)
{
  if ((key == "id") || (key == "uuid") || (key == "bootsenttime")) {
    RunBootThread(targetFs, key);
  } else {
    if ((key == eos::common::SCAN_IO_RATE_NAME) ||
        (key == eos::common::SCAN_ENTRY_INTERVAL_NAME) ||
        (key == eos::common::SCAN_DISK_INTERVAL_NAME) ||
        (key == eos::common::SCAN_NS_INTERVAL_NAME) ||
        (key == eos::common::SCAN_NS_RATE_NAME)) {
      try {
        long long val = std::stoll(value);

        if (val >= 0) {
          targetFs->ConfigScanner(&mFstLoad, key.c_str(), val);
        }
      } catch (...) {
        eos_static_err("msg=\"failed to convert value\" key=\"%s\" val=\"%s\"",
                       key.c_str(), value.c_str());
      }
    }
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
      eos::common::RWMutexWriteLock fs_wr_lock(mFsMutex);
      auto itv = std::find_if(mFsVect.begin(), mFsVect.end(),
      [&](fst::FileSystem * fs) {
        return (fs->GetQueuePath() == queuepath);
      });

      if (itv == mFsVect.end()) {
        eos_static_err("msg=\"no file system for id modification\" "
                       "qpath=\"%s\" key=\"%s\"", queuepath.c_str(),
                       key.c_str());
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
        return;
      }

      eos::common::FileSystem::fsid_t fsid = fs->GetLocalId();
      it = mFsMap.emplace(fsid, fs).first;
      eos_static_info("msg=\"fully register file system\" qpath=%s fsid=%u "
                      "uuid=\"%s\"", queuepath.c_str(), fs->GetLocalId(),
                      fs->GetLocalUuid().c_str());
      // Switch back to read lock and update the iterator
      fs_wr_lock.Release();
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

  hash.releaseLocks();
  return ProcessFsConfigChange(fs, key, value);
}

//------------------------------------------------------------------------------
// Communicator
//------------------------------------------------------------------------------
void
Storage::Communicator(ThreadAssistant& assistant) noexcept
{
  eos_static_info("%s", "msg=\"starting communicator thread\"");
  std::set<std::string> watch_modification_keys { "id", "uuid", "bootsenttime",
      eos::common::SCAN_IO_RATE_NAME, eos::common::SCAN_ENTRY_INTERVAL_NAME,
      eos::common::SCAN_DISK_INTERVAL_NAME, eos::common::SCAN_NS_INTERVAL_NAME,
      eos::common::SCAN_NS_RATE_NAME, "symkey", "manager", "publish.interval",
      "debug.level", "error.simulation"};
  bool ok = true;

  for (const auto& key : watch_modification_keys) {
    ok &= gOFS.ObjectNotifier.SubscribesToKey("communicator", key,
          XrdMqSharedObjectChangeNotifier::kMqSubjectModification);
  }

  std::string watch_regex = ".*";
  ok &= gOFS.ObjectNotifier.SubscribesToSubjectRegex("communicator", watch_regex,
        XrdMqSharedObjectChangeNotifier::kMqSubjectCreation);
  ok &= gOFS.ObjectNotifier.SubscribesToSubjectRegex("communicator", watch_regex,
        XrdMqSharedObjectChangeNotifier::kMqSubjectDeletion);

  if (!ok) {
    eos_crit("%s", "msg=\"error subscribing to shared object change "
             "notifications\"");
  }

  gOFS.ObjectNotifier.BindCurrentThread("communicator");

  if (!gOFS.ObjectNotifier.StartNotifyCurrentThread()) {
    eos_crit("%s", "msg=\"error starting shared object change notifier\"");
  }

  while (!assistant.terminationRequested()) {
    // Wait for new filesystem definitions
    gOFS.ObjectNotifier.tlSubscriber->mSubjSem.Wait();

    do {
      if (assistant.terminationRequested()) {
        break;
      }

      XrdMqSharedObjectManager::Notification event;
      {
        // Take an event from the queue under lock
        XrdSysMutexHelper lock(gOFS.ObjectNotifier.tlSubscriber->mSubjMtx);

        if (gOFS.ObjectNotifier.tlSubscriber->NotificationSubjects.size() == 0) {
          break;
        } else {
          event = gOFS.ObjectNotifier.tlSubscriber->NotificationSubjects.front();
          gOFS.ObjectNotifier.tlSubscriber->NotificationSubjects.pop_front();
        }
      }
      eos_static_info("msg=\"shared object notification\" type=%i subject=\"%s\"",
                      event.mType, event.mSubject.c_str());
      XrdOucString queue = event.mSubject.c_str();

      if (event.mType == XrdMqSharedObjectManager::kMqSubjectCreation) {
        // Skip fst wildcard queue creation
        if (queue == gConfig.FstQueueWildcard) {
          continue;
        }

        if (!queue.beginswith(gConfig.FstQueue)) {
          // ! endswith seems to be buggy if the comparable suffix is longer than the string !
          if (queue.beginswith("/config/") &&
              (queue.length() > gConfig.FstHostPort.length()) &&
              queue.endswith(gConfig.FstHostPort)) {
            // This is the configuration entry and we should store it to have
            // access to it since its name depends on the instance name and
            // we don't know (yet)
            gConfig.setFstNodeConfigQueue(queue.c_str());
            eos_static_info("msg=\"storing config queue name\" qpath=\"%s\"",
                            queue.c_str());
          } else {
            eos_static_info("msg=\"no action on subject creation\" qpath=\"%s\" "
                            "own_id=\"%s\"", queue.c_str(),
                            gConfig.FstQueue.c_str());
          }

          continue;
        }

        (void) RegisterFileSystem(queue.c_str());
      } else if (event.mType == XrdMqSharedObjectManager::kMqSubjectDeletion) {
        // Skip deletions that don't concern us
        if (queue.beginswith(gConfig.FstQueue) == false) {
          continue;
        } else {
          UnregisterFileSystem(event.mSubject);
        }
      } else if (event.mType == XrdMqSharedObjectManager::kMqSubjectModification) {
        // Handle subject modification, seperate <path> from <key>
        std::string key = queue.c_str();
        int dpos = 0;

        if ((dpos = queue.find(";")) != STR_NPOS) {
          key.erase(0, dpos + 1);
          queue.erase(dpos);
        }

        if (queue == gConfig.getFstNodeConfigQueue("communicator", false)) {
          std::string value;

          if (GetFstConfigValue(key, value)) {
            ProcessFstConfigChange(key, value);
          }
        } else {
          ProcessFsConfigChange(queue.c_str(), key);
        }
      }
    } while (true);
  }
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
// Update file system list given the QDB shared hash configuration -this
// update is done in a separate thread handling the trigger event otherwise
// we deadlock in the QClient code.
//------------------------------------------------------------------------------
void Storage::UpdateRegisteredFs(ThreadAssistant& assistant) noexcept
{
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
      std::this_thread::sleep_for(std::chrono::seconds(5));
    }

    mLastRoundFilesystems = std::move(new_filesystems);
  }
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

  if (!gOFS.mMessagingRealm->haveQDB()) {
    eos_static_info("%s", "msg=\"disable QDB communicator\"");
    return;
  }

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
  auto node_subscription = node_hash.subscribe();
  node_subscription->attachCallback(std::bind(&Storage::NodeUpdateCb,
                                    this, _1));

  // Broadcast FST node hearbeat
  while (!assistant.terminationRequested()) {
    node_hash.set(eos::common::FST_HEARTBEAT_KEY, std::to_string(time(0)));
    assistant.wait_for(std::chrono::seconds(1));
  }
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
