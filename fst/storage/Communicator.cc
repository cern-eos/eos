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
#include "fst/storage/FileSystem.hh"
#include "common/SymKeys.hh"
#include "common/Assert.hh"
#include "common/StringTokenizer.hh"
#include "mq/SharedHashWrapper.hh"
#include "common/Constants.hh"

#include <qclient/structures/QScanner.hh>

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Get configuration value from global FST config
//------------------------------------------------------------------------------
bool
Storage::GetFstConfigValue(const std::string& key, std::string& value) const
{
  common::SharedHashLocator locator =
    Config::gConfig.getNodeHashLocator("getConfigValue", false);

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
void
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
    return;
  }

  common::FileSystemLocator locator;

  if (!common::FileSystemLocator::fromQueuePath(queuepath, locator)) {
    eos_static_crit("msg=\"failed to parse queuepath\" qpath=%s",
                    queuepath.c_str());
    return;
  }

  fst::FileSystem* fs = new fst::FileSystem(locator, gOFS.mMessagingRealm.get());
  fs->SetStatus(eos::common::BootStatus::kDown);
  fs->SetLocalId();
  fs->SetLocalUuid();
  mFsVect.push_back(fs);
  eos_static_info("msg=\"attempt file system registration\" qpath=\"%s\" "
                  "fsid=%u uuid=\"%s\"", queuepath.c_str(),
                  fs->GetLocalId(), fs->GetLocalUuid().c_str());

  if ((fs->GetLocalId() == 0ul) || fs->GetLocalUuid().empty()) {
    eos_static_info("msg=\"partially register file system\" qpath=\"%s\"",
                    queuepath.c_str());
    return;
  }

  if (mFsMap.find(fs->GetLocalId()) != mFsMap.end()) {
    eos_static_crit("msg=\"trying to register an already existing file system\" "
                    "fsid=%u uuid=\"%s\"", fs->GetLocalId(),
                    fs->GetLocalUuid().c_str());
    std::abort();
  }

  mFsMap[fs->GetLocalId()] = fs;

  if (gOFS.mMessagingRealm->haveQDB()) {
    if (eos::fst::Config::gConfig.autoBoot &&
        (fs->GetStatus() <= eos::common::BootStatus::kDown) &&
        (fs->GetConfigStatus() > eos::common::ConfigStatus::kOff)) {
      RunBootThread(fs);
    }
  }
}

//------------------------------------------------------------------------------
// Process incoming configuration change
//------------------------------------------------------------------------------
void
Storage::ProcessFstConfigChange(const std::string& key,
                                const std::string& value)
{
  eos_static_info("FST configuration change - key=%s, value=%s", key.c_str(),
                  value.c_str());

  if (key == "symkey") {
    eos_static_info("symkey=%s", value.c_str());
    eos::common::gSymKeyStore.SetKey64(value.c_str(), 0);
    return;
  }

  if (key == "manager") {
    eos_static_info("manager=%s", value.c_str());
    XrdSysMutexHelper lock(Config::gConfig.Mutex);
    Config::gConfig.Manager = value.c_str();
    return;
  }

  if (key == "publish.interval") {
    eos_static_info("publish.interval=%s", value.c_str());
    XrdSysMutexHelper lock(Config::gConfig.Mutex);
    Config::gConfig.PublishInterval = atoi(value.c_str());
    return;
  }

  if (key == "debug.level") {
    std::string debuglevel = value;
    eos::common::Logging& g_logging = eos::common::Logging::GetInstance();
    int debugval = g_logging.GetPriorityByString(debuglevel.c_str());

    if (debugval < 0) {
      eos_static_err("debug level %s is not known!", debuglevel.c_str());
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

  // creation/deletion of gateway transfer queue
  if (key == "txgw") {
    eos_static_info("txgw=%s", value.c_str());

    if (value == "off") {
      // just stop the multiplexer
      mGwMultiplexer.Stop();
      eos_static_info("Stopping transfer multiplexer");
    }

    if (value == "on") {
      mGwMultiplexer.Run();
      eos_static_info("Starting transfer multiplexer");
    }

    return;
  }

  if (key == "gw.rate") {
    // modify the rate settings of the gw multiplexer
    std::string rate = value;
    eos_static_info("cmd=set gw.rate=%s", rate.c_str());
    mGwMultiplexer.SetBandwidth(atoi(rate.c_str()));
    return;
  }

  if (key == "gw.ntx") {
    // modify the parallel transfer settings of the gw multiplexer
    std::string ntx = value;
    eos_static_info("cmd=set gw.ntx=%s", ntx.c_str());
    mGwMultiplexer.SetSlots(atoi(ntx.c_str()));
    return;
  }

  if (key == "error.simulation") {
    eos_static_info("cmd=set error.simulation=%s", value.c_str());
    gOFS.SetSimulationError(value.c_str());
    return;
  }
}

//------------------------------------------------------------------------------
// Process incoming FST-level configuration change
//------------------------------------------------------------------------------
void
Storage::ProcessFstConfigChange(const std::string& key)
{
  std::string value;

  if (!GetFstConfigValue(key.c_str(), value)) {
    return;
  }

  ProcessFstConfigChange(key, value);
}

//------------------------------------------------------------------------------
// Process incoming filesystem-level configuration change
//
// Requires mFsMutex to be write-locked.
//------------------------------------------------------------------------------
void
Storage::ProcessFsConfigChange(fst::FileSystem* targetFs,
                               const std::string& queue,
                               const std::string& key, const std::string& value)
{
  if ((key == "id") || (key == "uuid")) {
    // Check if we are autobooting
    if (eos::fst::Config::gConfig.autoBoot &&
        (targetFs->GetStatus() <= eos::common::BootStatus::kDown) &&
        (targetFs->GetConfigStatus() > eos::common::ConfigStatus::kOff)) {
      RunBootThread(targetFs);
    }
  } else if (key == "bootsenttime") {
    // Request to (re-)boot a filesystem
    if (targetFs->GetInternalBootStatus() == eos::common::BootStatus::kBooted) {
      if (targetFs->GetLongLong("bootcheck")) {
        eos_static_info("queue=%s status=%d check=%lld msg='boot enforced'",
                        queue.c_str(), targetFs->GetStatus(),
                        targetFs->GetLongLong("bootcheck"));
        RunBootThread(targetFs);
      } else {
        eos_static_info("queue=%s status=%d check=%lld msg='skip boot - we are already booted'",
                        queue.c_str(), targetFs->GetStatus(),
                        targetFs->GetLongLong("bootcheck"));
        targetFs->SetStatus(eos::common::BootStatus::kBooted);
      }
    } else {
      eos_static_info("queue=%s status=%d check=%lld msg='booting - we are not booted yet'",
                      queue.c_str(), targetFs->GetStatus(),
                      targetFs->GetLongLong("bootcheck"));
      // start a boot thread;
      RunBootThread(targetFs);
    }
  } else {
    if ((key == eos::common::SCAN_IO_RATE_NAME) ||
        (key == eos::common::SCAN_ENTRY_INTERVAL_NAME) ||
        (key == eos::common::SCAN_DISK_INTERVAL_NAME) ||
        (key == eos::common::SCAN_NS_INTERVAL_NAME) ||
        (key == eos::common::SCAN_NS_RATE_NAME)) {
      long long value = targetFs->GetLongLong(key.c_str());

      if (value >= 0) {
        targetFs->ConfigScanner(&mFstLoad, key.c_str(), value);
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
  return ProcessFsConfigChange(fs, queuepath, key, value);
}

//------------------------------------------------------------------------------
// Communicator
//------------------------------------------------------------------------------
void
Storage::Communicator(ThreadAssistant& assistant)
{
  eos_static_info("%s", "msg=\"starting communicator thread\"");
  std::set<std::string> watch_modification_keys { "id", "uuid", "bootsenttime",
      eos::common::SCAN_IO_RATE_NAME, eos::common::SCAN_ENTRY_INTERVAL_NAME,
      eos::common::SCAN_DISK_INTERVAL_NAME, eos::common::SCAN_NS_INTERVAL_NAME,
      eos::common::SCAN_NS_RATE_NAME, "symkey", "manager", "publish.interval",
      "debug.level", "txgw", "gw.rate", "gw.ntx", "error.simulation"};
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
        // Skip fst wildcard queue creation and txqueue
        if ((queue == Config::gConfig.FstQueueWildcard) ||
            (queue.find("/txqueue/") != STR_NPOS)) {
          continue;
        }

        if (!queue.beginswith(Config::gConfig.FstQueue)) {
          // ! endswith seems to be buggy if the comparable suffix is longer than the string !
          if (queue.beginswith("/config/") &&
              (queue.length() > Config::gConfig.FstHostPort.length()) &&
              queue.endswith(Config::gConfig.FstHostPort)) {
            // This is the configuration entry and we should store it to have
            // access to it since it's name depends on the instance name and
            // we don't know (yet)
            Config::gConfig.setFstNodeConfigQueue(queue.c_str());
            eos_static_info("msg=\"storing config queue name\" qpath=\"%s\"",
                            queue.c_str());
          } else {
            eos_static_info("msg=\"no action on subject creation\" qpath=\"%s\" "
                            "own_id=\"%s\"", queue.c_str(),
                            Config::gConfig.FstQueue.c_str());
          }

          continue;
        }

        RegisterFileSystem(queue.c_str());
      } else if (event.mType == XrdMqSharedObjectManager::kMqSubjectDeletion) {
        // Skip txqueue and deletions that don't concern us
        if ((queue.find("/txqueue/") != STR_NPOS) ||
            (queue.beginswith(Config::gConfig.FstQueue) == false)) {
          continue;
        } else {
          UnregisterFileSystem(event.mSubject);
        }
      } else if (event.mType == XrdMqSharedObjectManager::kMqSubjectModification) {
        // Handle subject modification, seperate <path> from <key>
        XrdOucString key = queue;
        int dpos = 0;

        if ((dpos = queue.find(";")) != STR_NPOS) {
          key.erase(0, dpos + 1);
          queue.erase(dpos);
        }

        if (queue == Config::gConfig.getFstNodeConfigQueue("communicator", false)) {
          ProcessFstConfigChange(key.c_str());
        } else {
          ProcessFsConfigChange(queue.c_str(), key.c_str());
        }
      }
    } while (true);
  }
}

//------------------------------------------------------------------------------
// Extract filesystem path from QDB hash key
//------------------------------------------------------------------------------
static std::string extractFilesystemPath(const std::string& key)
{
  std::vector<std::string> parts =
    common::StringTokenizer::split<std::vector<std::string>>(key, '|');
  return parts[parts.size() - 1];
}

//------------------------------------------------------------------------------
// Register which filesystems are in QDB config
//------------------------------------------------------------------------------
void Storage::updateFilesystemDefinitions()
{
  qclient::QScanner scanner(*gOFS.mMessagingRealm->getQSom()->getQClient(),
                            SSTR("eos-hash||fs||" << eos::fst::Config::gConfig.FstHostPort << "||*"));
  std::set<std::string> mNewFilesystems;

  for (; scanner.valid(); scanner.next()) {
    std::string queuePath = SSTR("/eos/" << eos::fst::Config::gConfig.FstHostPort <<
                                 "/fst" <<
                                 extractFilesystemPath(scanner.getValue()));
    mNewFilesystems.insert(queuePath);
  }

  //----------------------------------------------------------------------------
  // Filesystems added?
  //----------------------------------------------------------------------------
  for (auto it = mNewFilesystems.begin(); it != mNewFilesystems.end(); it++) {
    if (mLastRoundFilesystems.find(*it) == mLastRoundFilesystems.end()) {
      RegisterFileSystem(*it);
    }
  }

  //----------------------------------------------------------------------------
  // Filesystems removed?
  //----------------------------------------------------------------------------
  for (auto it = mLastRoundFilesystems.begin(); it != mLastRoundFilesystems.end();
       it++) {
    if (mNewFilesystems.find(*it) == mNewFilesystems.end()) {
      UnregisterFileSystem(*it);
    }
  }

  mLastRoundFilesystems = std::move(mNewFilesystems);
}

//------------------------------------------------------------------------------
// QdbCommunicator
//------------------------------------------------------------------------------
void
Storage::QdbCommunicator(ThreadAssistant& assistant)
{
  //----------------------------------------------------------------------------
  // Stupid delay to have legacy MQ up and running before we start
  //----------------------------------------------------------------------------
  std::this_thread::sleep_for(std::chrono::seconds(5));
  eos::mq::MessagingRealm* realm = gOFS.mMessagingRealm.get();

  if (!realm->haveQDB()) {
    return;
  }

  //----------------------------------------------------------------------------
  // Process initial FST configuration.. discover instance name..
  //----------------------------------------------------------------------------
  std::string instanceName;

  for (size_t i = 0; i < 10; i++) {
    if (realm->getInstanceName(instanceName)) {
      break;
    }
  }

  if (instanceName.empty()) {
    eos_static_crit("unable to obtain instance name from QDB");
    exit(1);
  }

  std::string configQueue = SSTR("/config/" << instanceName << "/node/" <<
                                 eos::fst::Config::gConfig.FstHostPort);
  Config::gConfig.setFstNodeConfigQueue(configQueue);
  //----------------------------------------------------------------------------
  // Discover node-specific configuration..
  //----------------------------------------------------------------------------
  common::SharedHashLocator nodeLocator = Config::gConfig.getNodeHashLocator();
  mq::SharedHashWrapper hash(gOFS.mMessagingRealm.get(), nodeLocator, true,
                             false);
  //----------------------------------------------------------------------------
  // Discover MGM name..
  //----------------------------------------------------------------------------
  std::string mgmHost;

  for (size_t i = 0; i < 10; i++) {
    if (hash.get("manager", mgmHost)) {
      break;
    }

    std::this_thread::sleep_for(std::chrono::seconds(5));
  }

  if (!mgmHost.empty()) {
    ProcessFstConfigChange("manager", mgmHost);
  }

  //----------------------------------------------------------------------------
  // Discover rest of FST configuration..
  //----------------------------------------------------------------------------
  std::vector<std::string> keys = { "symkey", "publish.interval",
                                    "debug.level", "txgw", "gw.rate", "gw.ntx", "error.simulation"
                                  };

  for (size_t i = 0; i < keys.size(); i++) {
    std::string value;

    if (hash.get(keys[i], value)) {
      ProcessFstConfigChange(keys[i], value);
    }
  }

  //----------------------------------------------------------------------------
  // Discover filesystem configuration
  // TODO: Find a way to do this without polling?
  //----------------------------------------------------------------------------
  while (!assistant.terminationRequested()) {
    updateFilesystemDefinitions();
    assistant.wait_for(std::chrono::seconds(30));
  }
}

EOSFSTNAMESPACE_END
