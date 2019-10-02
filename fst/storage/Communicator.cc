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
#include "mq/SharedHashWrapper.hh"
#include "common/Constants.hh"

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

  mq::SharedHashWrapper hash(locator, true, false);
  return hash.get(key, value);
}

bool
Storage::GetFstConfigValue(const std::string& key, unsigned long long& value)
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
    eos_warning("msg=\"file system is already removed\" qpath=%s",
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
    eos_warning("msg=\"file system missing from map\" qpath=%s",
                queuepath.c_str());
  } else {
    mFsMap.erase(it_map);
  }

  eos_info("msg=\"deleting file system\" qpath=%s", fs->GetQueuePath().c_str());
  delete fs;
}

//------------------------------------------------------------------------------
// Register file system
//------------------------------------------------------------------------------
void
Storage::RegisterFileSystem(const std::string& queuepath)
{
  auto it = std::find_if(mFsVect.begin(), mFsVect.end(),
  [&](FileSystem * fs) {
    return (fs->GetQueuePath() == queuepath);
  });

  if (it != mFsVect.end()) {
    eos_warning("msg=\"file system is already registered\" qpath=%s",
                queuepath.c_str());
    return;
  }

  common::FileSystemLocator locator;

  if (!common::FileSystemLocator::fromQueuePath(queuepath, locator)) {
    eos_crit("msg=\"failed to parse queuepath\" qpath=%s",
             queuepath.c_str());
    return;
  }

  fst::FileSystem* fs = new fst::FileSystem(locator, &gOFS.ObjectManager,
      gOFS.mQSOM.get());
  fs->SetStatus(eos::common::BootStatus::kDown);
  mFsVect.push_back(fs);

  if (fs->GetId() == 0ul) {
    eos_info("msg=\"partially register file system\" qpath=\"%s\"",
             queuepath.c_str());
    return;
  }

  eos_info("msg=\"fully register filesystem\" qpath=\"%s\" fsid=%lu",
           queuepath.c_str(), fs->GetId());
  fs->SetStableId(fs->GetId());
  mFsMap[fs->GetId()] = fs;
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
                               const std::string& queue, const std::string& key, const std::string& value)
{
  if (key == "id") {
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
  eos::common::RWMutexWriteLock fs_wr_lock(mFsMutex);
  auto it = std::find_if(mFsMap.begin(), mFsMap.end(), [&](const auto & pair) {
    return (pair.second->GetQueuePath() == queuepath);
  });

  if (it == mFsMap.end()) {
    // If file system does not exist in the map and this an "id" info then
    // it could be that we have a partially registered file system
    if (key == "id") {
      auto itv = std::find_if(mFsVect.begin(), mFsVect.end(),
      [&](fst::FileSystem * fs) {
        return (fs->GetQueuePath() == queuepath);
      });

      if (itv == mFsVect.end()) {
        eos_err("msg=\"no file system for id modification\" qpath=\"%s\" "
                "key=\"%s\"", queuepath.c_str(), key.c_str());
        return;
      }

      fst::FileSystem* fs = *itv;
      fs->SetStableId(fs->GetId());
      it = mFsMap.emplace(fs->GetId(), fs).first;
      eos_info("msg=\"fully register file system\" qpath=%s fsid=%lu",
               queuepath.c_str(), fs->GetId());
    } else {
      eos_err("msg=\"no file system for modification\" qpath=\"%s\" "
              "key=\"%s\"", queuepath.c_str(), key.c_str());
      return;
    }
  }

  eos_info("msg=\"process modification\" qpath=\"%s\" key=\"%s\"",
           queuepath.c_str(), key.c_str());
  fst::FileSystem* fs = it->second;
  mq::SharedHashWrapper hash(fs->getHashLocator());
  std::string value;

  if (!hash.get(key, value)) {
    eos_err("msg=\"no such key in hash\" qpath=\"%s\" key=\"%s\"",
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
  std::string watch_id = "id";
  std::string watch_bootsenttime = "bootsenttime";
  std::string watch_scan_io_rate = eos::common::SCAN_IO_RATE_NAME;
  std::string watch_scan_entry_interval = eos::common::SCAN_ENTRY_INTERVAL_NAME;
  std::string watch_scan_disk_interval = eos::common::SCAN_DISK_INTERVAL_NAME;
  std::string watch_scan_ns_interval = eos::common::SCAN_NS_INTERVAL_NAME;
  std::string watch_scan_ns_rate = eos::common::SCAN_NS_RATE_NAME;
  std::string watch_symkey = "symkey";
  std::string watch_manager = "manager";
  std::string watch_publishinterval = "publish.interval";
  std::string watch_debuglevel = "debug.level";
  std::string watch_gateway = "txgw";
  std::string watch_gateway_rate = "gw.rate";
  std::string watch_gateway_ntx = "gw.ntx";
  std::string watch_error_simulation = "error.simulation";
  std::string watch_regex = ".*";
  bool ok = true;
  ok &= gOFS.ObjectNotifier.SubscribesToKey("communicator", watch_id,
        XrdMqSharedObjectChangeNotifier::kMqSubjectModification);
  ok &= gOFS.ObjectNotifier.SubscribesToKey("communicator", watch_bootsenttime,
        XrdMqSharedObjectChangeNotifier::kMqSubjectModification);
  ok &= gOFS.ObjectNotifier.SubscribesToKey("communicator", watch_scan_io_rate,
        XrdMqSharedObjectChangeNotifier::kMqSubjectModification);
  ok &= gOFS.ObjectNotifier.SubscribesToKey("communicator",
        watch_scan_entry_interval,
        XrdMqSharedObjectChangeNotifier::kMqSubjectModification);
  ok &= gOFS.ObjectNotifier.SubscribesToKey("communicator",
        watch_scan_disk_interval,
        XrdMqSharedObjectChangeNotifier::kMqSubjectModification);
  ok &= gOFS.ObjectNotifier.SubscribesToKey("communicator",
        watch_scan_ns_interval,
        XrdMqSharedObjectChangeNotifier::kMqSubjectModification);
  ok &= gOFS.ObjectNotifier.SubscribesToKey("communicator", watch_scan_ns_rate,
        XrdMqSharedObjectChangeNotifier::kMqSubjectModification);
  ok &= gOFS.ObjectNotifier.SubscribesToKey("communicator", watch_symkey,
        XrdMqSharedObjectChangeNotifier::kMqSubjectModification);
  ok &= gOFS.ObjectNotifier.SubscribesToKey("communicator", watch_manager,
        XrdMqSharedObjectChangeNotifier::kMqSubjectModification);
  ok &= gOFS.ObjectNotifier.SubscribesToKey("communicator", watch_publishinterval,
        XrdMqSharedObjectChangeNotifier::kMqSubjectModification);
  ok &= gOFS.ObjectNotifier.SubscribesToKey("communicator", watch_debuglevel,
        XrdMqSharedObjectChangeNotifier::kMqSubjectModification);
  ok &= gOFS.ObjectNotifier.SubscribesToKey("communicator", watch_gateway,
        XrdMqSharedObjectChangeNotifier::kMqSubjectModification);
  ok &= gOFS.ObjectNotifier.SubscribesToKey("communicator", watch_gateway_rate,
        XrdMqSharedObjectChangeNotifier::kMqSubjectModification);
  ok &= gOFS.ObjectNotifier.SubscribesToKey("communicator", watch_gateway_ntx,
        XrdMqSharedObjectChangeNotifier::kMqSubjectModification);
  ok &= gOFS.ObjectNotifier.SubscribesToKey("communicator",
        watch_error_simulation,
        XrdMqSharedObjectChangeNotifier::kMqSubjectModification);
  ok &= gOFS.ObjectNotifier.SubscribesToSubjectRegex("communicator", watch_regex,
        XrdMqSharedObjectChangeNotifier::kMqSubjectCreation);
  ok &= gOFS.ObjectNotifier.SubscribesToSubjectRegex("communicator", watch_regex,
        XrdMqSharedObjectChangeNotifier::kMqSubjectDeletion);

  if (!ok) {
    eos_crit("error subscribing to shared objects change notifications");
  }

  gOFS.ObjectNotifier.BindCurrentThread("communicator");

  if (!gOFS.ObjectNotifier.StartNotifyCurrentThread()) {
    eos_crit("error starting shared objects change notifications");
  }

  XrdSysThread::SetCancelDeferred();

  while (true) {
    // wait for new filesystem definitions
    gOFS.ObjectNotifier.tlSubscriber->mSubjSem.Wait();
    XrdSysThread::CancelPoint();
    // we always take a lock to take something from the queue and then release it
    gOFS.ObjectNotifier.tlSubscriber->mSubjMtx.Lock();

    while (gOFS.ObjectNotifier.tlSubscriber->NotificationSubjects.size()) {
      XrdMqSharedObjectManager::Notification event;
      event = gOFS.ObjectNotifier.tlSubscriber->NotificationSubjects.front();
      gOFS.ObjectNotifier.tlSubscriber->NotificationSubjects.pop_front();
      gOFS.ObjectNotifier.tlSubscriber->mSubjMtx.UnLock();
      eos_static_info("msg=\"shared object notification\" type=%i subject=\"%s\"",
                      event.mType, event.mSubject.c_str());
      XrdOucString queue = event.mSubject.c_str();

      if (event.mType == XrdMqSharedObjectManager::kMqSubjectCreation) {
        // Handle subject creation
        if (queue == Config::gConfig.FstQueueWildcard) {
          gOFS.ObjectNotifier.tlSubscriber->mSubjMtx.Lock();
          continue;
        }

        if ((queue.find("/txqueue/") != STR_NPOS)) {
          // this is a transfer queue we, don't need to take action
          gOFS.ObjectNotifier.tlSubscriber->mSubjMtx.Lock();
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
            Config::gConfig.setFstNodeConfigQueue(queue);
            eos_static_info("storing config queue name <%s>", queue.c_str());
          } else {
            eos_static_info("no action on creation of subject <%s> - we are <%s>",
                            event.mSubject.c_str(), Config::gConfig.FstQueue.c_str());
          }

          gOFS.ObjectNotifier.tlSubscriber->mSubjMtx.Lock();
          continue;
        }

        RegisterFileSystem(queue.c_str());
        gOFS.ObjectNotifier.tlSubscriber->mSubjMtx.Lock();
        continue;
      }

      if (event.mType == XrdMqSharedObjectManager::kMqSubjectDeletion) {
        // Handle subject deletion
        if ((queue.find("/txqueue/") != STR_NPOS)) {
          // this is a transfer queue we, don't need to take action
          gOFS.ObjectNotifier.tlSubscriber->mSubjMtx.Lock();
          continue;
        }

        if (!queue.beginswith(Config::gConfig.FstQueue)) {
          eos_static_err("msg=\"illegal deletion subject\" subject=\"%s\" "
                         "own_id=\"%s\"", event.mSubject.c_str(),
                         Config::gConfig.FstQueue.c_str());
          gOFS.ObjectNotifier.tlSubscriber->mSubjMtx.Lock();
          continue;
        } else {
          UnregisterFileSystem(event.mSubject);
        }

        gOFS.ObjectNotifier.tlSubscriber->mSubjMtx.Lock();
        continue;
      }

      if (event.mType == XrdMqSharedObjectManager::kMqSubjectModification) {
        // Handle subject modification
        // seperate <path> from <key>
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

        gOFS.ObjectNotifier.tlSubscriber->mSubjMtx.Lock();
        continue;
      }
    }

    gOFS.ObjectNotifier.tlSubscriber->mSubjMtx.UnLock();
    XrdSysThread::CancelPoint();
  }

  XrdSysThread::SetCancelOn();
}

//------------------------------------------------------------------------------
// QdbCommunicator
//------------------------------------------------------------------------------
void
Storage::QdbCommunicator(QdbContactDetails contactDetails,
                         ThreadAssistant& assistant)
{
  while (!assistant.terminationRequested()) {
    assistant.wait_for(std::chrono::seconds(1));
  }
}

EOSFSTNAMESPACE_END
