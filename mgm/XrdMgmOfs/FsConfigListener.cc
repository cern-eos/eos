// ----------------------------------------------------------------------
// File: FsConfigListener.cc
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


// -----------------------------------------------------------------------
// This file is included source code in XrdMgmOfs.cc to make the code more
// transparent without slowing down the compilation time.
// -----------------------------------------------------------------------

//------------------------------------------------------------------------------
// Process incoming MGM configuration change
//------------------------------------------------------------------------------
void
XrdMgmOfs::processIncomingMgmConfigurationChange(const std::string& key)
{
  std::string value = FsView::gFsView.GetGlobalConfig(key);

  // Here we might get a change without the namespace, in this
  // case we add the global namespace
  if ((key.substr(0, 4) != "map:") &&
      (key.substr(0, 3) != "fs:") &&
      (key.substr(0, 6) != "quota:") &&
      (key.substr(0, 4) != "vid:") &&
      (key.substr(0, 7) != "policy:")) {
    eos_info("msg=\"apply access config\" key=\"%s\" val=\"%s\"",
             key.c_str(), value.c_str());

    if (key.find("iostat:") == 0) {
      gOFS->mIoStats->ApplyConfig(&FsView::gFsView);
    } else {
      Access::EnforceConfig(false);
    }
  } else {
    eos_info("msg=\"set config value\" key=\"%s\" val=\"%s\"", key.c_str(),
             value.c_str());
    gOFS->mConfigEngine->SetConfigValue(0, key.c_str(), value.c_str(), false);

    // For fs modification we need to lock for write the FsView::ViewMutex
    if (key.find("fs:") == 0) {
      std::string fs_key = key;
      fs_key.erase(0, 3);
      eos::common::RWMutexWriteLock wr_view_lock(FsView::gFsView.ViewMutex);
      // To avoid issues when applying config changes in the slave we need to
      // unregister the file system first and then apply the new configuration
      const bool first_unregister = true;
      FsView::gFsView.ApplyFsConfig(fs_key.c_str(), value.c_str(),
                                    first_unregister);
    } else if (key.find("quota:") == 0) {
      eos_info("%s", "msg=\"skip quota update as it might mess with "
               "the namespace, will reload once we become master\"");
    } else {
      XrdOucString err;
      gOFS->mConfigEngine->ApplyEachConfig(key.c_str(), value.c_str(), &err);
    }
  }
}

//------------------------------------------------------------------------------
// Process geotag change on the specified filesystem
//------------------------------------------------------------------------------
void
XrdMgmOfs::ProcessGeotagChange(const std::string& queue)
{
  std::string newgeotag;
  eos::common::FileSystem::fsid_t fsid = 0;
  eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
  FileSystem* fs = FsView::gFsView.mIdView.lookupByQueuePath(queue);

  if (fs == nullptr) {
    return;
  }

  fsid = (eos::common::FileSystem::fsid_t) fs->GetLongLong("id");
  newgeotag = fs->GetString("stat.geotag");

  if (fsid == 0 && newgeotag.empty()) {
    return;
  }

  std::string oldgeotag = newgeotag;

  if (FsView::gFsView.mNodeView.count(fs->GetQueue())) {
    // Check if the change notification is an actual change in the geotag
    FsNode* node = FsView::gFsView.mNodeView[fs->GetQueue()];
    static_cast<GeoTree*>(node)->getGeoTagInTree(fsid, oldgeotag);
    oldgeotag.erase(0, 8); // to get rid of the "<ROOT>::" prefix
  }

  if (oldgeotag != newgeotag) {
    eos_warning("msg=\"received geotag change\" fsid=%lu old_geotag=\"%s\" "
                "new_geotag=\"%s\"", (unsigned long)fsid,
                oldgeotag.c_str(), newgeotag.c_str());
    // Release read lock and take write lock
    fs_rd_lock.Release();
    eos::common::RWMutexWriteLock fs_rw_lock(FsView::gFsView.ViewMutex);
    eos::common::FileSystem::fs_snapshot_t snapshot;
    fs->SnapShotFileSystem(snapshot);

    // Update node view tree structure
    if (FsView::gFsView.mNodeView.count(snapshot.mQueue)) {
      FsNode* node = FsView::gFsView.mNodeView[snapshot.mQueue];
      eos_debug("msg=\"update geotag of fsid=%lu in node=%s",
                (unsigned long)fsid, node->mName.c_str());

      if (!static_cast<GeoTree*>(node)->erase(fsid)) {
        eos_err("msg=\"error removing fsid=%lu from node=%s\"",
                (unsigned long)fsid, node->mName.c_str());
      }

      if (!static_cast<GeoTree*>(node)->insert(fsid)) {
        eos_err("msg=\"error inserting fsid=%lu into node=%s\"",
                (unsigned long)fsid, node->mName.c_str());
      }
    }

    // Update group view tree structure
    if (FsView::gFsView.mGroupView.count(snapshot.mGroup)) {
      FsGroup* group = FsView::gFsView.mGroupView[snapshot.mGroup];
      eos_debug("msg=\"updating geotag of fsid=%lu in group=%s\"",
                (unsigned long)fsid, group->mName.c_str());

      if (!static_cast<GeoTree*>(group)->erase(fsid)) {
        eos_err("msg=\"error removing fsid=%lu from group=%s\"",
                (unsigned long)fsid, group->mName.c_str());
      }

      if (!static_cast<GeoTree*>(group)->insert(fsid)) {
        eos_err("msg=\"error inserting fsid=%lu into group=%s\"",
                (unsigned long)fsid, group->mName.c_str());
      }
    }

    // Update space view tree structure
    if (FsView::gFsView.mSpaceView.count(snapshot.mSpace)) {
      FsSpace* space = FsView::gFsView.mSpaceView[snapshot.mSpace];
      eos_debug("msg=\"updating geotag of fsid=%lu in space=%s\"",
                (unsigned long)fsid, space->mName.c_str());

      if (!static_cast<GeoTree*>(space)->erase(fsid)) {
        eos_err("msg=\"error removing fsid=%lu from space=%s\"",
                (unsigned long)fsid, space->mName.c_str());
      }

      if (!static_cast<GeoTree*>(space)->insert(fsid)) {
        eos_err("msg=\"error inserting fsid=%lu into space=%s\"",
                (unsigned long)fsid, space->mName.c_str());
      }
    }
  }
}

//------------------------------------------------------------------------------
// A thread monitoring for important key-value changes in filesystems
//------------------------------------------------------------------------------
void XrdMgmOfs::FileSystemMonitorThread(ThreadAssistant& assistant) noexcept
{
  std::shared_ptr<eos::mq::FsChangeListener> fs_listener =
    mMessagingRealm->GetFsChangeListener("filesystem-listener-thread");
  const std::set<std::string> interests {"stat.errc", "stat.geotag",
                                         "configstatus", "stat.boot"};

  // Register all the interests for this listener so that new file systems
  // are properly registered with this listener based on it's interests
  for (const auto& key : interests) {
    fs_listener->subscribe(key);
  }

  FsView::gFsView.AddFsChangeListener(fs_listener, interests);

  if (!fs_listener->startListening()) {
    eos_static_crit("%s", "msg=\"unspecified problem when attempting to "
                    "subscribe to filesystem key changes\"");
  }

  while (!assistant.terminationRequested()) {
    eos::mq::FsChangeListener::Event event;

    if (fs_listener->fetch(assistant, event) && !event.isDeletion()) {
      if (event.key == "stat.geotag") {
        ProcessGeotagChange(event.fileSystemQueue);
      } else {
        // This is a filesystem status error
        if (gOFS->mMaster->IsMaster()) {
          // only an MGM master needs to initiate draining
          eos::common::FileSystem::fsid_t fsid = 0;
          long long errc = 0;
          std::string configstatus = "";
          std::string bootstatus = "";
          std::string space;
          eos::common::ConfigStatus cfgstatus = eos::common::ConfigStatus::kOff;
          eos::common::BootStatus bstatus = eos::common::BootStatus::kDown;
          // read the id from the hash and the current error value
          eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
          FileSystem* fs = FsView::gFsView.mIdView.lookupByQueuePath(
                             event.fileSystemQueue);

          if (fs) {
            fsid = (eos::common::FileSystem::fsid_t) fs->GetLongLong("id");
            errc = (int) fs->GetLongLong("stat.errc");
            configstatus = fs->GetString("configstatus");
            bootstatus = fs->GetString("stat.boot");
            cfgstatus = eos::common::FileSystem::GetConfigStatusFromString(
                          configstatus.c_str());
            bstatus = eos::common::FileSystem::GetStatusFromString(bootstatus.c_str());
            space = fs->getCoreParams().getSpace();

            if (fsid == 0 || space.empty()) {
              // We are in the initial state where most of the FS params are not populated
              // skip further processing
              continue;
            }

            if (gOFS->mFsScheduler != nullptr && gOFS->mFsScheduler->isRunning()) {
              bool status = gOFS->mFsScheduler->setDiskStatus(space,
                            fsid, cfgstatus);
              bool act_status = gOFS->mFsScheduler->setDiskStatus(space,
                                fsid, fs->GetActiveStatus(), bstatus);

              if (!status || !act_status) {
                eos_static_err("msg=\"Failed to set Disk Status in FsScheduler for disk\" %llu",
                               fsid);
              }
            }
          }

          if (fs && fsid && errc &&
              (cfgstatus >= eos::common::ConfigStatus::kRO) &&
              (bstatus == eos::common::BootStatus::kOpsError)) {
            // Case when we take action and explicitly ask to start a drain job
            fs->SetConfigStatus(eos::common::ConfigStatus::kDrain);

            if (gOFS->mFsScheduler != nullptr && gOFS->mFsScheduler->isRunning()) {
              bool status = gOFS->mFsScheduler->setDiskStatus(fs->getCoreParams().getSpace(),
                            fsid,
                            eos::common::ConfigStatus::kDrain);

              if (!status) {
                eos_static_err("msg=\"Failed to set Disk Status in FsScheduler for disk\" %llu",
                               fsid);
              }
            }
          }
        }
      }
    }
  }
}

/*----------------------------------------------------------------------------*/
/*
 * @brief file system listener agent starting drain jobs when receving opserror
 * and applying remote master configuration changes to the local configuration
 * object.
 *
 * This thread agent catches 'opserror' states on filesystems and executes the
 * drain job start routine on the referenced filesystem. If a filesystem
 * is removing the error code it also run's a stop drain job routine.
 * Additionally it applies changes in the MGM configuration which have been
 * broadcasted by a remote master MGM.
 */
/*----------------------------------------------------------------------------*/
void
XrdMgmOfs::FsConfigListener(ThreadAssistant& assistant) noexcept
{
  eos::mq::GlobalConfigChangeListener changeListener(mMessagingRealm.get(),
      "fs-config-listener-thread", MgmConfigQueue.c_str());

  // Thread listening on filesystem errors and configuration changes
  while (!assistant.terminationRequested()) {
    eos::mq::GlobalConfigChangeListener::Event event;

    if (changeListener.fetch(assistant, event)) {
      if (!gOFS->mMaster->IsMaster()) {
        if (!event.isDeletion()) {
          // This is an MGM configuration modification -
          // only an MGM slave needs to apply this.
          processIncomingMgmConfigurationChange(event.key);
        } else {
          eos_static_info("msg=\"handle deletion event\" key=\"%s\"",
                          event.key.c_str());
          gOFS->mConfigEngine->DeleteConfigValue(0, event.key.c_str(), false);
          gOFS->mConfigEngine->ApplyKeyDeletion(event.key.c_str());
        }
      }
    }
  }
}
