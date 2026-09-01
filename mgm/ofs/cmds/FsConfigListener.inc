// ----------------------------------------------------------------------
// File: FsConfigListener.inc
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
// Process a change in the space usage published by the given file system
//------------------------------------------------------------------------------
void
XrdMgmOfs::ProcessFsUsageChange(const std::string& queue)
{
  // Keep the scheduler's view of how full a disk is up to date. Only the flat
  // scheduler keeps a copy of these numbers, the geotree engine reads the very
  // same keys with its own listener. This fires on every heartbeat that moves
  // the values, so it must stay cheap: a lookup and a few atomic stores, no
  // snapshot rebuild.
  if ((mFsScheduler == nullptr) || !mFsScheduler->IsRunning()) {
    return;
  }

  eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
  FileSystem* fs = FsView::gFsView.mIdView.lookupByQueuePath(queue);

  if (fs == nullptr) {
    return;
  }

  const auto fsid = (eos::common::FileSystem::fsid_t)fs->GetLongLong("id");
  const std::string space = fs->getCoreParams().getSpace();

  if ((fsid == 0) || space.empty()) {
    return;
  }

  // Both keys come out of the same FST publish cycle, so either one refreshes
  // the fill level and the free space in one go. Filled is published as a
  // percentage between 0 and 100.
  const double filled = fs->GetDouble("stat.statfs.filled");
  mFsScheduler->SetDiskPercentUsed(
      space, fsid, static_cast<uint8_t>(std::max(0.0, std::min(100.0, filled))));
  mFsScheduler->SetDiskFreeSpace(space, fsid,
                                 eos::mgm::placement::GetUsableFreeBytes(*fs));
}

//------------------------------------------------------------------------------
// Process a change in the scheduling state or the health of the given
// file system
//------------------------------------------------------------------------------
void
XrdMgmOfs::ProcessFsStatusChange(const std::string& key, const std::string& queue)
{
  // Each key moves only a part of the state, so nothing is pushed to the
  // scheduler that this event cannot have changed:
  //   sched.ops, configstatus -> the operations mask of the disk
  //   stat.boot               -> the status of the disk
  // The automatic drain decision reads stat.errc and stat.boot, but is taken
  // for any of the keys so that a mask put back by hand on a broken disk is
  // caught as well. Only an MGM master takes it.
  const bool is_master = mMaster->IsMaster();
  const bool refresh_ops =
      ((key == eos::common::FS_SCHED_OPS_NAME) || (key == "configstatus"));
  const bool refresh_status = (key == "stat.boot");
  // Only the flat scheduler keeps a copy of the file system state that has to
  // be pushed to it, the geotree engine subscribes to the very same keys with
  // its own listener and refreshes itself
  const bool sched_running = ((mFsScheduler != nullptr) && mFsScheduler->IsRunning());

  if (!is_master && (!sched_running || (!refresh_ops && !refresh_status))) {
    return;
  }

  eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
  FileSystem* fs = FsView::gFsView.mIdView.lookupByQueuePath(queue);

  if (fs == nullptr) {
    return;
  }

  const auto fsid = (eos::common::FileSystem::fsid_t)fs->GetLongLong("id");
  const std::string space = fs->getCoreParams().getSpace();

  if ((fsid == 0) || space.empty()) {
    // Initial state in which most of the fs parameters are not yet populated
    return;
  }

  const eos::common::BootStatus bstatus =
      eos::common::FileSystem::GetStatusFromString(fs->GetString("stat.boot").c_str());

  if (refresh_status && sched_running &&
      !mFsScheduler->SetDiskStatus(space, fsid, fs->GetActiveStatus(), bstatus)) {
    eos_static_err("msg=\"failed to update the disk status in the scheduler\" "
                   "fsid=%lu space=\"%s\"",
                   (unsigned long)fsid, space.c_str());
  }

  if (!refresh_ops && !is_master) {
    // A boot status change on a slave, the mask cannot have moved
    return;
  }

  // sched.ops is the authoritative key, configstatus is only its projection,
  // so the mask is what both the auto-drain decision and the scheduler use
  eos::common::FsOpMask ops = fs->GetSchedOps();
  bool push_ops = refresh_ops;

  // An operations error on a file system is what triggers the automatic drain
  // and a file system that no longer takes any client traffic - off, empty or
  // internal only - is left alone: an operator took it out of service and an
  // error must not drain it back in.
  if (is_master && (fs->GetLongLong("stat.errc") != 0) &&
      (bstatus == eos::common::BootStatus::kOpsError) &&
      eos::common::AllowsAnyClient(ops)) {
    // Reads only - what the legacy "drain" status meant - and then the drain
    // request, which used to ride along with it. Both steps are skipped once
    // they are in place, so an error repeated on every heartbeat neither
    // rewrites the mask nor restarts the drain.
    if ((ops != eos::common::kMaskAllReads) &&
        fs->SetSchedOps(eos::common::kMaskAllReads)) {
      // Track what was written rather than re-reading it, the shared hash
      // update is not necessarily visible yet, and hand it to the scheduler
      // now rather than waiting for our own write to come back as an event
      ops = eos::common::kMaskAllReads;
      push_ops = true;
    }

    if (!fs->IsDrainRequested()) {
      fs->SetDrainRequested(true);
    }
  }

  if (push_ops && sched_running && !mFsScheduler->SetDiskOps(space, fsid, ops)) {
    eos_static_err("msg=\"failed to update the disk ops in the scheduler\" "
                   "fsid=%lu space=\"%s\"",
                   (unsigned long)fsid, space.c_str());
  }
}

//------------------------------------------------------------------------------
// A thread monitoring for important key-value changes in filesystems
//------------------------------------------------------------------------------
void XrdMgmOfs::FileSystemMonitorThread(ThreadAssistant& assistant) noexcept
{
  std::shared_ptr<eos::mq::FsChangeListener> fs_listener =
    mMessagingRealm->GetFsChangeListener("filesystem-listener-thread");
  const std::set<std::string> interests{"stat.errc",
                                        "stat.geotag",
                                        "configstatus",
                                        "stat.boot",
                                        eos::common::FS_SCHED_OPS_NAME,
                                        "stat.statfs.filled",
                                        "stat.statfs.freebytes"};

  // Register all the interests for this listener so that new file systems
  // are properly registered with this listener based on it's interests
  for (const auto& key : interests) {
    fs_listener->subscribe(key);
  }

  FsView::gFsView.AddFsChangeListener(fs_listener, interests);

  while (!assistant.terminationRequested()) {
    eos::mq::FsChangeListener::Event event;

    if (!fs_listener->fetch(assistant, event) || event.isDeletion()) {
      continue;
    }

    if (event.key == "stat.geotag") {
      ProcessGeotagChange(event.fileSystemQueue);
    } else if ((event.key == "stat.statfs.filled") ||
               (event.key == "stat.statfs.freebytes")) {
      ProcessFsUsageChange(event.fileSystemQueue);
    } else {
      ProcessFsStatusChange(event.key, event.fileSystemQueue);
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
