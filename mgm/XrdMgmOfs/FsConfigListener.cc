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
  // setup the modifications which the fs listener thread is waiting for
  std::string watch_errc = "stat.errc";
  std::string watch_geotag = "stat.geotag";
  std::string watch_proxygroups = "proxygroups";
  bool ok = true;
  // Need to notify the FsView when a geotag changes to keep the tree structure
  // up-to-date
  ok &= ObjectNotifier.SubscribesToKey("fsconfiglistener", watch_geotag,
                                       XrdMqSharedObjectChangeNotifier::kMqSubjectModification);
  // Need to take action on filesystem errors
  ok &= ObjectNotifier.SubscribesToKey("fsconfiglistener", watch_errc,
                                       XrdMqSharedObjectChangeNotifier::kMqSubjectModification);
  // Need to notify GeoTreeEngine when the proxygroups to which a node belongs to are changing
  ok &= ObjectNotifier.SubscribesToKey("fsconfiglistener", watch_proxygroups,
                                       XrdMqSharedObjectChangeNotifier::kMqSubjectModification);
  // This one would be necessary to be equivalent to beryl but it's probably not needed =>
  // Need to take action an filesystem errors
  // ok &= ObjectNotifier.SubscribesToKey("fsconfiglistener",watch_errc,
  //                                      XrdMqSharedObjectChangeNotifier::kMqSubjectDeletion);
  // Need to apply remote configuration changes
  ok &= ObjectNotifier.SubscribesToSubject("fsconfiglistener",
        MgmConfigQueue.c_str(),
        XrdMqSharedObjectChangeNotifier::kMqSubjectModification);
  // Need to apply remote configuration changes
  ok &= ObjectNotifier.SubscribesToSubject("fsconfiglistener",
        MgmConfigQueue.c_str(),
        XrdMqSharedObjectChangeNotifier::kMqSubjectDeletion);

  if (!ok) {
    eos_crit("msg=\"error subscribing to shared objects change notifications\"");
  }

  ObjectNotifier.BindCurrentThread("fsconfiglistener");

  if (!ObjectNotifier.StartNotifyCurrentThread()) {
    eos_crit("msg=\"error starting shared objects change notifications\"");
  }

  // Thread listening on filesystem errors and configuration changes
  while (!assistant.terminationRequested()) {
    gOFS->ObjectNotifier.tlSubscriber->mSubjSem.Wait();

    if (assistant.terminationRequested()) {
      break;
    }

    // we always take a lock to take something from the queue and then release it
    gOFS->ObjectNotifier.tlSubscriber->mSubjMtx.Lock();

    // Listens for modifications on filesystem objects
    while (gOFS->ObjectNotifier.tlSubscriber->NotificationSubjects.size()) {
      XrdMqSharedObjectManager::Notification event;
      event = gOFS->ObjectNotifier.tlSubscriber->NotificationSubjects.front();
      gOFS->ObjectNotifier.tlSubscriber->NotificationSubjects.pop_front();
      gOFS->ObjectNotifier.tlSubscriber->mSubjMtx.UnLock();
      eos_debug("msg=\"MGM shared object notification subject is %s\"",
                event.mSubject.c_str());
      std::string newsubject = event.mSubject.c_str();

      // Handle subject creation
      if (event.mType == XrdMqSharedObjectManager::kMqSubjectCreation) {
        eos_debug("msg=\"received creation on subject\" subject=\"%s\"",
                  newsubject.c_str());
        gOFS->ObjectNotifier.tlSubscriber->mSubjMtx.Lock();
        continue;
      }

      // Handle subject deletion
      if (event.mType == XrdMqSharedObjectManager::kMqSubjectDeletion) {
        eos_debug("msg=\"received deletion on subject\" subject=\"%s\"",
                  newsubject.c_str());
        gOFS->ObjectNotifier.tlSubscriber->mSubjMtx.Lock();
        continue;
      }

      // Handle subject modification
      if (event.mType == XrdMqSharedObjectManager::kMqSubjectModification) {
        eos_debug("msg=\"received modification on subject\" subject=\"%s\"",
                  newsubject.c_str());
        // if this is an error status on a file system, check if the filesystem
        // is > drained state and in this case launch a drain job with
        // the opserror flag by calling StartDrainJob
        // We use directly the ObjectManager Interface because it is more handy
        // with the available information we have at this point
        std::string key = newsubject;
        std::string queue = newsubject;
        size_t dpos = 0;

        if ((dpos = queue.find(";")) != std::string::npos) {
          key.erase(0, dpos + 1);
          queue.erase(dpos);
        }

        if (queue == MgmConfigQueue.c_str()) {
          // This is an MGM configuration modification
          if (!gOFS->mMaster->IsMaster()) {
            // only an MGM slave needs to apply this
            gOFS->ObjectManager.HashMutex.LockRead();
            XrdMqSharedHash* hash = gOFS->ObjectManager.GetObject(queue.c_str(), "hash");

            if (hash) {
              XrdOucString err;
              XrdOucString value = hash->Get(key.c_str()).c_str();
              gOFS->ObjectManager.HashMutex.UnLockRead();

              if (value.c_str()) {
                // Here we might get a change without the namespace, in this
                // case we add the global namespace
                if ((key.substr(0, 4) != "map:") &&
                    (key.substr(0, 3) != "fs:") &&
                    (key.substr(0, 6) != "quota:") &&
                    (key.substr(0, 4) != "vid:") &&
                    (key.substr(0, 7) != "policy:")) {
                  XrdOucString skey = key.c_str();
                  eos_info("msg=\"apply access config\" key=\"%s\" val=\"%s\"",
                           key.c_str(), value.c_str());
                  Access::ApplyAccessConfig(false);

                  if (skey.beginswith("iostat:")) {
                    gOFS->IoStats->ApplyIostatConfig();
                  }

                  if (skey.beginswith("fsck")) {
                    gOFS->FsCheck.ApplyFsckConfig();
                  }
                } else {
                  eos_info("msg=\"set config value\" key=\"%s\" val=\"%s\"",
                           key.c_str(), value.c_str());
                  gOFS->ConfEngine->SetConfigValue(0, key.c_str(),
                                                   value.c_str(), false);

                  // For file system modification we need to take the
                  // FsView::ViewMutex for write
                  if (key.find("fs:") == 0) {
                    eos::common::RWMutexWriteLock wr_view_lock(FsView::gFsView.ViewMutex);
                    gOFS->ConfEngine->ApplyEachConfig(key.c_str(), &value, (void*) &err);
                  } else {
                    gOFS->ConfEngine->ApplyEachConfig(key.c_str(), &value, (void*) &err);
                  }
                }
              }
            } else {
              gOFS->ObjectManager.HashMutex.UnLockRead();
            }
          }
        } else if (key == watch_geotag) {
          // Geotag update
          eos::common::FileSystem::fsid_t fsid = 0;
          std::string newgeotag, oldgeotag;
          {
            // Read the id from the hash and the new geotag
            eos::common::RWMutexReadLock hash_rd_lock(gOFS->ObjectManager.HashMutex);
            XrdMqSharedHash* hash = gOFS->ObjectManager.GetObject(queue.c_str(), "hash");

            if (hash) {
              fsid = (eos::common::FileSystem::fsid_t) hash->GetLongLong("id");
              oldgeotag = newgeotag = hash->Get("stat.geotag");
            }
          }

          if (fsid == 0) {
            eos_debug("msg=\"received a geotag modification (might be no change) for "
                      "queue=%s which is not registered\"", queue.c_str());
          } else {
            FsView::gFsView.ViewMutex.LockRead();
            auto it_fs = FsView::gFsView.mIdView.find(fsid);

            if (it_fs != FsView::gFsView.mIdView.end()) {
              FileSystem* fs = it_fs->second;

              if (fs && FsView::gFsView.mNodeView.count(fs->GetQueue())) {
                // check if the change notification is an actual change in the geotag
                FsNode* node = FsView::gFsView.mNodeView[fs->GetQueue()];
                static_cast<GeoTree*>(node)->getGeoTagInTree(fsid , oldgeotag);
                oldgeotag.erase(0, 8); // to get rid of the "<ROOT>::" prefix
              }

              if (fs && (oldgeotag != newgeotag)) {
                eos_warning("msg=\"received geotag change\" fsid=%lu old_geotag=%s "
                            "new_geotag=%s", (unsigned long)fsid,
                            oldgeotag.c_str(), newgeotag.c_str());
                FsView::gFsView.ViewMutex.UnLockRead();
                eos::common::RWMutexWriteLock fs_rw_lock(FsView::gFsView.ViewMutex);
                eos::common::FileSystem::fs_snapshot snapshot;
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
              } else {
                FsView::gFsView.ViewMutex.UnLockRead();
              }
            } else {
              FsView::gFsView.ViewMutex.UnLockRead();
            }
          }
        } else if (key == watch_proxygroups) {
          // This is a dataproxy / dataep status update
          std::string status;
          {
            // Read the proxygrouplist
            eos::common::RWMutexReadLock hash_rd_lock(gOFS->ObjectManager.HashMutex);
            XrdMqSharedHash* hash = gOFS->ObjectManager.GetObject(queue.c_str(), "hash");

            if (hash) {
              status = hash->Get("proxygroups");
            }
          }
          std::string hostport = "/eos/" + queue.substr(queue.rfind('/') + 1) + "/fst";
          eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);

          if (eos::mgm::FsView::gFsView.mNodeView.count(hostport)) {
            eos::mgm::FsNode* node = eos::mgm::FsView::gFsView.mNodeView[hostport];
            eos::mgm::gGeoTreeEngine.matchHostPxyGr(node, status, false, false);
          } else {
            eos_err("msg=\"no FsNode object associated with queue=%s and hostport=%s\"",
                    queue.c_str(), hostport.c_str());
          }
        } else {
          // This is a filesystem status error
          if (gOFS->mMaster->IsMaster()) {
            // only an MGM master needs to initiate draining
            eos::common::FileSystem::fsid_t fsid = 0;
            long long errc = 0;
            std::string configstatus = "";
            std::string bootstatus = "";
            int cfgstatus = 0;
            eos::common::BootStatus bstatus = eos::common::BootStatus::kDown;
            // read the id from the hash and the current error value
            gOFS->ObjectManager.HashMutex.LockRead();
            XrdMqSharedHash* hash = gOFS->ObjectManager.GetObject(queue.c_str(), "hash");

            if (hash) {
              fsid = (eos::common::FileSystem::fsid_t) hash->GetLongLong("id");
              errc = (int) hash->GetLongLong("stat.errc");
              configstatus = hash->Get("configstatus");
              bootstatus = hash->Get("stat.boot");
              cfgstatus = eos::common::FileSystem::GetConfigStatusFromString(
                            configstatus.c_str());
              bstatus = eos::common::FileSystem::GetStatusFromString(bootstatus.c_str());
            }

            gOFS->ObjectManager.HashMutex.UnLockRead();

            if (fsid && errc &&
                (cfgstatus >= eos::common::FileSystem::kRO) &&
                (bstatus == eos::common::BootStatus::kOpsError)) {
              // Case when we take action and explicitly ask to start a drain job
              eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
              auto it_fs = FsView::gFsView.mIdView.find(fsid);

              if (it_fs != FsView::gFsView.mIdView.end()) {
                it_fs->second->SetConfigStatus(eos::common::FileSystem::kDrain);
              }
            }

            if (fsid && (errc == 0)) {
              if (!gOFS->mIsCentralDrain) {
                // Make sure there is no drain job triggered by a previous
                // filesystem errc!=0
                eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
                auto it_fs = FsView::gFsView.mIdView.find(fsid);

                if (it_fs != FsView::gFsView.mIdView.end()) {
                  it_fs->second->StopDrainJob();
                }
              }
            }
          }
        }

        gOFS->ObjectNotifier.tlSubscriber->mSubjMtx.Lock();
        continue;
      }

      // Handle subject key deletion
      if (event.mType == XrdMqSharedObjectManager::kMqSubjectKeyDeletion) {
        eos_info("msg=\"received deletion on subject\" subject=\"%s\"",
                 newsubject.c_str());
        std::string key = newsubject;
        std::string queue = newsubject;
        size_t dpos = 0;

        if ((dpos = queue.find(";")) != std::string::npos) {
          key.erase(0, dpos + 1);
          queue.erase(dpos);
        }

        gOFS->ConfEngine->DeleteConfigValue(0, key.c_str(), false);
        gOFS->ConfEngine->ApplyKeyDeletion(key.c_str());
        gOFS->ObjectNotifier.tlSubscriber->mSubjMtx.Lock();
        continue;
      }

      eos_warning("msg=\"don't know what to do with subject\" subject=\"%s\"",
                  newsubject.c_str());
      gOFS->ObjectNotifier.tlSubscriber->mSubjMtx.Lock();
      continue;
    }

    gOFS->ObjectNotifier.tlSubscriber->mSubjMtx.UnLock();
  }
}
