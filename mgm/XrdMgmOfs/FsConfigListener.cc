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
void
XrdMgmOfs::FsConfigListener ()
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
{
  // setup the modifications which the fs listener thread is waiting for
  std::string watch_errc = "stat.errc";
  std::string watch_geotag = "stat.geotag";
  bool ok = true;
  ok &= ObjectNotifier.SubscribesToKey("fsconfiglistener",watch_geotag,XrdMqSharedObjectChangeNotifier::kMqSubjectModification); // we need to notify the FsView when a geotag changes to keep the tree structure up-to-date
  ok &= ObjectNotifier.SubscribesToKey("fsconfiglistener",watch_errc,XrdMqSharedObjectChangeNotifier::kMqSubjectModification); // we need to take action an filesystem errors
  ok &= ObjectNotifier.SubscribesToSubject("fsconfiglistener",MgmConfigQueue.c_str(),XrdMqSharedObjectChangeNotifier::kMqSubjectModification);  // we need to apply remote configuration changes
  ok &= ObjectNotifier.SubscribesToSubjectRegex("fsconfiglistener",".*",XrdMqSharedObjectChangeNotifier::kMqSubjectKeyDeletion);  // we need to apply remote configuration changes
  if(!ok)
    eos_crit("error subscribing to shared objects change notifications");

  ObjectNotifier.BindCurrentThread("fsconfiglistener");

  if(!ObjectNotifier.StartNotifyCurrentThread())
    eos_crit("error starting shared objects change notifications");

  //  leave some time to the notifier to start up
  //  XrdSysTimer sleeper;
  //  sleeper.Snooze(5);

  // thread listening on filesystem errors and configuration changes
  do
  {
    gOFS->ObjectNotifier.tlSubscriber->SubjectsSem.Wait();

    XrdSysThread::SetCancelOff();

    // we always take a lock to take something from the queue and then release it
    gOFS->ObjectNotifier.tlSubscriber->SubjectsMutex.Lock();

    // listens on modifications on filesystem objects
    while (gOFS->ObjectNotifier.tlSubscriber->NotificationSubjects.size())
    {
      XrdMqSharedObjectManager::Notification event;
      event = gOFS->ObjectNotifier.tlSubscriber->NotificationSubjects.front();
      gOFS->ObjectNotifier.tlSubscriber->NotificationSubjects.pop_front();
      gOFS->ObjectNotifier.tlSubscriber->SubjectsMutex.UnLock();

      eos_static_debug("MGM shared object notification subject is %s",event.mSubject.c_str());

      std::string newsubject = event.mSubject.c_str();


      if (event.mType == XrdMqSharedObjectManager::kMqSubjectCreation)
      {
        // ---------------------------------------------------------------------
        // handle subject creation
        // ---------------------------------------------------------------------
        eos_static_debug("received creation on subject %s\n", newsubject.c_str());
        gOFS->ObjectNotifier.tlSubscriber->SubjectsMutex.Lock();
        continue;
      }

      if (event.mType == XrdMqSharedObjectManager::kMqSubjectDeletion)
      {
        // ---------------------------------------------------------------------
        // handle subject deletion
        // ---------------------------------------------------------------------
        eos_static_debug("received deletion on subject %s\n", newsubject.c_str());

        gOFS->ObjectNotifier.tlSubscriber->SubjectsMutex.Lock();
        continue;
      }

      if (event.mType == XrdMqSharedObjectManager::kMqSubjectModification)
      {
        // ---------------------------------------------------------------------
        // handle subject modification
        // ---------------------------------------------------------------------

        eos_static_debug("received modification on subject %s", newsubject.c_str());
        // if this is an error status on a file system, check if the filesystem is > drained state and in this case launch a drain job with
        // the opserror flag by calling StartDrainJob
        // We use directly the ObjectManager Interface because it is more handy with the available information we have at this point

        std::string key = newsubject;
        std::string queue = newsubject;
        size_t dpos = 0;
        if ((dpos = queue.find(";")) != std::string::npos)
        {
          key.erase(0, dpos + 1);
          queue.erase(dpos);
        }

        if (queue == MgmConfigQueue.c_str())
        {
          // -------------------------------------------------------------------
          // this is an MGM configuration modification
          // -------------------------------------------------------------------
          if (!gOFS->MgmMaster.IsMaster())
          {
            // only an MGM slave needs to aplly this

            gOFS->ObjectManager.HashMutex.LockRead();
            XrdMqSharedHash* hash = gOFS->ObjectManager.GetObject(queue.c_str(), "hash");
            if (hash)
            {
              XrdOucString err;
              XrdOucString value = hash->Get(key).c_str();
              gOFS->ObjectManager.HashMutex.UnLockRead();
              if (value.c_str())
              {
                // here we might get a change without the namespace, in this case we add the global namespace
                if ((key.substr(0, 4) != "map:") &&
                    (key.substr(0, 3) != "fs:") &&
                    (key.substr(0, 6) != "quota:") &&
                    (key.substr(0, 4) != "vid:") &&
                    (key.substr(0, 7) != "policy:"))
                {
                  XrdOucString skey = key.c_str();
                  eos_info("Calling Apply for %s %s", key.c_str(), value.c_str());
                  Access::ApplyAccessConfig(false);

                  if (skey.beginswith("iostat:"))
                  {
                    gOFS->IoStats.ApplyIostatConfig();
                  }
                  if (skey.beginswith("fsck"))
                  {
                    gOFS->FsCheck.ApplyFsckConfig();
                  }
                }
                else
                {
                  eos_info("Call SetConfig %s %s", key.c_str(), value.c_str());

                  gOFS->ConfEngine->SetConfigValue(0,
                                                   key.c_str(),
                                                   value.c_str(),
                                                   false);

                  gOFS->ConfEngine->ApplyEachConfig(key.c_str(), &value, (void*) &err);
                }
              }
            }
            else
            {
              gOFS->ObjectManager.HashMutex.UnLockRead();
            }
          }
        }
        else if(key == watch_geotag)
        {
          // -------------------------------------------------------------------
          // this is a geotag update
          // -------------------------------------------------------------------
          eos::common::FileSystem::fsid_t fsid = 0;
        	std::string newgeotag,oldgeotag;
          FileSystem* fs = 0;
          // read the id from the hash and the new geotag
          gOFS->ObjectManager.HashMutex.LockRead();
          XrdMqSharedHash* hash = gOFS->ObjectManager.GetObject(queue.c_str(), "hash");
          if (hash)
          {
            fsid = (eos::common::FileSystem::fsid_t) hash->GetLongLong("id");
        		oldgeotag = newgeotag = hash->Get("stat.geotag");
          }
          gOFS->ObjectManager.HashMutex.UnLockRead();

        	FsView::gFsView.ViewMutex.LockRead();
        	fs = FsView::gFsView.mIdView[fsid];
        	if(fs && FsView::gFsView.mNodeView.count(fs->GetQueue()))
        	{
        		// check if the change notification is an actual change in the geotag
        		FsNode* node = FsView::gFsView.mNodeView[fs->GetQueue()];
        		static_cast<GeoTree*>(node)->getGeoTagInTree(fsid , oldgeotag);
        		oldgeotag.erase(0,8); // to get rid of the "<ROOT>::" prefix
        	}

        	if( oldgeotag != newgeotag)
          {
        		eos_warning("Received a geotag change for fsid %lu new geotag is %s, old geotag was %s ",(unsigned long)fsid,newgeotag.c_str(),oldgeotag.c_str());

        		{
        			FsView::gFsView.ViewMutex.UnLockRead();

            eos::common::RWMutexWriteLock lock(FsView::gFsView.ViewMutex);
            eos::common::FileSystem::fs_snapshot snapshot;
            if (FsView::gFsView.mIdView.count(fsid))
              fs = FsView::gFsView.mIdView[fsid];
            if(fs)
            {
              fs->SnapShotFileSystem(snapshot);

              //----------------------------------------------------------------
              //! update node view tree structure
              //----------------------------------------------------------------
              if (FsView::gFsView.mNodeView.count(snapshot.mQueue))
              {
                FsNode* node = FsView::gFsView.mNodeView[snapshot.mQueue];
                eos_static_info("updating geotag of fsid %lu in node %s",(unsigned long)fsid,node->mName.c_str());
                if(!static_cast<GeoTree*>(node)->erase(fsid))
                  eos_static_err("error removing fsid %lu from node %s",(unsigned long)fsid,node->mName.c_str());
                if(!static_cast<GeoTree*>(node)->insert(fsid))
                  eos_static_err("error inserting fsid %lu into node %s",(unsigned long)fsid,node->mName.c_str());
              }

              //----------------------------------------------------------------
              //! update group view tree structure
              //----------------------------------------------------------------
              if (FsView::gFsView.mGroupView.count(snapshot.mGroup))
              {
                FsGroup* group = FsView::gFsView.mGroupView[snapshot.mGroup];
                eos_static_info("updating geotag of fsid %lu in group %s",(unsigned long)fsid,group->mName.c_str());
                if(!static_cast<GeoTree*>(group)->erase(fsid))
                eos_static_err("error removing fsid %lu from group %s",(unsigned long)fsid,group->mName.c_str());
                if(!static_cast<GeoTree*>(group)->insert(fsid))
                eos_static_err("error inserting fsid %lu into group %s",(unsigned long)fsid,group->mName.c_str());
              }

              //----------------------------------------------------------------
              //! update space view tree structure
              //----------------------------------------------------------------
              if (FsView::gFsView.mSpaceView.count(snapshot.mSpace))
              {
                FsSpace* space = FsView::gFsView.mSpaceView[snapshot.mSpace];
                eos_static_info("updating geotag of fsid %lu in space %s",(unsigned long)fsid,space->mName.c_str());
                if(!static_cast<GeoTree*>(space)->erase(fsid))
                eos_static_err("error removing fsid %lu from space %s",(unsigned long)fsid,space->mName.c_str());
                if(!static_cast<GeoTree*>(space)->insert(fsid))
                eos_static_err("error inserting fsid %lu into space %s",(unsigned long)fsid,space->mName.c_str());
              }
            }
          }
        	}
        	else
        		FsView::gFsView.ViewMutex.UnLockRead();
        }
        else
        {
          // -------------------------------------------------------------------
          // this is a filesystem status error
          // -------------------------------------------------------------------
          if (gOFS->MgmMaster.IsMaster())
          {
            // only an MGM master needs to initiate draining
            eos::common::FileSystem::fsid_t fsid = 0;
            FileSystem* fs = 0;
            long long errc = 0;
            std::string configstatus = "";
            std::string bootstatus = "";
            int cfgstatus = 0;
            int bstatus = 0;

            // read the id from the hash and the current error value
            gOFS->ObjectManager.HashMutex.LockRead();
            XrdMqSharedHash* hash = gOFS->ObjectManager.GetObject(queue.c_str(), "hash");
            if (hash)
            {
              fsid = (eos::common::FileSystem::fsid_t) hash->GetLongLong("id");
              errc = (int) hash->GetLongLong("stat.errc");
              configstatus = hash->Get("configstatus");
              bootstatus = hash->Get("stat.boot");
              cfgstatus = eos::common::FileSystem::GetConfigStatusFromString(configstatus.c_str());
              bstatus = eos::common::FileSystem::GetStatusFromString(bootstatus.c_str());
            }
            gOFS->ObjectManager.HashMutex.UnLockRead();

            if (fsid && errc && (cfgstatus >= eos::common::FileSystem::kRO) && (bstatus == eos::common::FileSystem::kOpsError))
            {
              // this is the case we take action and explicitly ask to start a drain job
              eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
              if (FsView::gFsView.mIdView.count(fsid))
                fs = FsView::gFsView.mIdView[fsid];
              else
                fs = 0;
              if (fs)
              {
                fs->StartDrainJob();
              }
            }
            if (fsid && (!errc))
            {
              // make sure there is no drain job triggered by a previous filesystem errc!=0
              eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
              if (FsView::gFsView.mIdView.count(fsid))
                fs = FsView::gFsView.mIdView[fsid];
              else
                fs = 0;
              if (fs)
              {
                fs->StopDrainJob();
              }
            }
          }
        }

        gOFS->ObjectNotifier.tlSubscriber->SubjectsMutex.Lock();
        continue;
      }

      if (event.mType == XrdMqSharedObjectManager::kMqSubjectKeyDeletion)
      {
        // ---------------------------------------------------------------------
        // handle subject key deletion
        // ---------------------------------------------------------------------
        eos_static_info("received deletion on subject %s\n", newsubject.c_str());

        std::string key = newsubject;
        std::string queue = newsubject;
        size_t dpos = 0;
        if ((dpos = queue.find(";")) != std::string::npos)
        {
          key.erase(0, dpos + 1);
          queue.erase(dpos);
        }

        gOFS->ConfEngine->DeleteConfigValue(0,
                                            key.c_str(),
                                            false);

        gOFS->ConfEngine->ApplyKeyDeletion(key.c_str());

        gOFS->ObjectNotifier.tlSubscriber->SubjectsMutex.Lock();
        continue;
      }
      eos_static_warning("msg=\"don't know what to do with subject\" subject=%s", newsubject.c_str());
      gOFS->ObjectNotifier.tlSubscriber->SubjectsMutex.Lock();
      continue;
    }
    gOFS->ObjectNotifier.tlSubscriber->SubjectsMutex.UnLock();
    XrdSysThread::SetCancelOff();
  }
  while (1);
}

