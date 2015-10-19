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

/*----------------------------------------------------------------------------*/
#include "fst/storage/Storage.hh"
#include "fst/XrdFstOfs.hh"
#ifdef KINETICIO_FOUND
#include <kio/KineticIoFactory.hh>
#include <kio/LoggingException.hh>
#endif

/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
void
Storage::Communicator ()
{
  eos_static_info("Communicator activated ...");

  while (1)
  {
    // wait for new filesystem definitions
    gOFS.ObjectManager.SubjectsSem.Wait();

    XrdSysThread::SetCancelOff();

    eos_static_debug("received shared object notification ...");

    // we always take a lock to take something from the queue and then release it
    gOFS.ObjectManager.SubjectsMutex.Lock();

    while (gOFS.ObjectManager.NotificationSubjects.size())
    {
      XrdMqSharedObjectManager::Notification event;
      event = gOFS.ObjectManager.NotificationSubjects.front();
      gOFS.ObjectManager.NotificationSubjects.pop_front();
      gOFS.ObjectManager.SubjectsMutex.UnLock();

      XrdOucString queue = event.mSubject.c_str();

      if (event.mType == XrdMqSharedObjectManager::kMqSubjectCreation)
      {
        // ---------------------------------------------------------------------
        // handle subject creation
        // ---------------------------------------------------------------------

        if (queue == Config::gConfig.FstQueueWildcard)
        {
          gOFS.ObjectManager.SubjectsMutex.Lock();
          continue;
        }

        if ((queue.find("/txqueue/") != STR_NPOS))
        {
          // this is a transfer queue we, don't need to take action
          gOFS.ObjectManager.SubjectsMutex.Lock();
          continue;
        }

        if (!queue.beginswith(Config::gConfig.FstQueue))
        {
          // ! endswith seems to be buggy if the comparable suffix is longer than the string !
          if (queue.beginswith("/config/") && (queue.length() > Config::gConfig.FstHostPort.length()) && queue.endswith(Config::gConfig.FstHostPort))
          {
            // this is the configuration entry and we should store it to have access to it since it's name depends on the instance name and we don't know (yet)
            Config::gConfig.FstNodeConfigQueue = queue;
            eos_static_info("storing config queue name <%s>", Config::gConfig.FstNodeConfigQueue.c_str());
          }
          else
          {
            eos_static_info("no action on creation of subject <%s> - we are <%s>", event.mSubject.c_str(), Config::gConfig.FstQueue.c_str());
          }
          gOFS.ObjectManager.SubjectsMutex.Lock();
          continue;
        }
        else
        {
          eos_static_info("received creation notification of subject <%s> - we are <%s>", event.mSubject.c_str(), Config::gConfig.FstQueue.c_str());
        }

        eos::common::RWMutexWriteLock lock(fsMutex);
        FileSystem* fs = 0;

        if (!(fileSystems.count(queue.c_str())))
        {
          fs = new FileSystem(queue.c_str(), Config::gConfig.FstQueue.c_str(), &gOFS.ObjectManager);
          fileSystems[queue.c_str()] = fs;
          fileSystemsVector.push_back(fs);
          fileSystemsMap[fs->GetId()] = fs;
          eos_static_info("setting up filesystem %s", queue.c_str());
          fs->SetStatus(eos::common::FileSystem::kDown);
        }
        gOFS.ObjectManager.SubjectsMutex.Lock();
        continue;
      }


      if (event.mType == XrdMqSharedObjectManager::kMqSubjectDeletion)
      {
        // ---------------------------------------------------------------------
        // handle subject deletion
        // ---------------------------------------------------------------------

        if ((queue.find("/txqueue/") != STR_NPOS))
        {
          // this is a transfer queue we, don't need to take action
          gOFS.ObjectManager.SubjectsMutex.Lock();
          continue;
        }

        if (!queue.beginswith(Config::gConfig.FstQueue))
        {
          eos_static_err("illegal subject found in deletion list <%s> - we are <%s>", event.mSubject.c_str(), Config::gConfig.FstQueue.c_str());
          gOFS.ObjectManager.SubjectsMutex.Lock();
          continue;
        }
        else
        {
          eos_static_info("received deletion notification of subject <%s> - we are <%s>", event.mSubject.c_str(), Config::gConfig.FstQueue.c_str());
        }

        // we don't delete filesystem objects anymore ...
        gOFS.ObjectManager.SubjectsMutex.Lock();
        continue;
      }

      if (event.mType == XrdMqSharedObjectManager::kMqSubjectModification)
      {
        // ---------------------------------------------------------------------
        // handle subject modification
        // ---------------------------------------------------------------------

        // seperate <path> from <key>
        XrdOucString key = queue;
        int dpos = 0;
        if ((dpos = queue.find(";")) != STR_NPOS)
        {
          key.erase(0, dpos + 1);
          queue.erase(dpos);
        }

        if (queue == Config::gConfig.FstNodeConfigQueue)
        {
          if (key == "symkey")
          {
            gOFS.ObjectManager.HashMutex.LockRead();
            // we received a new symkey
            XrdMqSharedHash* hash = gOFS.ObjectManager.GetObject(queue.c_str(), "hash");
            if (hash)
            {
              std::string symkey = hash->Get("symkey");
              eos_static_info("symkey=%s", symkey.c_str());
              eos::common::gSymKeyStore.SetKey64(symkey.c_str(), 0);
            }
            gOFS.ObjectManager.HashMutex.UnLockRead();
          }

          if (key == "manager")
          {
            gOFS.ObjectManager.HashMutex.LockRead();
            // we received a manager
            XrdMqSharedHash* hash = gOFS.ObjectManager.GetObject(queue.c_str(), "hash");
            if (hash)
            {
              std::string manager = hash->Get("manager");
              eos_static_info("manager=%s", manager.c_str());
              XrdSysMutexHelper lock(Config::gConfig.Mutex);
              Config::gConfig.Manager = manager.c_str();
            }
            gOFS.ObjectManager.HashMutex.UnLockRead();
          }

          if (key == "publish.interval")
          {
            gOFS.ObjectManager.HashMutex.LockRead();
            // we received a manager
            XrdMqSharedHash* hash = gOFS.ObjectManager.GetObject(queue.c_str(), "hash");
            if (hash)
            {
              std::string publishinterval = hash->Get("publish.interval");
              eos_static_info("publish.interval=%s", publishinterval.c_str());
              XrdSysMutexHelper lock(Config::gConfig.Mutex);
              Config::gConfig.PublishInterval = atoi(publishinterval.c_str());
            }
            gOFS.ObjectManager.HashMutex.UnLockRead();
          }

          if (key == "debug.level")
          {
            gOFS.ObjectManager.HashMutex.LockRead();
            // we received a manager
            XrdMqSharedHash* hash = gOFS.ObjectManager.GetObject(queue.c_str(), "hash");
            if (hash)
            {
              std::string debuglevel = hash->Get("debug.level");
              int debugval = eos::common::Logging::GetPriorityByString(debuglevel.c_str());
              if (debugval < 0)
              {
                eos_static_err("debug level %s is not known!", debuglevel.c_str());
              }
              else
              {
                // we set the shared hash debug for the lowest 'debug' level
                if (debuglevel == "debug")
                {
                  gOFS.ObjectManager.SetDebug(true);
                }
                else
                {
                  gOFS.ObjectManager.SetDebug(false);
                }

                eos::common::Logging::SetLogPriority(debugval);
              }
            }
            gOFS.ObjectManager.HashMutex.UnLockRead();
          }

          // creation/deletion of gateway transfer queue
          if (key == "txgw")
          {
            gOFS.ObjectManager.HashMutex.LockRead();
            XrdMqSharedHash* hash = gOFS.ObjectManager.GetObject(queue.c_str(), "hash");
            if (hash)
            {
              std::string gw = hash->Get("txgw");
              eos_static_info("txgw=%s", gw.c_str());

              gOFS.ObjectManager.HashMutex.UnLockRead();

              if (gw == "off")
              {
                // just stop the multiplexer
                mGwMultiplexer.Stop();
                eos_static_info("Stopping transfer multiplexer on %s", queue.c_str());
              }

              if (gw == "on")
              {
                mGwMultiplexer.Run();
                eos_static_info("Starting transfer multiplexer on %s", queue.c_str());
              }
            }
            else
            {
              gOFS.ObjectManager.HashMutex.UnLockRead();
              eos_static_warning("Cannot get hash(queue)");
            }
          }

          if (key == "gw.rate")
          {
            // modify the rate settings of the gw multiplexer
            gOFS.ObjectManager.HashMutex.LockRead();
            XrdMqSharedHash* hash = gOFS.ObjectManager.GetObject(queue.c_str(), "hash");
            if (hash)
            {
              std::string rate = hash->Get("gw.rate");
              eos_static_info("cmd=set gw.rate=%s", rate.c_str());
              mGwMultiplexer.SetBandwidth(atoi(rate.c_str()));
            }
            gOFS.ObjectManager.HashMutex.UnLockRead();
          }

          if (key == "gw.ntx")
          {
            // modify the parallel transfer settings of the gw multiplexer
            gOFS.ObjectManager.HashMutex.LockRead();
            XrdMqSharedHash* hash = gOFS.ObjectManager.GetObject(queue.c_str(), "hash");
            if (hash)
            {
              std::string ntx = hash->Get("gw.ntx");
              eos_static_info("cmd=set gw.ntx=%s", ntx.c_str());
              mGwMultiplexer.SetSlots(atoi(ntx.c_str()));
            }
            gOFS.ObjectManager.HashMutex.UnLockRead();
          }

          if (key == "error.simulation")
          {
            gOFS.ObjectManager.HashMutex.LockRead();
            XrdMqSharedHash* hash = gOFS.ObjectManager.GetObject(queue.c_str(), "hash");
            if (hash)
            {
              std::string value = hash->Get("error.simulation");
              eos_static_info("cmd=set error.simulation=%s", value.c_str());
              gOFS.SetSimulationError(value.c_str());
            }
            gOFS.ObjectManager.HashMutex.UnLockRead();
          }

          if (key == "kinetic.reload")
          {
            bool do_reload = false;
            gOFS.ObjectManager.HashMutex.LockRead();
            XrdMqSharedHash* hash = gOFS.ObjectManager.GetObject(queue.c_str(), "hash");
            if (hash)
            {
              // retrieve new keys
              std::string space = hash->Get("kinetic.reload");
              eos_static_info("cmd=set kinetic.reload=%s", space.c_str());
              std::string kinetic_cluster_key = "kinetic.cluster.";
              kinetic_cluster_key += space;
              std::string kinetic_location_key = "kinetic.location.";
              kinetic_location_key += space;
              std::string kinetic_security_key = "kinetic.security.";
              kinetic_security_key += space;

              // base64 decode new keys
              XrdOucString k_cluster_64 = hash->Get(kinetic_cluster_key).c_str();
              XrdOucString k_location_64 = hash->Get(kinetic_location_key).c_str();
              XrdOucString k_security_64 = hash->Get(kinetic_security_key).c_str();
              XrdOucString k_cluster;
              XrdOucString k_location;
              XrdOucString k_security;

              eos::common::SymKey::DeBase64(k_cluster_64, k_cluster);
              eos::common::SymKey::DeBase64(k_location_64, k_location);
              eos::common::SymKey::DeBase64(k_security_64, k_security);

              if (k_cluster.length() && k_location.length() && k_security.length())
              {
                eos_static_info("msg=\"reloading kinetic configuration\" space=%s", space.c_str());
                eos_static_debug("\n%s", k_cluster.c_str());
                eos_static_debug("\n%s", k_location.c_str());
                eos_static_debug("'\n%s", k_security.c_str());
                // store the decoded json strings in the environment
                setenv("KINETIC_DRIVE_LOCATION", k_location.c_str(), 1);
                setenv("KINETIC_DRIVE_SECURITY", k_security.c_str(), 1);
                setenv("KINETIC_CLUSTER_DEFINITION", k_cluster.c_str(), 1);
                do_reload = true;
              }
            }
            gOFS.ObjectManager.HashMutex.UnLockRead();

#ifdef KINETICIO_FOUND
            if (do_reload)
            {
              try
              {
                kio::Factory::reloadConfiguration();
              }
              catch (kio::LoggingException &e)
              {
                eos_static_crit("msg=\"reload of kinetic configuration failed\" exception=\"%s\"", e.what());
              }
            }
#endif
          }
        }
        else
        {
          fsMutex.LockRead();
          if ((fileSystems.count(queue.c_str())))
          {
            eos_static_info("got modification on <subqueue>=%s <key>=%s", queue.c_str(), key.c_str());

            gOFS.ObjectManager.HashMutex.LockRead();

            XrdMqSharedHash* hash = gOFS.ObjectManager.GetObject(queue.c_str(), "hash");
            if (hash)
            {
              if (key == "id")
              {
                unsigned int fsid = hash->GetUInt(key.c_str());
                gOFS.ObjectManager.HashMutex.UnLockRead();

                if ((!fileSystemsMap.count(fsid)) || (fileSystemsMap[fsid] != fileSystems[queue.c_str()]))
                {
                  fsMutex.UnLockRead();
                  fsMutex.LockWrite();
                  // setup the reverse lookup by id

                  fileSystemsMap[fsid] = fileSystems[queue.c_str()];
                  eos_static_info("setting reverse lookup for fsid %u", fsid);
                  fsMutex.UnLockWrite();
                  fsMutex.LockRead();
                }
                // check if we are autobooting
                if (eos::fst::Config::gConfig.autoBoot && (fileSystems[queue.c_str()]->GetStatus() <= eos::common::FileSystem::kDown) && (fileSystems[queue.c_str()]->GetConfigStatus() > eos::common::FileSystem::kOff))
                {
                  // start a boot thread
                  RunBootThread(fileSystems[queue.c_str()]);
                }
              }
              else
              {
                if (key == "bootsenttime")
                {
                  gOFS.ObjectManager.HashMutex.UnLockRead();
                  // this is a request to (re-)boot a filesystem
                  if (fileSystems.count(queue.c_str()))
                  {
                    if ((fileSystems[queue.c_str()]->GetInternalBootStatus() == eos::common::FileSystem::kBooted))
                    {
                      if (fileSystems[queue.c_str()]->GetLongLong("bootcheck"))
                      {
                        eos_static_info("queue=%s status=%d check=%lld msg='boot enforced'", queue.c_str(), fileSystems[queue.c_str()]->GetStatus(), fileSystems[queue.c_str()]->GetLongLong("bootcheck"));
                        RunBootThread(fileSystems[queue.c_str()]);
                      }
                      else
                      {
                        eos_static_info("queue=%s status=%d check=%lld msg='skip boot - we are already booted'", queue.c_str(), fileSystems[queue.c_str()]->GetStatus(), fileSystems[queue.c_str()]->GetLongLong("bootcheck"));
                        fileSystems[queue.c_str()]->SetStatus(eos::common::FileSystem::kBooted);
                      }
                    }
                    else
                    {
                      eos_static_info("queue=%s status=%d check=%lld msg='booting - we are not booted yet'", queue.c_str(), fileSystems[queue.c_str()]->GetStatus(), fileSystems[queue.c_str()]->GetLongLong("bootcheck"));
                      // start a boot thread;
                      RunBootThread(fileSystems[queue.c_str()]);
                    }
                  }
                  else
                  {
                    eos_static_err("got boot time update on not existant filesystem %s", queue.c_str());
                  }
                }
                else
                {
                  if (key == "scaninterval")
                  {
                    gOFS.ObjectManager.HashMutex.UnLockRead();
                    if (fileSystems.count(queue.c_str()))
                    {
                      time_t interval = (time_t) fileSystems[queue.c_str()]->GetLongLong("scaninterval");
                      if (interval > 0)
                      {
                        fileSystems[queue.c_str()]->RunScanner(&fstLoad, interval);
                      }
                    }
                  }
                  else
                  {
                    gOFS.ObjectManager.HashMutex.UnLockRead();
                  }
                }
              }
            }
            else
            {
              gOFS.ObjectManager.HashMutex.UnLockRead();
            }
          }
          else
          {
            eos_static_err("illegal subject found - no filesystem object existing for modification %s;%s", queue.c_str(), key.c_str());
          }
          fsMutex.UnLockRead();
        }
        gOFS.ObjectManager.SubjectsMutex.Lock();
        continue;
      }
    }
    gOFS.ObjectManager.SubjectsMutex.UnLock();
    XrdSysThread::SetCancelOn();
  }
}

EOSFSTNAMESPACE_END


