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
    bool unlocked;

    eos_static_debug("received shared object notification ...");

    /////////////////////////////////////////////////////////////////////////////////////
    // => implements the creation of filesystem objects
    /////////////////////////////////////////////////////////////////////////////////////

    unlocked = false;
    gOFS.ObjectManager.SubjectsMutex.Lock(); // here we have to take care that we lock this only to retrieve the subject ... to create a new queue we have to free the lock

    while (gOFS.ObjectManager.CreationSubjects.size())
    {
      std::string newsubject = gOFS.ObjectManager.CreationSubjects.front();
      gOFS.ObjectManager.CreationSubjects.pop_front();
      gOFS.ObjectManager.SubjectsMutex.UnLock();
      unlocked = true;

      XrdOucString queue = newsubject.c_str();

      if (queue == Config::gConfig.FstQueueWildcard)
        continue;

      if ((queue.find("/txqueue/") != STR_NPOS))
      {
        // this is a transfer queue we, don't need to take action
        continue;
      }

      if (!queue.beginswith(Config::gConfig.FstQueue))
      {
        if (queue.beginswith("/config/") && queue.endswith(Config::gConfig.FstHostPort))
        {
          // this is the configuration entry and we should store it to have access to it since it's name depends on the instance name and we don't know (yet)
          Config::gConfig.FstNodeConfigQueue = queue;
          eos_static_info("storing config queue name <%s>", Config::gConfig.FstNodeConfigQueue.c_str());
        }
        else
        {
          eos_static_info("no action on creation of subject <%s> - we are <%s>", newsubject.c_str(), Config::gConfig.FstQueue.c_str());
        }
        continue;
      }
      else
      {
        eos_static_info("received creation notification of subject <%s> - we are <%s>", newsubject.c_str(), Config::gConfig.FstQueue.c_str());
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
    }

    if (!unlocked)
    {
      gOFS.ObjectManager.SubjectsMutex.UnLock();
      unlocked = true;
    }

    /////////////////////////////////////////////////////////////////////////////////////
    // => implements the deletion of filesystem objects
    /////////////////////////////////////////////////////////////////////////////////////    

    unlocked = false;
    gOFS.ObjectManager.SubjectsMutex.Lock(); // here we have to take care that we lock this only to retrieve the subject ... to create a new queue we have to free the lock

    // implements the deletion of filesystem objects
    if (gOFS.ObjectManager.DeletionSubjects.size())
    {
      std::string newsubject = gOFS.ObjectManager.DeletionSubjects.front();
      gOFS.ObjectManager.DeletionSubjects.pop_front();
      gOFS.ObjectManager.SubjectsMutex.UnLock();
      unlocked = true;

      XrdOucString queue = newsubject.c_str();

      if ((queue.find("/txqueue/") != STR_NPOS))
      {
        // this is a transfer queue we, don't need to take action
        continue;
      }

      if (!queue.beginswith(Config::gConfig.FstQueue))
      {
        eos_static_err("illegal subject found in deletion list <%s> - we are <%s>", newsubject.c_str(), Config::gConfig.FstQueue.c_str());
        continue;
      }
      else
      {
        eos_static_info("received deletion notification of subject <%s> - we are <%s>", newsubject.c_str(), Config::gConfig.FstQueue.c_str());
      }

      if (0)
      {
        // --------------------------------------------------------------------------------------------------------
        // deletion of filesystem objects is a problem, we delete them only if we are not in healthy rw state
        // --------------------------------------------------------------------------------------------------------
        eos::common::RWMutexWriteLock lock(fsMutex);
        if (fileSystems.count(queue.c_str()))
        {
          std::map<eos::common::FileSystem::fsid_t, FileSystem*>::iterator mit;

          eos::common::FileSystem::fsstatus_t bootstatus = fileSystems[queue.c_str()]->GetStatus();
          eos::common::FileSystem::fsstatus_t configstatus = fileSystems[queue.c_str()]->GetConfigStatus();

          if ((bootstatus != eos::common::FileSystem::kBooted) ||
              (configstatus == eos::common::FileSystem::kRW))
          {
            for (mit = fileSystemsMap.begin(); mit != fileSystemsMap.end(); mit++)
            {
              if (mit->second == fileSystems[queue.c_str()])
              {
                fileSystemsMap.erase(mit);
                break;
              }
            }

            std::vector <FileSystem*>::iterator it;
            for (it = fileSystemsVector.begin(); it != fileSystemsVector.end(); it++)
            {
              if (*it == fileSystems[queue.c_str()])
              {
                fileSystemsVector.erase(it);
                break;
              }
            }
            delete fileSystems[queue.c_str()];
            fileSystems.erase(queue.c_str());
            eos_static_info("deleting filesystem %s", queue.c_str());
          }
          else
          {
            eos_static_info("keeping filesystem %s alive", queue.c_str());
          }
        }
      }
    }

    if (!unlocked)
    {
      gOFS.ObjectManager.SubjectsMutex.UnLock();
      unlocked = true;
    }


    /////////////////////////////////////////////////////////////////////////////////////
    // => implements the modification notification of filesystem objects
    /////////////////////////////////////////////////////////////////////////////////////    

    unlocked = false;
    gOFS.ObjectManager.SubjectsMutex.Lock(); // here we have to take care that we lock this only to retrieve the subject ... to create a new queue we have to free the lock

    // listens on modifications on filesystem objects
    if (gOFS.ObjectManager.ModificationSubjects.size())
    {
      std::string newsubject = gOFS.ObjectManager.ModificationSubjects.front();
      gOFS.ObjectManager.ModificationSubjects.pop_front();
      gOFS.ObjectManager.SubjectsMutex.UnLock();
      unlocked = true;

      XrdOucString queue = newsubject.c_str();

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
            //	    gOFS.SetSimulationError(value.c_str());
          }
          gOFS.ObjectManager.HashMutex.UnLockRead();
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
    }

    if (!unlocked)
    {
      gOFS.ObjectManager.SubjectsMutex.UnLock();
      unlocked = true;
    }
  }
}

EOSFSTNAMESPACE_END


