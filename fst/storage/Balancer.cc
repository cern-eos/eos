// ----------------------------------------------------------------------
// File: Balancer.cc
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

EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
void
Storage::Balancer ()
{
  eos_static_info("Start Balancer ...");

  std::string nodeconfigqueue = "";
  const char* val = 0;
  // we have to wait that we know our node config queue
  while (!(val = eos::fst::Config::gConfig.FstNodeConfigQueue.c_str()))
  {
    XrdSysTimer sleeper;
    sleeper.Snooze(5);
    eos_static_info("Snoozing ...");
  }

  nodeconfigqueue = eos::fst::Config::gConfig.FstNodeConfigQueue.c_str();

  unsigned int cycler = 0;

  std::map<unsigned int, bool> got_work; // map having the result of last scheduling request
  std::map<unsigned int, time_t> last_asked; // map having the last scheduling request time

  unsigned long long nscheduled = 0;
  unsigned long long totalscheduled = 0;
  unsigned long long totalexecuted = 0;
  while (1)
  {
    eos_static_debug("Doing balancing round ...");

    bool ask = false;
    // ---------------------------------------
    // get some global variables
    // ---------------------------------------
    gOFS.ObjectManager.HashMutex.LockRead();

    XrdMqSharedHash* confighash = gOFS.ObjectManager.GetHash(nodeconfigqueue.c_str());
    std::string manager = confighash ? confighash->Get("manager") : "unknown";
    unsigned long long nparalleltx = confighash ? confighash->GetLongLong("stat.balance.ntx") : 0;
    unsigned long long ratetx = confighash ? confighash->GetLongLong("stat.balance.rate") : 0;

    if (nparalleltx == 0) nparalleltx = 0;
    if (ratetx == 0) ratetx = 25;

    eos_static_debug("manager=%s nparalleltransfers=%llu transferrate=%llu", manager.c_str(), nparalleltx, ratetx);
    gOFS.ObjectManager.HashMutex.UnLockRead();
    // ---------------------------------------

    unsigned int nfs = 0;
    {
      // determin the number of configured filesystems
      eos::common::RWMutexReadLock lock(fsMutex);
      nfs = fileSystemsVector.size();
      totalexecuted = 0;

      // sum up the current execution state e.g. number of jobs taken from the queue
      for (unsigned int s = 0; s < nfs; s++)
      {
        if (s < fileSystemsVector.size())
        {
          totalexecuted += fileSystemsVector[s]->GetBalanceQueue()->GetQueue()->GetJobCount();
        }
      }
      nscheduled = totalscheduled - totalexecuted;
    }

    time_t skiptime = 0;

    for (unsigned int i = 0; i < nfs; i++)
    {
      unsigned int index = (i + cycler) % nfs;
      eos::common::RWMutexReadLock lock(fsMutex);
      if (index < fileSystemsVector.size())
      {
        std::string path = fileSystemsVector[index]->GetPath();
        double nominal = fileSystemsVector[index]->GetDouble("stat.nominal.filled");
        double filled = fileSystemsVector[index]->GetDouble("stat.statfs.filled");
        double threshold = fileSystemsVector[index]->GetDouble("stat.balance.threshold");
        unsigned long id = fileSystemsVector[index]->GetId();

        if (!got_work[index])
        {
          if ((time(NULL) - last_asked[index]) < 60)
          {
            // if we didn't get a file during the last scheduling call we don't ask until 1 minute has passed
            time_t tdiff = time(NULL) - last_asked[index];
            if (!skiptime)
            {
              skiptime = 60 - tdiff;
            }
            else
            {
              // we have to wait the minimum time
              if ((60 - tdiff) < skiptime)
              {
                skiptime = 60 - tdiff;
              }
            }
            continue;
          }
          else
          {
            // after one minute we just ask again
            last_asked[index] = time(NULL);
          }
        }

        skiptime = 0;
        got_work[index] = false;

        eos_static_debug("FileSystem %lu %.02f %.02f", id, filled, nominal);

        // don't adjust more than the deviation defined by threshold
        if ((nominal) && (fabs(filled - threshold) < nominal))
        {
          ask = true;
          // if the fill status is less than nominal we can ask a balancer transfer to the MGM
          unsigned long long freebytes = fileSystemsVector[index]->GetLongLong("stat.statfs.freebytes");

          if (fileSystemsVector[index]->GetBalanceQueue()->GetBandwidth() != ratetx)
          {
            // modify the bandwidth setting for this queue
            fileSystemsVector[index]->GetBalanceQueue()->SetBandwidth(ratetx);
          }

          if (fileSystemsVector[index]->GetBalanceQueue()->GetSlots() != nparalleltx)
          {
            // modify slot settings for this queue
            fileSystemsVector[index]->GetBalanceQueue()->SetSlots(nparalleltx);
          }

          eos::common::FileSystem::fsstatus_t bootstatus = fileSystemsVector[index]->GetStatus();
          eos::common::FileSystem::fsstatus_t configstatus = fileSystemsVector[index]->GetConfigStatus();

          eos_static_info("id=%u nscheduled=%llu nparalleltx=%llu totalscheduled=%llu totalexecuted=%llu", id, nscheduled, nparalleltx, totalscheduled, totalexecuted);

          // ---------------------------------------------------------------------------------------------
          // we balance filesystems which are booted and 'in production' e.g. not draining or down or RO
          // ---------------------------------------------------------------------------------------------

          bool full = false;
          {
            XrdSysMutexHelper(fileSystemFullMapMutex);
            full = fileSystemFullWarnMap[id];
          }

          if ((bootstatus == eos::common::FileSystem::kBooted) &&
              (configstatus > eos::common::FileSystem::kRO) &&
              (!full))
          {

            // we schedule one transfer ahead !!
            if (nscheduled < (nparalleltx + 1))
            {
              XrdOucErrInfo error;
              XrdOucString managerQuery = "/?";
              managerQuery += "mgm.pcmd=schedule2balance";
              managerQuery += "&mgm.target.fsid=";
              char sid[1024];
              snprintf(sid, sizeof (sid) - 1, "%lu", id);
              managerQuery += sid;
              managerQuery += "&mgm.target.freebytes=";
              char sfree[1024];
              snprintf(sfree, sizeof (sfree) - 1, "%llu", freebytes);
              managerQuery += sfree;
              // the log ID to the schedule2balance
              managerQuery += "&mgm.logid=";
              managerQuery += logId;

              XrdOucString response = "";
              int rc = gOFS.CallManager(&error, "/", manager.c_str(), managerQuery, &response);
              if (rc)
              {
                eos_static_err("manager returned errno=%d", rc);
              }
              else
              {
                if (response == "submitted")
                {
                  eos_static_debug("id=%u result=%s", id, response.c_str());
                  totalscheduled++;
                  nscheduled++;
                  got_work[index] = true;
                }
                else
                {
                  eos_static_debug("manager returned no file to schedule [ENODATA]");
                }
              }
            }
            else
            {
              eos_static_info("asking for new job stopped");
              // if all slots are busy anyway, we leave the loop
              break;
            }
          }
        }
      }
    }

    if ((!ask))
    {
      // ---------------------------------------------------------------------------------------------
      // we have no filesystem which is member of a balancing group at the moment
      // ---------------------------------------------------------------------------------------------
      XrdSysTimer sleeper;
      // go to sleep for a while if there was nothing to do
      sleeper.Snooze(60);
      eos::common::RWMutexReadLock lock(fsMutex);
      nfs = fileSystemsVector.size();
      nscheduled = 0;
      totalexecuted = 0;
      for (unsigned int s = 0; s < nfs; s++)
      {
        if (s < fileSystemsVector.size())
        {
          nscheduled += fileSystemsVector[s]->GetBalanceQueue()->GetRunningAndQueued();
          totalexecuted += fileSystemsVector[s]->GetBalanceQueue()->GetQueue()->GetJobCount();
        }
        if (!nscheduled)
        {
          // if there is nothing visible totalexecuted must be set equal to totalscheduled - this is to prevent any kind of 'dead lock' situation by missing a job/message etc.
          totalscheduled = totalexecuted;
        }
      }
    }
    else
    {
      size_t cnt = 0;
      // wait until we have slot's to fill
      while (1)
      {
        eos::common::RWMutexReadLock lock(fsMutex);
        nfs = fileSystemsVector.size();
        totalexecuted = 0;
        for (unsigned int s = 0; s < nfs; s++)
        {
          if (s < fileSystemsVector.size())
          {
            totalexecuted += fileSystemsVector[s]->GetBalanceQueue()->GetQueue()->GetJobCount();
          }
        }
        if (cnt > 100)
        {
          // after 10 seconds we can just look into our queues to see the situation
          nscheduled = 0;
          for (unsigned int s = 0; s < nfs; s++)
          {
            if (s < fileSystemsVector.size())
            {
              nscheduled += fileSystemsVector[s]->GetDrainQueue()->GetRunningAndQueued();
            }
          }
          totalscheduled = totalexecuted + nscheduled;
        }
        else
        {
          nscheduled = (totalscheduled - totalexecuted);
        }

        if (nscheduled < (nparalleltx + 1))
        {
          // free slots, leave the loop
          break;
        }
        XrdSysTimer sleeper;
        sleeper.Wait(100);

        if (cnt > (4 * 3600 * 10))
        {
          eos_static_warning("breaking out slot-wait-loop after 4 hours ...");
          // after 4 hours we just reset the accounting and leave this loop
          totalscheduled = totalexecuted;
          break;
        }
      }
      if (skiptime)
      {
        eos_static_debug("skiptime=%d", skiptime);
        XrdSysTimer sleeper;

        // shouldn't happen
        if (skiptime > 60)
        {
          skiptime = 60;
        }
        sleeper.Snooze(skiptime);
      }
    }
    cycler++;
  }
}

EOSFSTNAMESPACE_END


