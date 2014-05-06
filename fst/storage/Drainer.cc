// ----------------------------------------------------------------------
// File: Drainer.cc
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
Storage::WaitConfigQueue (std::string & nodeconfigqueue)
/*----------------------------------------------------------------------------*/
/**
 * @brief wait until the configuration queue is defined
 * @param nodeconfigqueue configuration queue for our FST
 */
/*----------------------------------------------------------------------------*/
{
  const char* val = 0;

  while (!(val = eos::fst::Config::gConfig.FstNodeConfigQueue.c_str()))
  {
    XrdSysTimer sleeper;
    sleeper.Snooze(5);
    eos_static_info("Snoozing ...");
  }

  nodeconfigqueue = eos::fst::Config::gConfig.FstNodeConfigQueue.c_str();
}

/*----------------------------------------------------------------------------*/
void
Storage::GetDrainSlotVariables (unsigned long long &nparalleltx,
                                unsigned long long &ratetx,
                                std::string nodeconfigqueue,
                                std::string &manager
                                )
/*----------------------------------------------------------------------------*/
/**
 * @brief get the parallel transfer and transfer rate settings
 * @param nparalleltx number of parallel transfers to run
 * @param ratex rate per transfer
 * @param nodeconfigqueue config queue to use
 */
/*----------------------------------------------------------------------------*/
{
  gOFS.ObjectManager.HashMutex.LockRead();

  XrdMqSharedHash* confighash = gOFS.ObjectManager.GetHash(nodeconfigqueue.c_str());
  manager = confighash ? confighash->Get("manager") : "unknown";
  nparalleltx = confighash ? confighash->GetLongLong("stat.drain.ntx") : 0;
  ratetx = confighash ? confighash->GetLongLong("stat.drain.rate") : 0;

  if (nparalleltx == 0) nparalleltx = 0;
  if (ratetx == 0) ratetx = 25;

  eos_static_debug("manager=%s nparalleltransfers=%llu transferrate=%llu",
                   manager.c_str(),
                   nparalleltx,
                   ratetx);
  gOFS.ObjectManager.HashMutex.UnLockRead();
}

/*----------------------------------------------------------------------------*/
unsigned long long
Storage::GetScheduledDrainJobs (unsigned long long totalscheduled,
                                unsigned long long &totalexecuted)
/*----------------------------------------------------------------------------*/
/**
 * @brief return the number of already scheduled jobs
 * @param totalscheduled the total number of scheduled jobs
 * @param totalexecuted the total number of executed jobs
 * @return number of scheduled jobs
 * 
 * The time delay from scheduling on MGM and appearing in the queue on the FST
 * creates an accounting problem. The returned value is the currently known
 * value on the FST which can be wrong e.g. too small!
 */
/*----------------------------------------------------------------------------*/
{
  unsigned int nfs = 0;
  unsigned long long nscheduled = 0;
  {
    eos::common::RWMutexReadLock lock(fsMutex);
    nfs = fileSystemsVector.size();
    totalexecuted = 0;

    // sum up the current execution state e.g. number of jobs taken from the queue
    for (unsigned int s = 0; s < nfs; s++)
    {
      if (s < fileSystemsVector.size())
      {
        totalexecuted += fileSystemsVector[s]->GetDrainQueue()->GetDone();
      }
    }
    if (totalscheduled < totalexecuted)
      nscheduled = 0;

    else
      nscheduled = totalscheduled - totalexecuted;
  }

  eos_static_debug("nscheduled=%llu totalscheduled=%llu totalexecuted=%llu", nscheduled, totalscheduled, totalexecuted);
  return nscheduled;
}

/*----------------------------------------------------------------------------*/
unsigned long long
Storage::WaitFreeDrainSlot (unsigned long long &nparalleltx,
                            unsigned long long &totalscheduled,
                            unsigned long long &totalexecuted)
/*----------------------------------------------------------------------------*/
/**
 * @brief wait that there is a free slot to schedule a new drain
 * @param nparalleltx number of parallel transfers
 * @param totalscheduled number of total scheduled transfers
 * @param totalexecuted number of total executed transfers
 * @return number of used drain slots
 */
/*----------------------------------------------------------------------------*/
{
  size_t sleep_count = 0;
  unsigned long long nscheduled = 0;
  XrdSysTimer sleeper;

  while (1)
  {
    nscheduled = GetScheduledDrainJobs(totalscheduled, totalexecuted);
    if (nscheduled <= nparalleltx)
      break;
    sleep_count++;
    sleeper.Snooze(1);
    if (sleep_count > 3600)
    {

      eos_static_warning(
                         "msg=\"reset the total scheduled counter\""
                         " oldvalue=%llu newvalue=%llu",
                         totalscheduled,
                         totalexecuted
                         );
      // reset the accounting
      totalscheduled = totalexecuted;
    }
  }
  eos_static_debug("nscheduled=%llu totalscheduled=%llu totalexecuted=%llu", nscheduled, totalscheduled, totalexecuted);
  return nscheduled;
}

/*----------------------------------------------------------------------------*/
bool
Storage::GetFileSystemInDrainMode (std::vector<unsigned int> &drainfsvector,
                                   unsigned int &cycler,
                                   unsigned long long nparalleltx,
                                   unsigned long long ratetx)
/*----------------------------------------------------------------------------*/
/**
 * @brief return a filesystem vector which should ask for a drain job now
 * @param drainfsvector result vector with the indices of draining filesystems
 * @param cycler cyclic index guaranteeing round-robin selection
 * @return true if there is any filesystem in drain mode
 */
/*----------------------------------------------------------------------------*/
{
  unsigned int nfs = 0;
  {
    eos::common::RWMutexReadLock lock(fsMutex);
    nfs = fileSystemsVector.size();
  }

  for (unsigned int i = 0; i < nfs; i++)
  {
    unsigned int index = (i + cycler) % nfs;
    eos::common::RWMutexReadLock lock(fsMutex);
    if (index < fileSystemsVector.size())
    {
      std::string path = fileSystemsVector[index]->GetPath();
      unsigned long id = fileSystemsVector[index]->GetId();
      eos_static_debug("FileSystem %lu |%s|", id, fileSystemsVector[index]->GetString("stat.drainer").c_str());

      // check if this filesystem has to 'schedule2drain' 
      if (fileSystemsVector[index]->GetString("stat.drainer") != "on")
      {
        // nothing to do here
        continue;
      }

      // store our notification condition variable
      fileSystemsVector[index]->
        GetDrainQueue()->SetJobEndCallback(&drainJobNotification);

      // configure the proper rates and slots
      if (fileSystemsVector[index]->GetDrainQueue()->GetBandwidth() != ratetx)
      {
        // modify the bandwidth setting for this queue
        fileSystemsVector[index]->GetDrainQueue()->SetBandwidth(ratetx);
      }

      if (fileSystemsVector[index]->GetDrainQueue()->GetSlots() != nparalleltx)
      {
        // modify slot settings for this queue
        fileSystemsVector[index]->GetDrainQueue()->SetSlots(nparalleltx);
      }

      eos::common::FileSystem::fsstatus_t bootstatus =
        fileSystemsVector[index]->GetStatus();
      eos::common::FileSystem::fsstatus_t configstatus =
        fileSystemsVector[index]->GetConfigStatus();

      // check if the filesystem is full
      bool full = false;
      {
        XrdSysMutexHelper(fileSystemFullMapMutex);
        full = fileSystemFullWarnMap[id];
      }

      if ((bootstatus != eos::common::FileSystem::kBooted) ||
          (configstatus <= eos::common::FileSystem::kRO) ||
          (full))
      {
        // skip this one in bad state

        eos_static_debug("FileSystem %lu status=%u configstatus=%u", bootstatus, configstatus);
        continue;
      }

      eos_static_debug("id=%u nparalleltx=%llu",
                      id,
                      nparalleltx
                      );

      // add this filesystem to the vector of draining filesystems
      drainfsvector.push_back(index);
    }
  }
  cycler++;
  return (bool) drainfsvector.size();
}

/*----------------------------------------------------------------------------*/
bool
Storage::GetDrainJob (unsigned int index, std::string manager)
/*----------------------------------------------------------------------------*/
/**
 * @brief get a drain job for the filesystem in the filesystem vector with index
 * @param index index in the filesystem vector
 * @param manger to call
 * @return true if scheduled otherwise false
 */
/*----------------------------------------------------------------------------*/
{
  unsigned long long freebytes =
    fileSystemsVector[index]->GetLongLong("stat.statfs.freebytes");
  unsigned long id = fileSystemsVector[index]->GetId();

  XrdOucErrInfo error;
  XrdOucString managerQuery = "/?";
  managerQuery += "mgm.pcmd=schedule2drain";
  managerQuery += "&mgm.target.fsid=";
  char sid[1024];
  snprintf(sid, sizeof (sid) - 1, "%lu", id);
  managerQuery += sid;
  managerQuery += "&mgm.target.freebytes=";
  char sfree[1024];
  snprintf(sfree, sizeof (sfree) - 1, "%llu", freebytes);
  managerQuery += sfree;
  managerQuery += "&mgm.logid=";
  managerQuery += logId;

  XrdOucString response = "";
  int rc = gOFS.CallManager(&error,
                            "/",
                            manager.c_str(),
                            managerQuery,
                            &response);

  eos_static_debug("job-response=%s", response.c_str());
  if (rc)
  {
    eos_static_err("manager returned errno=%d for schedule2drain on fsid=%u",
                   rc, id);
  }
  else
  {
    if (response == "submitted")
    {
      eos_static_info("msg=\"new transfer job\" fsid=%u", id);
      return true;
    }
    else
    {

      eos_static_debug("manager returned no file to schedule [ENODATA]");
    }
  }
  return false;
}

/*----------------------------------------------------------------------------*/
void
Storage::Drainer ()
/*----------------------------------------------------------------------------*/
/**
 * @brief eternal thread loop pulling drain jobs
 */
/*----------------------------------------------------------------------------*/

{
  eos_static_info("Start Drainer ...");

  std::string nodeconfigqueue = "";
  std::string manager = "";
  unsigned long long nparalleltx = 0;
  unsigned long long ratetx = 0;
  unsigned long long nscheduled = 0;
  unsigned long long totalscheduled = 0;
  unsigned long long totalexecuted = 0;
  unsigned int cycler = 0;
  bool noDrainer = false;

  XrdSysTimer sleeper;

  // ---------------------------------------------------------------------------
  // wait for our configuration queue to be set 
  // ---------------------------------------------------------------------------

  WaitConfigQueue(nodeconfigqueue);

  while (1)
  {
    // -------------------------------------------------------------------------
    // -- 1 --
    // a drain round
    // -------------------------------------------------------------------------

    if (noDrainer)
    {
      // we can lay back for a minute if we have no drainer in our group
      sleeper.Snooze(60);
    }

    GetDrainSlotVariables(nparalleltx, ratetx, nodeconfigqueue, manager);

    // -------------------------------------------------------------------------
    // -- 2 --
    // wait that drain slots are free
    // -------------------------------------------------------------------------

    nscheduled = WaitFreeDrainSlot(nparalleltx, totalscheduled, totalexecuted);

    // -------------------------------------------------------------------------
    // -- 3 --
    // get the filesystems which are in drain mode and get their configuration
    // exclude filesystems which couldn't be scheduled for 1 minute
    // -------------------------------------------------------------------------
    std::vector<unsigned int> drainfsindex;
    std::vector<bool> drainfsindexSchedulingFailed;
    std::map<unsigned int, time_t> drainfsindexSchedulingTime;

    // ************************************************************************>
    // read lock the file system vector from now on 
    {
      eos::common::RWMutexReadLock lock(fsMutex);

      if (!GetFileSystemInDrainMode(drainfsindex,
                                    cycler,
                                    nparalleltx,
                                    ratetx))
      {
        noDrainer = true;
        continue;
      }
      else
      {
        noDrainer = false;
      }

      drainfsindexSchedulingFailed.resize(drainfsindex.size());

      // -------------------------------------------------------------------------
      // -- 4 --
      // cycle over all filesystems in drain mode until all slots are filled or
      // none can schedule anymore
      // -------------------------------------------------------------------------

      size_t slotstofill = ((nparalleltx - nscheduled) > 0) ?
        (nparalleltx - nscheduled) : 0;

      bool stillGotOneScheduled;

      eos_static_debug("slotstofill=%u nparalleltx=%u nscheduled=%u totalscheduled=%llu totalexecuted=%llu",
                      slotstofill,
                      nparalleltx,
                      nscheduled,
                      totalscheduled,
                      totalexecuted);

      if (slotstofill)
      {
        do
        {
          stillGotOneScheduled = false;
          for (size_t i = 0; i < drainfsindex.size(); i++)
          {
            // ---------------------------------------------------------------------
            // skip indices where we know we couldn't schedule
            // ---------------------------------------------------------------------
            if (drainfsindexSchedulingFailed[i])
              continue;

            // ---------------------------------------------------------------------
            // skip filesystems where we scheduling has been blocked for some time
            // ---------------------------------------------------------------------
            if ((drainfsindexSchedulingTime.count(drainfsindex[i])) &&
                (drainfsindexSchedulingTime[drainfsindex[i]] > time(NULL)))
              continue;


            // ---------------------------------------------------------------------
            // try to get a drainjob for the indexed filesystem
            // ---------------------------------------------------------------------
            if (GetDrainJob(drainfsindex[i], manager))
            {
              eos_static_debug("got scheduled totalscheduled=%llu slotstofill=%llu", totalscheduled, slotstofill);
              drainfsindexSchedulingTime[drainfsindex[i]] = 0;
              totalscheduled++;
              stillGotOneScheduled = true;
              slotstofill--;
            }
            else
            {
              drainfsindexSchedulingFailed[i] = true;
              drainfsindexSchedulingTime[drainfsindex[i]] = time(NULL) + 60;
            }
            // we stop if we have all slots full
            if (!slotstofill)
              break;
          }
        }
        while (slotstofill && stillGotOneScheduled);

	// reset the failed vector, otherwise we starve forever
	for (size_t i = 0; i < drainfsindex.size(); i++)
	{
	  drainfsindexSchedulingFailed[i] = false;
	}
      }
    }
    // ************************************************************************>
    drainJobNotification.WaitMS(1000);
  }
}

EOSFSTNAMESPACE_END


