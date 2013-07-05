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
Storage::GetBalanceSlotVariables (unsigned long long &nparalleltx,
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
  nparalleltx = confighash ? confighash->GetLongLong("stat.balance.ntx") : 0;
  ratetx = confighash ? confighash->GetLongLong("stat.balance.rate") : 0;

  if (nparalleltx == 0) nparalleltx = 0;
  if (ratetx == 0) ratetx = 25;

  eos_static_debug("manager=%s nparalleltransfers=%llu transferrate=%llu",
                   manager.c_str(),
                   nparalleltx,
                   ratetx
                   );
  gOFS.ObjectManager.HashMutex.UnLockRead();
}

/*----------------------------------------------------------------------------*/
unsigned long long
Storage::GetScheduledBalanceJobs (unsigned long long totalscheduled,
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

    // sum up the current execution state e.g. 
    // number of jobs taken from the queue
    for (unsigned int s = 0; s < nfs; s++)
    {
      if (s < fileSystemsVector.size())
      {
        totalexecuted += 
          fileSystemsVector[s]->GetBalanceQueue()->GetQueue()->GetJobCount();
      }
    }
    nscheduled = totalscheduled - totalexecuted;
  }

  return nscheduled;
}

/*----------------------------------------------------------------------------*/
unsigned long long
Storage::WaitFreeBalanceSlot (unsigned long long &nparalleltx,
                              unsigned long long &totalscheduled,
                              unsigned long long &totalexecuted)
/*----------------------------------------------------------------------------*/
/**
 * @brief wait that there is a free slot to schedule a new balance job
 * @param nparalleltx number of parallel transfers
 * @param totalscheduled number of total scheduled transfers
 * @param totalexecuted number of total executed transfers
 * @return number of used balance slots
 */
/*----------------------------------------------------------------------------*/
{
  size_t sleep_count = 0;
  unsigned long long nscheduled = 0;
  XrdSysTimer sleeper;

  while (1)
  {
    nscheduled = GetScheduledBalanceJobs(totalscheduled, totalexecuted);
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
  return nscheduled;
}

/*----------------------------------------------------------------------------*/
bool
Storage::GetFileSystemInBalanceMode (std::vector<unsigned int> &balancefsvector,
                                     unsigned int &cycler,
                                     unsigned long long nparalleltx,
                                     unsigned long long ratetx)
/*----------------------------------------------------------------------------*/
/**
 * @brief return a filesystem vector which should ask for a balance job now
 * @param balancefsvector result vector with the indices of balancing filesystems
 * @param cycler cyclic index guaranteeing round-robin selection
 * @return true if there is any filesystem in balance mode
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
    cycler++;
    if (index < fileSystemsVector.size())
    {
      std::string path = fileSystemsVector[index]->GetPath();
      unsigned long id = fileSystemsVector[index]->GetId();
      eos_static_debug("FileSystem %lu ", id);


      double nominal =
        fileSystemsVector[index]->GetDouble("stat.nominal.filled");

      double filled =
        fileSystemsVector[index]->GetDouble("stat.statfs.filled");

      double threshold =
        fileSystemsVector[index]->GetDouble("stat.balance.threshold");

      if ((!nominal) || (fabs(filled - threshold) >= nominal))
      {
        // we are more full than we should be , we are not a target
        continue;
      }

      // store our notification condition variable
      fileSystemsVector[index]->
        GetBalanceQueue()->SetJobEndCallback(&balanceJobNotification);
      
      // configure the proper rates and slots
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
        eos_static_debug("FileSystem %lu status=%u configstatus=%u", 
                         bootstatus, 
                         configstatus);
        continue;
      }

      eos_static_info("id=%u nparalleltx=%llu",
                      id,
                      nparalleltx
                      );

      // add this filesystem to the vector of balancing filesystems
      balancefsvector.push_back(index);
    }
  }
  return (bool) balancefsvector.size();
}

/*----------------------------------------------------------------------------*/
bool
Storage::GetBalanceJob (unsigned int index, std::string manager)
/*----------------------------------------------------------------------------*/
/**
 * @brief get a balance job for the filesystem in the filesystem vector with index
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
  managerQuery += "mgm.pcmd=schedule2balance";
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
  if (rc)
  {
    eos_static_err("manager returned errno=%d for schedule2balance on fsid=%u",
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
Storage::Balancer ()
/*----------------------------------------------------------------------------*/
/**
 * @brief eternal thread loop pulling balance jobs
 */
/*----------------------------------------------------------------------------*/

{
  eos_static_info("Start Balancer ...");

  std::string nodeconfigqueue = "";
  std::string manager = "";
  unsigned long long nparalleltx = 0;
  unsigned long long ratetx = 0;
  unsigned long long nscheduled = 0;
  unsigned long long totalscheduled = 0;
  unsigned long long totalexecuted = 0;
  unsigned int cycler = 0;
  bool noBalancer = 0;
  XrdSysTimer sleeper;

  // ---------------------------------------------------------------------------
  // wait for our configuration queue to be set 
  // ---------------------------------------------------------------------------

  WaitConfigQueue(nodeconfigqueue);

  while (1)
  {
    // -------------------------------------------------------------------------
    // -- 1 --
    // a balance round round
    // -------------------------------------------------------------------------

    if (noBalancer)
    {
      // we can lay back for a minute if we have no drainer in our group
      sleeper.Snooze(60);
    }
    
    GetBalanceSlotVariables(nparalleltx, ratetx, nodeconfigqueue, manager);

    // -------------------------------------------------------------------------
    // -- 2 --
    // wait that balance slots are free
    // -------------------------------------------------------------------------

    nscheduled = WaitFreeBalanceSlot(nparalleltx, totalscheduled, totalexecuted);

    // -------------------------------------------------------------------------
    // -- 3 --
    // get the filesystems which are in balance mode and get their configuration
    // exclude filesystems which couldn't be scheduled for 1 minute
    // -------------------------------------------------------------------------
    std::vector<unsigned int> balancefsindex;
    std::vector<bool> balancefsindexSchedulingFailed;
    std::map<unsigned int, time_t> balancefsindexSchedulingTime;

    // ************************************************************************>
    // read lock the file system vector from now on 

    eos::common::RWMutexReadLock lock(fsMutex);

    if (!GetFileSystemInBalanceMode(balancefsindex,
                                    cycler,
                                    nparalleltx,
                                    ratetx)
        )
    {
      noBalancer = true;
      continue;
    }

    balancefsindexSchedulingFailed.resize(balancefsindex.size());

    // -------------------------------------------------------------------------
    // -- 4 --
    // cycle over all filesystems until all slots are filled or
    // none can schedule anymore
    // -------------------------------------------------------------------------

    size_t slotstofill = nparalleltx - nscheduled;
    bool stillGotOneScheduled;

    do
    {
      stillGotOneScheduled = false;
      for (size_t i = 0; i < balancefsindex.size(); i++)
      {
        // ---------------------------------------------------------------------
        // skip indices where we know we couldn't schedule
        // ---------------------------------------------------------------------
        if (balancefsindexSchedulingFailed[i])
          continue;

        // ---------------------------------------------------------------------
        // skip filesystems where the scheduling has been blocked for some time
        // ---------------------------------------------------------------------
        if ((balancefsindexSchedulingTime.count(balancefsindex[i])) &&
            (balancefsindexSchedulingTime[balancefsindex[i]] > time(NULL)))
          continue;


        // ---------------------------------------------------------------------
        // try to get a balancejob for the indexed filesystem
        // ---------------------------------------------------------------------
        if (GetDrainJob(balancefsindex[i], manager))
        {
          balancefsindexSchedulingTime[balancefsindex[i]] = 0;
          totalscheduled++;
          stillGotOneScheduled = true;
          slotstofill--;
        }
        else
        {
          balancefsindexSchedulingFailed[i] = true;
          balancefsindexSchedulingTime[balancefsindex[i]] = time(NULL) + 60;
        }
      }
    }
    while (slotstofill && stillGotOneScheduled);

    balanceJobNotification.Wait(1);
    // ************************************************************************>
  }
}

EOSFSTNAMESPACE_END


