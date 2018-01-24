//------------------------------------------------------------------------------
// File: Drainer.cc
// Author: Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

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
#include "fst/txqueue/TransferJob.hh"
#include "fst/txqueue/TransferQueue.hh"
#include "fst/storage/FileSystem.hh"

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Get the number of parallel transfers and transfer rate settings
//------------------------------------------------------------------------------
void
Storage::GetDrainSlotVariables(unsigned long long& nparalleltx,
                               unsigned long long& ratetx,
                               std::string nodeconfigqueue)
{
  gOFS.ObjectManager.HashMutex.LockRead();
  XrdMqSharedHash* confighash = gOFS.ObjectManager.GetHash(
                                  nodeconfigqueue.c_str());
  std::string manager = confighash ? confighash->Get("manager") : "unknown";
  nparalleltx = confighash ? confighash->GetLongLong("stat.drain.ntx") : 0;
  ratetx = confighash ? confighash->GetLongLong("stat.drain.rate") : 0;

  if (nparalleltx == 0) {
    nparalleltx = 0;
  }

  if (ratetx == 0) {
    ratetx = 25;
  }

  eos_static_debug("manager=%s nparalleltransfers=%llu transferrate=%llu",
                   manager.c_str(),
                   nparalleltx,
                   ratetx);
  gOFS.ObjectManager.HashMutex.UnLockRead();
}

//------------------------------------------------------------------------------
// Get the number of already scheduled jobs
//------------------------------------------------------------------------------
unsigned long long
Storage::GetScheduledDrainJobs(unsigned long long totalscheduled,
                               unsigned long long& totalexecuted)
{
  unsigned int nfs = 0;
  unsigned long long nscheduled = 0;
  {
    eos::common::RWMutexReadLock lock(mFsMutex);
    nfs = mFsVect.size();
    totalexecuted = 0;

    // sum up the current execution state e.g. number of jobs taken from the queue
    for (unsigned int s = 0; s < nfs; s++) {
      if (s < mFsVect.size()) {
        totalexecuted += mFsVect[s]->GetDrainQueue()->GetDone();
      }
    }

    if (totalscheduled < totalexecuted) {
      nscheduled = 0;
    } else {
      nscheduled = totalscheduled - totalexecuted;
    }
  }
  eos_static_debug("nscheduled=%llu totalscheduled=%llu totalexecuted=%llu",
                   nscheduled, totalscheduled, totalexecuted);
  return nscheduled;
}

//------------------------------------------------------------------------------
// Wait that there is a free slot to schedule a new drain
//------------------------------------------------------------------------------
unsigned long long
Storage::WaitFreeDrainSlot(unsigned long long& nparalleltx,
                           unsigned long long& totalscheduled,
                           unsigned long long& totalexecuted)
{
  size_t sleep_count = 0;
  unsigned long long nscheduled = 0;
  XrdSysTimer sleeper;

  while (1) {
    nscheduled = GetScheduledDrainJobs(totalscheduled, totalexecuted);

    if (nscheduled < nparalleltx) {
      break;
    }

    sleep_count++;
    sleeper.Snooze(1);

    if (sleep_count > 3600) {
      eos_static_warning(
        "msg=\"reset the total scheduled counter\""
        " oldvalue=%llu newvalue=%llu",
        totalscheduled,
        totalexecuted
      );
      // reset the accounting
      totalscheduled = totalexecuted;
      sleep_count = 0;
    }
  }

  eos_static_debug("nscheduled=%llu totalscheduled=%llu totalexecuted=%llu",
                   nscheduled, totalscheduled, totalexecuted);
  return nscheduled;
}

//------------------------------------------------------------------------------
//! Get the list of filesystems which are in drain mode in current group
//------------------------------------------------------------------------------
bool
Storage::GetFileSystemInDrainMode(std::vector<unsigned int>& drainfsvector,
                                  unsigned int& cycler,
                                  unsigned long long nparalleltx,
                                  unsigned long long ratetx)
{
  unsigned int nfs = 0;
  {
    eos::common::RWMutexReadLock lock(mFsMutex);
    nfs = mFsVect.size();
  }

  for (unsigned int i = 0; i < nfs; i++) {
    unsigned int index = (i + cycler) % nfs;
    eos::common::RWMutexReadLock lock(mFsMutex);

    if (index < mFsVect.size()) {
      std::string path = mFsVect[index]->GetPath();
      unsigned long id = mFsVect[index]->GetId();
      eos_static_debug("FileSystem %lu |%s|", id,
                       mFsVect[index]->GetString("stat.drainer").c_str());

      // check if this filesystem has to 'schedule2drain'
      if (mFsVect[index]->GetString("stat.drainer") != "on") {
        // nothing to do here
        continue;
      }

      // store our notification condition variable
      mFsVect[index]->
      GetDrainQueue()->SetJobEndCallback(&drainJobNotification);

      // configure the proper rates and slots
      if (mFsVect[index]->GetDrainQueue()->GetBandwidth() != ratetx) {
        // modify the bandwidth setting for this queue
        mFsVect[index]->GetDrainQueue()->SetBandwidth(ratetx);
      }

      if (mFsVect[index]->GetDrainQueue()->GetSlots() != nparalleltx) {
        // modify slot settings for this queue
        mFsVect[index]->GetDrainQueue()->SetSlots(nparalleltx);
      }

      eos::common::FileSystem::fsstatus_t bootstatus =
        mFsVect[index]->GetStatus();
      eos::common::FileSystem::fsstatus_t configstatus =
        mFsVect[index]->GetConfigStatus();
      // check if the filesystem is full
      bool full = false;
      {
        XrdSysMutexHelper(mFsFullMapMutex);
        full = mFsFullWarnMap[id];
      }

      if ((bootstatus != eos::common::FileSystem::kBooted) ||
          (configstatus <= eos::common::FileSystem::kRO) ||
          (full)) {
        // skip this one in bad state
        eos_static_debug("FileSystem %lu status=%u configstatus=%i", id, bootstatus,
                         configstatus);
        continue;
      }

      eos_static_debug("id=%u nparalleltx=%llu", id, nparalleltx);
      // add this filesystem to the vector of draining filesystems
      drainfsvector.push_back(index);
    }
  }

  cycler++;
  return (bool) drainfsvector.size();
}

//------------------------------------------------------------------------------
// Get drain job for the reuqested filesystem
//------------------------------------------------------------------------------
bool
Storage::GetDrainJob(unsigned int index)

{
  unsigned long long freebytes =
    mFsVect[index]->GetLongLong("stat.statfs.freebytes");
  unsigned long id = mFsVect[index]->GetId();
  XrdOucErrInfo error;
  XrdOucString managerQuery = "/?";
  managerQuery += "mgm.pcmd=schedule2drain";
  managerQuery += "&mgm.target.fsid=";
  char sid[1024];
  snprintf(sid, sizeof(sid) - 1, "%lu", id);
  managerQuery += sid;
  managerQuery += "&mgm.target.freebytes=";
  char sfree[1024];
  snprintf(sfree, sizeof(sfree) - 1, "%llu", freebytes);
  managerQuery += sfree;
  managerQuery += "&mgm.logid=";
  managerQuery += logId;
  XrdOucString response = "";
  int rc = gOFS.CallManager(&error, "/", 0, managerQuery, &response);
  eos_static_debug("job-response=%s", response.c_str());

  if (rc) {
    eos_static_err("manager returned errno=%d for schedule2drain on fsid=%u",
                   rc, id);
  } else {
    if (response == "submitted") {
      eos_static_info("msg=\"new transfer job\" fsid=%u", id);
      return true;
    } else {
      eos_static_debug("manager returned no file to schedule [ENODATA]");
    }
  }

  return false;
}

//------------------------------------------------------------------------------
// Eternal thread loop pulling drain jobs
//------------------------------------------------------------------------------
void
Storage::Drainer()

{
  eos_static_info("Start Drainer ...");
  std::string nodeconfigqueue = "";
  unsigned long long nparalleltx = 0;
  unsigned long long ratetx = 0;
  unsigned long long nscheduled = 0;
  unsigned long long totalscheduled = 0;
  unsigned long long totalexecuted = 0;
  unsigned int cycler = 0;
  bool noDrainer = false;
  time_t last_config_update = 0;
  XrdSysTimer sleeper;
  nodeconfigqueue = eos::fst::Config::gConfig.getFstNodeConfigQueue().c_str();

  while (true) {
    time_t now = time(NULL);

    // -------------------------------------------------------------------------
    // -- 1 --
    // a drain round
    // -------------------------------------------------------------------------
    // Sleep of 1 minnute if there is no drainer in our group
    if (noDrainer) {
      sleeper.Snooze(60);
    }

    // -------------------------------------------------------------------------
    // -- W --
    // wait that we have a drain slot configured
    // -------------------------------------------------------------------------
    while (!nparalleltx) {
      GetDrainSlotVariables(nparalleltx, ratetx, nodeconfigqueue);
      last_config_update = time(NULL);
      XrdSysTimer sleeper;
      sleeper.Snooze(10);
    }

    // -------------------------------------------------------------------------
    // -- U --
    // update the config at least every minute
    // -------------------------------------------------------------------------
    if (!last_config_update ||
        (((long long) now - (long long) last_config_update) > 60)) {
      GetDrainSlotVariables(nparalleltx, ratetx, nodeconfigqueue);
      last_config_update = now;
    }

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
    {
      // Read lock the file system vector from now on
      eos::common::RWMutexReadLock lock(mFsMutex);

      if (!GetFileSystemInDrainMode(drainfsindex, cycler, nparalleltx, ratetx)) {
        noDrainer = true;
        continue;
      } else {
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
      eos_static_debug("slotstofill=%u nparalleltx=%u nscheduled=%u "
                       "totalscheduled=%llu totalexecuted=%llu",
                       slotstofill, nparalleltx, nscheduled, totalscheduled,
                       totalexecuted);

      if (slotstofill) {
        do {
          stillGotOneScheduled = false;

          for (size_t i = 0; i < drainfsindex.size(); i++) {
            // Skip indices where we know we couldn't schedule
            if (drainfsindexSchedulingFailed[i]) {
              continue;
            }

            // Skip filesystems where scheduling has been blocked for some time
            if ((drainfsindexSchedulingTime.count(drainfsindex[i])) &&
                (drainfsindexSchedulingTime[drainfsindex[i]] > time(NULL))) {
              continue;
            }

            // Try to get a drainjob for the indexed filesystem
            if (GetDrainJob(drainfsindex[i])) {
              eos_static_debug("got scheduled totalscheduled=%llu slotstofill=%llu",
                               totalscheduled, slotstofill);
              drainfsindexSchedulingTime[drainfsindex[i]] = 0;
              totalscheduled++;
              stillGotOneScheduled = true;
              slotstofill--;
            } else {
              drainfsindexSchedulingFailed[i] = true;
              drainfsindexSchedulingTime[drainfsindex[i]] = time(NULL) + 60;
            }

            // Stop if all slots are full
            if (!slotstofill) {
              break;
            }
          }
        } while (slotstofill && stillGotOneScheduled);

        // Reset the failed vector, otherwise we starve forever
        for (size_t i = 0; i < drainfsindex.size(); i++) {
          drainfsindexSchedulingFailed[i] = false;
        }
      }
    }
    drainJobNotification.WaitMS(1000);
  }
}

EOSFSTNAMESPACE_END
