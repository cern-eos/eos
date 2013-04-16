// ----------------------------------------------------------------------
// File: MgmSyncer.cc
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
Storage::MgmSyncer ()
{
  // this thread checks the synchronization between the local MD after a file
  // modification/write against the MGM server
  bool knowmanager = false;

  while (1)
  {
    XrdOucString manager = "";
    size_t cnt = 0;
    // get the currently active manager
    do
    {
      cnt++;
      {
        XrdSysMutexHelper lock(eos::fst::Config::gConfig.Mutex);
        manager = eos::fst::Config::gConfig.Manager.c_str();
      }
      if (manager != "")
      {
        if (!knowmanager)
        {
          eos_info("msg=\"manager known\" manager=\"%s\"", manager.c_str());
          knowmanager = true;
        }
        break;
      }

      XrdSysTimer sleeper;
      sleeper.Snooze(5);
      eos_info("msg=\"waiting to know manager\"");
      if (cnt > 20)
      {
        eos_static_alert("didn't receive manager name, aborting");
        XrdSysTimer sleeper;
        sleeper.Snooze(10);
        XrdFstOfs::xrdfstofs_shutdown(1);
      }
    }
    while (1);
    bool failure = false;

    gOFS.WrittenFilesQueueMutex.Lock();
    while (gOFS.WrittenFilesQueue.size() > 0)
    {
      // we enter this loop with the WrittenFilesQueueMutex locked
      time_t now = time(NULL);
      struct Fmd fmd = gOFS.WrittenFilesQueue.front();
      gOFS.WrittenFilesQueueMutex.UnLock();

      eos_static_info("fid=%llx mtime=%llu", fmd.fid, fmd.ctime);

      // guarantee that we delay the check by atleast 60 seconds to wait for the commit of all recplias

      if ((time_t) (fmd.mtime + 60) > now)
      {
        eos_static_debug("msg=\"postpone mgm sync\" delay=%d", (fmd.mtime + 60) - now);
        XrdSysTimer sleeper;
        sleeper.Snooze(((fmd.mtime + 60) - now));
        gOFS.WrittenFilesQueueMutex.Lock();
        continue;
      }

      bool isopenforwrite = false;

      // check if someone is still writing on that file
      gOFS.OpenFidMutex.Lock();
      if (gOFS.WOpenFid[fmd.fsid].count(fmd.fid))
      {
        if (gOFS.WOpenFid[fmd.fsid][fmd.fid] > 0)
        {
          isopenforwrite = true;
        }
      }
      gOFS.OpenFidMutex.UnLock();

      if (!isopenforwrite)
      {
        // now do the consistency check
        if (gFmdSqliteHandler.ResyncMgm(fmd.fsid, fmd.fid, manager.c_str()))
        {
          eos_static_debug("msg=\"resync ok\" fsid=%lu fid=%llx", (unsigned long) fmd.fsid, fmd.fid);
          gOFS.WrittenFilesQueueMutex.Lock();
          gOFS.WrittenFilesQueue.pop();
        }
        else
        {
          eos_static_err("msg=\"resync failed\" fsid=%lu fid=%llx", (unsigned long) fmd.fsid, fmd.fid);
          failure = true;
          gOFS.WrittenFilesQueueMutex.Lock(); // put back the lock
          break;
        }
      }
      else
      {
        // if there was still a reference, we can just discard this check since the other write open will trigger a new entry in the queue
        gOFS.WrittenFilesQueueMutex.Lock(); // put back the lock
        gOFS.WrittenFilesQueue.pop();
      }
    }
    gOFS.WrittenFilesQueueMutex.UnLock();

    if (failure)
    {
      // the last synchronization to the MGM failed, we wait longer
      XrdSysTimer sleeper;
      sleeper.Snooze(10);
    }
    else
    {
      // the queue was empty
      XrdSysTimer sleeper;
      sleeper.Snooze(1);
    }
  }
}

EOSFSTNAMESPACE_END


