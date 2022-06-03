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

#include "fst/storage/Storage.hh"
#include "fst/XrdFstOfs.hh"
#include "fst/filemd/FmdDbMap.hh"
#include "fst/Config.hh"

EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
void
Storage::MgmSyncer()
{
  // this thread checks the synchronization between the local MD after a file
  // modification/write against the MGM server
  bool knowmanager = false;

  while (1) {
    XrdOucString manager = "";
    size_t cnt = 0;

    // get the currently active manager
    do {
      cnt++;
      {
        XrdSysMutexHelper lock(gConfig.Mutex);
        manager = gConfig.Manager.c_str();
      }

      if (manager != "") {
        if (!knowmanager) {
          eos_info("msg=\"manager known\" manager=\"%s\"", manager.c_str());
          knowmanager = true;
        }

        break;
      }

      std::this_thread::sleep_for(std::chrono::seconds(5));
      eos_info("msg=\"waiting to know manager\"");

      if (cnt > 20) {
        eos_static_alert("didn't receive manager name, aborting");
        std::this_thread::sleep_for(std::chrono::seconds(10));
        XrdFstOfs::xrdfstofs_shutdown(1);
      }
    } while (1);

    bool failure = false;
    gOFS.WrittenFilesQueueMutex.Lock();

    while (gOFS.WrittenFilesQueue.size() > 0) {
      // we enter this loop with the WrittenFilesQueueMutex locked
      time_t now = time(NULL);
      eos::common::FmdHelper fmd = gOFS.WrittenFilesQueue.front();
      gOFS.WrittenFilesQueue.pop();
      gOFS.WrittenFilesQueueMutex.UnLock();
      // Guarantee that we delay the check by atleast 60 seconds to wait
      // for the commit of all recplias
      time_t delay = fmd.mProtoFmd.mtime() + 60 - now;

      if ((delay > 0) && (delay <= 60)) {
        // only values less than a minute should be taken into account here
        eos_static_debug("msg=\"postpone mgm sync\" delay=%d",
                         delay);
        std::this_thread::sleep_for(std::chrono::seconds(delay));
        gOFS.WrittenFilesQueueMutex.Lock();
        gOFS.WrittenFilesQueue.push(fmd);
        continue;
      }

      eos_static_info("fxid=%08llx mtime=%llu", fmd.mProtoFmd.fid(),
                      fmd.mProtoFmd.mtime());
      bool isopenforwrite = gOFS.openedForWriting.isOpen(fmd.mProtoFmd.fsid(),
                            fmd.mProtoFmd.fid());

      if (!isopenforwrite) {
        // now do the consistency check
        if (gOFS.mFmdHandler->ResyncMgm(fmd.mProtoFmd.fsid(),
                                       fmd.mProtoFmd.fid(), nullptr)) {
          eos_static_debug("msg=\"resync ok\" fsid=%lu fxid=%08llx",
                           (unsigned long) fmd.mProtoFmd.fsid(),
                           fmd.mProtoFmd.fid());
          gOFS.WrittenFilesQueueMutex.Lock();
        } else {
          eos_static_err("msg=\"resync failed\" fsid=%lu fxid=%08llx",
                         (unsigned long) fmd.mProtoFmd.fsid(),
                         fmd.mProtoFmd.fid());
          failure = true;
          gOFS.WrittenFilesQueueMutex.Lock(); // put back the lock and the entry
          gOFS.WrittenFilesQueue.push(fmd);
          break;
        }
      } else {
        // if there was still a reference, we can just discard this check
        // since the other write open will trigger a new entry in the queue
        gOFS.WrittenFilesQueueMutex.Lock(); // put back the lock
      }
    }

    gOFS.WrittenFilesQueueMutex.UnLock();

    if (failure) {
      // the last synchronization to the MGM failed, we wait longer
      std::this_thread::sleep_for(std::chrono::seconds(10));
    } else {
      // the queue was empty
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }
}

EOSFSTNAMESPACE_END
