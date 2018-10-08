// ----------------------------------------------------------------------
// File: Shutdown.cc
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

//------------------------------------------------------------------------------
/*
 * @brief shutdown function cleaning up running threads/objects for a clean exit
 *
 * @param sig signal catched
 *
 * This shutdown function tries to get a write lock before doing the namespace
 * shutdown. Since it is not guaranteed that one can always get a write lock
 * there is a timeout in requiring the write lock and then the shutdown is forced.
 * Depending on the role of the MGM it stop's the running namespace follower
 * and in all cases running sub-services of the MGM.
 */
//------------------------------------------------------------------------------
void
xrdmgmofs_shutdown(int sig)

{
  (void) signal(SIGINT, SIG_IGN);
  (void) signal(SIGTERM, SIG_IGN);
  (void) signal(SIGQUIT, SIG_IGN);
  eos_static_alert("msg=\"shutdown sequence started\'");

  // Avoid shutdown recursions
  if (gOFS->Shutdown) {
    return;
  }

  gOFS->Shutdown = true;
  // ---------------------------------------------------------------------------
  // handler to shutdown the daemon for valgrinding and clean server stop
  // (e.g. let's time to finish write operations)
  // ---------------------------------------------------------------------------
  eos_static_warning("Shutdown:: stop vst ... ");

  if (gOFS->MgmOfsVstMessaging) {
    delete gOFS->MgmOfsVstMessaging;
  }

  eos_static_warning("Shutdown:: stop recycler thread ... ");
  gOFS->Recycler->Stop();
  eos_static_warning("Shutdown:: stop deletion thread ... ");

  if (gOFS->deletion_tid) {
    XrdSysThread::Cancel(gOFS->deletion_tid);
    XrdSysThread::Join(gOFS->deletion_tid, 0);
  }

  eos_static_warning("Shutdown:: stop transfer engine thread ... ");
  gTransferEngine.Stop();
  eos_static_warning("Shutdown:: stop statistics thread ... ");

  if (gOFS->mStatsTid) {
    XrdSysThread::Cancel(gOFS->mStatsTid);
    XrdSysThread::Join(gOFS->mStatsTid, 0);
  }

  eos_static_warning("Shutdown:: stop fs listener thread ... ");

  if (gOFS->mFsConfigTid) {
    XrdSysThread::Cancel(gOFS->mFsConfigTid);
    XrdSysThread::Join(gOFS->mFsConfigTid, 0);
  }

  eos_static_warning("Shutdown:: stop egroup fetching ... ");
  gOFS->EgroupRefresh->Stop();
  eos_static_warning("Shutdown:: stop LRU thread ... ");
  gOFS->LRUd.Stop();
  eos_static_warning("Shutdown:: stop messaging ... ");

  if (gOFS->MgmOfsMessaging) {
    gOFS->MgmOfsMessaging->StopListener();
  }

  eos_static_warning("Shutdown:: stop fusex server ...");
  gOFS->zMQ->gFuseServer.shutdown();
  eos_static_warning("Shutdown:: remove messaging ...");

  if (gOFS->MgmOfsMessaging) {
    delete gOFS->MgmOfsMessaging;
  }

  gOFS->ConfEngine->SetAutoSave(false);
  eos_static_warning("Shutdown:: stop GeoTree engine ... ");

  if (!gGeoTreeEngine.StopUpdater()) {
    eos_static_crit("error Stopping the GeoTree engine");
  }

  eos_static_warning("Shutdown:: cleanup quota...");
  (void) Quota::CleanUp();
  eos_static_warning("Shutdown:: stop shared object modification notifier ... ");

  if (!gOFS->ObjectNotifier.Stop()) {
    eos_static_crit("error Stopping the shared object change notifier");
  }

  eos_static_warning("Shutdown:: stop config engine ... ");

  if (gOFS->ConfEngine) {
    delete gOFS->ConfEngine;
    gOFS->ConfEngine = nullptr;
    FsView::sConfEngine = nullptr;
  }

  eos_static_warning("Shutdown:: attempt graceful shutdown of FsView ...");
  FsView::gFsView.StopHeartBeat();
  FsView::gFsView.Clear();
  eos_static_warning("Shutdown:: grab write mutex");
  uint64_t timeout_ns = 3 * 1e9;

  while (!gOFS->eosViewRWMutex.TimedWrLock(timeout_ns)) {
    eos_static_warning("Trying to get the wr lock on eosViewRWMutex");
  }

  eos_static_warning("Shutdown:: set stall rule");
  eos::common::RWMutexWriteLock lock(Access::gAccessMutex);
  Access::gStallRules[std::string("*")] = "300";

  if (gOFS->ErrorLog) {
    XrdOucString errorlogkillline = "pkill -9 -f \"eos -b console log _MGMID_\"";
    int rrc = system(errorlogkillline.c_str());

    if (WEXITSTATUS(rrc)) {
      eos_static_info("%s returned %d", errorlogkillline.c_str(), rrc);
    }
  }

  // ---------------------------------------------------------------------------
  if (gOFS->mInitialized == gOFS->kBooted) {
    eos_static_warning("Shutdown:: finalizing views ... ");

    try {
      // These two views need to be deleted without holding the namespace mutex
      // as this might lead to a deadlock
      gOFS->eosViewRWMutex.UnLockWrite();

      if (gOFS->eosSyncTimeAccounting) {
        delete gOFS->eosSyncTimeAccounting;
      }

      if (gOFS->eosContainerAccounting) {
        delete gOFS->eosContainerAccounting;
      }

      while (!gOFS->eosViewRWMutex.TimedWrLock(timeout_ns)) {
        eos_static_warning("Trying to get the wr lock on eosViewRWMutex");
      }

      if (gOFS->eosFsView) {
        delete gOFS->eosFsView;
      }

      if (gOFS->eosView) {
        delete gOFS->eosView;
      }

      if (gOFS->eosDirectoryService) {
        gOFS->eosDirectoryService->finalize();
        delete gOFS->eosDirectoryService;
      }

      if (gOFS->eosFileService) {
        gOFS->eosFileService->finalize();
        delete gOFS->eosFileService;
      }
    } catch (eos::MDException& e) {
      // we don't really care about any exception here!
    }
  }

  gOFS->eosViewRWMutex.UnLockWrite();
  eos_static_warning("Shutdown:: stop master supervisor thread ...");
  gOFS->mMaster.reset();
  eos_static_warning("Shutdown complete");
  eos_static_alert("msg=\"shutdown complete\'");
  exit(9);
}
