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

/*----------------------------------------------------------------------------*/
void
xrdmgmofs_shutdown (int sig)
/*----------------------------------------------------------------------------*/
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
/*----------------------------------------------------------------------------*/
{

  (void) signal(SIGINT, SIG_IGN);
  (void) signal(SIGTERM, SIG_IGN);
  (void) signal(SIGQUIT, SIG_IGN);

  eos_static_alert("msg=\"shutdown sequence started\'");
  // avoid shutdown recursions
  if (gOFS->Shutdown)
    return;

  gOFS->Shutdown = true;

  // ---------------------------------------------------------------------------
  // handler to shutdown the daemon for valgrinding and clean server stop
  // (e.g. let's time to finish write operations)
  // ---------------------------------------------------------------------------

  // ---------------------------------------------------------------------------
  eos_static_warning("Shutdown:: stop vst ... ");
  if (gOFS->MgmOfsVstMessaging)
  {
    gOFS->MgmOfsVstMessaging->StopListener();
  }

  // ---------------------------------------------------------------------------
  eos_static_warning("Shutdown:: stop deletion thread ... ");
  if (gOFS->deletion_tid)
  {
    XrdSysThread::Cancel(gOFS->deletion_tid);
    XrdSysThread::Join(gOFS->deletion_tid, 0);
  }

  // ---------------------------------------------------------------------------
  eos_static_warning("Shutdown:: stop statistics thread ... ");
  if (gOFS->stats_tid)
  {
    XrdSysThread::Cancel(gOFS->stats_tid);
    XrdSysThread::Join(gOFS->stats_tid, 0);
  }

  // ---------------------------------------------------------------------------
  eos_static_warning("Shutdown:: stop fs listener thread ... ");
  if (gOFS->fsconfiglistener_tid)
  {
    XrdSysThread::Cancel(gOFS->fsconfiglistener_tid);
    XrdSysThread::Join(gOFS->fsconfiglistener_tid, 0);
  }

  // ---------------------------------------------------------------------------
  eos_static_warning("Shutdown:: stop egroup fetching ... ");
  gOFS->EgroupRefresh.Stop();

  // ---------------------------------------------------------------------------
  eos_static_warning("Shutdown:: stop LRU thread ... ");
  gOFS->LRUd.Stop();

  // ---------------------------------------------------------------------------
  eos_static_warning("Shutdown:: stop messaging ... ");
  if (gOFS->MgmOfsMessaging)
  {
    gOFS->MgmOfsMessaging->StopListener();
  }


  // ---------------------------------------------------------------------------
  eos_static_warning("Shutdown:: remove messaging ... ");
  if (gOFS->MgmOfsMessaging)
  {
    delete gOFS->MgmOfsMessaging;
  }

  eos_static_warning("Shutdown:: grab write mutex");
  gOFS->eosViewRWMutex.TimeoutLockWrite();

  // ---------------------------------------------------------------------------
  eos_static_warning("Shutdown:: set stall rule");
  eos::common::RWMutexWriteLock lock(Access::gAccessMutex);
  Access::gStallRules[std::string("*")] = "300";

  if (gOFS->ErrorLog)
  {
    XrdOucString errorlogkillline = "pkill -9 -f \"eos -b console log _MGMID_\"";
    int rrc = system(errorlogkillline.c_str());
    if (WEXITSTATUS(rrc))
    {
      eos_static_info("%s returned %d", errorlogkillline.c_str(), rrc);
    }
  }
  // ---------------------------------------------------------------------------
  if (gOFS->Initialized == gOFS->kBooted)
  {
    eos_static_warning("Shutdown:: finalizing views ... ");
    try
    {
      gOFS->MgmMaster.ShutdownSlaveFollower();

      if (gOFS->eosFsView)
      {	
        delete gOFS->eosFsView;
      }
      if (gOFS->eosView)
      {
        delete gOFS->eosView;
      }
      if (gOFS->eosDirectoryService)
      {
	gOFS->eosDirectoryService->finalize();
        delete gOFS->eosDirectoryService;
      }
      if (gOFS->eosFileService)
      {
	gOFS->eosFileService->finalize();
        delete gOFS->eosFileService;
      }

    }
    catch (eos::MDException &e)
    {
      // we don't really care about any exception here!
    }
  }

  gOFS->ConfEngine->SetAutoSave(false);

  // ---------------------------------------------------------------------------
  eos_static_warning("Shutdown:: stop GeoTree engine ... ");
  if(!gGeoTreeEngine.StopUpdater())
  	eos_static_crit("error Stopping the GeoTree engine");

  // ---------------------------------------------------------------------------
  eos_static_warning("Shutdown:: cleanup quota...");
  (void) Quota::CleanUp();

  // ----------------------------------------------------------------------------
  eos_static_warning("Shutdown:: stop shared object modification notifier ... ");
  if(!gOFS->ObjectNotifier.Stop())
    eos_static_crit("error Stopping the shared object change notifier");

  // ----------------------------------------------------------------------------
  eos_static_warning("Shutdown:: stop config engine ... ");
  if (gOFS->ConfEngine)
  {
    delete gOFS->ConfEngine;
    gOFS->ConfEngine = 0;
    FsView::ConfEngine = 0;
  }

  eos_static_warning("Shutdown complete");
  eos_static_alert("msg=\"shutdown complete\'");
  kill(getpid(), 9);
}
