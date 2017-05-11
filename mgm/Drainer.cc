// ----------------------------------------------------------------------
// File: Drainer.cc
// Author: Andrea Manzi - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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

#include "mgm/Drainer.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/XrdMgmOfsDirectory.hh"
#include "XrdSys/XrdSysTimer.hh"
#include "XrdSys/XrdSysError.hh"
#include "XrdOuc/XrdOucTrace.hh"
#include "Xrd/XrdScheduler.hh"

EOSMGMNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
/**
 * @brief Constructor
 *
 */
/*----------------------------------------------------------------------------*/
Drainer::Drainer()
{
    XrdSysThread::Run(&mThread, Drainer::StaticDrainer,
                    static_cast<void*>(this), XRDSYSTHREAD_HOLD,
                    "Drainer Thread");
}

/*----------------------------------------------------------------------------*/
/**
 * @brief thread stop function
 */
/*----------------------------------------------------------------------------*/
void
Drainer::Stop()
{
    XrdSysThread::Cancel(mThread);
}

/*----------------------------------------------------------------------------*/
/**
 * @brief Destructor
 */
/*----------------------------------------------------------------------------*/
Drainer::~Drainer()
{
    Stop();

    if (!gOFS->Shutdown) {
        XrdSysThread::Join(mThread, NULL);
    }

}

/*----------------------------------------------------------------------------*/
/**
 *  @brief Start Draining the given FS
 *  
*----------------------------------------------------------------------------*/
bool
Drainer::StartFSDrain(XrdOucEnv& env, XrdOucString& err)
{
    //check the status of the given FS and if it's already in the map for drain FS
    const char* fsId = env.Get("mgm.drain.fsid");
    eos_notice("fs to drain=%s ", fsId);


    auto it_fs = FsView::gFsView.mIdView.find(atoi(fsId));

    if (it_fs == FsView::gFsView.mIdView.end()){
        err = "error: the given FS does not exist";
        return false;

    }

    FileSystem::fsstatus_t configstatus = it_fs->second->GetConfigStatus();

    if (((configstatus == eos::common::FileSystem::kDrain)
         || (configstatus == eos::common::FileSystem::kDrainDead))) {

        err = "error: the given FS is already under draining ";
        return false;
    }


    auto it_drainfs = mDrainFS.find(fsId);

    if (it_drainfs != mDrainFS.end()){
        //fs is already draining
        err = "error: a central FS drain has already started for the given FS ";
        return false;

    }

    //start the drain
    DrainMapPair::first_type s(fsId);
    DrainMapPair::second_type d(new DrainFS(atoi(fsId)));
     
    mDrainMutex.Lock();
    mDrainFS.insert(std::make_pair(s, d));
    mDrainMutex.UnLock();

    return true;
}


/*----------------------------------------------------------------------------*/
/**
 *  @brief Stops Draining the given FS
 *    
 * ----------------------------------------------------------------------------*/
bool
Drainer::StopFSDrain(XrdOucEnv& env, XrdOucString& err)
{
    return true;
}

/*----------------------------------------------------------------------------*/
/**
 * @brief Get Draining status
 *   
 *----------------------------------------------------------------------------*/
bool
Drainer::GetDrainStatus(XrdOucString& out, XrdOucString& err,  std::string& fsid)
{
    return true;
}


/*----------------------------------------------------------------------------*/
/**
 * @brief Static thread startup function 
 */
/*----------------------------------------------------------------------------*/
void*
Drainer::StaticDrainer(void* arg)
{
    return reinterpret_cast<Drainer*>(arg)->Drain();
}


/*----------------------------------------------------------------------------*/
void*
Drainer::Drain()
/*----------------------------------------------------------------------------*/
/**
 * @brief eternal loop trying to run conversion jobs
 */
/*----------------------------------------------------------------------------*/
{
    XrdOucErrInfo error;
    XrdSysThread::SetCancelOn();
    // ---------------------------------------------------------------------------
    // wait that the namespace is initialized
    // ---------------------------------------------------------------------------
    bool go = false;

    eos_static_debug("Drainer starting");
    do {
        XrdSysThread::SetCancelOff();
       {
           XrdSysMutexHelper(gOFS->InitializationMutex);

           if (gOFS->Initialized == gOFS->kBooted) {
               go = true;
           }
       }
       XrdSysThread::SetCancelOn();
       XrdSysTimer sleeper;
       sleeper.Wait(1000);
    } while (!go);

    XrdSysTimer sleeper;
    sleeper.Snooze(10);

    // ---------------------------------------------------------------------------
    // loop forever until cancelled
    // ---------------------------------------------------------------------------
    while (1) {
      eos_static_debug("Drainer running");
  wait:
      XrdSysThread::SetCancelOn();
      // Let some time pass or wait for a notification
      XrdSysTimer sleeper;
      sleeper.Wait(10000);
      XrdSysThread::CancelPoint();
    }

    return 0;
}

EOSMGMNAMESPACE_END
