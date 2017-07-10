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
    const char* fsIdString = env.Get("mgm.drain.fsid");
 
    eos::common::FileSystem::fsid_t fsId = atoi(fsIdString);
     
    eos_notice("fs to drain=%d ", fsId);
    
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
    
    auto it_fs = FsView::gFsView.mIdView.find(fsId);

    if (it_fs == FsView::gFsView.mIdView.end()){
        err = "error: the given FS does not exist";
        return false;
    }

    FileSystem::fsstatus_t status = it_fs->second->GetDrainStatus();

    if (status != eos::common::FileSystem::kNoDrain ) {

        err = "error: the given FS is already under draining ";
        return false;
    }

    auto it_drainfs = mDrainFS.find(fsId);

    if (it_drainfs != mDrainFS.end()){
        //fs is already draining
        err = "error: a central FS drain has already started for the given FS ";
        return false;
    }

    if (!UpdateSpaceStats(it_fs->second,true)) {
      err = "error: maximum number of draing fs for the given space reached";	
      return false;
    }
    
    //start the drain
    DrainMapPair::first_type s(fsId);
    DrainMapPair::second_type d(new DrainFS(fsId));
     
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
    //getting the fs id
    const char* fsIdString = env.Get("mgm.drain.fsid");
    eos::common::FileSystem::fsid_t fsId = atoi(fsIdString);
    
    eos_notice("fs to stop draining=%d ", fsId);

    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);

    auto it_fs = FsView::gFsView.mIdView.find(fsId);

    if (it_fs == FsView::gFsView.mIdView.end()){
        err = "error: the given FS does not exist";
        return false;
    }
    auto it_drainfs = mDrainFS.find(fsId);

    if (it_drainfs == mDrainFS.end()){
        //fs is not drainin
        err = "error: a central FS drain has not started for the given FS ";
        return false;
    }

    (*it_drainfs).second->DrainStop();
    return true;
  
}


/*----------------------------------------------------------------------------*/
/**
 * @brief Clear FS from the given map
 *     
 * ----------------------------------------------------------------------------*/
bool
Drainer::ClearFSDrain(XrdOucEnv& env, XrdOucString& err)
{
    //getting the fs id
    const char* fsIdString = env.Get("mgm.drain.fsid");
    eos::common::FileSystem::fsid_t fsId = atoi(fsIdString);
    //
    eos_notice("fs to clear=%d ", fsId);
    //
    //
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);

    auto it_fs = FsView::gFsView.mIdView.find(fsId);
    //
    if (it_fs == FsView::gFsView.mIdView.end()){
        err = "error: the given FS does not exist";
        return false;
    }
    auto it_drainfs = mDrainFS.find(fsId);
    if (it_drainfs == mDrainFS.end()){
        //fs is not draining
        err = "error: the given FS is not drained or under drained";
        return false;
    }

    if (!UpdateSpaceStats(it_fs->second,false)) {
      return false;
    }

    mDrainMutex.Lock();
    mDrainFS.erase(it_drainfs);
    mDrainMutex.UnLock();

    return true;
}

/*----------------------------------------------------------------------------*/
/**
 * @brief Get Draining status
 *   
 *----------------------------------------------------------------------------*/
bool
Drainer::GetDrainStatus(XrdOucEnv& env,XrdOucString& out, XrdOucString& err)
{
    if (mDrainFS.size() > 0 ){
       out+=  "Status of the drain activities on the System:\n";
     } else {
       out+=  "No Drain activities are recorded on the System.\n";
       return true;
     }

    auto it_drainfs = mDrainFS.begin();
    
    while (it_drainfs != mDrainFS.end()){
      //fs is already draining
      out+= "Drain Status for FS with ID: ";
      out+= std::to_string((*it_drainfs).first).c_str();
      out+= " => ";
      out+= FileSystem::GetDrainStatusAsString((*it_drainfs).second->GetDrainStatus());
      out+= "\n";
       
      std::vector<shared_ptr<DrainTransferJob>> jobs;
      jobs = (*it_drainfs).second->GetJobs(DrainTransferJob::Failed);
      
      out+= "\tFailed Draining Files:\n";
      auto it_jobs = jobs.begin(); 
      
      while (it_jobs != jobs.end()){
        out+= "\tFile Id: ";
        out+= std::to_string((*it_jobs)->GetFileId()).c_str();
        out+= "\n";
        it_jobs++;
      }

      it_drainfs++;
      
    }
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
 * @brief lunch Drainer Thread // TO CHECK IF WE NEED TO HAVE A THREAD HERE 
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

    XrdSysThread::SetCancelOff();
    //load conf
    {
    eos::common::RWMutexReadLock vlock(FsView::gFsView.ViewMutex);
    
    auto space = FsView::gFsView.mSpaceView.begin();
    while (space != FsView::gFsView.mSpaceView.end()) {
        int maxdrainingfs = 0;
        std::string spacename = space->second->GetMember("name");
        if (FsView::gFsView.mSpaceView[spacename]->GetConfigMember("drainer.maxdrainingfs")!= "" ) {
          maxdrainingfs = stoi(FsView::gFsView.mSpaceView[spacename]->GetConfigMember("drainer.maxdrainingfs"));
        }
	else { 
          FsView::gFsView.mSpaceView[spacename]->SetConfigMember("drainer.maxdrainingfs", "10", true, "/eos/*/mgm");
        }
        //get the space configuration
        mDrainingFSMutex.Lock();
        std::pair<int, int> pair = std::make_pair(0, maxdrainingfs);
        drainingFSMap.insert(std::make_pair(spacename,pair));
        mDrainingFSMutex.UnLock();
        space++;
      }
    }
    // ---------------------------------------------------------------------------
    // loop forever until cancelled
    // ---------------------------------------------------------------------------
    while (1) {
      eos_static_debug("Drainer running");
      XrdSysThread::SetCancelOn();
      // Let some time pass or wait for a notification
      XrdSysTimer sleeper;
      sleeper.Wait(10000);
      XrdSysThread::CancelPoint();
    }

    return 0;
}

bool
Drainer::UpdateSpaceStats(eos::common::FileSystem* fs , bool isDrainStart)
{
    eos::common::FileSystem::fs_snapshot_t drain_snapshot;
    fs->SnapShotFileSystem(drain_snapshot, false);

    //check space conf to see if we reached the max configured drain fs
    mDrainingFSMutex.Lock();
    if (drainingFSMap.count(drain_snapshot.mSpace)) {
      int running = drainingFSMap[drain_snapshot.mSpace].first;
      int max = drainingFSMap[drain_snapshot.mSpace].second;

      if (isDrainStart) {
        if (running == max) {
          mDrainingFSMutex.UnLock();
          return false;
        }
        else {
          std::pair<int, int> pair = std::make_pair(++running, max);
          drainingFSMap.insert(std::make_pair(drain_snapshot.mSpace,pair));
        }
      }
      else {
         std::pair<int, int> pair = std::make_pair(--running, max);
         drainingFSMap.insert(std::make_pair(drain_snapshot.mSpace,pair));
      }
    } 
    mDrainingFSMutex.UnLock();
    return true;
}

EOSMGMNAMESPACE_END
