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
    eos::common::FileSystem::fs_snapshot_t drain_snapshot;
    it_fs->second->SnapShotFileSystem(drain_snapshot, false);
    
    FileSystem::fsstatus_t status = it_fs->second->GetDrainStatus();

    if (status != eos::common::FileSystem::kNoDrain ) {

        err = "error: the given FS is already under draining ";
        return false;
    }

    auto it_drainfs = mDrainFS.find(drain_snapshot.mHostPort);

    if (it_drainfs != mDrainFS.end()){
        //check if the FS is already under draining for this node
        auto it = std::find_if( it_drainfs->second.begin(), it_drainfs->second.end(),
          [fsId](const DrainMapPair& element){ return element.first == fsId;} );
        
        if (it != it_drainfs->second.end()) { //fs is already draining
          err = "error: a central FS drain has already started for the given FS ";
          return false;
        }
	else {
          //check if we have reached the max fs per node for this node
          if  (it_drainfs->second.size() >= GetSpaceConf(drain_snapshot.mSpace)) {
            err = "error: reached maximum number of draining fs for the node";
            return false;
          } 
        }
    } 
    //start the drain
    DrainMapPair::first_type s(fsId);
    DrainMapPair::second_type d(new DrainFS(fsId));
    
    mDrainMutex.Lock();
    if (it_drainfs != mDrainFS.end()){
      it_drainfs->second.push_back(std::make_pair(s, d));
    }else {
      std::vector<DrainMapPair> fs_vect;
      fs_vect.push_back(std::make_pair(s, d));
      mDrainFS.insert(std::make_pair(std::string(drain_snapshot.mHostPort),fs_vect));
    }
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
    eos::common::FileSystem::fs_snapshot_t drain_snapshot;
    it_fs->second->SnapShotFileSystem(drain_snapshot, false);

    auto it_drainfs = mDrainFS.find(drain_snapshot.mHostPort);

    if (it_drainfs == mDrainFS.end()){
        //fs is not drainin
        err = "error: a central FS drain has not started for the given FS ";
        return false;
    }
    //check if the FS is already under draining for this node
    auto it = std::find_if( it_drainfs->second.begin(), it_drainfs->second.end(),
             [fsId](const DrainMapPair& element){ return element.first == fsId;} );
    if (it == it_drainfs->second.end()) {
        //fs is not draining
        err = "error: a central FS drain has not started for the given FS ";
        return false;
    }                                 
    (*it).second->DrainStop();
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
    eos::common::FileSystem::fs_snapshot_t drain_snapshot;
    it_fs->second->SnapShotFileSystem(drain_snapshot, false);
    
    auto it_drainfs = mDrainFS.find(drain_snapshot.mHostPort);
    if (it_drainfs == mDrainFS.end()){
        //fs is not draining
        err = "error: the given FS is not drained or under drain";
        return false;
    }
    //check if the FS is already under draining for this node
    auto it = std::find_if( it_drainfs->second.begin(), it_drainfs->second.end(),
       [fsId](const DrainMapPair& element){ return element.first == fsId;} );
    if (it == it_drainfs->second.end()) {
        err = "error: a central FS drain has not started for the given FS ";
        return false;
    }
    mDrainMutex.Lock();
    it_drainfs->second.erase(it);
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
    } else {
       out+=  "No Drain activities are recorded on the System.\n";
       return true;
    }

    TableFormatterBase table;
    std::vector<std::string> selections;

    auto it_drainfs = mDrainFS.begin();
    while (it_drainfs != mDrainFS.end()){
      auto it_map = (*it_drainfs).second.begin();
      while ( it_map !=  (*it_drainfs).second.end()) {
        PrintTable(table, (*it_drainfs).first, *it_map);
        it_map++;
      }
      it_drainfs++;
    }

    
    TableHeader table_header;
    table_header.push_back(std::make_tuple("node", 30, "s"));
    table_header.push_back(std::make_tuple("fs id", 10, "s"));
    table_header.push_back(std::make_tuple("drain status", 30, "s"));
    
    table.SetHeader(table_header);
    out =  table.GenerateTable(HEADER, selections).c_str();
    return true;
}

void
Drainer::PrintTable(TableFormatterBase& table,std::string node, DrainMapPair& pair) {
  
  TableData table_data;
  table_data.emplace_back();
  table_data.back().push_back( TableCell(node, "s"));
  table_data.back().push_back( TableCell(pair.first, "s"));
  table_data.back().push_back( TableCell(FileSystem::GetDrainStatusAsString(pair.second->GetDrainStatus()),"s"));
  table.AddRows(table_data);
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
    while (1) 
    {
      eos::common::RWMutexReadLock vlock(FsView::gFsView.ViewMutex);
    
      auto space = FsView::gFsView.mSpaceView.begin();
      while (space != FsView::gFsView.mSpaceView.end()) {
        int maxdrainingfs = 5;

        std::string spacename = space->second->GetMember("name");
        if (FsView::gFsView.mSpaceView[spacename]->GetConfigMember("drainer.maxfsdrainpernode")!= "" ) {
          maxdrainingfs = atoi(FsView::gFsView.mSpaceView[spacename]->GetConfigMember("drainer.maxfsdrainpernode").c_str());
        }
	else { 
          FsView::gFsView.mSpaceView[spacename]->SetConfigMember("drainer.maxfsdrainpernode", "5", true, "/eos/*/mgm");
        }
        //get the space configuration
        drainConfMutex.Lock(); 
        if  (maxFSperNodeConfMap.count(spacename))  {
          auto it_conf = maxFSperNodeConfMap.find(spacename);
          (*it_conf).second = maxdrainingfs;
        } else {
          maxFSperNodeConfMap.insert(std::make_pair(spacename,maxdrainingfs));
        }
        drainConfMutex.UnLock();
        space++;
      }
      
      XrdSysThread::SetCancelOn();
      // Let some time pass or wait for a notification
      XrdSysTimer sleeper;
      sleeper.Wait(10000);
      XrdSysThread::CancelPoint();
    }

    return 0;
}

unsigned int
Drainer::GetSpaceConf(std::string space)
{
    
    //check space conf to see if we reached the max configured draining fs per node
    if (maxFSperNodeConfMap.count(space)) {
      return maxFSperNodeConfMap[space];
    } 
    else return 0;
}

EOSMGMNAMESPACE_END
