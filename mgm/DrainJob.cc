// ----------------------------------------------------------------------
// File: DrainJob.cc
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
#include "mgm/DrainJob.hh"
#include "mgm/FileSystem.hh"
#include "mgm/FsView.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Quota.hh"
#include "common/FileId.hh"
#include "common/LayoutId.hh"
#include "common/Logging.hh"
#include "common/TransferQueue.hh"
#include "common/TransferJob.hh"
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
DrainJob::~DrainJob() {
  //----------------------------------------------------------------
  //! destructor stops the draining thread 
  //----------------------------------------------------------------
  eos_static_info("waiting for join ...");
  if (thread) {
    XrdSysThread::Cancel(thread);
    if (!gOFS->Shutdown) { XrdSysThread::Join(thread,NULL); }
    thread=0;
  }
  ResetCounter();
  eos_static_notice("Stopping Drain Job for fs=%u", fsid);
}

void
DrainJob::ResetCounter() 
{
  // set all the drain counters back to 0 

  FileSystem* fs = 0;
  if (FsView::gFsView.mIdView.count(fsid)) {
    fs = FsView::gFsView.mIdView[fsid];
    if (fs) {
      //    fs->OpenTransaction();
      fs->SetLongLong("stat.drainbytesleft", 0);
      fs->SetLongLong("stat.drainfiles",     0);
      fs->SetLongLong("stat.timeleft", 0);
      fs->SetLongLong("stat.drainprogress",0);
      fs->SetLongLong("stat.drainretry", 0);
      fs->SetDrainStatus(eos::common::FileSystem::kNoDrain);
      SetDrainer();
      fs->CloseTransaction();
    }
  }
}

/*----------------------------------------------------------------------------*/
void* 
DrainJob::StaticThreadProc(void* arg)
{
  //----------------------------------------------------------------
  //! static thread startup function calling Drain
  //----------------------------------------------------------------
  return reinterpret_cast<DrainJob*>(arg)->Drain();
}


/*----------------------------------------------------------------------------*/
void
DrainJob::SetDrainer()
{
  //----------------------------------------------------------------
  //! routine enabling/disabling the pull thread on FST to participate in draining  
  //----------------------------------------------------------------

  FileSystem* fs = 0;


  fs = 0;
  if (FsView::gFsView.mIdView.count(fsid)) 
    fs = FsView::gFsView.mIdView[fsid];
  if (!fs) {
    return;
  }
  
  FsGroup::const_iterator git;
  bool setactive=false;
  if (FsView::gFsView.mGroupView.count(group)) {
    for ( git = FsView::gFsView.mGroupView[group]->begin(); git != FsView::gFsView.mGroupView[group]->end(); git++) {
      if (FsView::gFsView.mIdView.count(*git)) {
	int drainstatus = (eos::common::FileSystem::GetDrainStatusFromString(FsView::gFsView.mIdView[*git]->GetString("stat.drain").c_str()));
	if ( (drainstatus == eos::common::FileSystem::kDraining) ||
	     (drainstatus == eos::common::FileSystem::kDrainStalling ) ) {
	  // if any group filesystem is draining, all the others have to enable the pull for draining!
	  setactive=true;
	}
      }
    }
    // if the group get's disabled we stop the draining
    if (FsView::gFsView.mGroupView[group]->GetConfigMember("status") != "on") {
      setactive=false;
    }
    for ( git = FsView::gFsView.mGroupView[group]->begin(); git != FsView::gFsView.mGroupView[group]->end(); git++) {
      fs = FsView::gFsView.mIdView[*git];
      if (fs) {
	if (setactive) {
	  if (fs->GetString("stat.drainer") != "on") {
	    fs->SetString("stat.drainer","on");
	  }
	} else {
	  if (fs->GetString("stat.drainer") != "off") {
	    fs->SetString("stat.drainer","off");
	  }
	}
      }
    }
  }
}


void
DrainJob::SetSpaceNode()
{
  std::string SpaceNodeTransfers="";
  std::string SpaceNodeTransferRate="";
  FileSystem* fs = 0;

  eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);

  if (FsView::gFsView.mSpaceView.count(space)) {
    SpaceNodeTransfers       = FsView::gFsView.mSpaceView[space]->GetConfigMember("drainer.node.ntx");
    SpaceNodeTransferRate    = FsView::gFsView.mSpaceView[space]->GetConfigMember("drainer.node.rate");
  }

  FsGroup::const_iterator git;
  if (FsView::gFsView.mGroupView.count(group)) {
    for ( git = FsView::gFsView.mGroupView[group]->begin(); git != FsView::gFsView.mGroupView[group]->end(); git++) {
      if (FsView::gFsView.mIdView.count(*git)) {
	fs = FsView::gFsView.mIdView[*git];
	if (fs) {
	  FsNode* node = FsView::gFsView.mNodeView[fs->GetQueue()];
	  if (node) {
	    // broadcast the rate & stream configuration if changed
	    if (node->GetConfigMember("stat.drain.ntx") != SpaceNodeTransfers) {
	      node->SetConfigMember("stat.drain.ntx", SpaceNodeTransfers,false, "", true);
	    }
	    if (node->GetConfigMember("stat.drain.rate") != SpaceNodeTransferRate) {
	      node->SetConfigMember("stat.drain.rate", SpaceNodeTransferRate, false, "", true);
	    }
	  }
	}
      }
    }
  }
}

/*----------------------------------------------------------------------------*/
void*
DrainJob::Drain(void)
{  

  XrdSysThread::SetCancelOn();
  XrdSysThread::SetCancelDeferred();

  XrdSysTimer sleeper;

  // the retry is currently hardcoded to 1 e.g. the maximum time for a drain operation is 1 x <drainperiod>
  int maxtry=1;
  int ntried=0;

  long long filesleft=0;


 retry:
  ntried++;

  eos_static_notice("Starting Drain Job for fs=%u onOpsError=%d try=%d", fsid,onOpsError, ntried);

  FileSystem* fs = 0;

  {
    eos::common::RWMutexReadLock(FsView::gFsView.ViewMutex, true);
    ResetCounter();
  }


  time_t drainstart = time(NULL);
  time_t drainperiod = 0;
  time_t drainendtime = 0;
  eos::common::FileSystem::fs_snapshot_t drain_snapshot;

  XrdSysThread::SetCancelOff();
  {
    // set status to 'prepare'
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex, true);
    fs = 0;
    if (FsView::gFsView.mIdView.count(fsid)) 
      fs = FsView::gFsView.mIdView[fsid];
    if (!fs) {
      eos_static_notice("Filesystem fsid=%u has been removed during drain operation", fsid);
      return 0 ;
    }
    
    fs->SetDrainStatus(eos::common::FileSystem::kDrainPrepare);
    fs->SetLongLong("stat.drainretry", ntried-1);

    group = fs->GetString("schedgroup");

    fs->SnapShotFileSystem(drain_snapshot,false);
    drainperiod = fs->GetLongLong("drainperiod");
    drainendtime = drainstart + drainperiod;
    space = drain_snapshot.mSpace;
    group = drain_snapshot.mGroup;
  }

  XrdSysThread::SetCancelOn();
  // now we wait 60 seconds ...
  for (int k=0; k< 60; k++) {
    XrdSysThread::SetCancelOff();
    fs->SetLongLong("stat.timeleft", 59-k);
    XrdSysThread::SetCancelOn();
    sleeper.Snooze(1);
    XrdSysThread::CancelPoint();
  }

  //---------------------------------------
  // wait that the namespace is initialized
  //---------------------------------------
  fs->SetDrainStatus(eos::common::FileSystem::kDrainWait);
  bool go=false;
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
  

  // check if we should abort
  XrdSysThread::CancelPoint();

  // build the list of files to migrate
  long long totalfiles=0;

  XrdSysThread::SetCancelOff();
  {
    //------------------------------------
    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex,true);
    try {
      eos::FileSystemView::FileList filelist = gOFS->eosFsView->getFileList(fsid);
      eos::FileSystemView::FileIterator it;

      totalfiles = filelist.size();
      
    } catch ( eos::MDException &e ) {
    // there are no files in that view
    }    
  }
  //------------------------------------

  XrdSysThread::SetCancelOn();
  if (!totalfiles) {
    goto nofilestodrain;
  }

  XrdSysThread::SetCancelOff();
  // set the shared object counter
  {
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex,true);
    fs = 0;
    if (FsView::gFsView.mIdView.count(fsid)) 
      fs = FsView::gFsView.mIdView[fsid];
    if (!fs) {
      eos_static_notice("Filesystem fsid=%u has been removed during drain operation", fsid);
      XrdSysThread::SetCancelOn();
      return 0 ;
    }
    
    fs->SetLongLong("stat.drainbytesleft", fs->GetLongLong("stat.statfs.usedbytes"));
    fs->SetLongLong("stat.drainfiles",     totalfiles);
  } 
  

  if (onOpsError) {
    time_t waitendtime;
    time_t waitreporttime;
    time_t now;

    XrdSysThread::SetCancelOff();
    {
      // set status to 'waiting'
      eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex,true);
      fs = 0;
      if (FsView::gFsView.mIdView.count(fsid)) 
        fs = FsView::gFsView.mIdView[fsid];
      if (!fs) {
        eos_static_notice("Filesystem fsid=%u has been removed during drain operation", fsid);
        return 0 ;
      }
      
      fs->SetDrainStatus(eos::common::FileSystem::kDrainWait);
      
      waitendtime = time(NULL) + (time_t)fs->GetLongLong("graceperiod");
    }

    XrdSysThread::SetCancelOn();
    
    waitreporttime = time(NULL) + 10; // we report every 10 seconds

    while ( (now = time(NULL)) < waitendtime) {
      XrdSysTimer sleeper;
      sleeper.Wait(50);

      // check if we should abort
      XrdSysThread::CancelPoint();

      if (now > waitreporttime) {
        XrdSysThread::SetCancelOff();
        // update stat.timeleft
        eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex,true);
        fs = 0;
        if (FsView::gFsView.mIdView.count(fsid)) 
          fs = FsView::gFsView.mIdView[fsid];
        if (!fs) {
          eos_static_notice("Filesystem fsid=%u has been removed during drain operation", fsid);
          return 0 ;
        } 
        fs->SetLongLong("stat.timeleft", waitendtime-now);
        waitreporttime = now + 10;
        XrdSysThread::SetCancelOn();
      }
    }
    // set the new drain times
    drainstart = now;
    drainendtime = drainstart + drainperiod;
  }

  // check if we should abort
  XrdSysThread::CancelPoint();
  
  // extract all fids to drain
  
  // make statistics of files to be lost if we are in draindead

  XrdSysThread::SetCancelOff();

  // set status to 'draining'
  {
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex,true);
    fs = 0;
    if (FsView::gFsView.mIdView.count(fsid)) 
      fs = FsView::gFsView.mIdView[fsid];
    if (!fs) {
      eos_static_notice("Filesystem fsid=%u has been removed during drain operation", fsid);
      XrdSysThread::SetCancelOn();
      return 0 ;
    }
    
    fs->SetDrainStatus(eos::common::FileSystem::kDraining);

    // this enables the pull functionality on FST
    SetDrainer();
  }

  time_t last_filesleft_change;
  last_filesleft_change = time(NULL);
  long long last_filesleft;

  last_filesleft=0;
  filesleft=0;

  // enable draining
  do {
    XrdSysThread::SetCancelOff();

    bool stalled = ( (time(NULL)-last_filesleft_change) > 600);
    
    SetSpaceNode();
    
    {
      eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex,true);
      last_filesleft = filesleft;
      try {
	eos::FileSystemView::FileList filelist = gOFS->eosFsView->getFileList(fsid);
	filesleft = filelist.size();
      } catch ( eos::MDException &e ) {
	// there are no files in that view
      }
    }
    
    if (!last_filesleft) {
      last_filesleft = filesleft;
    }

    if (filesleft != last_filesleft) {
      last_filesleft_change = time(NULL);
    }

    eos_static_debug("stalled=%d now=%llu last_filesleft_change=%llu filesleft=%llu last_filesleft=%llu", stalled, time(NULL), last_filesleft_change, filesleft, last_filesleft);

    // update drain display variables
    if ( (filesleft != last_filesleft) || stalled ) {      
      
      // ---------------------------------------------
      // get a rough estimate about the drain progress
      // ---------------------------------------------      
      
      {
        eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex,true);
        fs = 0;
        if (FsView::gFsView.mIdView.count(fsid)) 
          fs = FsView::gFsView.mIdView[fsid];
        if (!fs) {
          eos_static_notice("Filesystem fsid=%u has been removed during drain operation", fsid);
          return 0 ;
        }
	fs->SetLongLong("stat.drainbytesleft", fs->GetLongLong("stat.statfs.usedbytes"));
        fs->SetLongLong("stat.drainfiles",     filesleft);
        if ( stalled )
          fs->SetDrainStatus(eos::common::FileSystem::kDrainStalling);
        else
          fs->SetDrainStatus(eos::common::FileSystem::kDraining);
      }

      int progress = (int)(totalfiles)?(100.0*(totalfiles-filesleft)/totalfiles):100;

      {
        eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex,true);
        fs->SetLongLong("stat.drainprogress",  progress, false);
        if ( (drainendtime-time(NULL)) >0) {
          fs->SetLongLong("stat.timeleft", drainendtime-time(NULL), false);
        } else {
          fs->SetLongLong("stat.timeleft", 99999999999LL, false);
        }
      }    
      if (!filesleft) 
        break;
    } 
    
    if (!filesleft) 
      break;

    if (!filesleft) 
      break;

    // check how long we do already draining
    drainperiod = fs->GetLongLong("drainperiod");
    drainendtime = drainstart + drainperiod;

    // set timeleft
    {
      eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex,true);
      int progress = (int)(totalfiles)?(100.0*(totalfiles-filesleft)/totalfiles):100;
      fs = 0;
      if (FsView::gFsView.mIdView.count(fsid)) 
        fs = FsView::gFsView.mIdView[fsid];
      if (!fs) {
        eos_static_notice("Filesystem fsid=%u has been removed during drain operation", fsid);
        XrdSysThread::SetCancelOn();      
        return 0 ;
      }
      fs->SetLongLong("stat.drainprogress",  progress, false);
      
      if ( (drainendtime-time(NULL)) >0) {
        fs->SetLongLong("stat.timeleft", drainendtime-time(NULL), false);
      } else {
        fs->SetLongLong("stat.timeleft", 99999999999LL, false);
      }
    }

    if ((drainperiod) && (drainendtime < time(NULL))) {
      eos_static_notice("Terminating drain operation after drainperiod of %lld seconds has been exhausted", drainperiod);
      // set status to 'drainexpired'
      {
        eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex,true);
        fs = 0;
        if (FsView::gFsView.mIdView.count(fsid)) 
          fs = FsView::gFsView.mIdView[fsid];
        if (!fs) {
          eos_static_notice("Filesystem fsid=%u has been removed during drain operation", fsid);
          XrdSysThread::SetCancelOn();      
          return 0 ;
        }
        
        fs->SetLongLong("stat.drainfiles",     filesleft);
        fs->SetDrainStatus(eos::common::FileSystem::kDrainExpired);
	SetDrainer();       

        // retry logic
        if (ntried < maxtry) {
          // trigger retry; 
        } else {
          XrdSysThread::SetCancelOn();      
          return 0;
        }
      }
      goto retry;
    }
    XrdSysThread::SetCancelOn();
    for (int k=0; k< 10; k++) {
      // check if we should abort
      XrdSysThread::CancelPoint();
      XrdSysTimer sleep;
      sleep.Wait(100);
    }
    XrdSysThread::SetCancelOff();
  } while (1);

  
 nofilestodrain:

  // set status to 'drained'
  {
    XrdSysThread::SetCancelOff();      
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex,true);
    fs = 0;
    if (FsView::gFsView.mIdView.count(fsid)) 
      fs = FsView::gFsView.mIdView[fsid];
    if (!fs) {
      eos_static_notice("Filesystem fsid=%u has been removed during drain operation", fsid);
      XrdSysThread::SetCancelOn();      
      return 0 ;
    }

    fs->SetLongLong("stat.drainfiles",     filesleft);    
    fs->SetDrainStatus(eos::common::FileSystem::kDrained);    
    fs->SetLongLong("stat.drainbytesleft", 0);
    fs->SetLongLong("stat.timeleft", 0);
    SetDrainer();
    // we automatically switch this filesystem to the 'empty' state
    fs->SetString("configstatus","empty");
    FsView::gFsView.StoreFsConfig(fs);
    fs->SetLongLong("stat.drainprogress",  100);
  }
  XrdSysThread::SetCancelOn();      
  return 0;
}

EOSMGMNAMESPACE_END

