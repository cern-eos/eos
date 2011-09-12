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
    XrdSysThread::Join(thread,NULL);
    thread=0;
  }
  ResetCounter(false);
  eos_static_notice("Stopping Drain Job for fs=%u", fsid);
}

void
DrainJob::ResetCounter(bool lockit) 
{
  // set all the drain counters back to 0 

  if (lockit) FsView::gFsView.ViewMutex.LockRead();
  FileSystem* fs = 0;
  if (FsView::gFsView.mIdView.count(fsid)) {
    fs = FsView::gFsView.mIdView[fsid];
    if (fs) {
      //    fs->OpenTransaction();
      fs->SetLongLong("stat.drainbytesleft", 0);
      fs->SetLongLong("stat.drainfiles",     0);
      fs->SetLongLong("stat.drainscheduledfiles",   0);
      fs->SetLongLong("stat.drainscheduledbytes",   0);
      fs->SetLongLong("stat.drainlostfiles", 0);
      fs->SetLongLong("stat.timeleft", 0);
      fs->SetLongLong("stat.drainprogress",0);
      fs->SetLongLong("stat.drainretry", 0);
      //    fs->CloseTransaction();
    }
  }
  if (lockit) FsView::gFsView.ViewMutex.UnLockRead();
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
void*
DrainJob::Drain(void)
{  
  XrdSysThread::SetCancelOn();
  XrdSysThread::SetCancelDeferred();

  XrdSysTimer sleeper;
  // the retry is currently hardcoded to 3 e.g. the maximum time for a drain operation is 3 x <drainperiod>
  int maxtry=3;
  int ntried=0;

 retry:
  ntried++;

  eos_static_notice("Starting Drain Job for fs=%u onOpsError=%d try=%d", fsid,onOpsError, ntried);

  FileSystem* fs = 0;
  ResetCounter();


  std::string group="";
  time_t drainstart = time(NULL);
  time_t drainperiod = 0;
  time_t drainendtime = 0;
  eos::common::FileSystem::fs_snapshot_t drain_snapshot;

  XrdSysThread::SetCancelOff();
  {
    // set status to 'prepare'
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
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
  }

  XrdSysThread::SetCancelOn();
  // now we wait 60 seconds ...
  for (int k=0; k< 60; k++) {
    sleeper.Snooze(1);
    XrdSysThread::CancelPoint();
  }

  // check if we should abort
  XrdSysThread::CancelPoint();

  // build the list of files to migrate
  long long totalbytes=0;
  long long totalfiles=0;
  long long totallostfiles=0;
  long long scheduledbytes=0;
  long long scheduledfiles=0;

  XrdSysThread::SetCancelOff();
  //------------------------------------
  gOFS->eosViewMutex.Lock(); 
  try {
    eos::FileMD* fmd = 0;
    eos::FileSystemView::FileList filelist = gOFS->eosFsView->getFileList(fsid);
    eos::FileSystemView::FileIterator it;
    
    for (it = filelist.begin(); it != filelist.end(); ++it) {
      fmd = gOFS->eosFileService->getFileMD(*it);
      if (fmd) {
        //eos::FileMD::LocationVector::const_iterator lociter;
        //      for ( lociter = fmd->locationsBegin(); lociter != fmd->locationsEnd(); ++lociter) {
        //      }
        totalbytes+= fmd->getSize();
        totalfiles++;

        // insert into the drainqueue
        fids.push_back((unsigned long long)fmd->getId());
      }
    }
  } catch ( eos::MDException &e ) {
    // there are no files in that view
  }
  
  gOFS->eosViewMutex.UnLock();
  //------------------------------------

  XrdSysThread::SetCancelOn();
  if (!totalfiles) {
    goto nofilestodrain;
  }

  XrdSysThread::SetCancelOff();
  // set the shared object counter
  {
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
    fs = 0;
    if (FsView::gFsView.mIdView.count(fsid)) 
      fs = FsView::gFsView.mIdView[fsid];
    if (!fs) {
      eos_static_notice("Filesystem fsid=%u has been removed during drain operation", fsid);
      XrdSysThread::SetCancelOn();
      return 0 ;
    }
    
    fs->SetLongLong("stat.drainbytesleft", totalbytes);
    fs->SetLongLong("stat.drainfiles",     totalfiles);
    fs->SetLongLong("stat.drainscheduledfiles",   0);
    fs->SetLongLong("stat.drainscheduledbytes",   0);
    fs->SetLongLong("stat.drainlostfiles", totallostfiles);
  } 
  

  if (onOpsError) {
    time_t waitendtime;
    time_t waitreporttime;
    time_t now;

    XrdSysThread::SetCancelOff();
    {
      // set status to 'waiting'
      eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
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
      usleep(50000);

      // check if we should abort
      XrdSysThread::CancelPoint();

      if (now > waitreporttime) {
        // update stat.timeleft
        eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
        fs = 0;
        if (FsView::gFsView.mIdView.count(fsid)) 
          fs = FsView::gFsView.mIdView[fsid];
        if (!fs) {
          eos_static_notice("Filesystem fsid=%u has been removed during drain operation", fsid);
          return 0 ;
        } 
        fs->SetLongLong("stat.timeleft", waitendtime-now);
        waitreporttime = now + 10;
      }
    }
  }

  // check if we should abort
  XrdSysThread::CancelPoint();
  
  // extract all fids to drain
  
  // make statistics of files to be lost if we are in draindead

  XrdSysThread::SetCancelOff();

  // set status to 'draining'
  {
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
    fs = 0;
    if (FsView::gFsView.mIdView.count(fsid)) 
      fs = FsView::gFsView.mIdView[fsid];
    if (!fs) {
      eos_static_notice("Filesystem fsid=%u has been removed during drain operation", fsid);
      XrdSysThread::SetCancelOn();
      return 0 ;
    }
    
    fs->SetDrainStatus(eos::common::FileSystem::kDraining);
  }

  time_t last_scheduled;
  last_scheduled = time(NULL);

  // start scheduling into the queues
  do {
    size_t fids_start = fids.size();
    {
      // do one loop over the scheduling group and check how many files are still scheduled
      eos::common::RWMutexReadLock viewlock(FsView::gFsView.ViewMutex);
      std::set<eos::common::FileSystem::fsid_t>::const_iterator it;
      if (FsView::gFsView.mGroupView.count(group)) {
        for (it = FsView::gFsView.mGroupView[group]->begin(); it != FsView::gFsView.mGroupView[group]->end(); it++) {
          eos::common::FileSystem::fs_snapshot_t target_snapshot;
          eos::common::FileSystem::fs_snapshot_t source_snapshot;
          FileSystem* target_fs = FsView::gFsView.mIdView[*it];
          FileSystem* source_fs = 0;
          target_fs->SnapShotFileSystem(target_snapshot,false);
          
          if ( (target_snapshot.mId != fsid ) && 
               (target_snapshot.mStatus       == eos::common::FileSystem::kBooted) && 
               (target_snapshot.mConfigStatus >= eos::common::FileSystem::kRW)     &&
               (target_snapshot.mErrCode      == 0 ) &&
               (fs->HasHeartBeat(target_snapshot)) &&
               (fs->GetActiveStatus(target_snapshot))
               ) {

            // this is a healthy filesystem and can be used
            eos::common::TransferQueue* queue = 0;
            if ( (queue = target_fs->GetDrainQueue())) {
              int n2submit = 10 - queue->Size();
              if (n2submit>0) {
                // submit n2submit jobs
                eos_static_info("submitting %d new transfer jobs", n2submit);

                queue->OpenTransaction();
                for (int nsubmit = 0; nsubmit < n2submit; nsubmit++) {
                  if (fids.size()==0) 
                    break;
                  
                  unsigned long long fid=0;
                  unsigned long long cid=0;
                  unsigned long long size=0;
                  long unsigned int lid = 0;
		  uid_t uid=0;
		  gid_t gid=0;
                  bool acceptid=false;
                  size_t q=0;
                  std::string fullpath = "";
                  std::vector<unsigned int> locationfs;


                  for (q=0; q< fids.size(); q++) {
                    fid = fids[q];
                    locationfs.clear();
                    // ------------------------------------------
                    gOFS->eosViewMutex.Lock();
                    eos::FileMD* fmd = 0;
                    unsigned long long size=0;
                    try {
                      fmd = gOFS->eosFileService->getFileMD(fid);
                      lid = fmd->getLayoutId();
                      cid = fmd->getContainerId();
                      size = fmd->getSize();
		      uid = fmd->getCUid();
		      gid = fmd->getCGid();

                      // push all the locations
                      eos::FileMD::LocationVector::const_iterator lociter;
                      for ( lociter = fmd->locationsBegin(); lociter != fmd->locationsEnd(); ++lociter) {
                        // ignore filesystem id 0
                        if ((*lociter)) {
                          locationfs.push_back(*lociter);
                        }
                      }
                      fullpath = gOFS->eosView->getUri(fmd);
                    } catch ( eos::MDException &e ) {
                      fmd = 0;
                    }
                    if (fmd && (!fmd->hasLocation(target_snapshot.mId))) {
                      // we can put a replica here ! 
                      size = fmd->getSize();

		      // check if there is space for that file
		      if (fs->ReserveSpace(target_snapshot, size))
			acceptid = true;
                    }


                    gOFS->eosViewMutex.UnLock();
                    // ------------------------------------------
                    if (acceptid) {
                      scheduledbytes += size;
                      break;
                    }
                  }

                  if (acceptid) {
                    eos::common::RWMutexReadLock lock(Quota::gQuotaMutex);

                    fids.erase(fids.begin() + q);
                    XrdOucString sizestring="";
                    long unsigned int fsindex = 0;
                    
                    // get the responsible quota space
                    SpaceQuota* space = Quota::GetResponsibleSpaceQuota(drain_snapshot.mSpace.c_str());
                    if (space) {
                      eos_static_info("Responsible space is %s\n", space->GetSpaceName());
                    } else {
		      eos_static_err("No responsible space for %s\n", drain_snapshot.mSpace.c_str());
		    }
                    // schedule access to that file as a plain file
                    int retc=0;
                    if ((!space) || (retc=space->FileAccess((uid_t)0,(gid_t)0,(long unsigned int)0, (const char*) 0, lid, locationfs, fsindex, false, (long long unsigned)0))) {
                      // uups, we cannot access this file at all, add it to the lost files
                      eos_static_crit("File fid=%lu [%d] is lost - no replica accessible during drain operation errno=%d\n", fid, locationfs.size(),retc);

                      //                      eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
                      fs = 0;
                      if (FsView::gFsView.mIdView.count(fsid)) 
                        fs = FsView::gFsView.mIdView[fsid];
                      if (!fs) {
                        eos_static_notice("Filesystem fsid=%u has been removed during drain operation", fsid);
                        return 0 ;
                      }

                      totallostfiles++;
                      fs->SetLongLong("stat.drainlostfiles", totallostfiles);
                    } else {                   
                      // create the capability for the transfer to allow the target machine read access on that file
                      source_fs = FsView::gFsView.mIdView[locationfs[fsindex]];
                      source_fs->SnapShotFileSystem(source_snapshot,false);                      
                      XrdOucString source_capability="";
                      XrdOucString sizestring;
                      source_capability += "mgm.access=read";
                      source_capability += "&mgm.lid=";        source_capability += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long)lid&0xffffff0f); // make's it a plain replica
                      source_capability += "&mgm.cid=";        source_capability += eos::common::StringConversion::GetSizeString(sizestring,cid);
                      source_capability += "&mgm.ruid=";       source_capability+=(int)1;
                      source_capability += "&mgm.rgid=";       source_capability+=(int)1;
                      source_capability += "&mgm.uid=";        source_capability+=(int)1;
                      source_capability += "&mgm.gid=";        source_capability+=(int)1;
                      source_capability += "&mgm.path=";       source_capability += fullpath.c_str();
                      source_capability += "&mgm.manager=";    source_capability += gOFS->ManagerId.c_str();
                      source_capability += "&mgm.fid=";    
                      XrdOucString hexfid; eos::common::FileId::Fid2Hex(fid,hexfid);source_capability += hexfid;
                      source_capability += "&mgm.drainfsid=";  source_capability += (int)fsid;
                     
                      // build the source_capability contents
                      source_capability += "&mgm.localprefix=";       source_capability += source_snapshot.mPath.c_str();
                      source_capability += "&mgm.fsid=";              source_capability += (int)source_snapshot.mId;
                      source_capability += "&mgm.sourcehostport=";    source_capability += source_snapshot.mHostPort.c_str();
                      source_capability += "&mgm.lfn=";               source_capability += fullpath.c_str();

                      XrdOucString target_capability="";
                      target_capability += "mgm.access=write";
                      target_capability += "&mgm.lid=";        target_capability += eos::common::StringConversion::GetSizeString(sizestring,(unsigned long long)lid&0xffffff0f); // make's it a plain replica
		      target_capability += "&mgm.source.lid="; target_capability += eos::common::StringConversion::GetSizeString(sizestring,(unsigned long long)lid);
		      target_capability += "&mgm.source.ruid="; target_capability += eos::common::StringConversion::GetSizeString(sizestring,(unsigned long long)uid);
		      target_capability += "&mgm.source.rgid="; target_capability += eos::common::StringConversion::GetSizeString(sizestring,(unsigned long long)gid);

		      target_capability += "&mgm.cid=";        target_capability += eos::common::StringConversion::GetSizeString(sizestring,cid);
                      target_capability += "&mgm.ruid=";       target_capability+=(int)1;
                      target_capability += "&mgm.rgid=";       target_capability+=(int)1;
                      target_capability += "&mgm.uid=";        target_capability+=(int)1;
                      target_capability += "&mgm.gid=";        target_capability+=(int)1;
                      target_capability += "&mgm.path=";       target_capability += fullpath.c_str();
                      target_capability += "&mgm.manager=";    target_capability += gOFS->ManagerId.c_str();
                      target_capability += "&mgm.fid=";    
                      target_capability += hexfid;
                      target_capability += "&mgm.drainfsid=";  target_capability += (int)fsid;
                     
                      // build the target_capability contents
                      target_capability += "&mgm.localprefix=";       target_capability += target_snapshot.mPath.c_str();
                      target_capability += "&mgm.fsid=";              target_capability += (int)target_snapshot.mId;
                      target_capability += "&mgm.targethostport=";    target_capability += target_snapshot.mHostPort.c_str();
                      target_capability += "&mgm.lfn=";               target_capability += fullpath.c_str();
                      target_capability += "&mgm.bookingsize=";       target_capability += eos::common::StringConversion::GetSizeString(sizestring, size);                      
                      // issue a source_capability
                      XrdOucEnv insource_capability(source_capability.c_str());
                      XrdOucEnv intarget_capability(target_capability.c_str());
                      XrdOucEnv* source_capabilityenv = 0;
                      XrdOucEnv* target_capabilityenv = 0;
                      XrdOucString fullcapability="";
                      eos::common::SymKey* symkey = eos::common::gSymKeyStore.GetCurrentKey();
                      
                      int caprc=0;
                      if ((caprc=gCapabilityEngine.Create(&insource_capability, source_capabilityenv, symkey)) || 
                          (caprc=gCapabilityEngine.Create(&intarget_capability, target_capabilityenv, symkey))){
                        eos_static_err("unable to create source/target capability - errno=%u", caprc);
                        errno = caprc;
                      } else {
                        errno = 0;
                        int caplen = 0;
                        XrdOucString source_cap = source_capabilityenv->Env(caplen); 
                        XrdOucString target_cap = target_capabilityenv->Env(caplen);
                        source_cap.replace("cap.sym","source.cap.sym");
                        target_cap.replace("cap.sym","target.cap.sym");
                        source_cap.replace("cap.msg","source.cap.msg");
                        target_cap.replace("cap.msg","target.cap.msg");
                        source_cap += "&source.url=root://"; source_cap += source_snapshot.mHostPort.c_str();source_cap += "//replicate:"; source_cap += hexfid;
                        target_cap += "&target.url=root://"; target_cap += target_snapshot.mHostPort.c_str();target_cap += "//replicate:"; target_cap += hexfid;
                        fullcapability += source_cap;
                        fullcapability += target_cap; 
                        
                        eos::common::TransferJob* txjob = new eos::common::TransferJob(fullcapability.c_str());
                        bool sub = queue->Add(txjob);
                        eos_static_info("Submitted %d %s\n", sub, fullcapability.c_str());
                        delete txjob;
                        if (source_capabilityenv)
                          delete source_capabilityenv;
                        if (target_capabilityenv)
                          delete target_capabilityenv;
			// book the space
			target_fs->PreBookSpace(size);
                      }
                      last_scheduled = time(NULL);
                    }
                  }
		}
                queue->CloseTransaction();
              }
            }
          }
	  target_fs->FreePreBookedSpace();
        }
      }
    }

    XrdSysThread::SetCancelOn();

    size_t fids_stop = fids.size();
    scheduledfiles = totalfiles-fids.size();
    bool stalled = ( (time(NULL)-last_scheduled) > 600);

    // update drain display variables
    if ( (fids_start != fids_stop) || stalled ) {
      XrdSysThread::SetCancelOff();
      {
        eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
        fs = 0;
        if (FsView::gFsView.mIdView.count(fsid)) 
          fs = FsView::gFsView.mIdView[fsid];
        if (!fs) {
          eos_static_notice("Filesystem fsid=%u has been removed during drain operation", fsid);
          return 0 ;
        }
        fs->SetLongLong("stat.drainbytesleft", totalbytes-scheduledbytes);
        fs->SetLongLong("stat.drainfiles",     totalfiles-scheduledfiles);
        fs->SetLongLong("stat.drainscheduledfiles",  scheduledfiles);
        fs->SetLongLong("stat.drainscheduledbytes",  scheduledbytes);
        if ( stalled )
          fs->SetDrainStatus(eos::common::FileSystem::kDrainStalling);
        else
          fs->SetDrainStatus(eos::common::FileSystem::kDraining);
      }
      // ---------------------------------------------
      // get a rough estimate about the drain progress
      // ---------------------------------------------      

      gOFS->eosViewMutex.Lock(); 
      long long filesleft=0;
      try {
        eos::FileSystemView::FileList filelist = gOFS->eosFsView->getFileList(fsid);
        filesleft = filelist.size();
      } catch ( eos::MDException &e ) {
        // there are no files in that view
      }
      gOFS->eosViewMutex.UnLock();
      int progress = (int)(totalfiles)?(100.0*(totalfiles-filesleft + totallostfiles)/totalfiles):100;

      {
        eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
        fs->SetLongLong("stat.drainprogress",  progress);
        if ( (drainendtime-time(NULL)) >0) {
          fs->SetLongLong("stat.timeleft", drainendtime-time(NULL));
        } else {
          fs->SetLongLong("stat.timeleft", 99999999999);
        }
      }

      XrdSysThread::SetCancelOn();
    }

    if (fids.size()==0) 
      break;
    
    for (int k=0; k< 10; k++) {
      // check if we should abort
      XrdSysThread::CancelPoint();
      usleep(100000);
    }
  } while (1);

  
  sleeper.Snooze(1);  

  // now wait that all files disappear from the view 

  do {
    XrdSysThread::SetCancelOff();         

    long long filesleft = 0;

    //------------------------------------
    gOFS->eosViewMutex.Lock(); 
    try {
      eos::FileSystemView::FileList filelist = gOFS->eosFsView->getFileList(fsid);
      filesleft = filelist.size();

    } catch ( eos::MDException &e ) {
      // there are no files in that view
    }
    
    gOFS->eosViewMutex.UnLock();
    //------------------------------------

    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
    fs = 0;
    if (FsView::gFsView.mIdView.count(fsid)) 
      fs = FsView::gFsView.mIdView[fsid];
    if (!fs) {
      eos_static_notice("Filesystem fsid=%u has been removed during drain operation", fsid);
      return 0 ;
    }
    
    drainperiod = fs->GetLongLong("drainperiod");
    drainendtime = drainstart + drainperiod;

    // set timeleft
    {
      eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
      int progress = (int)(totalfiles)?(100.0*(totalfiles-filesleft+totallostfiles)/totalfiles):100;
      fs = 0;
      if (FsView::gFsView.mIdView.count(fsid)) 
        fs = FsView::gFsView.mIdView[fsid];
      if (!fs) {
        eos_static_notice("Filesystem fsid=%u has been removed during drain operation", fsid);
	XrdSysThread::SetCancelOn();      
        return 0 ;
      }
      fs->SetLongLong("stat.drainprogress",  progress);
      
      if ( (drainendtime-time(NULL)) >0) {
        fs->SetLongLong("stat.timeleft", drainendtime-time(NULL));
      } else {
        fs->SetLongLong("stat.timeleft", 99999999999);
      }
    }

    if ((drainperiod) && (drainendtime < time(NULL))) {
      eos_static_notice("Terminating drain operation after drainperiod of %lld seconds has been exhausted", drainperiod);
      // set status to 'drainexpired'
      {
        eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
        fs = 0;
        if (FsView::gFsView.mIdView.count(fsid)) 
          fs = FsView::gFsView.mIdView[fsid];
        if (!fs) {
          eos_static_notice("Filesystem fsid=%u has been removed during drain operation", fsid);
	  XrdSysThread::SetCancelOn();      
          return 0 ;
        }
        
        fs->SetDrainStatus(eos::common::FileSystem::kDrainExpired);

        // retry logic
        if (ntried <=maxtry) {
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
      sleeper.Snooze(1);
    }

    // with successful drained, we have nothing left
    if (filesleft <= totallostfiles) {
      break;
    }
  } while (1);


 nofilestodrain:

  // set status to 'drained'
  {
    XrdSysThread::SetCancelOff();      
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
    fs = 0;
    if (FsView::gFsView.mIdView.count(fsid)) 
      fs = FsView::gFsView.mIdView[fsid];
    if (!fs) {
      eos_static_notice("Filesystem fsid=%u has been removed during drain operation", fsid);
      XrdSysThread::SetCancelOn();      
      return 0 ;
    }
    if (totallostfiles) 
      fs->SetDrainStatus(eos::common::FileSystem::kDrainLostFiles);
    else {
      fs->SetDrainStatus(eos::common::FileSystem::kDrained);
      // we automatically switch this filesystem to the 'empty' state
      fs->SetString("configstatus","empty");
      FsView::gFsView.StoreFsConfig(fs);
    }
    fs->SetLongLong("stat.drainprogress",  100);
  }
  XrdSysThread::SetCancelOn();      
  return 0;
}

EOSMGMNAMESPACE_END

