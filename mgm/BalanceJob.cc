/*----------------------------------------------------------------------------*/
#include "mgm/BalanceJob.hh"
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


XrdSysMutex BalanceJob::gSchedulingMutex;

/*----------------------------------------------------------------------------*/
BalanceJob::BalanceJob(FsGroup* group)
{
  //----------------------------------------------------------------
  //! constructor create a balancing thread
  //----------------------------------------------------------------
  mGroup = group;
  mThreadRunningLock.Lock();
  mThreadRunning=false;
  mThreadRunningLock.UnLock();
  if (group) {
    mName =  group->GetMember("name").c_str();
  } else {
    mName = "undef";
  }
  mThreadRunningLock.Lock();
  XrdSysThread::Run(&thread, BalanceJob::StaticThreadProc, static_cast<void *>(this),XRDSYSTHREAD_HOLD, "BalanceJob Thread");
  mThreadRunning=true;
  mThreadRunningLock.UnLock();
}

/*----------------------------------------------------------------------------*/
bool
BalanceJob::ReActivate()
{
  //----------------------------------------------------------------
  //! reactivate the balance thread if is has terminated already
  //----------------------------------------------------------------
  bool isrunning=false;
  mThreadRunningLock.Lock();
  isrunning = mThreadRunning;
  mThreadRunningLock.UnLock();
  if (!isrunning) {
    if (thread) {
      XrdSysThread::Cancel(thread);
      XrdSysThread::Join(thread,NULL);
      thread=0;
    }
    eos_static_notice("re-activating balancejob on %s", mName.c_str());
    XrdSysThread::Run(&thread, BalanceJob::StaticThreadProc, static_cast<void *>(this),XRDSYSTHREAD_HOLD, "BalanceJob Thread");
    return true;
  }
  return false;
}


/*----------------------------------------------------------------------------*/
BalanceJob::~BalanceJob() {
  //----------------------------------------------------------------
  //! destructor stops the balancing thread and clears the transfer queues
  //---------------------------------------------------------------
  eos_static_notice("Stoppging balancing in group=%s", mName.c_str());
  XrdSysThread::Cancel(thread);
  XrdSysThread::Join(thread,NULL);

  {
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
    std::set<eos::common::FileSystem::fsid_t>::const_iterator it;
    unsigned long long totalfiles = 0;
      
    for (it = mGroup->begin(); it != mGroup->end();it++){ 
      eos_static_notice("Clearing balance Queue of fsid=%u", *it);
      FsView::gFsView.mIdView[*it]->GetBalanceQueue()->Clear();
    }

    for (it = mGroup->begin(); it != mGroup->end();it++){ 
      totalfiles += FsView::gFsView.mIdView[*it]->GetBalanceQueue()->Size();
    }

    std::string squeued="0";
    eos::common::StringConversion::GetSizeString(squeued, totalfiles);    

    mGroup->SetConfigMember("stat.balancing.queued",squeued,false, "", true);
  }

  {
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
    mGroup->SetConfigMember("stat.balancing","idle",false, "", true);
    mGroup->SetConfigMember("stat.balancing.queued","0", false, "", true);
  }

}


/*----------------------------------------------------------------------------*/
void* 
BalanceJob::StaticThreadProc(void* arg)
{
  //----------------------------------------------------------------
  //! static thread startup function calling Balance
  //----------------------------------------------------------------
  return reinterpret_cast<BalanceJob*>(arg)->Balance();
}

/*----------------------------------------------------------------------------*/
void*
BalanceJob::Balance(void)
{
  unsigned long long nscheduled=0;
  XrdSysThread::SetCancelOn();
  XrdSysThread::SetCancelDeferred();

  mThreadRunningLock.Lock();
  mThreadRunning=true;
  mThreadRunningLock.UnLock();

  // clear all maps
  SourceFidMap.clear();
  SourceFidSet.clear();
  SourceSizeMap.clear();
  TargetSizeMap.clear();
  TargetQueues.clear();
  TargetFidMap.clear();

  unsigned int seed = (int) (XrdSysThread::ID() + time(NULL));
  int sleeper =100 + ( 20 * (rand_r(&seed))/(RAND_MAX+1.0));

  XrdSysThread::SetCancelOff();
  
  {
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
    mGroup->SetConfigMember("stat.balancing","activating",false, "", true);
  }

  XrdSysThread::SetCancelOn();

  for (int i=0; i< sleeper ; i++) {
    XrdSysTimer sleeper;
    sleeper.Snooze(1);
    XrdSysThread::CancelPoint();
  }

  eos_static_notice("Started balancing on group %s", mName.c_str());
  XrdSysThread::SetCancelOff();
  // set status to 'active'
  {
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
    mGroup->SetConfigMember("stat.balancing","scheduling",false, "", true);
  }
  XrdSysThread::SetCancelOn();

  XrdSysThread::CancelPoint();

  XrdSysThread::SetCancelOff();
  // look into all our group members how much they are off the avg
  {
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
    std::set<eos::common::FileSystem::fsid_t>::const_iterator it;

    // get the current average value
    unsigned long long avg = (unsigned long long) mGroup->AverageDouble("stat.statfs.usedbytes");

    // we cannot schedule too many transfers since the queue is updated with a transaction => we limit to max. 5000 transfers per balancing round
    size_t groupsize   = mGroup->size();
    size_t extractsize = groupsize?5000/groupsize:5000;

    for (it = mGroup->begin(); it != mGroup->end();it++) {
      eos::common::FileSystem::fs_snapshot snapshot;
      eos::common::FileSystem* fs = FsView::gFsView.mIdView[*it];
      if (fs) 
        fs->SnapShotFileSystem(snapshot);
      
      if ( fs && (snapshot.mConfigStatus >= eos::common::FileSystem::kRO) &&
           (snapshot.mStatus  == eos::common::FileSystem::kBooted) && 
           (snapshot.mErrCode      == 0 ) &&
           (fs->HasHeartBeat(snapshot)) &&
           (fs->GetActiveStatus(snapshot)) ) {
        // this has to be adjusted
        unsigned long long usedbytes = snapshot.mDiskCapacity - snapshot.mDiskFreeBytes;
        
        if (usedbytes <= avg) {
          // this is a target
          TargetSizeMap[snapshot.mId] = (avg-usedbytes);
          eos_static_debug("filesystem %u is a target with %llu bytes", snapshot.mId, (avg-usedbytes));
        } else {
          eos_static_debug("filesystem %u is a source with %llu bytes", snapshot.mId, (usedbytes-avg));
          // this is a source
          SourceSizeMap[snapshot.mId] = (usedbytes-avg);

          unsigned long long schedulebytes = (usedbytes-avg);

          // get enough FIDs to free the space
          //-------------------------------------------
          gOFS->eosViewMutex.Lock();
          try {
            eos::FileSystemView::FileList filelist = gOFS->eosFsView->getFileList(snapshot.mId);
            unsigned long long nfids = (unsigned long long) filelist.size();
            eos_static_notice("found %llu files in filesystem view %u", nfids,snapshot.mId);
            
            // we don't try to extract more then <extract-size> files
            for (size_t i=0; i< extractsize; i++) {
              unsigned long long rpos = (unsigned long long ) (( 0.999999 * random()* nfids )/RAND_MAX);
              eos::FileSystemView::FileIterator fit = filelist.begin();

              std::advance (fit,rpos);
              eos_static_debug("random selection %llu/%llu", rpos, nfids);
              if (fit != filelist.end()) {
                eos::FileMD::id_t fid = *fit;
                
                if ((!SourceFidMap[snapshot.mId].count(fid) ) && (!SourceFidSet.count(fid))){
                  eos::FileMD* fmd = 0;
                  fmd = gOFS->eosFileService->getFileMD(fid);
                  if (fmd) {
                    if (fmd->getSize() < schedulebytes) {
                      eos_static_info("adding file id %llu to be moved",fid);
                      SourceFidMap[snapshot.mId].insert(fid);
		      SourceFidSet.insert(fid);
                      schedulebytes-= fmd->getSize();
                      nscheduled++;
                    } else {
                      eos_static_debug("couldn't add file id %llu because %llu/%llu", fid, fmd->getSize(), schedulebytes);
                    }
                  }
                }
              }
            }
          } catch ( eos::MDException &e ) {
            errno = e.getErrno();
            eos_static_err("caught exception %d %s\n", e.getErrno(),e.getMessage().str().c_str());
          }
          gOFS->eosViewMutex.UnLock();
          //-------------------------------------------            
        }
      }
    }
  }

  XrdSysThread::SetCancelOn();

  XrdSysThread::CancelPoint();

  std::map<eos::common::FileSystem::fsid_t, unsigned long long >::const_iterator source_it;
  std::map<eos::common::FileSystem::fsid_t, unsigned long long >::const_iterator target_it;
  
  bool found=false;

  XrdSysThread::SetCancelOff();
  // now pickup the sources and distribute on targets


  eos_static_notice("Waiting to balance on group %s members=%lu sources=%lu targets=%lu", mName.c_str(), mGroup->size(), SourceSizeMap.size(), TargetQueues.size());
  {
    gSchedulingMutex.Lock();
    eos_static_notice("Balancing on group %s members=%lu sources=%lu targets=%lu", mName.c_str(), mGroup->size(), SourceSizeMap.size(), TargetQueues.size());
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
    

    for (target_it = TargetSizeMap.begin(); target_it != TargetSizeMap.end(); target_it++) {
      // get the balancing queues for all targets
      TargetQueues[target_it->first] = FsView::gFsView.mIdView[target_it->first]->GetBalanceQueue();
      TargetQueues[target_it->first]->OpenTransaction();
    }

    source_it = SourceSizeMap.begin();
    target_it = TargetSizeMap.begin();

    unsigned long long nloop=0;

    if ( (source_it != SourceSizeMap.end()) && 
         (target_it != TargetSizeMap.end()) ) {
      do {
        eos_static_info("checking %s balancing from %u => %u",mName.c_str(), source_it->first, target_it->first);
        // if there is still some file to schedule
        if ( (source_it->second > 0) && (SourceFidMap[source_it->first].size())) {
          eos_static_info("source %u has enough space", source_it->first);

          // check if source and target are the same ...
          if (source_it->first == target_it->first) {
            target_it++;
            if (target_it == TargetSizeMap.end())
              target_it= TargetSizeMap.begin();
          }
          
          // check if there is space on the next target
          if (target_it->second > 0) {
            eos_static_info("target %u has enough space", target_it->first);
            std::set<unsigned long long>::const_iterator fid_it;
            // take the first fid on from the fid list of the source filesystem
            fid_it = SourceFidMap[source_it->first].begin();
            unsigned long long fid = *fid_it;
            eos::common::FileSystem::fs_snapshot source_snapshot;
            eos::common::FileSystem::fs_snapshot target_snapshot;
            FsView::gFsView.mIdView[source_it->first]->SnapShotFileSystem(source_snapshot);
            FsView::gFsView.mIdView[target_it->first]->SnapShotFileSystem(target_snapshot);
            
            unsigned long long cid  = 0;
            unsigned long long size = 0;
            long unsigned int  lid  = 0;
	    uid_t uid=0;
	    gid_t gid=0;
            bool acceptid           = false;
            std::string fullpath = "";
            
            do {
              gOFS->eosViewMutex.Lock();
              eos::FileMD* fmd = 0;
              try {
		fid = *fid_it;
                fmd = gOFS->eosFileService->getFileMD(fid);
                lid = fmd->getLayoutId();
                cid = fmd->getContainerId();
                size = fmd->getSize();
		uid  = fmd->getCUid();
		gid  = fmd->getCGid();

              } catch ( eos::MDException &e ) {
                fmd = 0;
              }

	      // the target should not be already in the location vector and there shouldn't be already a file scheduled there
              if (fmd && (!fmd->hasLocation(target_snapshot.mId)) && (!TargetFidMap[target_snapshot.mId].count(fid))) {
                // we can put a replica here !
                acceptid = true;
                size = fmd->getSize();
                fullpath = gOFS->eosView->getUri(fmd);
              }
              gOFS->eosViewMutex.UnLock();
              // increment to the next fid in the source filesystem fid set
              if (!acceptid)
                fid_it++;
              // ------------------------------------------
            } while ( (!acceptid) && (fid_it != SourceFidMap[source_it->first].end()) );
            
            if (acceptid) {
              // we can schedule fid from source_it => target_it
              eos_static_info("scheduling fid %llu from %u => %u", fid, source_it->first, target_it->first);

              XrdOucString source_capability="";
              XrdOucString sizestring;
              source_capability += "mgm.access=read";
              source_capability += "&mgm.lid=";        source_capability += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long)lid&0xffffff0f);
              // make's it a plain replica
              source_capability += "&mgm.cid=";        source_capability += eos::common::StringConversion::GetSizeString(sizestring,cid);
              source_capability += "&mgm.ruid=";       source_capability+=(int)1;
              source_capability += "&mgm.rgid=";       source_capability+=(int)1;
              source_capability += "&mgm.uid=";        source_capability+=(int)1;
              source_capability += "&mgm.gid=";        source_capability+=(int)1;
              source_capability += "&mgm.path=";       source_capability += fullpath.c_str();
              source_capability += "&mgm.manager=";    source_capability += gOFS->ManagerId.c_str();
              source_capability += "&mgm.fid=";
              XrdOucString hexfid; eos::common::FileId::Fid2Hex(fid,hexfid);source_capability += hexfid;
              source_capability += "&mgm.drainfsid=";  source_capability += (int)source_it->first;
              
              // build the source_capability contents
              source_capability += "&mgm.localprefix=";       source_capability += source_snapshot.mPath.c_str();
              source_capability += "&mgm.fsid=";              source_capability += (int)source_snapshot.mId;
              source_capability += "&mgm.sourcehostport=";    source_capability += source_snapshot.mHostPort.c_str();
              source_capability += "&mgm.lfn=";               source_capability += fullpath.c_str();
              
              XrdOucString target_capability="";
              target_capability += "mgm.access=write";
              target_capability += "&mgm.lid=";        target_capability += eos::common::StringConversion::GetSizeString(sizestring,(unsigned long long)lid&0xffffff0f); 
              // make's it a plain replica
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
              target_capability += "&mgm.drainfsid=";  target_capability += (int)source_it->first;
              
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

                if ( TargetQueues.count(target_it->first)) {
                  bool sub = TargetQueues[target_it->first]->Add(txjob);
                  eos_static_info("Submitted %d %s\n", sub, fullcapability.c_str());
		  TargetFidMap[target_snapshot.mId].insert(fid);
                }
                if (txjob)
                  delete txjob;
                else
                  eos_static_err("Couldn't create transfer job balancing %s", mName.c_str());
                if (source_capabilityenv)
                  delete source_capabilityenv;
                if (target_capabilityenv)
                  delete target_capabilityenv;
              }

              // remove from the size on this source & target
              SourceFidMap[source_it->first].erase(fid_it);
              SourceSizeMap[source_it->first] -= size;
              TargetSizeMap[target_it->first] -= size;
              eos_static_info("source size=%llu target size=%llu", source_it->second, target_it->second);
            } else {
              eos_static_info("couldn't place fid %llu", fid);
	    }
            
            // go to next target group
            target_it++;
            
            if (target_it == TargetSizeMap.end()) {
              target_it = TargetSizeMap.begin();
            }
          }
        }
        source_it++;
        nloop++;
        
        // this is a safety stop to avoid possible endless loops
        if (nloop > (nscheduled))
          break;
        if (source_it == SourceSizeMap.end()) {
          source_it = SourceSizeMap.begin();
        }
        
        found=false;
        for (size_t n=0; n< SourceSizeMap.size(); n++) {
          if (SourceFidMap[source_it->first].size()) {
            found = true;
            break;
          }
          source_it++;
          if (source_it == SourceSizeMap.end()) {
            source_it = SourceSizeMap.begin();
          } 
        }
        // if there are no source files anymore, we just stop
      } while (found);
    }

    for (target_it = TargetSizeMap.begin(); target_it != TargetSizeMap.end(); target_it++) {
      // close all balancing queue transactions
      TargetQueues[target_it->first]->CloseTransaction();
    }

    gSchedulingMutex.UnLock();
  }

  eos_static_info("Finished balancing on group %s", mName.c_str());


  {
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
    mGroup->SetConfigMember("stat.balancing","running",false, "", true);
  }

  XrdSysThread::SetCancelOn();

  XrdSysThread::CancelPoint();

  unsigned long long totalfiles=0;
  unsigned long long prevtotalfiles=0;
  time_t lastchange=time(NULL);
  bool abort=false;
  bool wasstalled=false;
  do {
    XrdSysThread::SetCancelOff();
    {
      totalfiles = 0;
      eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
      for (target_it = TargetSizeMap.begin(); target_it != TargetSizeMap.end(); target_it++) {
        if (FsView::gFsView.mIdView.count(target_it->first)) {
          totalfiles += FsView::gFsView.mIdView[target_it->first]->GetBalanceQueue()->Size();
        }
      }
      if (totalfiles != prevtotalfiles) 
        lastchange=time(NULL);
      prevtotalfiles = totalfiles;
    }

    std::string squeued="0";
    eos::common::StringConversion::GetSizeString(squeued, totalfiles);    
    {
      eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
      mGroup->SetConfigMember("stat.balancing.queued",squeued,false, "", true);
    }
    XrdSysThread::SetCancelOn();
    for (int i=0; i< 10;i++) {
      XrdSysTimer sleeper;
      sleeper.Snooze(1);
      XrdSysThread::CancelPoint();
    }

    time_t diff = time(NULL) - lastchange;

    XrdSysThread::SetCancelOff();

    if ( (diff > 300) ) {
      if (diff < 3600) {

        eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
        mGroup->SetConfigMember("stat.balancing","stalled",false, "", true);
	wasstalled =true;
      } else {
        // clean-up the queues
        eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
        for (target_it = TargetSizeMap.begin(); target_it != TargetSizeMap.end(); target_it++) {
          if (FsView::gFsView.mIdView.count(target_it->first)) {
            FsView::gFsView.mIdView[target_it->first]->GetBalanceQueue()->Clear();
          }
        }
        abort = true;
        mGroup->SetConfigMember("stat.balancing","incomplete",false, "", true);
        mGroup->SetConfigMember("stat.balancing.queued","0",false, "", true);
      }
    } else {
      if (wasstalled) {
        eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
        mGroup->SetConfigMember("stat.balancing","running",false, "", true);
	wasstalled=false;
      }
    }

    XrdSysThread::SetCancelOn();
  } while (totalfiles && (!abort) );
  
  
  if (abort) {
    for (int i=0; i< 60; i++) {
      XrdSysTimer sleeper;
      sleeper.Snooze(1);          
      XrdSysThread::CancelPoint();
    }
  }

  XrdSysThread::SetCancelOff();

  {
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
    mGroup->SetConfigMember("stat.balancing","cooldown",false, "", true);
  }

  XrdSysThread::SetCancelOn();

  for (int i=0; i< 120; i++) {
    XrdSysTimer sleeper;
    sleeper.Snooze(1);
    XrdSysThread::CancelPoint();
  }

  XrdSysThread::SetCancelOff();

  {
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
    mGroup->SetConfigMember("stat.balancing","idle",false, "", true);
  }
  
  XrdSysThread::SetCancelOn();
  
  mThreadRunningLock.Lock();
  mThreadRunning=false;
  mThreadRunningLock.UnLock();
  pthread_exit(0);
  return 0;
}

EOSMGMNAMESPACE_END

