/*----------------------------------------------------------------------------*/
#include "mgm/DrainJob.hh"
#include "mgm/FileSystem.hh"
#include "mgm/FsView.hh"
#include "mgm/XrdMgmOfs.hh"
#include "common/Logging.hh"

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
DrainJob::~DrainJob() {
  //----------------------------------------------------------------
  //! destructor stops the draining thread 
  //----------------------------------------------------------------
  eos_static_info("waiting for join ...");
  sem_post(&semaphore);
  pthread_join(thread,NULL);
  eos_static_notice("Stopping Drain Job for fs=%u", fsid);
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
  FileSystem* fs = 0;
  int valSem = 0;
  eos_static_notice("Starting Drain Job for fs=%u onOpsError=%d", fsid,onOpsError);
  
  {
    // set status to 'prepare'
    eos::common::RWMutexReadLock(FsView::gFsView.ViewMutex);
    fs = FsView::gFsView.mIdView[fsid];
    if (!fs) {
      eos_static_notice("Filesystem fsid=%u has been removed during drain operation", fsid);
      return 0 ;
    }
    
    fs->SetDrainStatus(eos::common::FileSystem::kDrainPrepare);
  }

  // check if we should abort
  sem_getvalue(&semaphore, &valSem);
  if (valSem > 0) {
    return 0;
  }

  // build the list of files to migrate
  long long totalbytes=0;
  long long totalfiles=0;

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
        //        fids.insert((unsigned long long)fmd->getId());
      }
    }
  } catch ( eos::MDException &e ) {
    // there are no files in that view
  }
  
  gOFS->eosViewMutex.UnLock();
  //------------------------------------


  // set the shared object counter
  {
    eos::common::RWMutexReadLock(FsView::gFsView.ViewMutex);
    fs = FsView::gFsView.mIdView[fsid];
    if (!fs) {
      eos_static_notice("Filesystem fsid=%u has been removed during drain operation", fsid);
      return 0 ;
    }
    
    fs->SetLongLong("stat.drainbytesleft", totalbytes);
    fs->SetLongLong("stat.drainfiles",     totalfiles);
    fs->SetLongLong("stat.drainlostfiles", 0);
  } 
  

  if (onOpsError) {
    time_t waitendtime;
    time_t waitreporttime;
    time_t now;

    {
      // set status to 'waiting'
      eos::common::RWMutexReadLock(FsView::gFsView.ViewMutex);
      fs = FsView::gFsView.mIdView[fsid];
      if (!fs) {
        eos_static_notice("Filesystem fsid=%u has been removed during drain operation", fsid);
        return 0 ;
      }
      
      fs->SetDrainStatus(eos::common::FileSystem::kDrainWait);
      
      waitendtime = time(NULL) + (time_t)fs->GetLongLong("graceperiod");
    }

    waitreporttime = time(NULL) + 10; // we report every 10 seconds

    while ( (now = time(NULL)) < waitendtime) {
      usleep(50000);

      // check if we should abort
      sem_getvalue(&semaphore, &valSem);
      if (valSem > 0) {
        return 0;
      }
      if (now > waitreporttime) {
        // update stat.timeleft
        eos::common::RWMutexReadLock(FsView::gFsView.ViewMutex);
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
  sem_getvalue(&semaphore, &valSem);
  if (valSem > 0) {
    return 0;
  }
  
  // extract all fids to drain
  
  // make statistics of files to be lost if we are in draindead

  // set status to 'draining'
  {
    // set status to 'prepare'
    eos::common::RWMutexReadLock(FsView::gFsView.ViewMutex);
    fs = FsView::gFsView.mIdView[fsid];
    if (!fs) {
      eos_static_notice("Filesystem fsid=%u has been removed during drain operation", fsid);
      return 0 ;
    }
    
    fs->SetDrainStatus(eos::common::FileSystem::kDraining);
  }
  // start scheduling into the queues
  
  sleep(1);
  // set status to 'drained'
  // return if over
  {
    // set status to 'prepare'
    eos::common::RWMutexReadLock(FsView::gFsView.ViewMutex);
    fs = FsView::gFsView.mIdView[fsid];
    if (!fs) {
      eos_static_notice("Filesystem fsid=%u has been removed during drain operation", fsid);
      return 0 ;
    }
    
    fs->SetDrainStatus(eos::common::FileSystem::kDrained);
  }
  return 0;
}

EOSMGMNAMESPACE_END

