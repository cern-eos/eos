/*----------------------------------------------------------------------------*/
#include "mgm/DrainJob.hh"
#include "mgm/FileSystem.hh"
#include "mgm/FsView.hh"
#include "common/Logging.hh"

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
DrainJob::~DrainJob() {
  //----------------------------------------------------------------
  //! destructor stops the draining thread 
  //----------------------------------------------------------------
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

  eos_static_notice("Starting Drain Job for fs=%u", fsid);
  
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

  sleep(10);

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
  sleep(10);
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

