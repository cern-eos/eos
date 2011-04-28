#ifndef __EOSMGM_DRAINJOB_HH__
#define __EOSMGM_DRAINJOB_HH__
/*----------------------------------------------------------------------------*/
#include <pthread.h>
/*----------------------------------------------------------------------------*/
#include "common/FileSystem.hh"
#include "mgm/Namespace.hh"
/*----------------------------------------------------------------------------*/
#include "semaphore.h"
/*----------------------------------------------------------------------------*/


EOSMGMNAMESPACE_BEGIN

class DrainJob {
  // ---------------------------------------------------------------------------
  //! This class implements the drain procedure of a filesystem
  // ---------------------------------------------------------------------------
private:
  eos::common::FileSystem::fsid_t fsid;
  bool onOpsError;
  pthread_t thread;
  sem_t semaphore;
  
public:

  DrainJob(eos::common::FileSystem::fsid_t ifsid, bool opserror=false) {
    fsid = ifsid;
    onOpsError=opserror;
    sem_init(&semaphore, 0, 0);   
    pthread_create(&thread, NULL, &DrainJob::StaticThreadProc, this);
  }

  static void* StaticThreadProc(void*);
  void* Drain(); // the function scheduling from the drain map into shared queues
  
  virtual ~DrainJob();
};

EOSMGMNAMESPACE_END

#endif
