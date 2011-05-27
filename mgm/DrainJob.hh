#ifndef __EOSMGM_DRAINJOB_HH__
#define __EOSMGM_DRAINJOB_HH__
/*----------------------------------------------------------------------------*/
#include <pthread.h>
/*----------------------------------------------------------------------------*/
#include "common/FileSystem.hh"
#include "mgm/Namespace.hh"
/*----------------------------------------------------------------------------*/
#include "semaphore.h"
#include <queue>
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
  std::deque<unsigned long long> fids;

public:

  DrainJob(eos::common::FileSystem::fsid_t ifsid, bool opserror=false) {
    thread=0;
    fsid = ifsid;
    onOpsError=opserror;
    XrdSysThread::Run(&thread, DrainJob::StaticThreadProc, static_cast<void *>(this),XRDSYSTHREAD_HOLD, "DrainJob Thread");
  }

  void ResetCounter(bool lockit=true);

  static void* StaticThreadProc(void*);
  void* Drain(); // the function scheduling from the drain map into shared queues
  
  virtual ~DrainJob();
};

EOSMGMNAMESPACE_END

#endif
