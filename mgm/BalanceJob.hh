#ifndef __EOSMGM_BALANCEJOB_HH__
#define __EOSMGM_BALANCEJOB_HH__
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

class BalanceJob {
  // ---------------------------------------------------------------------------
  //! This class implements the balance procedure of a group
  // ---------------------------------------------------------------------------
private:
  eos::common::FileSystem::fsid_t fsid;
  bool onOpsError;
  pthread_t thread;

  std::deque<unsigned long long> fids;

public:

  BalanceJob(eos::common::FileSystem::fsid_t ifsid, bool opserror=false) {
    fsid = ifsid;
    onOpsError=opserror;
    XrdSysThread::Run(&thread, BalanceJob::StaticThreadProc, static_cast<void *>(this),0, "BalanceJob Thread");
  }

  static void* StaticThreadProc(void*);
  void* Balance(); // the function scheduling from the balance map into shared queues
  
  virtual ~BalanceJob();
};

EOSMGMNAMESPACE_END

#endif
