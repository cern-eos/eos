#ifndef __EOSMGM_BALANCEJOB_HH__
#define __EOSMGM_BALANCEJOB_HH__
/*----------------------------------------------------------------------------*/
#include <pthread.h>
/*----------------------------------------------------------------------------*/
#include "common/FileSystem.hh"
#include "common/TransferQueue.hh"
#include "mgm/Namespace.hh"
#include "mgm/FsView.hh"
/*----------------------------------------------------------------------------*/
#include "semaphore.h"
#include <queue>
/*----------------------------------------------------------------------------*/


EOSMGMNAMESPACE_BEGIN

class FsGroup;

class BalanceJob {
  // ---------------------------------------------------------------------------
  //! This class implements the balance procedure of a group
  // ---------------------------------------------------------------------------
private:
  FsGroup* mGroup;
  std::string mName;
  pthread_t thread;
  XrdSysMutex mThreadRunningLock;
  bool mThreadRunning;

  std::map<eos::common::FileSystem::fsid_t, std::set<unsigned long long > >    SourceFidMap; // the sources to schedule
  std::map<eos::common::FileSystem::fsid_t, unsigned long long>                SourceSizeMap;
  std::map<eos::common::FileSystem::fsid_t, unsigned long long>                TargetSizeMap;
  std::map<eos::common::FileSystem::fsid_t, eos::common::TransferQueue* >      TargetQueues;
  std::map<eos::common::FileSystem::fsid_t, std::set<unsigned long long > >    TargetFidMap; // the scheduled targets
public:

  BalanceJob(FsGroup* group);
  bool ReActivate();

  static void* StaticThreadProc(void*);
  void* Balance(); // the function scheduling from the balance map into shared queues
  
  virtual ~BalanceJob();
};

EOSMGMNAMESPACE_END

#endif
