#ifndef __EOSFST_TRANSFERQUEUE__
#define __EOSFST_TRANSFERQUEUE__

/* ------------------------------------------------------------------------- */
#include "fst/Namespace.hh"
#include "common/TransferQueue.hh"
/* ------------------------------------------------------------------------- */
#include "Xrd/XrdScheduler.hh"
/* ------------------------------------------------------------------------- */
#include <string>
#include <deque>
#include <cstring>
#include <pthread.h>
/* ------------------------------------------------------------------------- */

EOSFSTNAMESPACE_BEGIN

class TransferQueue {

private:
  //  std::deque <std::string> queue;
  eos::common::TransferQueue** mQueue;
  std::string mName;

  size_t nslots, bandwidth;

  size_t mJobsRunning;
  XrdSysMutex mJobsRunningMutex;

public: 

  TransferQueue(eos::common::TransferQueue** queue, const char* name, int slots=2, int band=100);
  ~TransferQueue();

  eos::common::TransferQueue* GetQueue() { return *mQueue;}

  const char* GetName() { return mName.c_str();}

  size_t  GetSlots() { return nslots; } ;
  void SetSlots(size_t slots) { nslots = slots; } ;

  size_t  GetBandwidth();
  void SetBandwidth(size_t band);
  
  void IncRunning() {
    mJobsRunningMutex.Lock();
    mJobsRunning++;
    mJobsRunningMutex.UnLock();
  }

  void DecRunning() {
    mJobsRunningMutex.Lock();
    mJobsRunning--;
    mJobsRunningMutex.UnLock();
  }

  size_t GetRunning() {
    size_t nrun=0;
    mJobsRunningMutex.Lock();
    nrun = mJobsRunning;
    mJobsRunningMutex.UnLock();
    return nrun;
  }
    
};

EOSFSTNAMESPACE_END
#endif

