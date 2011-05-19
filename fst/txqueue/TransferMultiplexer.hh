#ifndef __EOSFST_TRANSFERMULTIPLEXER__
#define __EOSFST_TRANSFERMULTIPLEXER__

/* ------------------------------------------------------------------------- */
#include "fst/Namespace.hh"
#include "fst/txqueue/TransferJob.hh"
/* ------------------------------------------------------------------------- */
#include "Xrd/XrdScheduler.hh"
/* ------------------------------------------------------------------------- */
#include <vector>
#include <string>
#include <deque>
#include <cstring>
#include <pthread.h>
/* ------------------------------------------------------------------------- */

EOSFSTNAMESPACE_BEGIN

class TransferMultiplexer {

private:
  //  std::deque <std::string> queue;
  std::vector<TransferQueue*> mQueues;
  pthread_t thread;

public: 

  TransferMultiplexer();
  ~TransferMultiplexer();

  void Add(TransferQueue* queue) {
    // add all queues and then call Run()
    mQueues.push_back(queue);
  }
  
  void Run(); // add all queues beforehand!

  static void* StaticThreadProc(void*);
  void* ThreadProc();
};

EOSFSTNAMESPACE_END
#endif

