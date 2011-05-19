/* ------------------------------------------------------------------------- */
#include "fst/txqueue/TransferQueue.hh"
#include "fst/txqueue/TransferJob.hh"
#include "common/Logging.hh"
/* ------------------------------------------------------------------------- */
#include <cstdio>
/* ------------------------------------------------------------------------- */

EOSFSTNAMESPACE_BEGIN

/* ------------------------------------------------------------------------- */
TransferQueue::TransferQueue(eos::common::TransferQueue** queue, const char* name, int slots, int band){
  mQueue = queue;
  mName  = name;
  mJobsRunning = 0;
  nslots = slots;
  bandwidth = band;
}

/* ------------------------------------------------------------------------- */
TransferQueue::~TransferQueue(){
}


/* ------------------------------------------------------------------------- */
size_t TransferQueue::GetBandwidth(){
  return bandwidth;
}

/* ------------------------------------------------------------------------- */
void TransferQueue::SetBandwidth(size_t band){
  bandwidth = band;
}

EOSFSTNAMESPACE_END
