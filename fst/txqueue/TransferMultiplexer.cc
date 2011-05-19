/* ------------------------------------------------------------------------- */
#include "fst/txqueue/TransferMultiplexer.hh"
#include "fst/txqueue/TransferJob.hh"
#include "fst/XrdFstOfs.hh"
#include "common/Logging.hh"
/* ------------------------------------------------------------------------- */
#include <cstdio>
/* ------------------------------------------------------------------------- */

EOSFSTNAMESPACE_BEGIN

/* ------------------------------------------------------------------------- */
TransferMultiplexer::TransferMultiplexer() 
{
  thread=0;
}

/* ------------------------------------------------------------------------- */
TransferMultiplexer::~TransferMultiplexer(){
 
  if (thread) {
    XrdSysThread::Cancel(thread);
    XrdSysThread::Join(thread,NULL);
    //    pthread_cancel(thread);
    //    pthread_join(thread, NULL);
  }
}

/* ------------------------------------------------------------------------- */
void
TransferMultiplexer::Run() {
  XrdSysThread::Run(&thread, TransferMultiplexer::StaticThreadProc, static_cast<void *>(this),0, "Multiplexer Thread");
  //  pthread_create(&thread, NULL, &TransferMultiplexer::StaticThreadProc, this);
}


/* ------------------------------------------------------------------------- */
void* TransferMultiplexer::StaticThreadProc(void* arg){
  return reinterpret_cast<TransferMultiplexer*>(arg)->ThreadProc();
}


/* ------------------------------------------------------------------------- */
void* TransferMultiplexer::ThreadProc(void){
  std::string sTmp, src, dest;

  eos_static_info("running transfer multiplexer with %d queues", mQueues.size());

  size_t loopsleep=100000;

  XrdSysThread::SetCancelOn();
  //  pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, 0);

  while (1) {
    bool found=false;
    for (size_t i=0; i< mQueues.size(); i++) {
      while( mQueues[i]->GetQueue()->Size()) {
        // look in all registered queues
        // take an entry from the queue
      
        int freeslots = mQueues[i]->GetSlots() - mQueues[i]->GetRunning();
        
        if (freeslots <=0 )
          break;
        
        //        fprintf(stderr,"Found %u transfers in queue %s\n", (unsigned int) mQueues[i]->GetQueue()->Size(), mQueues[i]->GetName());
        
        mQueues[i]->GetQueue()->OpenTransaction();
        eos::common::TransferJob* cjob = mQueues[i]->GetQueue()->Get();
        mQueues[i]->GetQueue()->CloseTransaction();
        
        if (!cjob)
          break;

        XrdOucString out="";
        cjob->PrintOut(out);
        //        fprintf(stderr, "New transfer %s\n", out.c_str());
        
        //create new TransferJob and submit it to the scheduler
        TransferJob* job = new TransferJob(mQueues[i], cjob, mQueues[i]->GetBandwidth());
        gOFS.TransferSchedulerMutex.Lock();
        gOFS.TransferScheduler->Schedule(job);
        gOFS.TransferSchedulerMutex.UnLock();
        mQueues[i]->IncRunning();
      }
    }

    // we do relaxed self pacing
    if (!found) {
      for (size_t i=0; i< loopsleep/10000; i++) {
        usleep(10000);
        XrdSysThread::CancelPoint();
        //        pthread_testcancel();
      }
      loopsleep*=2;
      if (loopsleep>2000000) {
        loopsleep=2000000;
      }
    } else {
      loopsleep=100000;
    }
  }

  // we wait that the scheduler is empty, otherwise we might have call backs to our queues
  return NULL;
}

EOSFSTNAMESPACE_END
