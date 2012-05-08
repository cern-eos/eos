// ----------------------------------------------------------------------
// File: TransferMultiplexer.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

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

  size_t loopsleep=2000000;

  XrdSysThread::SetCancelOn();
  //  pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, 0);

  while (1) {
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
	//	fprintf(stderr, "New transfer %s\n", out.c_str());
        
        //create new TransferJob and submit it to the scheduler
        TransferJob* job = new TransferJob(mQueues[i], cjob, mQueues[i]->GetBandwidth());
        gOFS.TransferSchedulerMutex.Lock();
        gOFS.TransferScheduler->Schedule(job);
        gOFS.TransferSchedulerMutex.UnLock();
        mQueues[i]->IncRunning();
      }
    }

    for (size_t i=0; i< loopsleep/10000; i++) {
      usleep(10000);
      XrdSysThread::CancelPoint();
    }
  }

  // we wait that the scheduler is empty, otherwise we might have call backs to our queues
  return NULL;
}

EOSFSTNAMESPACE_END
