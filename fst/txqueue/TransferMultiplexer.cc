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

#include "fst/txqueue/TransferMultiplexer.hh"
#include "fst/XrdFstOfs.hh"
#include "common/Logging.hh"
#include <cstdio>

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
TransferMultiplexer::TransferMultiplexer():
  mTid(0)
{}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
TransferMultiplexer::~TransferMultiplexer()
{
  Stop();
}

//------------------------------------------------------------------------------
// Stop multiplexer thread
//------------------------------------------------------------------------------
void
TransferMultiplexer::Stop()
{
  if (mTid) {
    XrdSysThread::Cancel(mTid);
    XrdSysThread::Join(mTid, NULL);
    mTid = 0;
  }
}

//------------------------------------------------------------------------------
// Run multiplexer thread
//------------------------------------------------------------------------------
void
TransferMultiplexer::Run()
{
  if (!mTid) {
    XrdSysThread::Run(&mTid, TransferMultiplexer::StaticThreadProc,
                      static_cast<void*>(this), XRDSYSTHREAD_HOLD,
                      "Multiplexer Thread");
  }
}

//------------------------------------------------------------------------------
// Add queue to multiplexer
//------------------------------------------------------------------------------
void
TransferMultiplexer::Add(TransferQueue* queue)
{
  eos::common::RWMutexWriteLock lock(mMutex);
  mQueues.push_back(queue);
}

//------------------------------------------------------------------------------
// Set bandwith for each of the queues attached
//------------------------------------------------------------------------------
void
TransferMultiplexer::SetBandwidth(size_t band)
{
  eos::common::RWMutexWriteLock lock(mMutex);

  for (size_t i = 0; i < mQueues.size(); i++) {
    mQueues[i]->SetBandwidth(band);
  }

  return;
}

//------------------------------------------------------------------------------
// Set number of slots for each of the queues attached
//------------------------------------------------------------------------------
void
TransferMultiplexer::SetSlots(size_t slots)
{
  eos::common::RWMutexWriteLock lock(mMutex);

  for (size_t i = 0; i < mQueues.size(); i++) {
    mQueues[i]->SetSlots(slots);
  }

  return;
}

//------------------------------------------------------------------------------
// Static helper function to run the thread
//------------------------------------------------------------------------------
void*
TransferMultiplexer::StaticThreadProc(void* arg)
{
  return reinterpret_cast<TransferMultiplexer*>(arg)->ThreadProc();
}

//------------------------------------------------------------------------------
// Multiplexer thread loop
//------------------------------------------------------------------------------
void*
TransferMultiplexer::ThreadProc(void)
{
  std::string sTmp, src, dest;
  eos_static_info("running transfer multiplexer with %d queues", mQueues.size());

  while (1) {
    {
      XrdSysThread::SetCancelOff();
      eos::common::RWMutexReadLock lock(mMutex);

      for (size_t i = 0; i < mQueues.size(); i++) {
        while (mQueues[i]->GetQueue()->Size()) {
          // look in all registered queues
          // take an entry from the queue
          int freeslots = mQueues[i]->GetSlots() - mQueues[i]->GetRunning();

          if (freeslots <= 0) {
            break;
          }

          eos_static_info("Found %u transfers in queue %s", (unsigned int)
                          mQueues[i]->GetQueue()->Size(), mQueues[i]->GetName());
          mQueues[i]->GetQueue()->OpenTransaction();
          eos::common::TransferJob* cjob = mQueues[i]->GetQueue()->Get();
          mQueues[i]->GetQueue()->CloseTransaction();

          if (!cjob) {
            eos_static_err("No transfer job created");
            break;
          }

          XrdOucString out = "";
          cjob->PrintOut(out);
          eos_static_info("New transfer %s", out.c_str());
          //create new TransferJob and submit it to the scheduler
          TransferJob* job = new TransferJob(mQueues[i], cjob,
                                             mQueues[i]->GetBandwidth());
          gOFS.TransferSchedulerMutex.Lock();
          gOFS.TransferScheduler->Schedule(job);
          gOFS.TransferSchedulerMutex.UnLock();
          mQueues[i]->IncRunning();
        }
      }
    }
    XrdSysThread::SetCancelOn();
    XrdSysTimer sleeper;
    sleeper.Wait(100);
  }

  // Wait that the scheduler is empty, otherwise we might have callbacks
  // to our queues
  return NULL;
}

EOSFSTNAMESPACE_END
