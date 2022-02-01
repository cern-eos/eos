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
#include "fst/txqueue/TransferQueue.hh"
#include "fst/txqueue/TransferJob.hh"
#include "fst/XrdFstOfs.hh"
#include "common/Logging.hh"
#include "Xrd/XrdScheduler.hh"
#include <cstdio>

EOSFSTNAMESPACE_BEGIN

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
  mThread.join();
}

//------------------------------------------------------------------------------
// Run multiplexer thread
//------------------------------------------------------------------------------
void
TransferMultiplexer::Run()
{
  mThread.reset(&TransferMultiplexer::ThreadLoop, this);
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
// Multiplexer thread loop
//------------------------------------------------------------------------------
void
TransferMultiplexer::ThreadLoop(ThreadAssistant& assistant) noexcept
{
  eos_info("msg=\"starting transfer multiplexer with %d queues\"",
           mQueues.size());

  while (!assistant.terminationRequested()) {
    {
      eos::common::RWMutexReadLock lock(mMutex);

      for (size_t i = 0; i < mQueues.size(); i++) {
        while (mQueues[i]->GetQueue()->Size()) {
          if (assistant.terminationRequested()) {
            break;
          }

          // look in all registered queues
          // take an entry from the queue
          int freeslots = mQueues[i]->GetSlots() - mQueues[i]->GetRunning();

          if (freeslots <= 0) {
            break;
          }

          eos_info("Found %u transfers in queue %s", (unsigned int)
                   mQueues[i]->GetQueue()->Size(),
                   mQueues[i]->GetQueue()->getQueuePath().c_str());
          std::unique_ptr<eos::common::TransferJob> cjob = mQueues[i]->GetQueue()->Get();

          if (!cjob) {
            eos_err("%s", "msg=\"no transfer job created\"");
            break;
          }

          XrdOucString out = "";
          cjob->PrintOut(out);
          eos_info("msg=\"new transfer %s\"", out.c_str());
          //create new TransferJob and submit it to the scheduler
          TransferJob* job = new TransferJob(mQueues[i], std::move(cjob),
                                             mQueues[i]->GetBandwidth());
          gOFS.TransferSchedulerMutex.Lock();
          gOFS.TransferScheduler->Schedule(job);
          gOFS.TransferSchedulerMutex.UnLock();
          mQueues[i]->IncRunning();
        }
      }
    }
    assistant.wait_for(std::chrono::milliseconds(100));
  }

  // Wait that the scheduler is empty, otherwise we might have callbacks
  // to our queues
  eos_notice("%s", "msg=\"wait for all scheduled jobs to finish\"");
  bool is_active = false;

  while (true) {
    {
      XrdSysMutexHelper lock(gOFS.TransferSchedulerMutex);
      is_active = (gOFS.TransferScheduler->Active() != 0);
    }

    if (is_active) {
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    } else {
      break;
    }
  }

  eos_notice("%s", "msg=\"stopped transfer multiplexer\"");
}

EOSFSTNAMESPACE_END
