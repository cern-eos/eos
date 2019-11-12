// ----------------------------------------------------------------------
// File: TransferQueue.cc
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

#include "common/TransferQueue.hh"
#include "mq/SharedQueueWrapper.hh"

/*----------------------------------------------------------------------------*/
/**
 * Constructor for a transfer queue
 *
 * @param queue name of the queue e.g. /eos/'host'/fst/
 * @param queuepath name of the queue path e.g. /eos/'host'/fst/'mountpoint'/
 * @param subqueue name of the subqueue e.g. drainq,balanceq,externalq
 * @param fs pointer to filesytem object to add the queue
 * @param som pointer to shared object manager
 * @param bc2mgm broadcast-to-manager flag indicating if changes are broadcasted to manager nodes
 */
/*----------------------------------------------------------------------------*/
TransferQueue::TransferQueue(const TransferQueueLocator &locator, mq::MessagingRealm *realm, bool bc2mgm)
: mRealm(realm), mLocator(locator), mBroadcast(bc2mgm)
{
  mJobGetCount = 0;
  eos::mq::SharedQueueWrapper queue(mRealm, mLocator, mBroadcast);
  if(mBroadcast) {
    queue.clear();
  }
}

//------------------------------------------------------------------------------
//! Get queue path
//------------------------------------------------------------------------------
std::string TransferQueue::getQueuePath() const {
  return mLocator.getQueuePath();
}

/*----------------------------------------------------------------------------*/
//! Destructor
/*----------------------------------------------------------------------------*/
TransferQueue::~TransferQueue()
{
  if (mBroadcast) {
    Clear();
  }
}

/*----------------------------------------------------------------------------*/
/**
 * Add a transfer job to the queue
 *
 * @param job pointer to job to add
 *
 * @return true if successful otherwise false
 */

/*----------------------------------------------------------------------------*/
bool
TransferQueue::Add(eos::common::TransferJob* job)
{
  return eos::mq::SharedQueueWrapper(mRealm, mLocator, mBroadcast).push_back(job->GetSealed());
}

/*----------------------------------------------------------------------------*/
/**
 * Get a job from the queue. The caller has to clean-up the job object.
 *
 *
 * @return pointer to job
 */

/*----------------------------------------------------------------------------*/
std::unique_ptr<TransferJob>
TransferQueue::Get()
{
  std::string item = eos::mq::SharedQueueWrapper(mRealm, mLocator, mBroadcast).getItem();

  if(item.empty()) {
    return {};
  }

  std::unique_ptr<TransferJob> job = TransferJob::Create(item.c_str());
  IncGetJobCount();
  return job;
}

// ---------------------------------------------------------------------------
//! Clear all jobs from the queue
// ---------------------------------------------------------------------------
void TransferQueue::Clear() {
  eos::mq::SharedQueueWrapper(mRealm, mLocator, mBroadcast).clear();
}

//------------------------------------------------------------------------------
//! Get the current size of the queue
//------------------------------------------------------------------------------
size_t TransferQueue::Size()
{
  return eos::mq::SharedQueueWrapper(mRealm, mLocator, mBroadcast).size();
}

/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_END
