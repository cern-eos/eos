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

/*----------------------------------------------------------------------------*/
#include "common/TransferQueue.hh"
/*----------------------------------------------------------------------------*/
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/


EOSCOMMONNAMESPACE_BEGIN

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
TransferQueue::TransferQueue (const char* queue, const char* queuepath, const char* subqueue, FileSystem* fs, XrdMqSharedObjectManager* som, bool bc2mgm)
{
  mFileSystem = fs;

  constructorLock.Lock();
  mQueue = queue;
  mFullQueue = queuepath;
  mFullQueue += "/txqueue/";
  mFullQueue += subqueue;
  mTxQueue = subqueue;
  mJobGetCount = 0;

  if (bc2mgm)
  {
    // the fst has to reply to the mgm and set up the right broadcast queue
    mQueue = "/eos/*/mgm";
    mSlave = true;
  }
  else
  {
    mSlave = false;
  }


  mSom = som;
  if (mSom)
  {
    mSom->HashMutex.LockRead();
    if (!(mHashQueue = (XrdMqSharedQueue*) mSom->GetObject(mFullQueue.c_str(), "queue")))
    {
      mSom->HashMutex.UnLockRead();
      // create the hash object
      if (mSom->CreateSharedQueue(mFullQueue.c_str(), mQueue.c_str(), som))
      {
        mSom->HashMutex.LockRead();
        mHashQueue = (XrdMqSharedQueue*) mSom->GetObject(mFullQueue.c_str(), "queue");
        //        if (mHashQueue) {
        //          mHashQueue->OpenTransaction();
        //          mHashQueue->CloseTransaction();
        //        }           

        mSom->HashMutex.UnLockRead();
      }
      else
      {
        mHashQueue = 0;
      }
    }
    else
    {
      // remove all scheduled objects
      if (!mSlave)
      {
        mHashQueue->Clear();
      }
      mSom->HashMutex.UnLockRead();
    }
  }
  else
  {
    mHashQueue = 0;
  }
  constructorLock.UnLock();
}

/*----------------------------------------------------------------------------*/
//! Destructor

/*----------------------------------------------------------------------------*/
TransferQueue::~TransferQueue ()
{
  if (!mSlave)
  {
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
TransferQueue::Add (eos::common::TransferJob* job)
{
  bool retc = false;
  if (mSom)
  {
    mSom->HashMutex.LockRead();
    if ((mHashQueue = mSom->GetQueue(mFullQueue.c_str())))
    {
      retc = mHashQueue->PushBack(0, job->GetSealed());
    }
    else
    {
      fprintf(stderr, "Couldn't get queue %s!\n", mFullQueue.c_str());
    }
    mSom->HashMutex.UnLockRead();
  }
  return retc;
}

/*----------------------------------------------------------------------------*/
/** 
 * Get a job from the queue. The caller has to clean-up the job object.
 * 
 * 
 * @return pointer to job
 */

/*----------------------------------------------------------------------------*/
TransferJob*
TransferQueue::Get ()
{
  XrdMqSharedHashEntry* entry = 0;
  if (mSom)
  {
    mSom->HashMutex.LockRead();
    if ((mHashQueue = mSom->GetQueue(mFullQueue.c_str())))
    {
      mHashQueue->QueueMutex.Lock();
      entry = mHashQueue->GetQueue()->front();
      if (!entry)
      {
        mHashQueue->QueueMutex.UnLock();
        mSom->HashMutex.UnLockRead();
        return 0;
      }
      else
      {
        if (!entry->GetEntry())
        {
          mHashQueue->QueueMutex.UnLock();
          mSom->HashMutex.UnLockRead();
          return 0;
        }
        TransferJob* job = TransferJob::Create(entry->GetEntry());
        mHashQueue->QueueMutex.UnLock();
        // remove it from the queue
        mHashQueue->Delete(entry);
        mSom->HashMutex.UnLockRead();
        IncGetJobCount();
        return job;
      }
    }
    else
    {
      fprintf(stderr, "Couldn't get queue %s!\n", mFullQueue.c_str());
    }
    mSom->HashMutex.UnLockRead();
  }
  return 0;
}

/*----------------------------------------------------------------------------*/
/** 
 * Remove a job from the queue. Currently this is not implemented!
 * 
 * @param job pointer to job object
 * 
 * @return true if successful otherwise false
 */

/*----------------------------------------------------------------------------*/
bool
TransferQueue::Remove (eos::common::TransferJob* job)
{
  return false;
}

/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_END

