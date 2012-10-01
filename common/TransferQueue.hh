// ----------------------------------------------------------------------
// File: TransferQueue.hh
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

/**
 * @file   TransferQueue.hh
 * 
 * @brief  Base class for transfer queues.
 * 
 * 
 */

#ifndef __EOSCOMMON_TRANSFERQUEUE_HH__
#define __EOSCOMMON_TRANSFERQUEUE_HH__

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
#include "common/StringConversion.hh"
#include "common/FileSystem.hh"
#include "common/TransferJob.hh"
#include "mq/XrdMqRWMutex.hh"
#include "mq/XrdMqSharedObject.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
/*----------------------------------------------------------------------------*/
#include <string>
#include <stdint.h>
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

class FileSystem;

/*----------------------------------------------------------------------------*/
//! Class implementing the base class of a transfer queue used in FST & MGM
/*----------------------------------------------------------------------------*/

class TransferQueue {
private:
  // ---------------------------------------------------------------------------
  //! Queue name e.g. /eos/host/fst/mntpoint
  // ---------------------------------------------------------------------------
  std::string mQueue;

  // ---------------------------------------------------------------------------
  //! Full Queue name e.g. /eos/'host'/fst/mntpoint/txqueue/'txname'
  // ---------------------------------------------------------------------------
  std::string mFullQueue;

  // ---------------------------------------------------------------------------
  //! Transfer Queue name e.g. 'txname' e.g. balanceq, drainq, externq
  // ---------------------------------------------------------------------------
  std::string mTxQueue;   

  // ---------------------------------------------------------------------------
  //! Reference to parent object hosting this queue e.g. a filesystem object
  // ---------------------------------------------------------------------------
  FileSystem* mFileSystem;

  // ---------------------------------------------------------------------------
  //! Indicator for a queue slave e.g. if the object is deleted it __does__ __not__ clear the queue!
  // ---------------------------------------------------------------------------
  bool        mSlave;    

  // ---------------------------------------------------------------------------
  //! Reference to the underlying shared queue maintained by the shared object manager
  //! Usage of this object requires a read lock on the shared object manager and the hash has to be validated!
  // ---------------------------------------------------------------------------
  XrdMqSharedQueue* mHashQueue; 
  XrdMqSharedObjectManager* mSom;
  XrdSysMutex constructorLock;

  // ---------------------------------------------------------------------------
  //! Count number of jobs executed + mutex
  // ---------------------------------------------------------------------------
  XrdSysMutex mJobGetCountMutex;
  unsigned long long mJobGetCount;

public:
  // ---------------------------------------------------------------------------
  //! Constructor
  // ---------------------------------------------------------------------------
  TransferQueue(const char* queue, const char* queuepath, const char* subqueue, eos::common::FileSystem* fs, XrdMqSharedObjectManager* som, bool bc2mgm=false);

  // ---------------------------------------------------------------------------
  //! Add a transfer job to the queue
  // ---------------------------------------------------------------------------
  bool Add   (eos::common::TransferJob* job);

  // ---------------------------------------------------------------------------
  //! Get a transfer job from the queue
  // ---------------------------------------------------------------------------
  eos::common::TransferJob* Get();

  // ---------------------------------------------------------------------------
  //! Remove a transfer job from the queue
  // ---------------------------------------------------------------------------
  bool Remove(eos::common::TransferJob* job);

  // ---------------------------------------------------------------------------
  //! Get the count of retrieved transfers
  // ---------------------------------------------------------------------------
  unsigned long long GetJobCount() {
    unsigned long long count;
    {
      XrdSysMutexHelper cLock(mJobGetCountMutex);
      count = mJobGetCount;
    }
    return count;
  }

  // ---------------------------------------------------------------------------
  //! Increment the count of retrieved transfers
  // ---------------------------------------------------------------------------
  void IncGetJobCount() {
    XrdSysMutexHelper cLock(mJobGetCountMutex);
    mJobGetCount++;
  }

  // ---------------------------------------------------------------------------
  //! Get the current size of the queue
  // ---------------------------------------------------------------------------
  size_t Size() {
    if (mSom) {
      XrdMqRWMutexReadLock lock(mSom->HashMutex);
      mHashQueue = (XrdMqSharedQueue*) mSom->GetObject(mFullQueue.c_str(),"queue");
      if (mHashQueue) {
        if (mHashQueue->GetQueue()) {
          return mHashQueue->GetQueue()->size();
        }
      }
    }
    return 0;
  }

  // ---------------------------------------------------------------------------
  //! Clear all jobs from the queue
  // ---------------------------------------------------------------------------
  bool Clear () {    
    if (mSom) {
      XrdMqRWMutexReadLock lock(mSom->HashMutex);
      mHashQueue = (XrdMqSharedQueue*) mSom->GetObject(mFullQueue.c_str(),"queue");
      if (mHashQueue) {
        if (mHashQueue->GetQueue()) {
          mHashQueue->Clear();
          return true;
        }
      }
    }
    return false;
  };

  // ---------------------------------------------------------------------------
  //! Open a transaction for a bulk injection
  // ---------------------------------------------------------------------------
  bool OpenTransaction () {
    if (mSom) {
      XrdMqRWMutexReadLock lock(mSom->HashMutex);
      mHashQueue = (XrdMqSharedQueue*) mSom->GetObject(mFullQueue.c_str(),"queue");
      if (mHashQueue) {
        if (mHashQueue->GetQueue()) {
          return mHashQueue->OpenTransaction();
        }
      }
    }
    return false;
  }

  // ---------------------------------------------------------------------------
  //! Close a transaction after a bulk injection
  // ---------------------------------------------------------------------------
  bool CloseTransaction () {
    if (mSom) {
      XrdMqRWMutexReadLock lock(mSom->HashMutex);
      mHashQueue = (XrdMqSharedQueue*) mSom->GetObject(mFullQueue.c_str(),"queue");
      if (mHashQueue) {
        if (mHashQueue->GetQueue()) {
          return mHashQueue->CloseTransaction();
        }
      }
    }
    return false;
  }
  
  // ---------------------------------------------------------------------------
  //! Destructor
  // ---------------------------------------------------------------------------
  virtual ~TransferQueue();
};

EOSCOMMONNAMESPACE_END

#endif
