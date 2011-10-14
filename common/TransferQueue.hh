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

class TransferQueue {
private:
  std::string mQueue;      // = <queue>              e.g. /eos/<host>/fst/<mntpoint>
  std::string mFullQueue;  // = <fullqueue>          e.g. /eos/<host>/fst/<mntpoint>/txqueue/<txqueue>
  std::string mTxQueue;    // = <txqueue>
  FileSystem* mFileSystem; // -> pointer to parent object
  bool        mSlave;      // -> this is a queue slave, it won't clear the queue on deletion

  XrdMqSharedQueue* mHashQueue;  // before usage mSom needs a read lock and mHash has to be validated to avoid race conditions in deletion
  XrdMqSharedObjectManager* mSom;
  XrdSysMutex constructorLock;

public:
  //------------------------------------------------------------------------
  //! Constructor
  //------------------------------------------------------------------------

  TransferQueue(const char* queue, const char* queuepath, const char* subqueue, eos::common::FileSystem* fs, XrdMqSharedObjectManager* som, bool bc2mgm=false);

  bool Add   (eos::common::TransferJob* job);
  eos::common::TransferJob* Get();
  bool Remove(eos::common::TransferJob* job);

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
  
  virtual ~TransferQueue();
};

EOSCOMMONNAMESPACE_END

#endif
