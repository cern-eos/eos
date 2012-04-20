// ----------------------------------------------------------------------
// File: RWMutex.hh
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
 * @file   RWMutex.hh
 * 
 * @brief  Class implementing a fair read-write Mutex.
 * 
 * 
 */

#ifndef __EOSCOMMON_RWMUTEX_HH__
#define __EOSCOMMON_RWMUTEX_HH__

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysPthread.hh"
#include "XrdSys/XrdSysAtomics.hh"
/*----------------------------------------------------------------------------*/
#include <stdio.h>
#define _MULTI_THREADED
#include <pthread.h>
/*----------------------------------------------------------------------------*/


EOSCOMMONNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
//! Class implements a fair rw mutex prefering writers
/*----------------------------------------------------------------------------*/
class RWMutex  
{
private:
  pthread_rwlock_t       rwlock;
  pthread_rwlockattr_t   attr;
  struct timespec        wlocktime;
  bool                   blocking;

  ssize_t                readLockCounter;
  ssize_t                writeLockCounter;

public:
  // ---------------------------------------------------------------------------
  //! Constructor
  // ---------------------------------------------------------------------------
  RWMutex() {
    // by default we are not a blocking write mutex
    blocking = false;
    // try to get write lock in 5 seconds, then release quickly and retry
    wlocktime.tv_sec=5; 
    wlocktime.tv_nsec=0;
    // readers don't go ahead of writers!
    if (pthread_rwlockattr_setkind_np(&attr,PTHREAD_RWLOCK_PREFER_WRITER_NP)) { throw "pthread_rwlockattr_setkind_np failed";}
    if (pthread_rwlockattr_setpshared(&attr,PTHREAD_PROCESS_SHARED)){ throw "pthread_rwlockattr_setpshared failed";}
    if (pthread_rwlock_init(&rwlock, &attr)) {throw "pthread_rwlock_init failed";}}

  // ---------------------------------------------------------------------------
  //! Destructor
  // ---------------------------------------------------------------------------
  ~RWMutex() {}

  // ---------------------------------------------------------------------------
  //! Set the write lock to blocking or not blocking
  // ---------------------------------------------------------------------------
  void SetBlocking(bool block) {
    blocking = block;
  }

  // ---------------------------------------------------------------------------
  //! Lock for read
  // ---------------------------------------------------------------------------
  void LockRead() {
    AtomicInc(readLockCounter);
    if (pthread_rwlock_rdlock(&rwlock)) { throw "pthread_rwlock_rdlock failed";}
  }
  
  // ---------------------------------------------------------------------------
  //! Unlock a read lock
  void UnLockRead() { 
    if (pthread_rwlock_unlock(&rwlock)) { throw "pthread_rwlock_unlock failed";}
  }
  
  // ---------------------------------------------------------------------------
  //! Lock for write
  // ---------------------------------------------------------------------------
  void LockWrite() {
    AtomicInc(writeLockCounter);
    if (blocking) {
    // a blocking mutex is just a normal lock for write
      if (pthread_rwlock_wrlock(&rwlock)) { throw "pthread_rwlock_rdlock failed";}
    } else {
      // a non-blocking mutex tries for few seconds to write lock, then releases
      // this has the side effect, that it allows dead locked readers to jump ahead the lock queue
      while (1) {
	int rc = pthread_rwlock_timedwrlock(&rwlock, &wlocktime);
	if ( rc ) {
	  if (rc != ETIMEDOUT) {
	    fprintf(stderr,"=== WRITE LOCK EXCEPTION == TID=%llu OBJECT=%llx rc=%d\n", (unsigned long long)XrdSysThread::ID(), (unsigned long long)this,rc);
	    throw "pthread_rwlock_wrlock failed";
	  } else {
	    fprintf(stderr,"==== WRITE LOCK PENDING ==== TID=%llu OBJECT=%llx\n",(unsigned long long)XrdSysThread::ID(), (unsigned long long)this); 
	    usleep(100000);
	  }
	} else {
	  break;
	}
      }
    }
  }

  // ---------------------------------------------------------------------------
  //! Lock for write but give up after wlocktime
  // ---------------------------------------------------------------------------
  int TimeoutLockWrite() {
    return pthread_rwlock_timedwrlock(&rwlock, &wlocktime);
  }
  
  // ---------------------------------------------------------------------------
  //! Unlock a write lock
  // ---------------------------------------------------------------------------
  void UnLockWrite() { 
    if (pthread_rwlock_unlock(&rwlock)) { throw "pthread_rwlock_unlock failed";}
  }

  // ---------------------------------------------------------------------------
  //! Get Readlock Counter
  // ---------------------------------------------------------------------------
  ssize_t GetReadLockCounter() {
    return AtomicGet(readLockCounter);
  }

  // ---------------------------------------------------------------------------
  //! Get Writelock Counter
  // ---------------------------------------------------------------------------
  ssize_t GetWriteLockCounter() {
    return AtomicGet(writeLockCounter);
  }


};

/*----------------------------------------------------------------------------*/
//! Class implementing a monitor for write locking
/*----------------------------------------------------------------------------*/
class RWMutexWriteLock
{
private:
  RWMutex* Mutex;

public:
  // ---------------------------------------------------------------------------
  //! Constructor
  // ---------------------------------------------------------------------------
  RWMutexWriteLock(RWMutex &mutex) { Mutex = &mutex; Mutex->LockWrite();}

  // ---------------------------------------------------------------------------
  //! Destructor
  // ---------------------------------------------------------------------------
  ~RWMutexWriteLock() { Mutex->UnLockWrite();}
};

/*----------------------------------------------------------------------------*/
//! Class implementing a monitor for read locking
/*----------------------------------------------------------------------------*/
class RWMutexReadLock
{
private:
  RWMutex* Mutex;

public:
  // ---------------------------------------------------------------------------
  //! Constructor
  // ---------------------------------------------------------------------------
  
  RWMutexReadLock(RWMutex &mutex) { Mutex = &mutex; Mutex->LockRead();}

  // ---------------------------------------------------------------------------
  //! Destructor
  // ---------------------------------------------------------------------------
  ~RWMutexReadLock() { Mutex->UnLockRead();}
};

/*----------------------------------------------------------------------------*/
EOSCOMMONNAMESPACE_END

#endif
