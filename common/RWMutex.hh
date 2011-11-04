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

#ifndef __EOSCOMMON_RWMUTEX_HH__
#define __EOSCOMMON_RWMUTEX_HH__

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysPthread.hh"
/*----------------------------------------------------------------------------*/
#include <stdio.h>
#define _MULTI_THREADED
#include <pthread.h>
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/
/* THIS CLASS IMPLEMENTS A FAIR RW MUTEX                                      */
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

class RWMutex  
{
private:
  pthread_rwlock_t       rwlock;
  pthread_rwlockattr_t   attr;
  struct timespec        wlocktime;
  
public:
  RWMutex() {
    wlocktime.tv_sec=5; // try to get write lock in 5 seconds, then release quickly and retry
    wlocktime.tv_nsec=0;
    if (pthread_rwlockattr_setkind_np(&attr,PTHREAD_RWLOCK_PREFER_WRITER_NP)) { throw "pthread_rwlockattr_setkind_np failed";}
    if (pthread_rwlockattr_setpshared(&attr,PTHREAD_PROCESS_SHARED)){ throw "pthread_rwlockattr_setpshared failed";}
    if (pthread_rwlock_init(&rwlock, &attr)) {throw "pthread_rwlock_init failed";}}
  ~RWMutex() {}

  void LockRead() {
    if (pthread_rwlock_rdlock(&rwlock)) { throw "pthread_rwlock_rdlock failed";}
  }
  
  void UnLockRead() { 
    if (pthread_rwlock_unlock(&rwlock)) { throw "pthread_rwlock_unlock failed";}
  }
  
  void LockWrite() {
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
  
  void UnLockWrite() { 
    if (pthread_rwlock_unlock(&rwlock)) { throw "pthread_rwlock_unlock failed";}
  }
};


class RWMutexWriteLock
{
private:
  RWMutex* Mutex;

public:
  RWMutexWriteLock(RWMutex &mutex) { Mutex = &mutex; Mutex->LockWrite();}
  ~RWMutexWriteLock() { Mutex->UnLockWrite();}
};

class RWMutexReadLock
{
private:
  RWMutex* Mutex;

public:
  RWMutexReadLock(RWMutex &mutex) { Mutex = &mutex; Mutex->LockRead();}
  ~RWMutexReadLock() { Mutex->UnLockRead();}
};

EOSCOMMONNAMESPACE_END

#endif
