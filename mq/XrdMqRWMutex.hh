// ----------------------------------------------------------------------
// File: XrdMqRWMutex.hh
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

#ifndef __XRDMQ_RWMUTEX_HH__
#define __XRDMQ_RWMUTEX_HH__

/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysPthread.hh"
#include "XrdSys/XrdSysAtomics.hh"
/*----------------------------------------------------------------------------*/

#include <stdio.h>
#define _MULTI_THREADED
#include <pthread.h>

/*----------------------------------------------------------------------------*/
/* THIS CLASS IMPLEMENTS A FAIR RW MUTEX                                      */
/*----------------------------------------------------------------------------*/

class XrdMqRWMutex  
{
private:
  pthread_rwlock_t       rwlock;
  pthread_rwlockattr_t   attr;
  unsigned long long     wlockid;

public:
  XrdMqRWMutex() { 
    int retc;
    pthread_rwlockattr_init(&attr);
    wlockid=0;
    if (pthread_rwlockattr_setkind_np(&attr,PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP)) { throw "pthread_rwlockattr_setkind_np failed";}
    if (pthread_rwlockattr_setpshared(&attr,PTHREAD_PROCESS_SHARED)){ throw "pthread_rwlockattr_setpshared failed";}
    if ((retc=pthread_rwlock_init(&rwlock, &attr))) {fprintf(stderr,"LockInit: retc=%d\n", retc);throw "pthread_rwlock_init failed";} }

  ~XrdMqRWMutex() {}

  void LockRead() {
    int retc; 
    if (AtomicGet(wlockid) == XrdSysThread::ID()) {
      fprintf(stderr,"MQ === WRITE LOCK FOLLOWED BY READ === TID=%llu OBJECT=%llx\n",(unsigned long long)XrdSysThread::ID(), (unsigned long long)this);
      throw "pthread_rwlock_wrlock write then read lock";
    }

    // fprintf(stderr,"MQ --- READ  LOCK WANTED    ---- TID=%llu OBJECT=%llx\n",(unsigned long long)XrdSysThread::ID(), (unsigned long long)this);

    if ((retc=pthread_rwlock_rdlock(&rwlock))) { fprintf(stderr,"LockRead: retc=%d\n", retc);throw "pthread_rwlock_rdlock failed";}

    //fprintf(stderr,"MQ ... READ  LOCK ACQUIRED  .... TID=%llu OBJECT=%llx\n",(unsigned long long)XrdSysThread::ID(), (unsigned long long)this);
  }
  
  void UnLockRead() { 
    int retc; 
    if ((retc=pthread_rwlock_unlock(&rwlock))) { fprintf(stderr,"UnLockRead: retc=%d\n", retc);throw "pthread_rwlock_unlock failed";}
    //    fprintf(stderr,"MQ ... READ  LOCK RELEASED  .... TID=%llu OBJECT=%llx\n",(unsigned long long)XrdSysThread::ID(), (unsigned long long)this);
  }

  void LockWrite() {
    int retc; 
    if (AtomicGet(wlockid) == XrdSysThread::ID()) {
      fprintf(stderr,"MQ === WRITE LOCK DOUBLELOCK === TID=%llu OBJECT=%llx\n",(unsigned long long)XrdSysThread::ID(), (unsigned long long)this);
      throw "pthread_rwlock_wrlock double lock";
    }
    //    fprintf(stderr,"MQ --- WRITE LOCK WANTED    ---- TID=%llu OBJECT=%llx\n",(unsigned long long)XrdSysThread::ID(), (unsigned long long)this);

    if ((retc=pthread_rwlock_wrlock(&rwlock))) { fprintf(stderr,"LockWrite: retc=%d\n", retc);throw "pthread_rwlock_wrlock failed";}

    AtomicFAZ(wlockid);
    AtomicAdd(wlockid,(unsigned long long)XrdSysThread::ID());

    //    fprintf(stderr,"MQ === WRITE LOCK ACQUIRED  ==== TID=%llu OBJECT=%llx\n",(unsigned long long)XrdSysThread::ID(), (unsigned long long)this);
  }
  
  void UnLockWrite() { 
    int retc; 
    if ((retc=pthread_rwlock_unlock(&rwlock))) { fprintf(stderr,"UnLockWrite: retc=%d\n", retc);throw "pthread_rwlock_unlock failed";}
    //    fprintf(stderr,"MQ --- WRITE LOCK RELEASED  ---- TID=%llu OBJECT=%llx\n",(unsigned long long)XrdSysThread::ID(), (unsigned long long)this);

    AtomicFAZ(wlockid);
  }
};


class XrdMqRWMutexWriteLock
{
private:
  XrdMqRWMutex* Mutex;

public:
  XrdMqRWMutexWriteLock(XrdMqRWMutex &mutex) { Mutex = &mutex; Mutex->LockWrite();}
  ~XrdMqRWMutexWriteLock() { Mutex->UnLockWrite();}
};

class XrdMqRWMutexReadLock
{
private:
  XrdMqRWMutex* Mutex;

public:
  XrdMqRWMutexReadLock(XrdMqRWMutex &mutex) { Mutex = &mutex; Mutex->LockRead();}
  ~XrdMqRWMutexReadLock() { Mutex->UnLockRead();}
};




#endif
