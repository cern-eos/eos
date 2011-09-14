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
public:
  RWMutex() {
    if (pthread_rwlockattr_setkind_np(&attr,PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP)) { throw "pthread_rwlockattr_setkind_np failed";}
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
    if (pthread_rwlock_wrlock(&rwlock)) { throw "pthread_rwlock_wrlock failed";}
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
