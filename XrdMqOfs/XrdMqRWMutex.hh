#ifndef __XRDMQ_RWMUTEX_HH__
#define __XRDMQ_RWMUTEX_HH__


#define _XOPEN_SOURCE 600

/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysPthread.hh"
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

public:
  XrdMqRWMutex() { if (pthread_rwlock_init(&rwlock, NULL)) {throw "pthread_rwlock_init failed";} }
  ~XrdMqRWMutex() {}

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
