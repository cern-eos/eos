#ifndef __XRDMQ_RWMUTEX_HH__
#define __XRDMQ_RWMUTEX_HH__

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
  pthread_rwlockattr_t   attr;
  int retc; 
  
public:
  XrdMqRWMutex() { 

    pthread_rwlockattr_init(&attr);
    if (pthread_rwlockattr_setkind_np(&attr,PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP)) { throw "pthread_rwlockattr_setkind_np failed";}
    if (pthread_rwlockattr_setpshared(&attr,PTHREAD_PROCESS_SHARED)){ throw "pthread_rwlockattr_setpshared failed";}
    if ((retc=pthread_rwlock_init(&rwlock, &attr))) {fprintf(stderr,"LockInit: retc=%d\n", retc);throw "pthread_rwlock_init failed";} }
  ~XrdMqRWMutex() {}

  void LockRead() {
    if ((retc=pthread_rwlock_rdlock(&rwlock))) { fprintf(stderr,"LockRead: retc=%d\n", retc);throw "pthread_rwlock_rdlock failed";}
    //    else 
    //      fprintf(stderr,"+++R %llu\n", (unsigned long long) this);

  }
  
  void UnLockRead() { 
    if ((retc=pthread_rwlock_unlock(&rwlock))) { fprintf(stderr,"UnLockRead: retc=%d\n", retc);throw "pthread_rwlock_unlock failed";}
    //   else 
    //    fprintf(stderr,"---R %llu\n", (unsigned long long) this);
  }

  void LockWrite() {
    if ((retc=pthread_rwlock_wrlock(&rwlock))) { fprintf(stderr,"LockWrite: retc=%d\n", retc);throw "pthread_rwlock_wrlock failed";}
    //   else 
    //    fprintf(stderr,"+++W %llu\n", (unsigned long long) this);
  }
  
  void UnLockWrite() { 
    if ((retc=pthread_rwlock_unlock(&rwlock))) { fprintf(stderr,"UnLockWrite: retc=%d\n", retc);throw "pthread_rwlock_unlock failed";}
    //   else 
    //    fprintf(stderr,"---W %llu\n", (unsigned long long) this);
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
