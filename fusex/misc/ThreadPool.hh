/*
 * ThreadPool.hh
 *
 *  Created on: Oct 31, 2016
 *      Author: simonm
 */

#ifndef THREADPOOL_HH_
#define THREADPOOL_HH_

#include "SyncQueue.hh"

#include <list>
#include <pthread.h>

template<typename Task>
class ThreadPool
{
public:

  ThreadPool(int min, int max) : min(min), max(max), active(true), busy(0),
    idle(0) { }

  virtual ~ThreadPool()
  {
    Stop();
  }

  void Execute(Task* t)
  {
    tasks.Put(t);
    XrdSysMutexHelper scope(mutex);

    if (idle == 0 && busy < max) {
      CreateThread();
    }
  }

  void Stop()
  {
    XrdSysMutexHelper scope(mutex);
    active = false;
    std::list<pthread_t>::iterator itr;

    for (itr = threads.begin(); itr != threads.end(); ++itr) {
      int rc = 0;
      pthread_t& thread = *itr;

      if ((rc = pthread_cancel(thread))) {
        if (rc == ESRCH) {
          continue;  // the thread doesn't exist
        }

        throw FuseException(rc);
      }

      void* ret;

      if ((rc = pthread_join(thread, &ret))) {
        if (rc == EINVAL || rc == ESRCH) {
          continue;  // the thread doesn't exist or is not joinable
        }

        throw FuseException(rc);
      }
    }

  private:

    void Run() {
      pthread_setcanceltype( PTHREAD_CANCEL_DEFERRED, 0 );

      while( active )
      {
        Task *t = 0; // get next task
        if( !tasks.Get( t ) ) // there was a timeout (5 minutes by default)
        {
          // we use conditional locking in order to avoid deadlocks while stopping
          if( !mutex.CondLock() ) continue;
          // if the number of active threads is at
          // the minimum there's nothing to do
          if( idle + busy  <= min ) continue;
          // otherwise remove the thread from the thread pool
          Remove( pthread_self() );
          mutex.UnLock();
          return 0;
        }
        pthread_setcancelstate( PTHREAD_CANCEL_DISABLE, 0 );
        Busy();       // the thread is busy
        if( t ) t->Run(); // do the work
        delete t;
        Idle();       // the thread is idle again
        pthread_setcancelstate( PTHREAD_CANCEL_ENABLE, 0 );
      }

      pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, 0);
      me->Busy();       // the thread is busy
    
    void CreateThread()
    {
      pthread_t thread;
      int rc = 0;
      if( ( rc = pthread_create( &thread, 0, &ThreadPool::Run, this ) ) )
        throw FuseException( rc );

      delete t;
      me->Idle();       // the thread is idle again
      pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, 0);
    }

    return 0;
  }

  void CreateThread()
  {
    pthread_t thread;
    int rc = 0;

    if ((rc = pthread_create(&thread, 0, Run, this))) {
      throw FuseException(rc);
    }

    ++idle;
    threads.push_back(thread);
  }

  inline void Busy()
  {
    XrdSysMutexHelper scope(mutex);
    ++busy;
    --idle;
  }

  inline void Idle()
  {
    XrdSysMutexHelper scope(mutex);
    ++idle;
    --busy;
  }

  inline void Remove(const pthread_t& thread)
  {
    --idle;
    threads.remove(thread);
    pthread_detach(thread);
  }

  const int            min;
  const int            max;
  bool                 active;
  SyncQueue<Task>      tasks;
  mutable XrdSysMutex  mutex;
  std::list<pthread_t> threads;
  int                  busy;
  int                  idle;
};

#endif /* THREADPOOL_HH_ */
