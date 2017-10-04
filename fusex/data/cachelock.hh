/*
 * cachelock.hh
 *
 *  Created on: May 10, 2017
 *      Author: simonm
 */

#ifndef FUSEX_CACHELOCK_HH_
#define FUSEX_CACHELOCK_HH_

#include <pthread.h>
#include <errno.h>

#include <exception>

class cachelock_error
{
public:

  cachelock_error(int errcode) : errcode(errcode) { }

  cachelock_error(const cachelock_error& err) : errcode(err.errcode) { }

  cachelock_error& operator= (const cachelock_error& err)
  {
    errcode = err.errcode;
    return *this;
  }

  virtual ~cachelock_error() { }

  virtual const char* what() const throw()
  {
    return strerror(errcode);
  }

private:

  int errcode;
};


class cachelock
{
public:

  cachelock() : readers(0), sync(false)
  {
    int rc = 0;
    rc = pthread_cond_init(&cvar, NULL);

    if (rc) {
      throw cachelock_error(rc);
    }

    rc = pthread_cond_init(&rwvar, NULL);

    if (rc) {
      throw cachelock_error(rc);
    }

    rc = pthread_mutex_init(&mtx, NULL);

    if (rc) {
      throw cachelock_error(rc);
    }
  }

  ~cachelock()
  {
    pthread_cond_destroy(&cvar);
    pthread_cond_destroy(&rwvar);
    pthread_mutex_destroy(&mtx);
  }

  void read_lock()
  {
    lock(mtx);
    ++readers;
    unlock(mtx);
  }

  void read_unlock()
  {
    lock(mtx);
    --readers;

    if (readers == 0) {
      broadcast(rwvar, mtx);
    }

    unlock(mtx);
  }

  void read_wait()
  {
    lock(mtx);
    --readers;
    wait(cvar, mtx);
    ++readers;
    unlock(mtx);
  }

  void write_lock()
  {
    lock(mtx);

    while (readers > 0) {
      wait(rwvar, mtx);
    }
  }

  void write_unlock()
  {
    unlock(mtx);
  }

  void write_wait()
  {
    wait(cvar, mtx);

    while (readers > 0) {
      wait(rwvar, mtx);
    }
  }

  void broadcast()
  {
    broadcast(cvar, mtx);
  }

private:

  static void lock(pthread_mutex_t& mtx)
  {
    int rc = pthread_mutex_lock(&mtx);

    if (rc) {
      throw cachelock_error(rc);
    }
  }

  static void unlock(pthread_mutex_t& mtx)
  {
    int rc = pthread_mutex_unlock(&mtx);

    if (rc) {
      throw cachelock_error(rc);
    }
  }

  static void wait(pthread_cond_t& var, pthread_mutex_t& mtx)
  {
    int rc = pthread_cond_wait(&var, &mtx);

    if (rc) {
      pthread_mutex_unlock(&mtx);
      throw cachelock_error(rc);
    }
  }

  static void broadcast(pthread_cond_t& var, pthread_mutex_t& mtx)
  {
    int rc = pthread_cond_broadcast(&var);

    if (rc) {
      pthread_mutex_unlock(&mtx);
      throw cachelock_error(rc);
    }
  }

  size_t readers;
  bool   sync;

  pthread_cond_t  cvar;
  pthread_cond_t  rwvar;
  pthread_mutex_t mtx;
};

class read_lock
{
public:

  read_lock(cachelock& lck) : lck(lck)
  {
    lck.read_lock();
  }

  ~read_lock()
  {
    try {
      lck.read_unlock();
    } catch (const cachelock_error& ex) {
    }
  }

private:

  cachelock& lck;
};

class write_lock
{
public:

  write_lock(cachelock& lck) : lck(lck)
  {
    lck.write_lock();
  }

  ~write_lock()
  {
    try {
      lck.write_unlock();
    } catch (const cachelock_error& ex) {
    }
  }

private:

  cachelock& lck;
};

#endif /* FUSEX_CACHELOCK_HH_ */
