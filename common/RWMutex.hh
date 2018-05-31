//------------------------------------------------------------------------------
// File: RWMutex.hh
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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

//------------------------------------------------------------------------------
//! @brief Class implementing a fair read-write Mutex.
//!
//! @description When compiled with EOS_INSTRUMENTED_RWMUTEX, this class
//! provides also timing features. The timing can be taken exhaustively or it
//! can be sampled. Then some basics statistics are available.
//! The statistics are available at the instance granularity or at the class
//! granularity. If the timing is turned off (at both levels), the instrumented
//! class is approximatively as fast as the native class.
//! Note that taking the timing of a mutex lock multiply the lock exec time by
//! a factor from 4 up to 6. An estimation of the added latency is provided as
//! well as a mechanism for timing compensation.
//!
//! Locking/unlocking order checking features
//! Rules can be defined. Then, each time a mutex is involved in one of these
//! rules is locked/unlocked. There is bookkeeping for each thread. If a thread
//! doesn't respect the order when locking/unlocking, a message is issued to
//! std err.
//! For performance reasons, the maximum number of rules is static and defined
//! in the macro EOS_RWMUTEX_ORDER_NRULES (default 4).
//! A rule is defined by a locking order (a sequence of pointers to RWMutex
//! instances). The maximum length of this sequence is 63.
//! The added latency by order checking for 3 mutexes and 1 rule is about 15%
//! of the locking/unlocking execution time. An estimation of this added latency
//! is provided.
//------------------------------------------------------------------------------

#pragma once
#ifndef __APPLE__
#define EOS_INSTRUMENTED_RWMUTEX
#endif

#include "common/Namespace.hh"
#include "common/Timing.hh"
#include "common/IRWMutex.hh"
#include "XrdSys/XrdSysPthread.hh"
#include "XrdSys/XrdSysTimer.hh"
#include "XrdSys/XrdSysAtomics.hh"
#include <stdio.h>
#include <stdint.h>
#ifdef EOS_INSTRUMENTED_RWMUTEX
#include <map>
#include <vector>
#include <ostream>
#include <fstream>
#include <iostream>
#include <execinfo.h>
#include <limits>
#include <cmath>
#include <set>
#include <thread>
#endif

#define _MULTI_THREADED
#include <pthread.h>

#define EOS_RWMUTEX_ORDER_NRULES 4

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class RWMutex
//------------------------------------------------------------------------------
class RWMutex
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  // ---------------------------------------------------------------------------
  RWMutex(bool prefer_readers = false)
  {
    mMutexImpl = static_cast<IRWMutex*>(new PthreadRWMutex(prefer_readers));

    if (getenv("EOS_PTHREAD_RW_MUTEX")) {
    } else {
      // todo
    }
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~RWMutex() {}

  //----------------------------------------------------------------------------
  //! Get raw ptr
  //----------------------------------------------------------------------------
  IRWMutex* GetRawPtr()
  {
    return mMutexImpl;
  }

  //----------------------------------------------------------------------------
  //! Move constructor
  //----------------------------------------------------------------------------
  RWMutex(RWMutex&& other) noexcept;

  //----------------------------------------------------------------------------
  //! Move assignment operator
  //----------------------------------------------------------------------------
  RWMutex& operator=(RWMutex&& other) noexcept;

  //----------------------------------------------------------------------------
  //! Copy constructor
  //----------------------------------------------------------------------------
  RWMutex(const RWMutex&) = delete;

  //----------------------------------------------------------------------------
  //! Copy assignment operator
  //----------------------------------------------------------------------------
  RWMutex& operator=(const RWMutex&) = delete;

  //----------------------------------------------------------------------------
  //! Set the write lock to blocking or not blocking
  //!
  //! @param block blocking mode
  //----------------------------------------------------------------------------
  void SetBlocking(bool block)
  {
    mMutexImpl->SetBlocking(block);
  }

  //----------------------------------------------------------------------------
  //! Lock for read
  //----------------------------------------------------------------------------
  void LockRead();

  //----------------------------------------------------------------------------
  //! Unlock a read lock
  //----------------------------------------------------------------------------
  void UnLockRead();

  //----------------------------------------------------------------------------
  //! Try to read lock the mutex within the timeout
  //!
  //! @param timeout_ns nano seconds timeout
  //!
  //! @return true if lock acquired successfully, otherwise false
  //----------------------------------------------------------------------------
  bool TimedRdLock(uint64_t timeout_ns);

  //----------------------------------------------------------------------------
  //! Lock for write
  //----------------------------------------------------------------------------
  void LockWrite();

  //----------------------------------------------------------------------------
  //! Unlock a write lock
  //----------------------------------------------------------------------------
  void UnLockWrite();

  //----------------------------------------------------------------------------
  //! Try to write lock the mutex within the timeout
  //!
  //! @param timeout_ns nano seconds timeout
  //!
  //! @return true if lock acquired successfully, otherwise false
  //----------------------------------------------------------------------------
  bool TimedWrLock(uint64_t timeout_ns);

  //----------------------------------------------------------------------------
  //! Get Readlock Counter
  //----------------------------------------------------------------------------
  inline uint64_t GetReadLockCounter()
  {
    return AtomicGet(mRdLockCounter);
  }

  //----------------------------------------------------------------------------
  //! Get Writelock Counter
  //----------------------------------------------------------------------------
  inline uint64_t GetWriteLockCounter()
  {
    return AtomicGet(mWrLockCounter);
  }

#ifdef EOS_INSTRUMENTED_RWMUTEX

  struct TimingStats {
    double averagewaitread;
    double averagewaitwrite;
    double minwaitwrite, maxwaitwrite;
    double minwaitread, maxwaitread;
    size_t readLockCounterSample, writeLockCounterSample;
  };

  //----------------------------------------------------------------------------
  //! Performs the initialization of the class
  //----------------------------------------------------------------------------
  static void InitializeClass();

  //----------------------------------------------------------------------------
  //! Reset statistics at the class level
  //----------------------------------------------------------------------------
  static void ResetTimingStatisticsGlobal();

  //----------------------------------------------------------------------------
  //! Turn on/off timings at the class level
  //----------------------------------------------------------------------------
  inline static void SetTimingGlobal(bool on)
  {
    mMutexImpl->SetWLockTime(nsec);
  }

  //----------------------------------------------------------------------------
  //! Lock for read
  //----------------------------------------------------------------------------
  void LockRead()
  {
    mMutexImpl->LockRead();
  }

  //----------------------------------------------------------------------------
  //! Lock for read allowing to be canceled waiting for the lock
  //----------------------------------------------------------------------------
  void LockReadCancel()
  {
    mMutexImpl->LockReadCancel();
  }

  //----------------------------------------------------------------------------
  //! Unlock a read lock
  //----------------------------------------------------------------------------
  void UnLockRead()
  {
    mMutexImpl->UnLockRead();
  }

  //----------------------------------------------------------------------------
  //! Lock for write
  //----------------------------------------------------------------------------
  void LockWrite()
  {
    mMutexImpl->LockWrite();
  }

  //----------------------------------------------------------------------------
  //! Unlock a write lock
  //----------------------------------------------------------------------------
  void UnLockWrite()
  {
    mMutexImpl->UnLockWrite();
  }

  //----------------------------------------------------------------------------
  //! Try to read lock the mutex within the timout value
  //!
  //! @param timeout_ms time duration in milliseconds we can wait for the lock
  //!
  //! @return 0 if lock aquired, ETIMEOUT if timeout occured
  //----------------------------------------------------------------------------
  int TimedRdLock(uint64_t timeout_ms)
  {
    return mMutexImpl->TimedRdLock(timeout_ms);
  }

  //----------------------------------------------------------------------------
  //! Lock for write but give up after wlocktime
  //----------------------------------------------------------------------------
  int TimeoutLockWrite()
  {
    return mMutexImpl->TimeoutLockWrite();
  }

  //----------------------------------------------------------------------------
  //! Get read lock counter
  //----------------------------------------------------------------------------
  size_t GetReadLockCounter()
  {
    return mMutexImpl->GetReadLockCounter();
  }

  //----------------------------------------------------------------------------
  //! Get write lock counter
  //----------------------------------------------------------------------------
  size_t GetWriteLockCounter()
  {
    return mMutexImpl->GetWriteLockCounter();
  }

private:
  bool mBlocking;
  // @todo (esindril): implement proper copy and move constructors
  //std::unique_ptr<IRWMutex> mMutexImp;
  IRWMutex* mMutexImp;
  pthread_rwlock_t rwlock;
  pthread_rwlockattr_t attr;
  struct timespec wlocktime;
  uint64_t mRdLockCounter;
  uint64_t mWrLockCounter;
  bool mPreferRd; ///< If true reads go ahead of wr and are reentrant

#ifdef EOS_INSTRUMENTED_RWMUTEX
  std::string mDebugName;
  int mCounter;
  int mSamplingModulo;
  bool mEnableTiming, mEnableSampling;
  //! Specific type of counters
  size_t mRdCumulatedWait, mWrCumulatedWait;
  size_t mRdMaxWait, mWrMaxWait, mRdMinWait, mWrMinWait;
  size_t mRdLockCounterSample, mWrLockCounterSample;

  std::map<std::thread::id, int> mThreadsRdLock; ///< Threads holding a read lock
  std::set<std::thread::id> mThreadsWrLock; ///< Threads holding a write lock
  pthread_mutex_t mCollectionMutex; ///< Mutex protecting the sets above
  bool mEnableDeadlockCheck; ///< Check for deadlocks
  bool mTransientDeadlockCheck; ///< Enabled by the global flag

  static bool staticInitialized;
  static bool sEnableGlobalTiming;
  static int sSamplingModulo;
  static size_t timingCompensation, timingLatency, lockUnlockDuration;
  static size_t mRdCumulatedWait_static, mWrCumulatedWait_static;
  static size_t mRdMaxWait_static, mWrMaxWait_static;
  static size_t mRdMinWait_static, mWrMinWait_static;
  static size_t mRdLockCounterSample_static, mWrLockCounterSample_static;

  // Actual order checking
  // Pointers referring to a memory location not thread specific so that if the
  // thread terminates, this location is still valid. This flag is triggered by
  // the class management and indicates to each thread to reset some thread
  // specific stuff.
  static thread_local bool* orderCheckReset_staticthread;
  // Map containing the previously referenced flag for reset
  static std::map<pthread_t, bool>* threadOrderCheckResetFlags_static;
  // Each unsigned long is used as 64 bit flags to trace the lock status of
  // mutexes for a given rule
  static thread_local unsigned long
  ordermask_staticthread[EOS_RWMUTEX_ORDER_NRULES];
  // A mutex can be associated to up to EOS_RWMUTEX_ORDER_NRULES and the
  // following array gives the locking rank for "this" RWMutex
  unsigned char rankinrule[EOS_RWMUTEX_ORDER_NRULES];
  // the number of rules "this" RWMutex
  unsigned char nrules;

  // ******** Order Rules management  *******
  // Order rules management
  // to issue the message and to manage the rules. Not involved in the online checking.
  static std::map<unsigned char, std::string>* ruleIndex2Name_static;
  static std::map<std::string, unsigned char>* ruleName2Index_static;
  unsigned char ruleLocalIndexToGlobalIndex[EOS_RWMUTEX_ORDER_NRULES];
  // rulename -> order
  typedef std::map< std::string , std::vector<RWMutex*> > rules_t;
  static rules_t* rules_static;
  // lock to guarantee unique access to rules management
  static pthread_rwlock_t mOrderChkLock;
  static bool sEnableGlobalOrderCheck;
  static bool sEnableGlobalDeadlockCheck;
  static size_t orderCheckingLatency;
#endif
};

// undefine the timer stuff
#ifdef EOS_INSTRUMENTED_RWMUTEX
#undef EOS_RWMUTEX_TIMER_START
#undef EOS_RWMUTEX_TIMER_STOP_AND_UPDATE
#endif

//------------------------------------------------------------------------------
//! Class RWMutexReadLock
//------------------------------------------------------------------------------
class RWMutexWriteLock
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  RWMutexWriteLock(): mWrMutex(nullptr) {};

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param mutex mutex to lock for write
  //----------------------------------------------------------------------------
  RWMutexWriteLock(RWMutex& mutex);

  //----------------------------------------------------------------------------
  //! Grab mutex and write lock it
  //!
  //! @param mutex mutex to lock for write
  //----------------------------------------------------------------------------
  void Grab(RWMutex& mutex);

  //----------------------------------------------------------------------------
  //! Release the write lock after grab
  //----------------------------------------------------------------------------
  void Release();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~RWMutexWriteLock();

private:
  RWMutex* mWrMutex;
};

//------------------------------------------------------------------------------
//! Class RWMutexReadLock
//------------------------------------------------------------------------------
class RWMutexReadLock
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  RWMutexReadLock(): mRdMutex(nullptr) {};

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param mutex mutex to handle
  //----------------------------------------------------------------------------
  RWMutexReadLock(RWMutex& mutex);

  //----------------------------------------------------------------------------
  //! Grab mutex and read lock it
  //!
  //! @param mutex mutex to lock for read
  //----------------------------------------------------------------------------
  void Grab(RWMutex& mutex);

  //----------------------------------------------------------------------------
  //! Release the write lock after grab
  //----------------------------------------------------------------------------
  void Release();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~RWMutexReadLock();

private:
  RWMutex* mRdMutex;
};

//------------------------------------------------------------------------------
//! RW Mutex prefereing the reader
//------------------------------------------------------------------------------
class RWMutexR : public RWMutex
{
public:
  RWMutexR() : RWMutex(true) { }
  virtual ~RWMutexR() {}
};

//------------------------------------------------------------------------------
//! RW Mutex prefereing the writerr
//------------------------------------------------------------------------------
class RWMutexW : public RWMutex
{
public:
  RWMutexW() : RWMutex(false) { }
  virtual ~RWMutexW() {}
};

EOSCOMMONNAMESPACE_END
