//------------------------------------------------------------------------------
// File: PthreadRWMutex.cc
//------------------------------------------------------------------------------

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

#include "XrdSys/XrdSysAtomics.hh"
#include "common/backward-cpp/backward.hpp"
#include "common/PthreadRWMutex.hh"
#include "common/RWMutex.hh"
#include <exception>

EOSCOMMONNAMESPACE_BEGIN

#ifdef EOS_INSTRUMENTED_RWMUTEX
size_t PthreadRWMutex::mRdCumulatedWait_static = 0;
size_t PthreadRWMutex::mWrCumulatedWait_static = 0;
size_t PthreadRWMutex::mRdMaxWait_static = 0;
size_t PthreadRWMutex::mWrMaxWait_static = 0;
size_t PthreadRWMutex::mRdMinWait_static = 1e12;
size_t PthreadRWMutex::mWrMinWait_static = 1e12;
size_t PthreadRWMutex::mRdLockCounterSample_static = 0;
size_t PthreadRWMutex::mWrLockCounterSample_static = 0;
size_t PthreadRWMutex::timingCompensation = 0;
size_t PthreadRWMutex::timingLatency = 0;
size_t PthreadRWMutex::orderCheckingLatency = 0;
size_t PthreadRWMutex::lockUnlockDuration = 0;
int PthreadRWMutex::sSamplingModulo = (int)(0.01 * RAND_MAX);
bool PthreadRWMutex::staticInitialized = false;
bool PthreadRWMutex::sEnableGlobalTiming = false;
bool PthreadRWMutex::sEnableGlobalDeadlockCheck = false;
bool PthreadRWMutex::sEnableGlobalOrderCheck = false;
PthreadRWMutex::rules_t* PthreadRWMutex::rules_static = NULL;
std::map<unsigned char, std::string>* PthreadRWMutex::ruleIndex2Name_static =
  NULL;
std::map<std::string, unsigned char>* PthreadRWMutex::ruleName2Index_static =
  NULL;
thread_local bool* PthreadRWMutex::orderCheckReset_staticthread = NULL;
thread_local unsigned long
PthreadRWMutex::ordermask_staticthread[EOS_RWMUTEX_ORDER_NRULES];
std::map<pthread_t, bool>* PthreadRWMutex::threadOrderCheckResetFlags_static =
  NULL;
pthread_rwlock_t PthreadRWMutex::orderChkMgmLock;

#define EOS_RWMUTEX_CHECKORDER_LOCK if(sEnableGlobalOrderCheck) CheckAndLockOrder();
#define EOS_RWMUTEX_CHECKORDER_UNLOCK if(sEnableGlobalOrderCheck) CheckAndUnlockOrder();

#define EOS_RWMUTEX_TIMER_START                                         \
  bool issampled = false; size_t tstamp = 0;                            \
  if(mEnableTiming || sEnableGlobalTiming) {                            \
    issampled=mEnableSampling ? (!((++mCounter)%mSamplingModulo)) : true; \
    if( issampled ) tstamp = Timing::GetNowInNs();                      \
  }

// what = mRd or mWr
#define EOS_RWMUTEX_TIMER_STOP_AND_UPDATE(what)                         \
  AtomicInc(what##LockCounter);                                         \
  if(issampled) {                                                       \
    tstamp = Timing::GetNowInNs() - tstamp;                             \
    if(mEnableTiming) {                                                 \
      AtomicInc(what##LockCounterSample);                               \
      AtomicAdd(what##CumulatedWait, tstamp);                           \
      bool needloop=true;                                               \
      do {size_t mymax = AtomicGet(what##MaxWait);                      \
        if (tstamp > mymax)                                             \
          needloop = !AtomicCAS(what##MaxWait, mymax, tstamp);          \
        else needloop = false; }                                        \
      while(needloop);                                                  \
      do {size_t mymin = AtomicGet(what##MinWait);                      \
        if (tstamp < mymin)                                             \
          needloop = !AtomicCAS(what##MinWait, mymin, tstamp);          \
        else needloop = false; }                                        \
      while(needloop);                                                  \
    }                                                                   \
    if(sEnableGlobalTiming) {                                           \
      AtomicInc(what##LockCounterSample_static);                        \
      AtomicAdd(what##CumulatedWait_static,tstamp);                     \
      bool needloop = true;                                             \
      do {size_t mymax = AtomicGet(what##MaxWait_static);               \
        if (tstamp > mymax)                                             \
          needloop = !AtomicCAS(what##MaxWait_static, mymax, tstamp);   \
        else needloop = false; }                                        \
      while(needloop);                                                  \
      do {size_t mymin = AtomicGet(what##MinWait_static);               \
        if (tstamp < mymin)                                             \
          needloop = !AtomicCAS(what##MinWait_static, mymin, tstamp);   \
        else needloop = false; }                                        \
      while(needloop);                                                  \
    }                                                                   \
  }
#else
#define EOS_RWMUTEX_CHECKORDER_LOCK
#define EOS_RWMUTEX_CHECKORDER_UNLOCK
#define EOS_RWMUTEX_TIMER_START
#define EOS_RWMUTEX_TIMER_STOP_AND_UPDATE(what) AtomicInc(what##LockCounter);
#endif

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
PthreadRWMutex::PthreadRWMutex(bool prefer_rd):
  mBlocking(false), mRdLockCounter(0), mWrLockCounter(0), mPreferRd(prefer_rd)
{
  int retc = 0;
  // Try to get write lock in 5 seconds, then release quickly and retry
  wlocktime.tv_sec = 5;
  wlocktime.tv_nsec = 0;
#ifdef EOS_INSTRUMENTED_RWMUTEX
  mSamplingModulo = 300;

  if (!staticInitialized) {
    staticInitialized = true;
    InitializeClass();
  }

  mCounter = 0;
  mEnableTiming = false;
  mEnableSampling = false;
  mEnableDeadlockCheck = false;
  mTransientDeadlockCheck = false;
  nrules = 0;
  mCollectionMutex = PTHREAD_MUTEX_INITIALIZER;
  ResetTimingStatistics();
#endif
  pthread_rwlockattr_init(&attr);
#ifndef __APPLE__

  if (mPreferRd) {
    // Readers go ahead of writers and are reentrant
    if ((retc = pthread_rwlockattr_setkind_np(&attr,
                PTHREAD_RWLOCK_PREFER_WRITER_NP))) {
      fprintf(stderr, "%s Failed to set readers priority: %s\n", __FUNCTION__,
              strerror(retc));
      std::terminate();
    }
  } else {
    // Readers don't go ahead of writers!
    if ((retc = pthread_rwlockattr_setkind_np(&attr,
                PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP))) {
      fprintf(stderr, "%s Failed to set writers priority: %s\n", __FUNCTION__,
              strerror(retc));
      std::terminate();
    }
  }

#endif

  if ((retc = pthread_rwlockattr_setpshared(&attr, PTHREAD_PROCESS_SHARED))) {
    fprintf(stderr, "%s Failed to set process shared mutex: %s\n",
            __FUNCTION__, strerror(retc));
    std::terminate();
  }

  if ((retc = pthread_rwlock_init(&rwlock, &attr))) {
    fprintf(stderr, "%s Failed to initialize mutex: %s\n",
            __FUNCTION__, strerror(retc));
    std::terminate();
  }
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
PthreadRWMutex::~PthreadRWMutex()
{
#ifdef EOS_INSTRUMENTED_RWMUTEX
  pthread_rwlock_rdlock(&orderChkMgmLock);
  std::map<std::string, std::vector<IRWMutex*> >* rules = NULL;

  for (auto rit = rules_static->begin(); rit != rules_static->end(); rit++) {
    // for each rule
    for (auto it = rit->second.begin(); it != rit->second.end(); it++) {
      // for each RWMutex involved in that rule
      if ((*it) == dynamic_cast<IRWMutex*>(this)) {
        if (rules == NULL) {
          rules = new std::map<std::string, std::vector<IRWMutex*> >(*rules_static);
        }

        rules->erase(rit->first); // remove the rule if it contains this
      }
    }
  }

  pthread_rwlock_unlock(&orderChkMgmLock);

  if (rules != NULL) {
    // Erase the rules
    ResetOrderRule();

    // Inserts the remaining rules
    for (auto it = rules->begin(); it != rules->end(); it++) {
      AddOrderRule(it->first, it->second);
    }

    delete rules;
  }

#endif
}

//------------------------------------------------------------------------------
// Try to read lock the mutex within the timout value
//------------------------------------------------------------------------------
bool
PthreadRWMutex::TimedRdLock(uint64_t timeout_ns)
{
  EOS_RWMUTEX_CHECKORDER_LOCK;
  EOS_RWMUTEX_TIMER_START;
#ifdef EOS_INSTRUMENTED_RWMUTEX

  if (sEnableGlobalDeadlockCheck) {
    mTransientDeadlockCheck = true;
  }

  if (mEnableDeadlockCheck || mTransientDeadlockCheck) {
    EnterCheckDeadlock(true);
  }

#endif
  int retc = 0;
  struct timespec timeout = {0};
  _clock_gettime(CLOCK_REALTIME, &timeout);

  if (timeout_ns) {
    if (timeout_ns > 1e9) {
      timeout.tv_sec += (timeout_ns / 1e9);
    }

    timeout.tv_nsec += (timeout_ns % (unsigned long long)1e9);
  }

#ifdef __APPLE__
  // Mac does not support timed mutexes
  retc = pthread_rwlock_rdlock(&rwlock);
#else
  retc = pthread_rwlock_timedrdlock(&rwlock, &timeout);
#endif
#ifdef EOS_INSTRUMENTED_RWMUTEX

  if (retc && (mEnableDeadlockCheck || mTransientDeadlockCheck)) {
    ExitCheckDeadlock(true);
  }

#endif
  EOS_RWMUTEX_TIMER_STOP_AND_UPDATE(mRd);
  return (retc == 0);
}

//----------------------------------------------------------------------------
// Get Writelock Counter
//----------------------------------------------------------------------------
uint64_t
PthreadRWMutex::GetWriteLockCounter()
{
  return AtomicGet(mWrLockCounter);
}

//----------------------------------------------------------------------------
// Get ReadLock Counter
//----------------------------------------------------------------------------
uint64_t
PthreadRWMutex::GetReadLockCounter()
{
  return AtomicGet(mRdLockCounter);
}


//------------------------------------------------------------------------------
// Lock for read
//------------------------------------------------------------------------------
void
PthreadRWMutex::LockRead()
{
  EOS_RWMUTEX_CHECKORDER_LOCK;
  EOS_RWMUTEX_TIMER_START;
#ifdef EOS_INSTRUMENTED_RWMUTEX

  if (sEnableGlobalDeadlockCheck) {
    mTransientDeadlockCheck = true;
  }

  if (mEnableDeadlockCheck || mTransientDeadlockCheck) {
    EnterCheckDeadlock(true);
  }

#endif
  int retc = 0;

  if ((retc = pthread_rwlock_rdlock(&rwlock))) {
    fprintf(stderr, "%s Failed to read-lock: %s\n", __FUNCTION__,
            strerror(retc));
    std::terminate();
  }

  EOS_RWMUTEX_TIMER_STOP_AND_UPDATE(mRd);
}


//------------------------------------------------------------------------------
// Unlock a read lock
//------------------------------------------------------------------------------
void
PthreadRWMutex::UnLockRead()
{
  EOS_RWMUTEX_CHECKORDER_UNLOCK;
#ifdef EOS_INSTRUMENTED_RWMUTEX

  if (mEnableDeadlockCheck || mTransientDeadlockCheck) {
    ExitCheckDeadlock(true);
  }

#endif
  int retc = 0;

  if ((retc = pthread_rwlock_unlock(&rwlock))) {
    fprintf(stderr, "%s Failed to read-unlock: %s\n", __FUNCTION__,
            strerror(retc));
    std::terminate();
  }

#ifdef EOS_INSTRUMENTED_RWMUTEX

  if (!sEnableGlobalDeadlockCheck) {
    mTransientDeadlockCheck = false;
  }

  if (!mEnableDeadlockCheck && !mTransientDeadlockCheck) {
    DropDeadlockCheck();
  }

#endif
}

//------------------------------------------------------------------------------
// Lock for write
//------------------------------------------------------------------------------
void
PthreadRWMutex::LockWrite()
{
  EOS_RWMUTEX_CHECKORDER_LOCK;
  EOS_RWMUTEX_TIMER_START;
#ifdef EOS_INSTRUMENTED_RWMUTEX

  if (sEnableGlobalDeadlockCheck) {
    mTransientDeadlockCheck = true;
  }

  if (mEnableDeadlockCheck || mTransientDeadlockCheck) {
    EnterCheckDeadlock(false);
  }

#endif
  int retc = 0;

  if (mBlocking) {
    // A blocking mutex is just a normal lock for write
    if ((retc = pthread_rwlock_wrlock(&rwlock))) {
      fprintf(stderr, "%s Failed to write-lock: %s\n", __FUNCTION__,
              strerror(retc));
      std::terminate();
    }
  } else {
#ifdef __APPLE__

    // Mac does not support timed mutexes
    if ((retc = pthread_rwlock_wrlock(&rwlock))) {
      fprintf(stderr, "%s Failed to write-lock: %s\n", __FUNCTION__,
              strerror(retc));
      std::terminate();
    }

#else

    // A non-blocking mutex tries for few seconds to write lock, then releases.
    // It has the side effect, that it allows dead locked readers to jump ahead
    // the lock queue.
    while (1) {
      struct timespec writetimeout = {0};
      _clock_gettime(CLOCK_REALTIME, &writetimeout);
      // Add time for timeout value
      writetimeout.tv_sec  += wlocktime.tv_sec;
      writetimeout.tv_nsec += wlocktime.tv_nsec;
      int rc = pthread_rwlock_timedwrlock(&rwlock, &writetimeout);

      if (rc) {
        if (rc != ETIMEDOUT) {
          fprintf(stderr, "=== WRITE LOCK EXCEPTION == TID=%llu OBJECT=%llx rc=%d\n",
                  (unsigned long long) XrdSysThread::ID(), (unsigned long long) this, rc);
          std::terminate();
        } else {
          // fprintf(stderr,"==== WRITE LOCK PENDING ==== TID=%llu OBJECT=%llx\n",
          // (unsigned long long)XrdSysThread::ID(), (unsigned long long)this);
          XrdSysTimer msSleep;
          msSleep.Wait(500);
        }
      } else {
        // fprintf(stderr,"=== WRITE LOCK ACQUIRED  ==== TID=%llu OBJECT=%llx\n",
        // (unsigned long long)XrdSysThread::ID(), (unsigned long long)this);
        break;
      }
    }

#endif
  }

  EOS_RWMUTEX_TIMER_STOP_AND_UPDATE(mWr);
}

//------------------------------------------------------------------------------
// Unlock a write lock
//------------------------------------------------------------------------------
void
PthreadRWMutex::UnLockWrite()
{
  EOS_RWMUTEX_CHECKORDER_UNLOCK;
#ifdef EOS_INSTRUMENTED_RWMUTEX

  if (mEnableDeadlockCheck || mTransientDeadlockCheck) {
    ExitCheckDeadlock(false);
  }

#endif
  int retc = 0;

  if ((retc = pthread_rwlock_unlock(&rwlock))) {
    fprintf(stderr, "%s Failed to write-unlock: %s\n", __FUNCTION__,
            strerror(retc));
    std::terminate();
  }

  // fprintf(stderr,"*** WRITE LOCK RELEASED  **** TID=%llu OBJECT=%llx\n",
  // (unsigned long long)XrdSysThread::ID(), (unsigned long long)this);
#ifdef EOS_INSTRUMENTED_RWMUTEX

  if (!sEnableGlobalDeadlockCheck) {
    mTransientDeadlockCheck = false;

    if (!mEnableDeadlockCheck) {
      DropDeadlockCheck();
    }
  }

#endif
}

//------------------------------------------------------------------------------
// Lock for write but give up after wlocktime
//------------------------------------------------------------------------------
bool
PthreadRWMutex::TimedWrLock(uint64_t timeout_ns)
{
  EOS_RWMUTEX_CHECKORDER_LOCK;
  int retc = 0;
#ifdef EOS_INSTRUMENTED_RWMUTEX

  if (sEnableGlobalDeadlockCheck) {
    mTransientDeadlockCheck = true;
  }

  if (mEnableDeadlockCheck || mTransientDeadlockCheck) {
    EnterCheckDeadlock(false);
  }

#endif
  struct timespec timeout = {0};
  _clock_gettime(CLOCK_REALTIME, &timeout);

  if (timeout_ns) {
    if (timeout_ns > 1e9) {
      timeout.tv_sec += (timeout_ns / 1e9);
    }

    timeout.tv_nsec += (timeout_ns % (unsigned long long) 1e9);
  }

#ifdef __APPLE__
  retc =  pthread_rwlock_wrlock(&rwlock);
#else
  retc = pthread_rwlock_timedwrlock(&rwlock, &timeout);
#endif
#ifdef EOS_INSTRUMENTED_RWMUTEX

  if (retc && (mEnableDeadlockCheck || mTransientDeadlockCheck)) {
    ExitCheckDeadlock(false);
  }

#endif
  return (retc == 0);
}

#ifdef EOS_INSTRUMENTED_RWMUTEX

//------------------------------------------------------------------------------
// Performs the initialization of the class
//------------------------------------------------------------------------------
void
PthreadRWMutex::InitializeClass()
{
  int retc = 0;

  if ((retc = pthread_rwlock_init(&orderChkMgmLock, NULL))) {
    fprintf(stderr, "%s Failed to initialize order check lock: %s\n",
            __FUNCTION__, strerror(retc));
    std::terminate();
  }

  rules_static = new PthreadRWMutex::rules_t();
  PthreadRWMutex::ruleIndex2Name_static = new
  std::map<unsigned char, std::string>;
  PthreadRWMutex::ruleName2Index_static = new
  std::map<std::string, unsigned char>;
  PthreadRWMutex::threadOrderCheckResetFlags_static = new
  std::map<pthread_t, bool>;
}

//------------------------------------------------------------------------------
// Reset statistics at the instance level
//------------------------------------------------------------------------------
void
PthreadRWMutex::ResetTimingStatistics()
{
  // Might need a mutex or at least a flag!!!
  mRdCumulatedWait = mWrCumulatedWait = 0;
  mRdMaxWait = mWrMaxWait = std::numeric_limits<size_t>::min();
  mRdMinWait = mWrMinWait = std::numeric_limits<long long>::max();
  mRdLockCounterSample = mWrLockCounterSample = 0;
}

//-----------------------------------------------------------------------------
// Reset statistics at the class level
//------------------------------------------------------------------------------
void
PthreadRWMutex::ResetTimingStatisticsGlobal()
{
  // Might need a mutex or at least a flag!!!
  mRdCumulatedWait_static = mWrCumulatedWait_static = 0;
  mRdMaxWait_static = mWrMaxWait_static = std::numeric_limits<size_t>::min();
  mRdMinWait_static = mWrMinWait_static = std::numeric_limits<long long>::max();
  mRdLockCounterSample_static = mWrLockCounterSample_static = 0;
}

#ifdef __APPLE__
int
PthreadRWMutex::round(double number)
{
  return (number < 0.0 ? ceil(number - 0.5) : floor(number + 0.5));
}
#endif

//------------------------------------------------------------------------------
// Check for deadlocks
//------------------------------------------------------------------------------
void
PthreadRWMutex::EnterCheckDeadlock(bool rd_lock)
{
  std::thread::id tid = std::this_thread::get_id();
  pthread_mutex_lock(&mCollectionMutex);

  if (rd_lock) {
    auto it = mThreadsRdLock.find(tid);

    if (it != mThreadsRdLock.end()) {
      ++it->second;

      // For non-preferred rd lock - since is a re-entrant read lock, if there
      // is any write lock pending then this will deadlock
      if (!mPreferRd && mThreadsWrLock.size()) {
        using namespace backward;
        StackTrace st;
        st.load_here(32);
        Printer p;
        p.object = true;
        p.address = true;
        p.print(st, std::cerr);
        pthread_mutex_unlock(&mCollectionMutex);
        throw std::runtime_error("double read lock during write lock");
      }
    } else {
      mThreadsRdLock.insert(std::make_pair(tid, 1));
    }
  } else {
    if (mThreadsWrLock.find(tid) != mThreadsWrLock.end()) {
      // This is a case of double write lock
      using namespace backward;
      StackTrace st;
      st.load_here(32);
      Printer p;
      p.object = true;
      p.address = true;
      p.print(st, std::cerr);
      pthread_mutex_unlock(&mCollectionMutex);
      throw std::runtime_error("double write lock");
    }

    mThreadsWrLock.insert(tid);
  }

  pthread_mutex_unlock(&mCollectionMutex);
}

//------------------------------------------------------------------------------
// Helper function to check for deadlocks
//------------------------------------------------------------------------------
void
PthreadRWMutex::ExitCheckDeadlock(bool rd_lock)
{
  std::thread::id tid = std::this_thread::get_id();
  pthread_mutex_lock(&mCollectionMutex);

  if (rd_lock) {
    auto it = mThreadsRdLock.find(tid);

    if (it == mThreadsRdLock.end()) {
      fprintf(stderr, "%s Extra read unlock\n", __FUNCTION__);
      pthread_mutex_unlock(&mCollectionMutex);
      throw std::runtime_error("extra read unlock");
    }

    if (--it->second == 0) {
      mThreadsRdLock.erase(it);
    }
  } else {
    auto it = mThreadsWrLock.find(tid);

    if (it == mThreadsWrLock.end()) {
      fprintf(stderr, "%s Extra write unlock\n", __FUNCTION__);
      pthread_mutex_unlock(&mCollectionMutex);
      throw std::runtime_error("extra write unlock");
    }

    mThreadsWrLock.erase(it);
  }

  pthread_mutex_unlock(&mCollectionMutex);
}

//------------------------------------------------------------------------------
// Clear the data structures used for detecting deadlocks
//------------------------------------------------------------------------------
void
PthreadRWMutex::DropDeadlockCheck()
{
  pthread_mutex_lock(&mCollectionMutex);
  mThreadsRdLock.clear();
  mThreadsWrLock.clear();
  pthread_mutex_unlock(&mCollectionMutex);
}

//------------------------------------------------------------------------------
// Enable sampling of timings
//------------------------------------------------------------------------------
void
PthreadRWMutex::SetSampling(bool on, float rate)
{
  mEnableSampling = on;
  ResetTimingStatistics();

  if (rate < 0) {
    mSamplingModulo = sSamplingModulo;
  } else
#ifdef __APPLE__
    mSamplingModulo = std::min(RAND_MAX, std::max(0, (int) round(1.0 / rate)));

#else
    mSamplingModulo = std::min(RAND_MAX, std::max(0, (int) std::round(1.0 / rate)));
#endif
}

//------------------------------------------------------------------------------
//   Return the timing sampling rate/status
//------------------------------------------------------------------------------
float
PthreadRWMutex::GetSampling()
{
  if (!mEnableSampling) {
    return -1.0;
  } else {
    return 1.0 / mSamplingModulo;
  }
}

//------------------------------------------------------------------------------
// Compute the SamplingRate corresponding to a given CPU overhead
//------------------------------------------------------------------------------
float
PthreadRWMutex::GetSamplingRateFromCPUOverhead(const double& overhead)
{
  PthreadRWMutex mutex;
  bool entimglobbak = PthreadRWMutex::GetTimingGlobal();
  mutex.SetTiming(true);
  mutex.SetSampling(true, 1.0);
  PthreadRWMutex::SetTimingGlobal(true);
  size_t monitoredTiming = Timing::GetNowInNs();

  for (int k = 0; k < 1e6; k++) {
    mutex.LockWrite();
    mutex.UnLockWrite();
  }

  monitoredTiming = Timing::GetNowInNs() - monitoredTiming;
  mutex.SetTiming(false);
  mutex.SetSampling(false);
  PthreadRWMutex::SetTimingGlobal(false);
  size_t unmonitoredTiming = Timing::GetNowInNs();

  for (int k = 0; k < 1e6; k++) {
    mutex.LockWrite();
    mutex.UnLockWrite();
  }

  unmonitoredTiming = Timing::GetNowInNs() - unmonitoredTiming;
  PthreadRWMutex::SetTimingGlobal(entimglobbak);
  float mutexShare = unmonitoredTiming;
  float timingShare = monitoredTiming - unmonitoredTiming;
  float samplingRate = std::min(1.0, std::max(0.0,
                                overhead * mutexShare / timingShare));
  PthreadRWMutex::sSamplingModulo = (int)(1.0 / samplingRate);
  return samplingRate;
}

//------------------------------------------------------------------------------
// Reset order checking rules
//------------------------------------------------------------------------------
void
PthreadRWMutex::ResetOrderRule()
{
  bool sav = sEnableGlobalOrderCheck;
  sEnableGlobalOrderCheck = false;
  // Leave some time to all the threads to finish their book keeping activity
  // regarding order checking
  usleep(100000);
  pthread_rwlock_wrlock(&orderChkMgmLock);
  // Remove all the dead threads from the map
  // ~~~~~~~~~~~~~~~~~~~~~~~~~~ NOTICE ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  // !!! THIS DOESN'T WORK SO DEAD THREADS ARE NOT REMOVED FROM THE MAP
  // !!! THIS IS BECAUSE THERE IS NO RELIABLE WAY TO CHECK IF A THREAD IS STILL
  // !!! RUNNING. THIS IS NOT A PROBLEM FOR EOS BECAUSE XROOTD REUSES ITS THREADS
  // !!! SO THE MAP DOESN'T GO INTO AN INFINITE GROWTH.
  // ~~~~~~~~~~~~~~~~~~~~~~~~~~ NOTICE ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#if 0

  for (auto it = threadOrderCheckResetFlags_static.begin();
       it != threadOrderCheckResetFlags_static.end(); ++it) {
    if (XrdSysThread::Signal(it->first, 0)) {
      // this line crashes when the threads is no more valid.
      threadOrderCheckResetFlags_static.erase(it);
      it = threadOrderCheckResetFlags_static.begin();
    }
  }

#endif

  // Tell the threads to reset the states of the order mask (because it's thread-local)
  for (auto it = threadOrderCheckResetFlags_static->begin();
       it != threadOrderCheckResetFlags_static->end(); ++it) {
    it->second = true;
  }

  // Tell all the RWMutex that they are not involved in any order checking anymore
  for (auto rit = rules_static->begin(); rit != rules_static->end(); ++rit) {
    for (auto it = rit->second.begin(); it != rit->second.end(); ++it) {
      // For each RWMutex involved in that rule
      static_cast<PthreadRWMutex*>(*it)->nrules = 0; // no rule involved
    }
  }

  // Clear the manager side.
  ruleName2Index_static->clear();
  ruleIndex2Name_static->clear();
  rules_static->clear();
  pthread_rwlock_unlock(&orderChkMgmLock);
  sEnableGlobalOrderCheck = sav;
}

//------------------------------------------------------------------------------
// Remove an order checking rule
//------------------------------------------------------------------------------
int
PthreadRWMutex::RemoveOrderRule(const std::string& rulename)
{
  // Make a local copy of the rules and remove the required rule
  std::map<std::string, std::vector<IRWMutex*> > rules = (*rules_static);

  if (!rules.erase(rulename)) {
    return 0;
  }

  // Reset the rules
  ResetOrderRule();

  // Add all the rules but the removed one
  for (auto it = rules.begin(); it != rules.end(); ++it) {
    AddOrderRule(it->first, it->second);
  }

  return 1;
}

//------------------------------------------------------------------------------
// Compute the cost in time of taking timings so that it can be compensated in
// the statistics
//------------------------------------------------------------------------------
size_t
PthreadRWMutex::EstimateTimingCompensation(size_t loopsize)
{
  size_t t = Timing::GetNowInNs();

  for (unsigned long k = 0; k < loopsize; k++) {
    struct timespec ts;
    eos::common::Timing::GetTimeSpec(ts);
  }

  t = Timing::GetNowInNs() - t;
  return size_t(double(t) / loopsize);
}

//------------------------------------------------------------------------------
// Compute the speed for lock/unlock cycle
//------------------------------------------------------------------------------
size_t
PthreadRWMutex::EstimateLockUnlockDuration(size_t loopsize)
{
  PthreadRWMutex mutex;
  bool sav = PthreadRWMutex::GetTimingGlobal();
  bool sav2 = PthreadRWMutex::GetOrderCheckingGlobal();
  PthreadRWMutex::SetTimingGlobal(false);
  PthreadRWMutex::SetOrderCheckingGlobal(false);
  mutex.SetTiming(false);
  mutex.SetSampling(false);
  size_t t = Timing::GetNowInNs();

  for (size_t k = 0; k < loopsize; k++) {
    mutex.LockWrite();
    mutex.UnLockWrite();
  }

  t = Timing::GetNowInNs() - t;
  PthreadRWMutex::SetTimingGlobal(sav);
  PthreadRWMutex::SetOrderCheckingGlobal(sav2);
  return size_t(double(t) / loopsize);
}

//------------------------------------------------------------------------------
// Compute the latency introduced by taking timings
//------------------------------------------------------------------------------
size_t
PthreadRWMutex::EstimateTimingAddedLatency(size_t loopsize, bool globaltiming)
{
  PthreadRWMutex mutex;
  bool sav = PthreadRWMutex::GetTimingGlobal();
  bool sav2 = PthreadRWMutex::GetOrderCheckingGlobal();
  PthreadRWMutex::SetTimingGlobal(globaltiming);
  PthreadRWMutex::SetOrderCheckingGlobal(false);
  mutex.SetTiming(true);
  mutex.SetSampling(true, 1.0);
  size_t t = Timing::GetNowInNs();

  for (size_t k = 0; k < loopsize; k++) {
    mutex.LockWrite();
    mutex.UnLockWrite();
  }

  size_t s = Timing::GetNowInNs() - t;
  PthreadRWMutex::SetTimingGlobal(false);
  mutex.SetTiming(false);
  mutex.SetSampling(false);
  t = Timing::GetNowInNs();

  for (size_t k = 0; k < loopsize; k++) {
    mutex.LockWrite();
    mutex.UnLockWrite();
  }

  t = Timing::GetNowInNs() - t;
  PthreadRWMutex::SetTimingGlobal(sav);
  PthreadRWMutex::SetOrderCheckingGlobal(sav2);
  return size_t(double(s - t) / loopsize);
}

//------------------------------------------------------------------------------
// Compute the latency introduced by checking the mutexes locking orders
//------------------------------------------------------------------------------
size_t
PthreadRWMutex::EstimateOrderCheckingAddedLatency(size_t nmutexes,
    size_t loopsize)
{
  std::vector<PthreadRWMutex> mutexes;
  mutexes.resize(nmutexes);
  std::vector<IRWMutex*> order;
  order.resize(nmutexes);
  int count = 0;

  for (auto it = mutexes.begin(); it != mutexes.end(); ++it) {
    it->SetTiming(false);
    it->SetSampling(false);
    order[count++] = &(*it);
  }

  PthreadRWMutex::AddOrderRule("estimaterule", order);
  bool sav = PthreadRWMutex::GetTimingGlobal();
  bool sav2 = PthreadRWMutex::GetOrderCheckingGlobal();
  PthreadRWMutex::SetTimingGlobal(false);
  PthreadRWMutex::SetOrderCheckingGlobal(true);
  size_t t = Timing::GetNowInNs();

  for (size_t k = 0; k < loopsize; k++) {
    for (auto it = mutexes.begin(); it != mutexes.end(); ++it) {
      it->LockWrite();
    }

    for (auto rit = mutexes.rbegin(); rit != mutexes.rend(); ++rit) {
      rit->UnLockWrite();
    }
  }

  size_t s = Timing::GetNowInNs() - t;
  PthreadRWMutex::SetOrderCheckingGlobal(false);
  t = Timing::GetNowInNs();

  for (size_t k = 0; k < loopsize; ++k) {
    for (auto it = mutexes.begin(); it != mutexes.end(); ++it) {
      it->LockWrite();
    }

    for (auto rit = mutexes.rbegin(); rit != mutexes.rend(); ++rit) {
      rit->UnLockWrite();
    }
  }

  t = Timing::GetNowInNs() - t;
  PthreadRWMutex::SetTimingGlobal(sav);
  PthreadRWMutex::SetOrderCheckingGlobal(sav2);
  RemoveOrderRule("estimaterule");
  return size_t(double(s - t) / (loopsize * nmutexes));
}

//-------------------------------------------------------------------------------
// Estimate latencies and compensation
//------------------------------------------------------------------------------
void
PthreadRWMutex::EstimateLatenciesAndCompensation(size_t loopsize)
{
  timingCompensation = EstimateTimingCompensation(loopsize);
  timingLatency = EstimateTimingAddedLatency(loopsize);
  orderCheckingLatency = EstimateOrderCheckingAddedLatency(3, loopsize);
  lockUnlockDuration = EstimateLockUnlockDuration(loopsize);
  //std::cerr<< " timing compensation = "<<timingCompensation<<std::endl;
  //std::cerr<< " timing latency = "<<timingLatency<<std::endl;
  //std::cerr<< " order  latency = "<<orderCheckingLatency<<std::endl;
  //std::cerr<< " lock/unlock duration = "<<lockUnlockDuration<<std::endl;
}

//------------------------------------------------------------------------------
// Get the timing statistics at the instance level
//------------------------------------------------------------------------------
void
PthreadRWMutex::GetTimingStatistics(TimingStats& stats, bool compensate)
{
  size_t compensation = (compensate ? timingCompensation : 0);
  stats.readLockCounterSample = AtomicGet(mRdLockCounterSample);
  stats.writeLockCounterSample = AtomicGet(mWrLockCounterSample);
  stats.averagewaitread = 0;

  if (AtomicGet(mRdLockCounterSample) != 0) {
    double avg = (double(AtomicGet(mRdCumulatedWait)) /
                  AtomicGet(mRdLockCounterSample) - compensation);

    if (avg > 0) {
      stats.averagewaitread = avg;
    }
  }

  stats.averagewaitwrite = 0;

  if (AtomicGet(mWrLockCounterSample) != 0) {
    double avg = (double(AtomicGet(mWrCumulatedWait)) / AtomicGet(
                    mWrLockCounterSample) - compensation);

    if (avg > 0) {
      stats.averagewaitwrite = avg;
    }
  }

  if (AtomicGet(mRdMinWait) != std::numeric_limits<size_t>::max()) {
    long long compensated = AtomicGet(mRdMinWait) - compensation;

    if (compensated > 0) {
      stats.minwaitread = compensated;
    } else {
      stats.minwaitread = 0;
    }
  } else {
    stats.minwaitread = std::numeric_limits<long long>::max();
  }

  if (AtomicGet(mRdMaxWait) != std::numeric_limits<size_t>::min()) {
    long long compensated = AtomicGet(mRdMaxWait) - compensation;

    if (compensated > 0) {
      stats.maxwaitread = compensated;
    } else {
      stats.maxwaitread = 0;
    }
  } else {
    stats.maxwaitread = std::numeric_limits<size_t>::min();
  }

  if (AtomicGet(mWrMinWait) != std::numeric_limits<size_t>::max()) {
    long long compensated = AtomicGet(mWrMinWait) - compensation;

    if (compensated > 0) {
      stats.minwaitwrite = compensated;
    } else {
      stats.minwaitwrite = 0;
    }
  } else {
    stats.minwaitwrite = std::numeric_limits<long long>::max();
  }

  if (AtomicGet(mWrMaxWait) != std::numeric_limits<size_t>::min()) {
    long long compensated = AtomicGet(mWrMaxWait) - compensation;

    if (compensated > 0) {
      stats.maxwaitwrite = compensated;
    } else {
      stats.maxwaitwrite = 0;
    }
  } else {
    stats.maxwaitwrite = std::numeric_limits<size_t>::min();
  }
}

//------------------------------------------------------------------------------
// Check the order defined by the rules and update
//------------------------------------------------------------------------------
void
PthreadRWMutex::OrderViolationMessage(unsigned char rule,
                                      const std::string& message)
{
  void* array[10];
  unsigned long threadid = XrdSysThread::Num();
  // Get void*'s for all entries on the stack
  size_t size = backtrace(array, 10);
  const std::string& rulename =
    (*ruleIndex2Name_static)[ruleLocalIndexToGlobalIndex[rule]];
  fprintf(stderr, "RWMutex: Order Checking Error in thread %lu\n %s\n in rule "
          "%s :\nLocking Order should be:\n", threadid, message.c_str(),
          rulename.c_str());
  std::vector<IRWMutex*> order = (*rules_static)[rulename];

  for (auto ito = order.begin(); ito != order.end(); ++ito) {
    fprintf(stderr, "\t%12s (%p)",
            static_cast<PthreadRWMutex*>(*ito)->mDebugName.c_str(), (*ito));
  }

  fprintf(stderr, "\nThe lock states of these mutexes are (before the violating"
          " lock/unlock) :\n");

  for (unsigned char k = 0; k < order.size(); k++) {
    unsigned long int mask = (1 << k);
    fprintf(stderr, "\t%d", int((ordermask_staticthread[rule] & mask) != 0));
  }

  fprintf(stderr, "\n");
  backtrace_symbols_fd(array, size, 2);
}

//------------------------------------------------------------------------------
// Check the orders defined by the rules and update for a lock
//------------------------------------------------------------------------------
void
PthreadRWMutex::CheckAndLockOrder()
{
  // Initialize the thread local ordermask if not already done
  if (orderCheckReset_staticthread == NULL) {
    ResetCheckOrder();
  }

  if (*orderCheckReset_staticthread) {
    ResetCheckOrder();
    *orderCheckReset_staticthread = false;
  }

  for (unsigned char k = 0; k < nrules; k++) {
    unsigned long int mask = (1 << rankinrule[k]);

    // Check if following mutex is already locked in the same thread
    if (ordermask_staticthread[k] >= mask) {
      char strmess[1024];
      sprintf(strmess, "locking %s at address %p", mDebugName.c_str(), this);
      OrderViolationMessage(k, strmess);
    }

    ordermask_staticthread[k] |= mask;
  }
}

//------------------------------------------------------------------------------
// Check the orders defined by the rules and update for an unlock
//------------------------------------------------------------------------------
void
PthreadRWMutex::CheckAndUnlockOrder()
{
  // Initialize the thread local ordermask if not already done
  if (orderCheckReset_staticthread == NULL) {
    ResetCheckOrder();
  }

  if (*orderCheckReset_staticthread) {
    ResetCheckOrder();
    *orderCheckReset_staticthread = false;
  }

  for (unsigned char k = 0; k < nrules; k++) {
    unsigned long int mask = (1 << rankinrule[k]);

    // check if following mutex is already locked in the same thread
    if (ordermask_staticthread[k] >= (mask << 1)) {
      char strmess[1024];
      sprintf(strmess, "unlocking %s at address %p", mDebugName.c_str(), this);
      OrderViolationMessage(k, strmess);
    }

    ordermask_staticthread[k] &= (~mask);
  }
}

//------------------------------------------------------------------------------
// Get the timing statistics at the class level
//------------------------------------------------------------------------------
void
PthreadRWMutex::GetTimingStatisticsGlobal(TimingStats& stats, bool compensate)
{
  size_t compensation = compensate ? timingCompensation : 0;
  stats.readLockCounterSample = AtomicGet(mRdLockCounterSample_static);
  stats.writeLockCounterSample = AtomicGet(mWrLockCounterSample_static);
  stats.averagewaitread = 0;

  if (AtomicGet(mRdLockCounterSample_static) != 0) {
    double avg = (double(AtomicGet(mRdCumulatedWait_static)) / AtomicGet(
                    mRdLockCounterSample_static) - compensation);

    if (avg > 0) {
      stats.averagewaitread = avg;
    }
  }

  stats.averagewaitwrite = 0;

  if (AtomicGet(mWrLockCounterSample_static) != 0) {
    double avg = (double(AtomicGet(mWrCumulatedWait_static)) / AtomicGet(
                    mWrLockCounterSample_static) - compensation);

    if (avg > 0) {
      stats.averagewaitwrite = avg;
    }
  }

  if (AtomicGet(mRdMinWait_static) != std::numeric_limits<size_t>::max()) {
    long long compensated = AtomicGet(mRdMinWait_static) - compensation;

    if (compensated > 0) {
      stats.minwaitread = compensated;
    } else {
      stats.minwaitread = 0;
    }
  } else {
    stats.minwaitread = std::numeric_limits<long long>::max();
  }

  if (AtomicGet(mWrMaxWait_static) != std::numeric_limits<size_t>::min()) {
    long long compensated = AtomicGet(mWrMaxWait_static) - compensation;

    if (compensated > 0) {
      stats.maxwaitread = compensated;
    } else {
      stats.maxwaitread = 0;
    }
  } else {
    stats.maxwaitread = std::numeric_limits<size_t>::min();
  }

  if (AtomicGet(mWrMinWait_static) != std::numeric_limits<size_t>::max()) {
    long long compensated = AtomicGet(mWrMinWait_static) - compensation;

    if (compensated > 0) {
      stats.minwaitwrite = compensated;
    } else {
      stats.minwaitwrite = 0;
    }
  } else {
    stats.minwaitwrite = std::numeric_limits<long long>::max();
  }

  if (AtomicGet(mWrMaxWait_static) != std::numeric_limits<size_t>::min()) {
    long long compensated = AtomicGet(mWrMaxWait_static) - compensation;

    if (compensated > 0) {
      stats.maxwaitwrite = compensated;
    } else {
      stats.maxwaitwrite = 0;
    }
  } else {
    stats.maxwaitwrite = std::numeric_limits<size_t>::min();
  }
}

//------------------------------------------------------------------------------
// Add or overwrite an order checking rule
//------------------------------------------------------------------------------
int
PthreadRWMutex::AddOrderRule(const std::string& rulename,
                             const std::vector<IRWMutex*>& order)
{
  bool sav = sEnableGlobalOrderCheck;
  sEnableGlobalOrderCheck = false;
  // Leave time to all the threads to finish their book-keeping activity
  // regarding order checking
  usleep(100000);
  pthread_rwlock_wrlock(&orderChkMgmLock);

  // If we reached the max number of rules, ignore
  if (rules_static->size() == EOS_RWMUTEX_ORDER_NRULES || order.size() > 63) {
    sEnableGlobalOrderCheck = sav;
    pthread_rwlock_unlock(&orderChkMgmLock);
    return -1;
  }

  (*rules_static)[rulename] = order;
  int ruleIdx = rules_static->size() - 1;
  // update the maps
  (*ruleName2Index_static)[rulename] = ruleIdx;
  (*ruleIndex2Name_static)[ruleIdx] = rulename;
  // update each object
  unsigned char count = 0;

  for (auto it = order.begin(); it != order.end(); it++) {
    // ruleIdx begin at 0
    // Each RWMutex has its own number of rules, they are all <= than
    // EOS_RWMUTEX_ORDER_NRULES
    static_cast<PthreadRWMutex*>
    (*it)->rankinrule[static_cast<PthreadRWMutex*>
                      (*it)->nrules] = count;
    static_cast<PthreadRWMutex*>
    (*it)->ruleLocalIndexToGlobalIndex[static_cast<PthreadRWMutex*>
                                       (*it)->nrules++] = ruleIdx;
    count++;
  }

  pthread_rwlock_unlock(&orderChkMgmLock);
  sEnableGlobalOrderCheck = sav;
  return 0;
}

//------------------------------------------------------------------------------
// Reset the order checking mechanism for the current thread
//------------------------------------------------------------------------------
void
PthreadRWMutex::ResetCheckOrder()
{
  // Reset the order mask
  for (int k = 0; k < EOS_RWMUTEX_ORDER_NRULES; k++) {
    ordermask_staticthread[k] = 0;
  }

  // Update orderCheckReset_staticthread, this memory should be specific to
  // this thread
  pthread_t tid = XrdSysThread::ID();
  pthread_rwlock_rdlock(&orderChkMgmLock);

  if (threadOrderCheckResetFlags_static->find(tid) ==
      threadOrderCheckResetFlags_static->end()) {
    pthread_rwlock_unlock(&orderChkMgmLock);
    pthread_rwlock_wrlock(&orderChkMgmLock);
    (*threadOrderCheckResetFlags_static)[tid] = false;
  }

  orderCheckReset_staticthread = &(*threadOrderCheckResetFlags_static)[tid];
  pthread_rwlock_unlock(&orderChkMgmLock);
}

#endif

EOSCOMMONNAMESPACE_END
