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
//! class is approximately as fast as the native class.
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
#include "common/IRWMutex.hh"
#include "common/concurrency/AlignedArray.hh"
#include <XrdSys/XrdSysPthread.hh>
#include <stdio.h>
#include <stdint.h>
#include <atomic>
#include <array>
#include <chrono>
#include <map>
#include <string>
#include <ostream>
#include <fstream>
#include <iostream>
#include <cmath>
#include <vector>
#include <execinfo.h>
#include <limits>
#include <set>
#include <thread>
#include <mutex>

#define _MULTI_THREADED
#include <pthread.h>

#define EOS_RWMUTEX_ORDER_NRULES 4

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! struct TimingArray aligned to a cacheline
//------------------------------------------------------------------------------
struct TimingArray {
  AlignedAtomicArray<int64_t, 4> items;
  TimingArray() : items{} {}
};
//------------------------------------------------------------------------------
//! Class RWMutex
//------------------------------------------------------------------------------
class RWMutex
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  // ---------------------------------------------------------------------------
  RWMutex(bool prefer_rd = false);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~RWMutex();

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
  inline void SetBlocking(bool block)
  {
    mBlocking = block;
  }

  //----------------------------------------------------------------------------
  //! Set the time interval when to stacktrace a long lasting lock
  //!
  //! @param blockedfor time in ms
  //----------------------------------------------------------------------------
  inline void SetBlockedForMsInterval(int64_t blockedfor)
  {
    mBlockedForInterval = blockedfor;
  }

  int64_t BlockedForMsInterval() const
  {
    return mBlockedForInterval;
  }

  //----------------------------------------------------------------------------
  //! En-/Disable stack tracing of locks lasting longer then the interval
  //!
  //! @param onoff true=enable false=disable
  inline void SetBlockedStackTracing(bool onoff)
  {
    mBlockedStackTracing = onoff;
  }

  bool BlockedStackTracing() const
  {
    return mBlockedStackTracing;
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
  //! @param timeout_ns nanoseconds timeout
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
  //! @param timeout_ns nanoseconds timeout
  //!
  //! @return true if lock acquired successfully, otherwise false
  //----------------------------------------------------------------------------
  bool TimedWrLock(uint64_t timeout_ns);

  //----------------------------------------------------------------------------
  //! Get Readlock Counter
  //----------------------------------------------------------------------------
  inline uint64_t GetReadLockCounter()
  {
    return mRdLockCounter.load();
  }

  //----------------------------------------------------------------------------
  //! Get Writelock Counter
  //----------------------------------------------------------------------------
  inline uint64_t GetWriteLockCounter()
  {
    return mWrLockCounter.load();
  }

  //----------------------------------------------------------------------------
  //! Get Readlock Time
  //----------------------------------------------------------------------------
  inline uint64_t GetReadLockTime()
  {
    uint64_t rlt = mRdLockTime.load();
    mRdLockTime = 0;
    return rlt;
  }

  //----------------------------------------------------------------------------
  //! Get Writelock Time
  //----------------------------------------------------------------------------
  inline uint64_t GetWriteLockTime()
  {
    uint64_t wlt = mWrLockTime.load();
    mWrLockTime = 0;
    return wlt;
  }

  //----------------------------------------------------------------------------
  //! Get Readlock Time
  //----------------------------------------------------------------------------
  inline uint64_t GetReadLockLeadTime()
  {
    uint64_t rlt = mRdLockLeadTime.load();
    mRdLockLeadTime = 0;
    return rlt;
  }

  //----------------------------------------------------------------------------
  //! Get Writelock Time
  //----------------------------------------------------------------------------
  inline uint64_t GetWriteLockLeadTime()
  {
    uint64_t wlt = mWrLockLeadTime.load();
    mWrLockLeadTime = 0;
    return wlt;
  }

  //----------------------------------------------------------------------------
  //! Add Readlock Time
  //----------------------------------------------------------------------------
  inline void AddReadLockTime(uint64_t t)
  {
    mRdLockTime += t;
  }

  //----------------------------------------------------------------------------
  //! Add Writelock Time
  //----------------------------------------------------------------------------
  inline void AddWriteLockTime(uint64_t t)
  {
    mWrLockTime += t;
  }

  enum class LOCK_T { eNone, eWantLockRead, eWantUnLockRead, eLockRead, eWantLockWrite, eWantUnLockWrite, eLockWrite };

  //----------------------------------------------------------------------------
  //! Record mutex operation type
  //!
  //! @param ptr_val pointer value of the mutex concerned
  //! @param op type of operation on the given mutex
  //----------------------------------------------------------------------------
  static void RecordMutexOp(uint64_t ptr_val, LOCK_T op);

  //----------------------------------------------------------------------------
  //! Print the status of the mutex locks for the calling thread id
  //!
  //! @param out output string
  //----------------------------------------------------------------------------
  static void PrintMutexOps(std::ostringstream& oss);

  //----------------------------------------------------------------------------
  //! Get the name
  //----------------------------------------------------------------------------
  std::string getName() const
  {
    return mName;
  }

  void addBlockingTimeInfos(std::chrono::system_clock::time_point acquiredAt,
                            std::chrono::system_clock::time_point releasedAt)
  {
    auto acquiredAtSinceEpoch = acquiredAt.time_since_epoch();
    auto releasedAtSinceEpoch = releasedAt.time_since_epoch();
    auto acquiredAtSecondsSinceEpoch =
      std::chrono::duration_cast<std::chrono::seconds>(acquiredAtSinceEpoch).count();
    auto releasedAtSecondsSinceEpoch =
      std::chrono::duration_cast<std::chrono::seconds>(releasedAtSinceEpoch).count();
    auto acquiredAtMsSinceEpoch =
      std::chrono::duration_cast<std::chrono::milliseconds>
      (acquiredAtSinceEpoch).count();
    auto releasedAtMsSinceEpoch =
      std::chrono::duration_cast<std::chrono::milliseconds>
      (releasedAtSinceEpoch).count();
    auto blockedForTimepoint = releasedAt - acquiredAt;
    auto blockedForMs = std::chrono::duration_cast<std::chrono::milliseconds>
                        (blockedForTimepoint).count();
    auto blockedForSeconds = std::chrono::duration_cast<std::chrono::seconds>
                             (blockedForTimepoint).count();

    if (blockedForSeconds >= 2) {
      //The second before the current one, the mutex was locked the entire time
      mNbMsMutexLocked.items[(releasedAtSecondsSinceEpoch - 1) % 4].fetch_add(1000);
      //We add to the current second the amount of milliseconds between the start of the current second and the releasedAt milliseconds
      mNbMsMutexLocked.items[releasedAtSecondsSinceEpoch % 4].fetch_add(
        std::chrono::milliseconds(releasedAtMsSinceEpoch -
                                  (releasedAtSecondsSinceEpoch * 1000)).count());
    } else if (blockedForSeconds >= 1) {
      //The lock time is overlapping between the previous second and the current second
      //compute lock time during last second to add it to last second
      mNbMsMutexLocked.items[(releasedAtSecondsSinceEpoch - 1) % 4].fetch_add(
        std::chrono::milliseconds((releasedAtSecondsSinceEpoch * 1000) -
                                  acquiredAtMsSinceEpoch).count());
      //Compute lock time during the current second and add it to the current second
      mNbMsMutexLocked.items[(releasedAtSecondsSinceEpoch) % 4].fetch_add(
        std::chrono::milliseconds(releasedAtMsSinceEpoch -
                                  (releasedAtSecondsSinceEpoch * 1000)).count());
    } else {
      //The lock was acquired and released within the current second, just add the amount of milliseconds
      mNbMsMutexLocked.items[(releasedAtSecondsSinceEpoch) % 4].fetch_add(
        blockedForMs);
    }

    //Reset the next second lock time
    mNbMsMutexLocked.items[(releasedAtSecondsSinceEpoch + 1) % 4].store(0);
  }

  const std::chrono::milliseconds
  getNbMsMutexWriteLockedPenultimateSecond()
  {
    auto now = std::chrono::duration_cast<std::chrono::seconds>
               (std::chrono::system_clock::now().time_since_epoch()).count();
    //We take the amount of milliseconds the mutex was locked 2 seconds before the current second
    return std::chrono::milliseconds(mNbMsMutexLocked.items[(now - 2) % 4].load(
                                       std::memory_order_relaxed));
  }

#ifdef EOS_INSTRUMENTED_RWMUTEX
  typedef std::map<uint64_t, std::string> MapMutexNameT;
  typedef std::map<std::thread::id, std::map<uint64_t, LOCK_T>> MapMutexOpT;
  static const char* LOCK_STATE[];
  static std::mutex sOpMutex;
  static MapMutexNameT sMtxNameMap;
  static MapMutexOpT sTidMtxOpMap;

  struct TimingStats {
    double averagewaitread;
    double averagewaitwrite;
    double minwaitwrite, maxwaitwrite;
    double minwaitread, maxwaitread;
    std::atomic<uint64_t> readLockCounterSample, writeLockCounterSample;
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
    sEnableGlobalTiming = on;
  }

  //----------------------------------------------------------------------------
  //! Get the timing status at the class level
  //----------------------------------------------------------------------------
  inline static bool GetTimingGlobal()
  {
    return sEnableGlobalTiming;
  }

  //----------------------------------------------------------------------------
  //! Turn on/off order checking at the class level
  //----------------------------------------------------------------------------
  inline static void SetOrderCheckingGlobal(bool on)
  {
    sEnableGlobalOrderCheck = on;
  }

  //----------------------------------------------------------------------------
  //! Get the order checking status at the class level
  //----------------------------------------------------------------------------
  inline static bool GetOrderCheckingGlobal()
  {
    return sEnableGlobalOrderCheck;
  }

  //----------------------------------------------------------------------------
  //! Turn on/off deadlock checking
  //----------------------------------------------------------------------------
  inline static void SetDeadlockCheckingGlobal(bool on)
  {
    sEnableGlobalDeadlockCheck = on;
  }

  //----------------------------------------------------------------------------
  //! Get the global deadlock check status
  //----------------------------------------------------------------------------
  inline static bool GetDeadlockCheckingGlobal()
  {
    return sEnableGlobalDeadlockCheck;
  }

  //----------------------------------------------------------------------------
  //! Get the timing statistics at the class level
  //----------------------------------------------------------------------------
  static void GetTimingStatisticsGlobal(TimingStats& stats,
                                        bool compensate = true);

  //----------------------------------------------------------------------------
  //! Compute the SamplingRate corresponding to a given CPU overhead
  //!
  //! @param overhead the ratio between the timing cost and the mutexing cost
  //!
  //! @return sampling rate (the ratio of mutex to time so that the argument
  //!         value is not violated)
  //----------------------------------------------------------------------------
  static float GetSamplingRateFromCPUOverhead(const double& overhead);

  //----------------------------------------------------------------------------
  //! Compute the cost in time of taking timings so that it can be compensated
  //! in the statistics
  //!
  //! @param loopsize size of the loop to estimate the compensation
  //!
  //! @return the compensation in nanoseconds
  //----------------------------------------------------------------------------
  static size_t EstimateTimingCompensation(size_t loopsize = 1e6);

  //----------------------------------------------------------------------------
  //! Compute the speed for lock/unlock cycle
  //!
  //! @param loopsize size of the loop to estimate the compensation
  //!
  //! @return duration of the cycle in nanoseconds
  //----------------------------------------------------------------------------
  static size_t EstimateLockUnlockDuration(size_t loopsize = 1e6);

  //----------------------------------------------------------------------------
  //! Compute the latency introduced by taking timings
  //!
  //! @param loopsize size of the loop to estimate the compensation
  //! @param globaltiming enable global timing in the estimation
  //!
  //! @return latency in nanoseconds
  //----------------------------------------------------------------------------
  static size_t EstimateTimingAddedLatency(size_t loopsize = 1e6,
      bool globaltiming = false);

  //----------------------------------------------------------------------------
  //! Compute the latency introduced by checking the mutexes locking orders
  //! @param mutexes the number of nested mutexes in the loop
  //! @param loopsize the size of the loop to estimate the compensation
  //!
  //! @return the latency in nanoseconds
  //----------------------------------------------------------------------------
  static size_t EstimateOrderCheckingAddedLatency(size_t nmutexes = 3,
      size_t loopsize = 1e6);

  //----------------------------------------------------------------------------
  //! Remove an order checking rule
  //! @param rulename name of the rule
  //!
  //! @return the number of rules removed (0 or 1)
  //----------------------------------------------------------------------------
  static int RemoveOrderRule(const std::string& rulename);

  //----------------------------------------------------------------------------
  //! Estimate latencies and compensation
  //!
  //! @param loopsize number of lock/unlock operations
  //----------------------------------------------------------------------------
  static void EstimateLatenciesAndCompensation(size_t loopsize = 1e6);

  inline static size_t GetTimingCompensation()
  {
    return timingCompensation; // in nsec
  }

  inline static size_t GetOrderCheckingLatency()
  {
    return orderCheckingLatency; // in nsec
  }

  inline static size_t GetTimingLatency()
  {
    return timingLatency; // in nsec
  }

  static size_t GetLockUnlockDuration()
  {
    return lockUnlockDuration; // in nsec
  }

  //----------------------------------------------------------------------------
  //! Add or overwrite an order checking rule
  //! @param rulename  name of the rule
  //! @param order vector containing the address of the RWMutex instances in the
  //! locking order
  //!
  //! @return 0 if successful, otherwise -1
  //----------------------------------------------------------------------------
  static int AddOrderRule(const std::string& rulename,
                          const std::vector<RWMutex*>& order);

  //----------------------------------------------------------------------------
  //! Reset order checking rules
  //----------------------------------------------------------------------------
  static void ResetOrderRule();

  //----------------------------------------------------------------------------
  //! Reset statistics at the instance level
  //----------------------------------------------------------------------------
  void ResetTimingStatistics();

  //----------------------------------------------------------------------------
  //! Turn on/off timings at the instance level
  //----------------------------------------------------------------------------
  inline void SetTiming(bool on)
  {
    mEnableTiming = on;
  }

  //----------------------------------------------------------------------------
  //! Get the timing status at the class level
  //----------------------------------------------------------------------------
  inline bool GetTiming()
  {
    return mEnableTiming;
  }

  //----------------------------------------------------------------------------
  //! Set the name
  //----------------------------------------------------------------------------
  inline void SetDebugName(const std::string& name)
  {
    mName = name;
    std::unique_lock<std::mutex> lock(sOpMutex);
    sMtxNameMap[(uint64_t) GetRawPtr()] = name;
  }

  //----------------------------------------------------------------------------
  //! Enable sampling of timings
  //! @param $first turns on or off the sampling
  //! @param $second sampling between 0 and 1 (if <0, use the precomputed level
  //! for the class, see GetSamplingRateFromCPUOverhead)
  //----------------------------------------------------------------------------
  void SetSampling(bool on, float rate = -1.0);

  //----------------------------------------------------------------------------
  //! Return the timing sampling rate/status
  // @return the sample rate if the sampling is turned on, -1.0 if the sampling is off
  //----------------------------------------------------------------------------
  float GetSampling();

  //----------------------------------------------------------------------------
  //! Get the timing statistics at the instance level
  //----------------------------------------------------------------------------
  void GetTimingStatistics(TimingStats& stats, bool compensate = true);

  //----------------------------------------------------------------------------
  //! Check the orders defined by the rules and update
  //----------------------------------------------------------------------------
  void OrderViolationMessage(unsigned char rule, const std::string& message = "");

  //----------------------------------------------------------------------------
  //! Check the orders defined by the rules and update for a lock
  //----------------------------------------------------------------------------
  void CheckAndLockOrder();

  //----------------------------------------------------------------------------
  //!  Check the orders defined by the rules and update for an unlock
  //----------------------------------------------------------------------------
  void CheckAndUnlockOrder();

  //----------------------------------------------------------------------------
  //! Reset the order checking mechanism for the current thread
  //----------------------------------------------------------------------------
  void ResetCheckOrder();

  //----------------------------------------------------------------------------
  //! Enable/disable deadlock check
  //!
  //! @param on if true then enable, otherwise disable
  //----------------------------------------------------------------------------
  inline void SetDeadlockCheck(bool status)
  {
    mEnableDeadlockCheck = status;
  }

  //----------------------------------------------------------------------------
  //! Check for deadlocks
  //!
  //! @param rd_lock true if this is about to take a read lock, otherwise false
  //! @note it throws an exception if it causes a deadlock
  //----------------------------------------------------------------------------
  void EnterCheckDeadlock(bool rd_lock);

  //----------------------------------------------------------------------------
  //! Exit check for deadlocks
  //!
  //! @param rd_lock true if this is was read lock, otherwise false
  //----------------------------------------------------------------------------
  void ExitCheckDeadlock(bool rd_lock);

  //----------------------------------------------------------------------------
  //! Clear the data structures used for detecting deadlocks
  //----------------------------------------------------------------------------
  void DropDeadlockCheck();

#ifdef __APPLE__
  static int round(double number);
#endif

#endif


protected:
  bool mBlocking;

private:
  IRWMutex* mMutexImpl;
  struct timespec wlocktime;
  std::atomic<uint64_t> mRdLockCounter;
  std::atomic<uint64_t> mWrLockCounter;
  std::atomic<uint64_t> mRdLockTime;
  std::atomic<uint64_t> mWrLockTime;
  std::atomic<uint64_t> mRdLockLeadTime;
  std::atomic<uint64_t> mWrLockLeadTime;
  bool mPreferRd; ///< If true reads go ahead of wr and are reentrant
  int64_t mBlockedForInterval; // interval in ms after which we might stacktrace a long-lasted mutex
  bool mBlockedStackTracing; // en-disable stacktracing long-lasted mutexes

  /**
   * Array of 4 elements representing the amount of milliseconds the mutex
   * was locked per second
   * This array therefore represent 4 seconds of run.
   * - Second 3: represents the current second of run
   * - Second 2: the last second before the current one
   * - Second 1: the second that will be displayed by eos ns stat
   * - Second 0: Will be set to 0 to reset the counter because this second
   * will be the one to be considered after the current one
   */
  TimingArray mNbMsMutexLocked;

  std::string mName;

#ifdef EOS_INSTRUMENTED_RWMUTEX

  int mCounter;
  int mSamplingModulo;
  std::atomic<bool> mEnableTiming, mEnableSampling;
  //! Specific type of counters
  std::atomic<uint64_t> mRdMaxWait, mWrMaxWait, mRdMinWait, mWrMinWait;
  std::atomic<uint64_t> mRdCumulatedWait, mWrCumulatedWait;
  std::atomic<uint64_t> mRdLockCounterSample, mWrLockCounterSample;

  std::map<std::thread::id, int> mThreadsRdLock; ///< Threads holding a read lock
  std::set<std::thread::id> mThreadsWrLock; ///< Threads holding a write lock
  pthread_mutex_t mCollectionMutex; ///< Mutex protecting the sets above
  bool mEnableDeadlockCheck; ///< Check for deadlocks
  std::atomic<bool> mTransientDeadlockCheck; ///< Enabled by the global flag

  static bool staticInitialized;
  static bool sEnableGlobalTiming;
  static int sSamplingModulo;
  static size_t timingCompensation, timingLatency, lockUnlockDuration;
  static std::atomic<uint64_t> mRdMaxWait_static, mWrMaxWait_static;
  static std::atomic<uint64_t> mRdMinWait_static, mWrMinWait_static;
  static std::atomic<uint64_t> mRdLockCounterSample_static,
         mWrLockCounterSample_static;
  static std::atomic<uint64_t> mRdCumulatedWait_static, mWrCumulatedWait_static;

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
  typedef std::map< std::string, std::vector<RWMutex*> > rules_t;
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

// For old clang avoid the use of builtin functions
#if defined(__clang__) && defined(__clang_major__) && (__clang_major__ < 12)
#define EOS_FUNCTION __FUNCTION__
#define EOS_FILE     __FILE__
#define EOS_LINE     __LINE__
#else
#define EOS_FUNCTION __builtin_FUNCTION()
#define EOS_FILE     __builtin_FILE()
#define EOS_LINE     __builtin_LINE()
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
  RWMutexWriteLock():
    mWrMutex(nullptr), mFile("unknown"), mFunction("unknown"), mLine(0)
  {}

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param mutex mutex to lock for write
  //! @param function caller function name, or empty string if not in the scope
  //!        of a function
  //! @param file caller file name, or empty string if not in the scope of a
  //!        function
  //! @param line caller line number in file
  //----------------------------------------------------------------------------
  RWMutexWriteLock(RWMutex& mutex,
                   const char* function = EOS_FUNCTION,
                   const char* file = EOS_FILE,
                   int line = EOS_LINE);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~RWMutexWriteLock()
  {
    Release();
  }

  //----------------------------------------------------------------------------
  //! Grab mutex and write lock it
  //!
  //! @param mutex mutex to lock for write
  //! @param function caller function name, or empty string if not in the scope
  //!        of a function
  //! @param file caller file name, or empty string if not in the scope of a
  //!        function
  //! @param line caller line number in file
  //----------------------------------------------------------------------------
  void Grab(RWMutex& mutex,
            const char* function = EOS_FUNCTION,
            const char* file = EOS_FILE,
            int line = EOS_LINE);

  //----------------------------------------------------------------------------
  //! Release the write lock after grab
  //----------------------------------------------------------------------------
  void Release();

private:
  std::chrono::steady_clock::time_point mAcquiredAt;
  std::chrono::system_clock::time_point mAcquiredAtSystem;
  std::chrono::steady_clock::time_point mReleasedAt;
  std::chrono::system_clock::time_point mReleasedAtSystem;
  RWMutex* mWrMutex {nullptr};
  const char* mFile;
  const char* mFunction;
  int mLine;
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
  RWMutexReadLock():
    mRdMutex(nullptr), mFunction("unknown"), mFile("unknown"), mLine(0)
  {}

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param mutex mutex to lock for read
  //! @param function caller function name, or empty string if not in the scope
  //!        of a function
  //! @param file caller file name, or empty string if not in the scope of a
  //!        function
  //! @param line caller line number in file
  //----------------------------------------------------------------------------
  RWMutexReadLock(RWMutex& mutex,
                  const char* function = EOS_FUNCTION,
                  const char* file = EOS_FILE,
                  int line = EOS_LINE);

  //----------------------------------------------------------------------------
  //! Grab mutex and read lock it
  //!
  //! @param mutex mutex to lock for read
  //! @param function caller function name, or empty string if not in the scope
  //!        of a function
  //! @param file caller file name, or empty string if not in the scope of a
  //!        function
  //! @param line caller line number in file
  //----------------------------------------------------------------------------
  void Grab(RWMutex& mutex,
            const char* function = EOS_FUNCTION,
            const char* file = EOS_FILE,
            int line = EOS_LINE);

  //----------------------------------------------------------------------------
  //! Release the read lock after grab
  //----------------------------------------------------------------------------
  void Release();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~RWMutexReadLock()
  {
    Release();
  }

private:
  std::chrono::steady_clock::time_point mAcquiredAt;
  RWMutex* mRdMutex {nullptr};
  const char* mFunction;
  const char* mFile;
  int mLine;
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
