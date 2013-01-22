// ----------------------------------------------------------------------
// File: RWMutex.hh
// Author: Andreas-Joachim Peters & Geoffrey Adde - CERN
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

/**
 * @file   RWMutex.hh
 * 
 * @brief  Class implementing a fair read-write Mutex.
 *         When compiled with EOS_INSTRUMENTED_RWMUTEX, this class provide also
 *          - timing features.
 *            The timing can be taken exhaustively or it can be sampled. Then some basics statistics are available.
 *            The statistics are available at the instance granularity or at the class granularity.
 *            If the timing is turned off (at both levels), the instrumented class is approximatively as fast as the native class
 *            Note that taking the timing of a mutex lock multiply the lock exec time by a factor from 4 up to 6.
 *            An estimation of the added latency is provided as well as a mechanism for timing compensation.
 *          - locking/unlocking order checking features
 *            Rules can be defined.
 *            Then each time a mutex involved in one of these rules is locked/unlocked.
 *            There is a book keeping for each thread. If a thread doesn't respect the order when locking/unlocking, a message is issued to the std err.
 *            For performance reasons, the maximum number of rules is static and defined in the macro EOS_RWMUTEX_ORDER_NRULES (default 4).
 *            A rule is defined by a locking order ( a sequence of pointers to RWMutex instances ). The maximum length of this sequence is 63.
 *            The added latency by order checking for 3 mutexes and 1 rule is about 15% of the locking/unlocking execution time.
 *            An estimation of this added latency is provided.
 */

#ifndef __EOSCOMMON_RWMUTEX_HH__
#define __EOSCOMMON_RWMUTEX_HH__

#ifndef __APPLE__
#define EOS_INSTRUMENTED_RWMUTEX
#endif

/*----------------------------------------------------------------------------*/
#include "common/Namespace.hh"
#include "common/Timing.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysPthread.hh"
#include "XrdSys/XrdSysAtomics.hh"
#include "XrdSys/XrdSysTimer.hh"
#include <stdio.h>
/*----------------------------------------------------------------------------*/
#ifdef EOS_INSTRUMENTED_RWMUTEX
#include <map>
#include <vector>
#include <ostream>
#include <fstream>
#include <iostream>
#include <execinfo.h>
#include <limits>
#include <cmath>
#endif

#define _MULTI_THREADED
#include <pthread.h>
/*----------------------------------------------------------------------------*/

EOSCOMMONNAMESPACE_BEGIN

inline size_t NowInt()
{
  struct timespec ts;
  eos::common::Timing::GetTimeSpec(ts);
  return 1000000000*ts.tv_sec+ts.tv_nsec;
}

#ifdef EOS_INSTRUMENTED_RWMUTEX

struct RWMutexTimingStats
{
  double averagewaitread;
  double averagewaitwrite;
  double minwaitwrite,maxwaitwrite;
  double minwaitread,maxwaitread;
  size_t readLockCounterSample, writeLockCounterSample;
};

inline std::ostream& operator << (std::ostream &os, const RWMutexTimingStats &stats )
{
  os<<"\t"<<"RWMutex Read  Wait (number : min , avg , max)"<<" = "<<stats.readLockCounterSample<<" : "<<stats.minwaitread<<" , "<<stats.averagewaitread<<" , "<<stats.maxwaitread<<std::endl;
  os<<"\t"<<"RWMutex Write Wait (number : min , avg , max)"<<" = "<<stats.writeLockCounterSample<<" : "<<stats.minwaitwrite<<" , "<<stats.averagewaitwrite<<" , "<<stats.maxwaitwrite<<std::endl;
  return os;
}

#define EOS_RWMUTEX_CHECKORDER_LOCK if(enableordercheckglobal) CheckAndLockOrder();
#define EOS_RWMUTEX_CHECKORDER_UNLOCK if(enableordercheckglobal) CheckAndUnlockOrder();

#define EOS_RWMUTEX_TIMER_START \
    bool issampled=false; size_t tstamp=0; \
    if( enabletiming || enabletimingglobal ) { \
      issampled=enablesampling?(!((++counter)%samplingModulo)):true; \
      if( issampled ) tstamp=NowInt(); \
    }

// what = write or what = read
#define EOS_RWMUTEX_TIMER_STOP_AND_UPDATE(what) \
    AtomicInc(what##LockCounter); \
    if( issampled ) { \
      tstamp=NowInt()-tstamp; \
      if(enabletiming) { \
        AtomicInc(what##LockCounter##Sample); \
        AtomicAdd(cumulatedwait##what,tstamp);\
        bool needloop=true; \
        do {size_t mymax=AtomicGet(maxwait##what); if (tstamp > mymax) needloop=!AtomicCAS(maxwait##what, mymax, tstamp); else needloop=false; }while(needloop); \
        do {size_t mymin=AtomicGet(minwait##what); if (tstamp < mymin) needloop=!AtomicCAS(minwait##what, mymin, tstamp); else needloop=false; }while(needloop); \
      }\
      if(enabletimingglobal) { \
        AtomicInc(what##LockCounter##Sample_static); \
        AtomicAdd(cumulatedwait##what##_static,tstamp);\
        bool needloop=true; \
        do {size_t mymax=AtomicGet(maxwait##what##_static); if (tstamp > mymax) needloop=!AtomicCAS(maxwait##what##_static, mymax, tstamp); else needloop=false; }while(needloop); \
        do {size_t mymin=AtomicGet(minwait##what##_static); if (tstamp < mymin) needloop=!AtomicCAS(minwait##what##_static, mymin, tstamp); else needloop=false; }while(needloop); \
      }\
    }
#else
#define EOS_RWMUTEX_CHECKORDER_LOCK
#define EOS_RWMUTEX_CHECKORDER_UNLOCK
#define EOS_RWMUTEX_TIMER_START
#define EOS_RWMUTEX_TIMER_STOP_AND_UPDATE(what) AtomicInc(what##LockCounter);
#endif

/*----------------------------------------------------------------------------*/
//! Class implements a fair rw mutex prefering writers
/*----------------------------------------------------------------------------*/
#define EOS_RWMUTEX_ORDER_NRULES 4
class RWMutex
{
private:
  pthread_rwlock_t rwlock;
  pthread_rwlockattr_t attr;
  struct timespec wlocktime;
  struct timespec rlocktime;
  bool blocking;
  size_t readLockCounter;
  size_t writeLockCounter;

#ifdef EOS_INSTRUMENTED_RWMUTEX
  // ############# MISCELLANEOUS #############
  static bool staticInitialized;
  int counter;
  std::string debugname;
  // #########################################

  // ############# TIMING MEMBERS ##############
  bool enabletiming,enablesampling;
  static bool enabletimingglobal;
  static size_t timingCompensation, timingLatency, lockUnlockDuration ;
  int samplingModulo;
  static int samplingModulo_static;
  // ************* Counters ************
  // these counters come in addition to the counters in the non instrumented version of the class
  size_t cumulatedwaitread,cumulatedwaitwrite,maxwaitread,maxwaitwrite,minwaitread,minwaitwrite,readLockCounterSample,writeLockCounterSample;
  static size_t cumulatedwaitread_static,cumulatedwaitwrite_static,maxwaitread_static,maxwaitwrite_static,minwaitread_static,minwaitwrite_static;
  static size_t readLockCounterSample_static,writeLockCounterSample_static;
  // ***********************************
  // ###########################################

  // ######### ORDER CHECKING MEMBERS ###########
  // ********* Actual Order Checking ********
  // this pointers refer to a memory location not thread specific so that if the thread terminates, this location is still valid
  // this flag is triggered by the class management and indicate to each thread to reset some thread specific stuff
  static __thread bool *orderCheckReset_staticthread;
  // this map contains the previously referenced flag for reset
  static std::map<pthread_t,bool> threadOrderCheckResetFlags_static;
  // each unsigned long is used as 64 bit flags to trace the lock status of mutexes for a given rule
  static __thread unsigned long ordermask_staticthread[EOS_RWMUTEX_ORDER_NRULES];
  // a mutex can be associated to up to EOS_RWMUTEX_ORDER_NRULES and the following array gives the locking rank for "this" RWMutex
  unsigned char rankinrule[EOS_RWMUTEX_ORDER_NRULES];
  // the number of rules "this" RWMutex
  unsigned char nrules;

  // ****************************************
  // ******** Order Rules management  *******
  // to issue the message and to manage the rules. Not involved in the online checking.
  static std::map<unsigned char,std::string> ruleIndex2Name_static;
  static std::map<std::string,unsigned char> ruleName2Index_static;
  unsigned char ruleLocalIndexToGlobalIndex[EOS_RWMUTEX_ORDER_NRULES];
  // rulename -> order
  typedef std::map< std::string , std::vector<RWMutex*> > rules_t;
  static rules_t rules_static;
  // lock to guarantee unique access to rules management
  static pthread_rwlock_t orderChkMgmLock;
  // ****************************************
  // ***************** Other  ***************
  static bool enableordercheckglobal;
  static size_t orderCheckingLatency;
  // ****************************************
  // ###########################################
#endif
public:
  // ---------------------------------------------------------------------------
  //! Constructor
  // ---------------------------------------------------------------------------
  RWMutex();

  // ---------------------------------------------------------------------------
  //! Destructor
  // ---------------------------------------------------------------------------
  ~RWMutex();

  // ---------------------------------------------------------------------------
  //! Set the write lock to blocking or not blocking
  // ---------------------------------------------------------------------------
  void SetBlocking(bool block);

  // ---------------------------------------------------------------------------
  //! Set the time to wait the acquisition of the write mutex before releasing quicky and retrying
  // ---------------------------------------------------------------------------
  void SetWLockTime(const size_t &nsec);

#ifdef EOS_INSTRUMENTED_RWMUTEX
  // ---------------------------------------------------------------------------
  //! Reset statistics at the instance level
  // ---------------------------------------------------------------------------
  void ResetTimingStatistics();

  // ---------------------------------------------------------------------------
  //! Reset statistics at the class level
  // ---------------------------------------------------------------------------
  static void ResetTimingStatisticsGlobal();

  // ---------------------------------------------------------------------------
  //! Turn on/off timings at the instance level
  // ---------------------------------------------------------------------------
  void SetTiming(bool on);

  // ---------------------------------------------------------------------------
  //! Get the timing status at the class level
  // ---------------------------------------------------------------------------
  bool GetTiming();


  // ---------------------------------------------------------------------------
  //! Turn on/off timings at the class level
  // ---------------------------------------------------------------------------
  static void SetTimingGlobal(bool on);

  // ---------------------------------------------------------------------------
  //! Get the timing status  at the class level
  // ---------------------------------------------------------------------------
  static bool GetTimingGlobal();

  // ---------------------------------------------------------------------------
  //! Turn on/off order checking at the class level
  // ---------------------------------------------------------------------------
  static void SetOrderCheckingGlobal(bool on);

  // ---------------------------------------------------------------------------
  //! Get the order checking status at the class level
  // ---------------------------------------------------------------------------
  static bool GetOrderCheckingGlobal();

  // ---------------------------------------------------------------------------
  //! Set the debug name
  // ---------------------------------------------------------------------------
  void SetDebugName(const std::string &name);

#ifdef __APPLE__
  static int round(double number);
#endif

  // ---------------------------------------------------------------------------
  //! Enable sampling of timings
  // @param $first
  //   turns on or off the sampling
  // @param $second
  //   sampling between 0 and 1 (if <0, use the precomputed level for the class, see GetSamplingRateFromCPUOverhead)
  // ---------------------------------------------------------------------------
  void SetSampling(bool on, float rate=-1.0);

  // ---------------------------------------------------------------------------
  //! Return the timing sampling rate/status
  // @return the sample rate if the sampling is turned on, -1.0 if the sampling is off
  // ---------------------------------------------------------------------------
  float GetSampling();
  // ---------------------------------------------------------------------------
  //! Get the timing statistics at the instance level
  // ---------------------------------------------------------------------------
  void GetTimingStatistics(RWMutexTimingStats &stats, bool compensate=true);

  // ---------------------------------------------------------------------------
  //! Check the orders defined by the rules and update
  // ---------------------------------------------------------------------------
  void OrderViolationMessage(unsigned char rule, const std::string &message="");

  // ---------------------------------------------------------------------------
  //! Check the orders defined by the rules and update for a lock
  // ---------------------------------------------------------------------------
  void CheckAndLockOrder();

  // ---------------------------------------------------------------------------
  //! Get the timing statistics at the instance level for an unlock
  // ---------------------------------------------------------------------------
  void CheckAndUnlockOrder();

  // ---------------------------------------------------------------------------
  //! Get the timing statistics at the class level
  // ---------------------------------------------------------------------------
  static void GetTimingStatisticsGlobal(RWMutexTimingStats &stats, bool compensate=true);

  // ---------------------------------------------------------------------------
  //! Compute the SamplingRate corresponding to a given CPU overhead
  // @param $first
  //   the ratio between the timing cost and the mutexing cost
  // @return
  //   sampling rate (the ratio of mutex to time so that the argument value is not violated)
  // ---------------------------------------------------------------------------
  static float GetSamplingRateFromCPUOverhead(const double &overhead);

  // ---------------------------------------------------------------------------
  //! Compute the cost in time of taking timings so that it can be compensated in the statistics
  // @param $first
  //   the size of the loop to estimate the compensation (default 1e7)
  // @return
  //   the compensation in nanoseconds
  // ---------------------------------------------------------------------------
  static size_t EstimateTimingCompensation(size_t loopsize=1e6);

  // ---------------------------------------------------------------------------
  //! Compute the speed for lock/unlock cycle
  // @param $first
  //   the size of the loop to estimate the compensation (default 1e6)
  // @return
  //   the duration of the cycle in nanoseconds
  // ---------------------------------------------------------------------------
  static size_t EstimateLockUnlockDuration(size_t loopsize=1e6);

  // ---------------------------------------------------------------------------
  //! Compute the latency introduced by taking timings
  // @param $first
  //   the size of the loop to estimate the compensation (default 1e6)
  // @param $second
  //   enable global timing in the estimation (default false)
  // @return
  //   the latency in nanoseconds
  // ---------------------------------------------------------------------------
  static size_t EstimateTimingAddedLatency(size_t loopsize=1e6, bool globaltiming=false);

  // ---------------------------------------------------------------------------
  //! Compute the latency introduced by checking the mutexes locking orders
  // @param $first
  //   the number of nested mutexes in the loop
  // @param $second
  //   the size of the loop to estimate the compensation (default 1e6)
  // @return
  //   the latency in nanoseconds
  // ---------------------------------------------------------------------------
  static size_t EstimateOrderCheckingAddedLatency(size_t nmutexes=3, size_t loopsize=1e6);

  // ---------------------------------------------------------------------------
  //! Performs the initialization of the class
  // ---------------------------------------------------------------------------
  static void InitializeClass();

  static void EstimateLatenciesAndCompensation(size_t loopsize=1e6);

  static size_t GetTimingCompensation();

  static size_t GetOrderCheckingLatency();

  static size_t GetTimingLatency();

  static size_t GetLockUnlockDuration();

  // ---------------------------------------------------------------------------
  //! Add or overwrite an order checking rule
  // @param $first
  //   name of the rule
  // @param $second
  //   a vector contaning the adress of the RWMutex instances in the locking order
  // @return
  //
  // ---------------------------------------------------------------------------
  static int AddOrderRule(const std::string &rulename, const std::vector<RWMutex*> &order);

  // ---------------------------------------------------------------------------
  //! Reset order checking rules
  // ---------------------------------------------------------------------------
  static void ResetOrderRule();

  // ---------------------------------------------------------------------------
  //! Remove an order checking rule
  // @param $first
  //   name of the rule
  // @return
  //   the number of rules removed (0 or 1)
  //
  // ---------------------------------------------------------------------------
  static int RemoveOrderRule(const std::string &rulename);


  // ---------------------------------------------------------------------------
  //! Reset the order checking mechanism for the current thread
  // ---------------------------------------------------------------------------
  void ResetCheckOrder();

#endif

  // ---------------------------------------------------------------------------
  //! Lock for read
  // ---------------------------------------------------------------------------
  void LockRead();


  // ---------------------------------------------------------------------------
  //! Lock for read allowing to be canceled waiting for a lock
  // ---------------------------------------------------------------------------
  void LockReadCancel();


  // ---------------------------------------------------------------------------
  //! Unlock a read lock
  // ---------------------------------------------------------------------------
  void UnLockRead();

  // ---------------------------------------------------------------------------
  //! Lock for write
  // ---------------------------------------------------------------------------
  void LockWrite();

  // ---------------------------------------------------------------------------
  //! Unlock a write lock
  // ---------------------------------------------------------------------------
  void UnLockWrite();

  // ---------------------------------------------------------------------------
  //! Lock for write but give up after wlocktime
  // ---------------------------------------------------------------------------
  int TimeoutLockWrite();

  // ---------------------------------------------------------------------------
  //! Get Readlock Counter
  // ---------------------------------------------------------------------------
  size_t GetReadLockCounter();
 

  // ---------------------------------------------------------------------------
  //! Get Writelock Counter
  // ---------------------------------------------------------------------------
  size_t GetWriteLockCounter();
 

};

/*----------------------------------------------------------------------------*/
//! Class implementing a monitor for write locking
/*----------------------------------------------------------------------------*/
class RWMutexWriteLock
{
private:
  RWMutex* Mutex;

public:
  // ---------------------------------------------------------------------------
  //! Constructor
  // ---------------------------------------------------------------------------
  RWMutexWriteLock(RWMutex &mutex);

  // ---------------------------------------------------------------------------
  //! Destructor
  // ---------------------------------------------------------------------------
  ~RWMutexWriteLock();
};

/*----------------------------------------------------------------------------*/
//! Class implementing a monitor for read locking
/*----------------------------------------------------------------------------*/
class RWMutexReadLock
{
private:
  RWMutex* Mutex;

public:
  // ---------------------------------------------------------------------------
  //! Constructor
  // ---------------------------------------------------------------------------
  RWMutexReadLock(RWMutex &mutex);
  
  RWMutexReadLock(RWMutex &mutex, bool allowcancel);
  // ---------------------------------------------------------------------------
  //! Destructor
  // ---------------------------------------------------------------------------
  ~RWMutexReadLock();
};

// undefine the timer stuff
#ifdef EOS_INSTRUMENTED_RWMUTEX
#undef EOS_RWMUTEX_TIMER_START
#undef EOS_RWMUTEX_TIMER_STOP_AND_UPDATE
#endif

/*----------------------------------------------------------------------------*/
EOSCOMMONNAMESPACE_END

#endif

