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
  RWMutex()
  {
    // by default we are not a blocking write mutex
    blocking = false;
    // try to get write lock in 5 seconds, then release quickly and retry
    wlocktime.tv_sec=5;
    wlocktime.tv_nsec=0;
    // try to get read lock in 100ms, otherwise allow this thread to be canceled - used by LockReadCancel
    rlocktime.tv_sec=0;
    rlocktime.tv_nsec=1000000;
    readLockCounter=writeLockCounter=0;
#ifdef EOS_INSTRUMENTED_RWMUTEX
    if(!staticInitialized) {
      staticInitialized=true;
      InitializeClass();
    }
    counter=0;
    ResetTimingStatistics();
    enabletiming=false;
    enablesampling=false;
    nrules=0;


#endif
#ifndef __APPLE__
    pthread_rwlockattr_init(&attr);

    // readers don't go ahead of writers!
    if (pthread_rwlockattr_setkind_np(&attr,PTHREAD_RWLOCK_PREFER_WRITER_NP))
    { throw "pthread_rwlockattr_setkind_np failed";}
    if (pthread_rwlockattr_setpshared(&attr,PTHREAD_PROCESS_SHARED))
      { throw "pthread_rwlockattr_setpshared failed";}

    if ((pthread_rwlock_init(&rwlock, &attr)))
      { throw "pthread_rwlock_init failed";}}
#else
  pthread_rwlockattr_init(&attr);
  if (pthread_rwlockattr_setpshared(&attr,PTHREAD_PROCESS_SHARED))
    { throw "pthread_rwlockattr_setpshared failed";}
  if ((pthread_rwlock_init(&rwlock, &attr))) 
    { throw "pthread_rwlock_init failed";}
  }
#endif

  // ---------------------------------------------------------------------------
  //! Destructor
  // ---------------------------------------------------------------------------
  ~RWMutex()
  {
#ifdef EOS_INSTRUMENTED_RWMUTEX
    pthread_rwlock_rdlock(&orderChkMgmLock);
    std::map<std::string,std::vector<RWMutex*> > *rules=NULL;
    for(auto rit=rules_static.begin(); rit!= rules_static.end() ;rit++) { // for each rule
      for(auto it=rit->second.begin(); it !=rit->second.end(); it++) { // for each RWMutex involved in that rule
        if((*it) == this) {
          if(rules==NULL) rules=new std::map<std::string,std::vector<RWMutex*> >(rules_static);
          rules->erase(rit->first); // remove the rule if it contains this
        }
      }
    }
    pthread_rwlock_unlock(&orderChkMgmLock);

    if(rules!=NULL) {
      // erase the rules
      ResetOrderRule();
      // inserts the remaining rules
      for(auto it=rules->begin(); it != rules->end(); it++)
        AddOrderRule(it->first,it->second);
      delete rules;
    }
#endif
  }

  // ---------------------------------------------------------------------------
  //! Set the write lock to blocking or not blocking
  // ---------------------------------------------------------------------------
  void SetBlocking(bool block) {
    blocking = block;
  }

  // ---------------------------------------------------------------------------
  //! Set the time to wait the acquisition of the write mutex before releasing quicky and retrying
  // ---------------------------------------------------------------------------
  void SetWLockTime(const size_t &nsec)
  {
    wlocktime.tv_sec=nsec/1000000;
    wlocktime.tv_nsec=nsec%1000000;
  }

#ifdef EOS_INSTRUMENTED_RWMUTEX
  // ---------------------------------------------------------------------------
  //! Reset statistics at the instance level
  // ---------------------------------------------------------------------------
  void ResetTimingStatistics()
  {
    // might need a mutex or at least a flag!!!
    cumulatedwaitread=cumulatedwaitwrite=0;
    maxwaitread=maxwaitwrite=std::numeric_limits<size_t>::min();
    minwaitread=minwaitwrite=std::numeric_limits<long long>::max();
    readLockCounterSample=writeLockCounterSample=0;
  }

  // ---------------------------------------------------------------------------
  //! Reset statistics at the class level
  // ---------------------------------------------------------------------------
  static void ResetTimingStatisticsGlobal()
  {
    // might need a mutex or at least a flag!!!
    cumulatedwaitread_static=cumulatedwaitwrite_static=0;
    maxwaitread_static=maxwaitwrite_static=std::numeric_limits<size_t>::min();
    minwaitread_static=minwaitwrite_static=std::numeric_limits<long long>::max();
    readLockCounterSample_static=writeLockCounterSample_static=0;
  }

  // ---------------------------------------------------------------------------
  //! Turn on/off timings at the instance level
  // ---------------------------------------------------------------------------
  void SetTiming(bool on)
  {
    enabletiming=on;
  }

  // ---------------------------------------------------------------------------
  //! Get the timing status at the class level
  // ---------------------------------------------------------------------------
  bool GetTiming()
  {
    return enabletiming;
  }


  // ---------------------------------------------------------------------------
  //! Turn on/off timings at the class level
  // ---------------------------------------------------------------------------
  static void SetTimingGlobal(bool on)
  {
    enabletimingglobal=on;
  }

  // ---------------------------------------------------------------------------
  //! Get the timing status  at the class level
  // ---------------------------------------------------------------------------
  static bool GetTimingGlobal()
  {
    return enabletimingglobal;
  }

  // ---------------------------------------------------------------------------
  //! Turn on/off order checking at the class level
  // ---------------------------------------------------------------------------
  static void SetOrderCheckingGlobal(bool on)
  {
    enableordercheckglobal=on;
  }

  // ---------------------------------------------------------------------------
  //! Get the order checking status at the class level
  // ---------------------------------------------------------------------------
  static bool GetOrderCheckingGlobal()
  {
    return enableordercheckglobal;
  }

  // ---------------------------------------------------------------------------
  //! Set the debug name
  // ---------------------------------------------------------------------------
  void SetDebugName(const std::string &name)
  {
    debugname=name;
  }

#ifdef __APPLE__
  int round(double number)
  {
    return number < 0.0 ? ceil(number - 0.5) : floor(number + 0.5);
  }
#endif

  // ---------------------------------------------------------------------------
  //! Enable sampling of timings
  // @param $first
  //   turns on or off the sampling
  // @param $second
  //   sampling between 0 and 1 (if <0, use the precomputed level for the class, see GetSamplingRateFromCPUOverhead)
  // ---------------------------------------------------------------------------
  void SetSampling(bool on, float rate=-1.0)
  {
    enablesampling=on;
    ResetTimingStatistics();
    if(rate<0)
      samplingModulo=samplingModulo_static;
    else
#ifdef __APPLE__
      samplingModulo= std::min( RAND_MAX, std::max(0, (int)round(1.0/rate) ) );
#else
    samplingModulo= std::min( RAND_MAX, std::max(0, (int)std::round(1.0/rate) ) );
#endif
  }

  // ---------------------------------------------------------------------------
  //! Return the timing sampling rate/status
  // @return the sample rate if the sampling is turned on, -1.0 if the sampling is off
  // ---------------------------------------------------------------------------
  float GetSampling()
  {
    if(!enablesampling) return -1.0;
    else return 1.0/samplingModulo;
  }

  // ---------------------------------------------------------------------------
  //! Get the timing statistics at the instance level
  // ---------------------------------------------------------------------------
  void GetTimingStatistics(RWMutexTimingStats &stats, bool compensate=true)
  {
    size_t compensation=compensate?timingCompensation:0;
    stats.readLockCounterSample=AtomicGet(readLockCounterSample);
    stats.writeLockCounterSample=AtomicGet(writeLockCounterSample);

    stats.averagewaitread=0;
    if(AtomicGet(readLockCounterSample)!=0) {
      double avg=(double(AtomicGet(cumulatedwaitread))/AtomicGet(readLockCounterSample)-compensation);
      if(avg>0)
        stats.averagewaitread=avg;
    }
    stats.averagewaitwrite=0;
    if(AtomicGet(writeLockCounterSample)!=0) {
      double avg=(double(AtomicGet(cumulatedwaitwrite))/AtomicGet(writeLockCounterSample)-compensation);
      if(avg>0)
        stats.averagewaitwrite=avg;
    }
    if(AtomicGet(minwaitread)!=std::numeric_limits<size_t>::max()) {
      long long compensated=AtomicGet(minwaitread)-compensation;
      if(compensated>0)
        stats.minwaitread=compensated;
      else
        stats.minwaitread=0;
    }
    else
      stats.minwaitread=std::numeric_limits<long long>::max();
    if(AtomicGet(maxwaitread)!=std::numeric_limits<size_t>::min()) {
      long long compensated=AtomicGet(maxwaitread)-compensation;
      if(compensated>0)
        stats.maxwaitread=compensated;
      else
        stats.maxwaitread=0;
    }
    else
      stats.maxwaitread=std::numeric_limits<size_t>::min();
    if(AtomicGet(minwaitwrite)!=std::numeric_limits<size_t>::max()) {
      long long compensated=AtomicGet(minwaitwrite)-compensation;
      if(compensated>0)
        stats.minwaitwrite=compensated;
      else
        stats.minwaitwrite=0;
    }
    else
      stats.minwaitwrite=std::numeric_limits<long long>::max();
    if(AtomicGet(maxwaitwrite)!=std::numeric_limits<size_t>::min()) {
      long long compensated=AtomicGet(maxwaitwrite)-compensation;
      if(compensated>0)
        stats.maxwaitwrite=compensated;
      else
        stats.maxwaitwrite=0;
    }
    else
      stats.maxwaitwrite=std::numeric_limits<size_t>::min();
  }

  // ---------------------------------------------------------------------------
  //! Check the orders defined by the rules and update
  // ---------------------------------------------------------------------------
  void OrderViolationMessage(unsigned char rule, const std::string &message="") {
    void *array[10];
    size_t size;
    unsigned long threadid=XrdSysThread::Num();

    // get void*'s for all entries on the stack
    size = backtrace(array, 10);

    const std::string &rulename=ruleIndex2Name_static[ruleLocalIndexToGlobalIndex[rule]];
    fprintf(stderr, "RWMutex: Order Checking Error in thread %lu\n %s\n In rule %s :\nLocking Order should be:\n",
        threadid,
        message.c_str(),
        rulename.c_str());
    std::vector<RWMutex*> order=rules_static[rulename];
    for(std::vector<RWMutex*>::iterator ito=order.begin(); ito!=order.end(); ito++)
      fprintf(stderr,"\t%12s (%p)",(*ito)->debugname.c_str(), (*ito) );

    fprintf(stderr,"\nThe lock states of these mutexes are (before the violating lock/unlock) :\n");
    for(unsigned char k=0; k<order.size() ; k++) {
      unsigned long int mask= (1<<k);
      fprintf(stderr,"\t%d", int( (ordermask_staticthread[rule] & mask)!=0 ) );
    }
    fprintf(stderr, "\n");

    backtrace_symbols_fd(array, size, 2);
  }

  // ---------------------------------------------------------------------------
  //! Check the orders defined by the rules and update for a lock
  // ---------------------------------------------------------------------------
  void CheckAndLockOrder()
  {
    // initialize the thread local ordermask if not already done
    if(orderCheckReset_staticthread==NULL) {
      ResetCheckOrder();
    }
    if(*orderCheckReset_staticthread) {
      ResetCheckOrder();
      *orderCheckReset_staticthread=false;
    }

    for(unsigned char k=0;k<nrules;k++) {
      unsigned long int mask=(1<<rankinrule[k]);
      // check if following mutex is already locked in the same thread
      if(ordermask_staticthread[k] >= mask ) {
        char strmess[1024];
        sprintf(strmess,"locking %s at address %p",debugname.c_str(),this);
        OrderViolationMessage(k,strmess);
      }
      ordermask_staticthread[k] |= mask;
    }
  }

  // ---------------------------------------------------------------------------
  //! Get the timing statistics at the instance level for an unlock
  // ---------------------------------------------------------------------------
  void CheckAndUnlockOrder()
  {
    // initialize the thread local ordermask if not already done
    if(orderCheckReset_staticthread==NULL)
      ResetCheckOrder();
    if(*orderCheckReset_staticthread) {
      ResetCheckOrder();
      *orderCheckReset_staticthread=false;
    }

    for(unsigned char k=0;k<nrules;k++) {
      unsigned long int mask=(1<<rankinrule[k]);
      // check if following mutex is already locked in the same thread
      if(ordermask_staticthread[k] >= (mask<<1) ) {
        char strmess[1024];
        sprintf(strmess,"unlocking %s at address %p",debugname.c_str(),this);
        OrderViolationMessage(k,strmess);
      }
      ordermask_staticthread[k] &= (~mask);
    }
  }

  // ---------------------------------------------------------------------------
  //! Get the timing statistics at the class level
  // ---------------------------------------------------------------------------
  static void GetTimingStatisticsGlobal(RWMutexTimingStats &stats, bool compensate=true)
  {
    size_t compensation=compensate?timingCompensation:0;
    stats.readLockCounterSample=AtomicGet(readLockCounterSample_static);
    stats.writeLockCounterSample=AtomicGet(writeLockCounterSample_static);

    stats.averagewaitread=0;
    if(AtomicGet(readLockCounterSample_static)!=0) {
      double avg=(double(AtomicGet(cumulatedwaitread_static))/AtomicGet(readLockCounterSample_static)-compensation);
      if(avg>0)
        stats.averagewaitread=avg;
    }
    stats.averagewaitwrite=0;
    if(AtomicGet(writeLockCounterSample_static)!=0) {
      double avg=(double(AtomicGet(cumulatedwaitwrite_static))/AtomicGet(writeLockCounterSample_static)-compensation);
      if(avg>0)
        stats.averagewaitwrite=avg;
    }
    if(AtomicGet(minwaitread_static)!=std::numeric_limits<size_t>::max()) {
      long long compensated=AtomicGet(minwaitread_static)-compensation;
      if(compensated>0)
        stats.minwaitread=compensated;
      else
        stats.minwaitread=0;
    }
    else
      stats.minwaitread=std::numeric_limits<long long>::max();
    if(AtomicGet(maxwaitread_static)!=std::numeric_limits<size_t>::min()) {
      long long compensated=AtomicGet(maxwaitread_static)-compensation;
      if(compensated>0)
        stats.maxwaitread=compensated;
      else
        stats.maxwaitread=0;
    }
    else
      stats.maxwaitread=std::numeric_limits<size_t>::min();
    if(AtomicGet(minwaitwrite_static)!=std::numeric_limits<size_t>::max()) {
      long long compensated=AtomicGet(minwaitwrite_static)-compensation;
      if(compensated>0)
        stats.minwaitwrite=compensated;
      else
        stats.minwaitwrite=0;
    }
    else
      stats.minwaitwrite=std::numeric_limits<long long>::max();
    if(AtomicGet(maxwaitwrite_static)!=std::numeric_limits<size_t>::min()) {
      long long compensated=AtomicGet(maxwaitwrite_static)-compensation;
      if(compensated>0)
        stats.maxwaitwrite=compensated;
      else
        stats.maxwaitwrite=0;
    }
    else
      stats.maxwaitwrite=std::numeric_limits<size_t>::min();
  }

  // ---------------------------------------------------------------------------
  //! Compute the SamplingRate corresponding to a given CPU overhead
  // @param $first
  //   the ratio between the timing cost and the mutexing cost
  // @return
  //   sampling rate (the ratio of mutex to time so that the argument value is not violated)
  // ---------------------------------------------------------------------------
  static float GetSamplingRateFromCPUOverhead(const double &overhead)
  {
    RWMutex mutex;
    bool entimglobbak=enabletimingglobal;

    mutex.SetTiming(true); mutex.SetSampling(true,1.0);
    RWMutex::SetTimingGlobal(true);
    size_t monitoredTiming=NowInt();
    for(int k=0;k<1e6;k++)
    {
      mutex.LockWrite();
      mutex.UnLockWrite();
    }
    monitoredTiming=NowInt()-monitoredTiming;

    mutex.SetTiming(false); mutex.SetSampling(false);
    RWMutex::SetTimingGlobal(false);
    size_t unmonitoredTiming=NowInt();
    for(int k=0;k<1e6;k++)
    {
      mutex.LockWrite();
      mutex.UnLockWrite();
    }
    unmonitoredTiming=NowInt()-unmonitoredTiming;

    RWMutex::SetTimingGlobal(entimglobbak);

    float mutexShare=unmonitoredTiming;
    float timingShare=monitoredTiming-unmonitoredTiming;
    float samplingRate=std::min(1.0, std::max(0.0,overhead*mutexShare/timingShare) );
    samplingModulo_static=(int) (1.0/samplingRate);

    return samplingRate;
  }

  // ---------------------------------------------------------------------------
  //! Compute the cost in time of taking timings so that it can be compensated in the statistics
  // @param $first
  //   the size of the loop to estimate the compensation (default 1e7)
  // @return
  //   the compensation in nanoseconds
  // ---------------------------------------------------------------------------
  static size_t EstimateTimingCompensation(size_t loopsize=1e6)
  {
    size_t t = NowInt();
    for (unsigned long k = 0; k < loopsize; k++)
    {
      struct timespec ts;
      eos::common::Timing::GetTimeSpec(ts);
    }
    t = NowInt() - t;

    return size_t(double(t)/loopsize);
  }

  // ---------------------------------------------------------------------------
  //! Compute the speed for lock/unlock cycle
  // @param $first
  //   the size of the loop to estimate the compensation (default 1e6)
  // @return
  //   the duration of the cycle in nanoseconds
  // ---------------------------------------------------------------------------
  static size_t EstimateLockUnlockDuration(size_t loopsize=1e6)
  {
    RWMutex mutex;

    bool sav=enabletimingglobal;
    bool sav2=enableordercheckglobal;
    RWMutex::SetTimingGlobal(false);
    RWMutex::SetOrderCheckingGlobal(false);
    mutex.SetTiming(false);
    mutex.SetSampling(false);
    size_t t = NowInt();
    for (size_t k = 0; k < loopsize; k++)
    {
      mutex.LockWrite();
      mutex.UnLockWrite();
    }
    t = NowInt() - t;
    enabletimingglobal=sav;
    enableordercheckglobal=sav2;

    return size_t(double(t)/loopsize);
  }

  // ---------------------------------------------------------------------------
  //! Compute the latency introduced by taking timings
  // @param $first
  //   the size of the loop to estimate the compensation (default 1e6)
  // @param $second
  //   enable global timing in the estimation (default false)
  // @return
  //   the latency in nanoseconds
  // ---------------------------------------------------------------------------
  static size_t EstimateTimingAddedLatency(size_t loopsize=1e6, bool globaltiming=false)
  {
    RWMutex mutex;

    bool sav=enabletimingglobal;
    bool sav2=enableordercheckglobal;
    RWMutex::SetTimingGlobal(globaltiming);
    RWMutex::SetOrderCheckingGlobal(false);
    mutex.SetTiming(true);
    mutex.SetSampling(true,1.0);
    size_t t = NowInt();
    for (size_t k = 0; k < loopsize; k++)
    {
      mutex.LockWrite();
      mutex.UnLockWrite();
    }
    size_t s = NowInt() - t;

    RWMutex::SetTimingGlobal(false);
    mutex.SetTiming(false);
    mutex.SetSampling(false);
    t = NowInt();
    for (size_t k = 0; k < loopsize; k++)
    {
      mutex.LockWrite();
      mutex.UnLockWrite();
    }
    t = NowInt() - t;
    enabletimingglobal=sav;
    enableordercheckglobal=sav2;

    return size_t(double(s-t)/loopsize);
  }

  // ---------------------------------------------------------------------------
  //! Compute the latency introduced by checking the mutexes locking orders
  // @param $first
  //   the number of nested mutexes in the loop
  // @param $second
  //   the size of the loop to estimate the compensation (default 1e6)
  // @return
  //   the latency in nanoseconds
  // ---------------------------------------------------------------------------
  static size_t EstimateOrderCheckingAddedLatency(size_t nmutexes=3, size_t loopsize=1e6)
  {
    std::vector<RWMutex> mutexes; mutexes.resize(nmutexes);
    std::vector<RWMutex*> order; order.resize(nmutexes);

    int count=0;
    for(std::vector<RWMutex>::iterator it=mutexes.begin();it != mutexes.end(); it++) {
      it->SetTiming(false);
      it->SetSampling(false);
      order[count++]=&(*it);
    }
    RWMutex::AddOrderRule("estimaterule",order);

    bool sav=enabletimingglobal;
    bool sav2=enableordercheckglobal;
    RWMutex::SetTimingGlobal(false);
    RWMutex::SetOrderCheckingGlobal(true);

    std::vector<RWMutex>::iterator it;
    std::vector<RWMutex>::reverse_iterator rit;
    size_t t = NowInt();
    for (size_t k = 0; k < loopsize; k++) {
      for(it=mutexes.begin();it != mutexes.end(); it++) {
        it->LockWrite();
      }
      for(rit=mutexes.rbegin();rit != mutexes.rend(); rit++) {
        rit->UnLockWrite();
      }
    }
    size_t s = NowInt() - t;

    RWMutex::SetOrderCheckingGlobal(false);
    t = NowInt();
    for (size_t k = 0; k < loopsize; k++) {
      for(it=mutexes.begin();it != mutexes.end(); it++) {
        it->LockWrite();
      }
      for(rit=mutexes.rbegin();rit != mutexes.rend(); rit++) {
        rit->UnLockWrite();
      }
    }
    t = NowInt() - t;
    enabletimingglobal=sav;
    enableordercheckglobal=sav2;

    RemoveOrderRule("estimaterule");

    return size_t(double(s-t)/(loopsize*nmutexes));
  }

  // ---------------------------------------------------------------------------
  //! Performs the initialization of the class
  // ---------------------------------------------------------------------------
  static void InitializeClass()
  {
    if (pthread_rwlock_init(&orderChkMgmLock, NULL))
    { throw "pthread_orderChkMgmLock_init failed";}
  }

  static void EstimateLatenciesAndCompensation(size_t loopsize=1e6) {
    timingCompensation=EstimateTimingCompensation(loopsize);
    timingLatency=EstimateTimingAddedLatency(loopsize);
    orderCheckingLatency=EstimateOrderCheckingAddedLatency(3,loopsize);
    lockUnlockDuration=EstimateLockUnlockDuration(loopsize);
    //std::cerr<< " timing compensation = "<<timingCompensation<<std::endl;
    //std::cerr<< " timing latency = "<<timingLatency<<std::endl;
    //std::cerr<< " order  latency = "<<orderCheckingLatency<<std::endl;
    //std::cerr<< " lock/unlock duration = "<<lockUnlockDuration<<std::endl;
  }

  static size_t GetTimingCompensation() {
    return timingCompensation; // in nsec
  }

  static size_t GetOrderCheckingLatency() {
    return orderCheckingLatency; // in nsec
  }

  static size_t GetTimingLatency() {
    return timingLatency; // in nsec
  }

  static size_t GetLockUnlockDuration() {
    return lockUnlockDuration; // in nsec
  }

  // ---------------------------------------------------------------------------
  //! Add or overwrite an order checking rule
  // @param $first
  //   name of the rule
  // @param $second
  //   a vector contaning the adress of the RWMutex instances in the locking order
  // @return
  //
  // ---------------------------------------------------------------------------
  static int AddOrderRule(const std::string &rulename, const std::vector<RWMutex*> &order)
  {
    bool sav=enableordercheckglobal;
    enableordercheckglobal=false;
    usleep(100000);  // let the time to all the threads to finish their bookkeeping activity regarding order checking

    pthread_rwlock_wrlock(&orderChkMgmLock);

    // if we reached the max number of rules, ignore
    if(rules_static.size()==EOS_RWMUTEX_ORDER_NRULES || order.size()>63)  {
      enableordercheckglobal=sav;
      pthread_rwlock_unlock(&orderChkMgmLock);
      return -1;
    }

    rules_static[rulename]=order;
    int ruleIdx=rules_static.size()-1;

    // update the maps
    ruleName2Index_static[rulename]=ruleIdx;
    ruleIndex2Name_static[ruleIdx]=rulename;

    // update each object
    unsigned char count=0;
    for(std::vector<RWMutex*>::const_iterator it=order.begin(); it !=order.end(); it++) {
      //ruleIdx begin at 0
      (*it)->rankinrule[(*it)->nrules]=count; // each RWMutex has hits own number of rules, they are all <= EOS_RWMUTEX_ORDER_NRULES
      (*it)->ruleLocalIndexToGlobalIndex[(*it)->nrules++]=ruleIdx;
      count++;
    }
    pthread_rwlock_unlock(&orderChkMgmLock);

    enableordercheckglobal=sav;

    return 0;
  }

  // ---------------------------------------------------------------------------
  //! Reset order checking rules
  // ---------------------------------------------------------------------------
  static void ResetOrderRule()
  {
    bool sav=enableordercheckglobal;
    enableordercheckglobal=false;
    usleep(100000); // let some time to all the threads to finish their book keeping activity regarding order checking

    pthread_rwlock_wrlock(&orderChkMgmLock);
    // remove all the dead threads from the map
    // #### !!!!!!! THIS DOESN'T WORK SO DEAD THREADS ARE NOT REMOVED FROM THE MAP ##### //
    // #### !!!!!!! THIS IS BECAUSE THERE IS NO RELIABLE WAY TO CHECK IF A THREAD IS STILL RUNNING ##### //
    // #### !!!!!!! THIS IS NOT A PROBLEM FOR EOS BECAUSE XROOTD REUSES ITS THREADS ##### //
    // #### !!!!!!! SO THE MAP DOESN'T GO INTO AN INFINITE GROWTH ####################### //
#if 0
    for(auto it=threadOrderCheckResetFlags_static.begin(); it != threadOrderCheckResetFlags_static.end(); it++) {
      if(XrdSysThread::Signal(it->first,0)) { // this line crashes when the threads is no more valid.
        threadOrderCheckResetFlags_static.erase(it);
        it=threadOrderCheckResetFlags_static.begin();
      }
    }
#endif

    // tell the threads to reset the states of the order mask (because it's thread-local)
    for(auto it=threadOrderCheckResetFlags_static.begin(); it != threadOrderCheckResetFlags_static.end(); it++)
      it->second=true;

    // tell all the RWMutex that they are not involved in any order checking anymore
    for(auto rit=rules_static.begin(); rit!= rules_static.end() ;rit++) { // for each rule
      for(auto it=rit->second.begin(); it !=rit->second.end(); it++) { // for each RWMutex involved in that rule
        (*it)->nrules=0; // no rule involved
      }
    }

    // clean the manager side.
    ruleName2Index_static.clear();
    ruleIndex2Name_static.clear();
    rules_static.clear();

    pthread_rwlock_unlock(&orderChkMgmLock);

    enableordercheckglobal=sav;
  }

  // ---------------------------------------------------------------------------
  //! Remove an order checking rule
  // @param $first
  //   name of the rule
  // @return
  //   the number of rules removed (0 or 1)
  //
  // ---------------------------------------------------------------------------
  static int RemoveOrderRule(const std::string &rulename)
  {
    // make a local copy of the rules and remove the rule to remove
    std::map<std::string,std::vector<RWMutex*> > rules=rules_static;
    if(!rules.erase(rulename)) return 0;

    // reset the rules
    ResetOrderRule();

    // add all the rules but the removed one
    for(auto it=rules.begin(); it != rules.end(); it++)
      AddOrderRule(it->first,it->second);

    // one rule was removed
    return 1;
  }


  // ---------------------------------------------------------------------------
  //! Reset the order checking mechanism for the current thread
  // ---------------------------------------------------------------------------
  void ResetCheckOrder()
  {
    // reset the order mask
    for(int k=0;k<EOS_RWMUTEX_ORDER_NRULES;k++) ordermask_staticthread[k]=0;

    // update orderCheckReset_staticthread, this memory should be specific to this thread
    pthread_t tid = XrdSysThread::ID();
    pthread_rwlock_rdlock(&orderChkMgmLock);
    if(threadOrderCheckResetFlags_static.find(tid)==threadOrderCheckResetFlags_static.end()) {
      pthread_rwlock_unlock(&orderChkMgmLock);
      pthread_rwlock_wrlock(&orderChkMgmLock);
      threadOrderCheckResetFlags_static[tid]=false;
    }
    orderCheckReset_staticthread=&threadOrderCheckResetFlags_static[tid];
    pthread_rwlock_unlock(&orderChkMgmLock);
  }

#endif

  // ---------------------------------------------------------------------------
  //! Lock for read
  // ---------------------------------------------------------------------------
  void LockRead()
  {
    EOS_RWMUTEX_CHECKORDER_LOCK;
    EOS_RWMUTEX_TIMER_START;
    if (pthread_rwlock_rdlock(&rwlock))
    { throw "pthread_rwlock_rdlock failed";}
    EOS_RWMUTEX_TIMER_STOP_AND_UPDATE(read);
  }


  // ---------------------------------------------------------------------------
  //! Lock for read allowing to be canceled waiting for a lock
  // ---------------------------------------------------------------------------
  void LockReadCancel() {
    EOS_RWMUTEX_CHECKORDER_LOCK;
    EOS_RWMUTEX_TIMER_START;
#ifndef __APPLE__
    while (1) {
      int rc = pthread_rwlock_timedrdlock(&rwlock, &rlocktime);
      if ( rc ) {
        if (rc == ETIMEDOUT) {
          fprintf(stderr,"=== READ LOCK CANCEL POINT == TID=%llu OBJECT=%llx\n", (unsigned long long)XrdSysThread::ID(), (unsigned long long)this);
          XrdSysThread::SetCancelOn();
          XrdSysThread::CancelPoint();
          XrdSysTimer msSleep;
          msSleep.Wait(100);
          XrdSysThread::SetCancelOff();
        } else {
          fprintf(stderr,"=== READ LOCK EXCEPTION == TID=%llu OBJECT=%llx rc=%d\n", (unsigned long long)XrdSysThread::ID(), (unsigned long long)this,rc);
          throw "pthread_rwlock_timedrdlock failed";
        }
      } else {
        break;
      }
    }
#else
    LockRead();
#endif
    EOS_RWMUTEX_TIMER_STOP_AND_UPDATE(read);
  }


  // ---------------------------------------------------------------------------
  //! Unlock a read lock
  // ---------------------------------------------------------------------------
  void UnLockRead()
  {
    EOS_RWMUTEX_CHECKORDER_UNLOCK;
    if (pthread_rwlock_unlock(&rwlock))
    { throw "pthread_rwlock_unlock failed";}
  }

  // ---------------------------------------------------------------------------
  //! Lock for write
  // ---------------------------------------------------------------------------
  void LockWrite()
  {
    //AtomicInc(writeLockCounter);  // not needed anymore because of the macro EOS_RWMUTEX_TIMER_STOP_AND_UPDATE
    EOS_RWMUTEX_CHECKORDER_LOCK;
    EOS_RWMUTEX_TIMER_START;
    if (blocking)
    {
      // a blocking mutex is just a normal lock for write
      if (pthread_rwlock_wrlock(&rwlock))
      { throw "pthread_rwlock_rdlock failed";}
    }
    else
    {
#ifdef __APPLE__
      // -------------------------------------------------
      // Mac does not support timed mutexes
      // -------------------------------------------------
      if (pthread_rwlock_wrlock(&rwlock))
      { throw "pthread_rwlock_rdlock failed";}
#else
      // a non-blocking mutex tries for few seconds to write lock, then releases
      // this has the side effect, that it allows dead locked readers to jump ahead the lock queue
      while (1)
      {
        int rc = pthread_rwlock_timedwrlock(&rwlock, &wlocktime);
        if ( rc )
        {
          if (rc != ETIMEDOUT)
          {
            fprintf(stderr,"=== WRITE LOCK EXCEPTION == TID=%llu OBJECT=%llx rc=%d\n", (unsigned long long)XrdSysThread::ID(), (unsigned long long)this,rc);
            throw "pthread_rwlock_wrlock failed";
          }
          else
          {
            //fprintf(stderr,"==== WRITE LOCK PENDING ==== TID=%llu OBJECT=%llx\n",(unsigned long long)XrdSysThread::ID(), (unsigned long long)this);
            XrdSysTimer msSleep;
            msSleep.Wait(500);
          }
        }
        else
        {
          //	  fprintf(stderr,"=== WRITE LOCK ACQUIRED  ==== TID=%llu OBJECT=%llx\n",(unsigned long long)XrdSysThread::ID(), (unsigned long long)this);
          break;
        }
      }
#endif
    }
    EOS_RWMUTEX_TIMER_STOP_AND_UPDATE(write);
  }

  // ---------------------------------------------------------------------------
  //! Unlock a write lock
  // ---------------------------------------------------------------------------
  void UnLockWrite()
  {
    EOS_RWMUTEX_CHECKORDER_UNLOCK;
    if (pthread_rwlock_unlock(&rwlock))
    { throw "pthread_rwlock_unlock failed";}
    //    fprintf(stderr,"*** WRITE LOCK RELEASED  **** TID=%llu OBJECT=%llx\n",(unsigned long long)XrdSysThread::ID(), (unsigned long long)this);

  }

  // ---------------------------------------------------------------------------
  //! Lock for write but give up after wlocktime
  // ---------------------------------------------------------------------------
  int TimeoutLockWrite()
  {
    EOS_RWMUTEX_CHECKORDER_LOCK;
#ifdef __APPLE__
    return pthread_rwlock_wrlock(&rwlock);
#else
    return pthread_rwlock_timedwrlock(&rwlock, &wlocktime);
#endif
  }

  // ---------------------------------------------------------------------------
  //! Get Readlock Counter
  // ---------------------------------------------------------------------------
  size_t GetReadLockCounter()
  {
    return AtomicGet(readLockCounter);
  }

  // ---------------------------------------------------------------------------
  //! Get Writelock Counter
  // ---------------------------------------------------------------------------
  size_t GetWriteLockCounter()
  {
    return AtomicGet(writeLockCounter);
  }

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
  RWMutexWriteLock(RWMutex &mutex)
  { Mutex = &mutex; Mutex->LockWrite();}

  // ---------------------------------------------------------------------------
  //! Destructor
  // ---------------------------------------------------------------------------
  ~RWMutexWriteLock()
  { Mutex->UnLockWrite();}
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
  RWMutexReadLock(RWMutex &mutex)
  { Mutex = &mutex; Mutex->LockRead();}

  RWMutexReadLock(RWMutex &mutex, bool allowcancel) {
    if (allowcancel) {
      Mutex = &mutex; Mutex->LockReadCancel();
    } else {
      Mutex = &mutex; Mutex->LockRead();
    }
  }

  // ---------------------------------------------------------------------------
  //! Destructor
  // ---------------------------------------------------------------------------
  ~RWMutexReadLock()
  { Mutex->UnLockRead();}
};

// undefine the timer stuff
#ifdef EOS_INSTRUMENTED_RWMUTEX
#undef EOS_RWMUTEX_TIMER_START
#undef EOS_RWMUTEX_TIMER_STOP_AND_UPDATE
#endif

/*----------------------------------------------------------------------------*/
EOSCOMMONNAMESPACE_END

#endif

