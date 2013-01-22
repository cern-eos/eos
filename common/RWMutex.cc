// ----------------------------------------------------------------------
// File: RWMutex.cc
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

/*----------------------------------------------------------------------------*/
#include "common/RWMutex.hh"
/*----------------------------------------------------------------------------*/


EOSCOMMONNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
#ifdef EOS_INSTRUMENTED_RWMUTEX
size_t RWMutex::cumulatedwaitread_static = 0;
size_t RWMutex::cumulatedwaitwrite_static = 0;
size_t eos::common::RWMutex::maxwaitread_static = 0;
size_t RWMutex::maxwaitwrite_static = 0;
size_t RWMutex::minwaitread_static = 1e12;
size_t RWMutex::minwaitwrite_static = 1e12;
size_t RWMutex::readLockCounterSample_static = 0;
size_t RWMutex::writeLockCounterSample_static = 0;
size_t RWMutex::timingCompensation = 0;
size_t RWMutex::timingLatency = 0;
size_t RWMutex::orderCheckingLatency = 0;
size_t RWMutex::lockUnlockDuration = 0;
int    RWMutex::samplingModulo_static = (int) (0.01 * RAND_MAX);
bool   RWMutex::staticInitialized = false;
bool   RWMutex::enabletimingglobal = false;
bool   RWMutex::enableordercheckglobal = false;
RWMutex::rules_t                     RWMutex::rules_static;
std::map<unsigned char, std::string> RWMutex::ruleIndex2Name_static;
std::map<std::string, unsigned char> RWMutex::ruleName2Index_static;
__thread bool                       *RWMutex::orderCheckReset_staticthread = NULL;
__thread unsigned long               RWMutex::ordermask_staticthread[EOS_RWMUTEX_ORDER_NRULES];
std::map<pthread_t, bool>            RWMutex::threadOrderCheckResetFlags_static;
pthread_rwlock_t                     RWMutex::orderChkMgmLock;

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



RWMutex::RWMutex ()
{
 // ---------------------------------------------------------------------------
 //! Constructor
 // ---------------------------------------------------------------------------

 // by default we are not a blocking write mutex
 blocking = false;
 // try to get write lock in 5 seconds, then release quickly and retry
 wlocktime.tv_sec = 5;
 wlocktime.tv_nsec = 0;
 // try to get read lock in 100ms, otherwise allow this thread to be canceled - used by LockReadCancel
 rlocktime.tv_sec = 0;
 rlocktime.tv_nsec = 1000000;
 readLockCounter = writeLockCounter = 0;
#ifdef EOS_INSTRUMENTED_RWMUTEX
 if (!staticInitialized)
 {
   staticInitialized = true;
   InitializeClass ();
 }
 counter = 0;
 ResetTimingStatistics ();
 enabletiming = false;
 enablesampling = false;
 nrules = 0;


#endif
#ifndef __APPLE__
 pthread_rwlockattr_init (&attr);

 // readers don't go ahead of writers!
 if (pthread_rwlockattr_setkind_np (&attr, PTHREAD_RWLOCK_PREFER_WRITER_NP))
 {
   throw "pthread_rwlockattr_setkind_np failed";
 }
 if (pthread_rwlockattr_setpshared (&attr, PTHREAD_PROCESS_SHARED))
 {
   throw "pthread_rwlockattr_setpshared failed";
 }

 if ((pthread_rwlock_init (&rwlock, &attr)))
 {
   throw "pthread_rwlock_init failed";
 }
}
#else
 pthread_rwlockattr_init (&attr);
 if (pthread_rwlockattr_setpshared (&attr, PTHREAD_PROCESS_SHARED))
 {
   throw "pthread_rwlockattr_setpshared failed";
 }
 if ((pthread_rwlock_init (&rwlock, &attr)))
 {
   throw "pthread_rwlock_init failed";
 }
}
#endif

RWMutex::~RWMutex ()
{
 // ---------------------------------------------------------------------------
 //! Destructor
 // ---------------------------------------------------------------------------

#ifdef EOS_INSTRUMENTED_RWMUTEX
 pthread_rwlock_rdlock (&orderChkMgmLock);
 std::map<std::string, std::vector<RWMutex*> > *rules = NULL;
 for (auto rit = rules_static.begin (); rit != rules_static.end (); rit++)
 { // for each rule
   for (auto it = rit->second.begin (); it != rit->second.end (); it++)
   { // for each RWMutex involved in that rule
     if ((*it) == this)
     {
       if (rules == NULL) rules = new std::map<std::string, std::vector<RWMutex*> >(rules_static);
       rules->erase (rit->first); // remove the rule if it contains this
     }
   }
 }
 pthread_rwlock_unlock (&orderChkMgmLock);

 if (rules != NULL)
 {
   // erase the rules
   ResetOrderRule ();
   // inserts the remaining rules
   for (auto it = rules->begin (); it != rules->end (); it++)
     AddOrderRule (it->first, it->second);
   delete rules;
 }
#endif
}

void
RWMutex::SetBlocking (bool block)
{
 // ---------------------------------------------------------------------------
 //! Set the write lock to blocking or not blocking
 // ---------------------------------------------------------------------------

 blocking = block;
}

void
RWMutex::SetWLockTime (const size_t &nsec)
{
 // ---------------------------------------------------------------------------
 //! Set the time to wait the acquisition of the write mutex before releasing quicky and retrying
 // ---------------------------------------------------------------------------

 wlocktime.tv_sec = nsec / 1000000;
 wlocktime.tv_nsec = nsec % 1000000;
}

#ifdef EOS_INSTRUMENTED_RWMUTEX

void
RWMutex::ResetTimingStatistics ()
{
 // ---------------------------------------------------------------------------
 //! Reset statistics at the instance level
 // ---------------------------------------------------------------------------

 // might need a mutex or at least a flag!!!
 cumulatedwaitread = cumulatedwaitwrite = 0;
 maxwaitread = maxwaitwrite = std::numeric_limits<size_t>::min ();
 minwaitread = minwaitwrite = std::numeric_limits<long long>::max ();
 readLockCounterSample = writeLockCounterSample = 0;
}

void
RWMutex::ResetTimingStatisticsGlobal ()
{
 // ---------------------------------------------------------------------------
 //! Reset statistics at the class level
 // ---------------------------------------------------------------------------

 // might need a mutex or at least a flag!!!
 cumulatedwaitread_static = cumulatedwaitwrite_static = 0;
 maxwaitread_static = maxwaitwrite_static = std::numeric_limits<size_t>::min ();
 minwaitread_static = minwaitwrite_static = std::numeric_limits<long long>::max ();
 readLockCounterSample_static = writeLockCounterSample_static = 0;
}

void
RWMutex::SetTiming (bool on)
{
 // ---------------------------------------------------------------------------
 //! Turn on/off timings at the instance level
 // ---------------------------------------------------------------------------

 enabletiming = on;
}

bool
RWMutex::GetTiming ()
{
 // ---------------------------------------------------------------------------
 //! Get the timing status at the class level
 // ---------------------------------------------------------------------------

 return enabletiming;
}

void
RWMutex::SetTimingGlobal (bool on)
{
 // ---------------------------------------------------------------------------
 //! Turn on/off timings at the class level
 // ---------------------------------------------------------------------------

 enabletimingglobal = on;
}

bool
RWMutex::GetTimingGlobal ()
{

 // ---------------------------------------------------------------------------
 //! Get the timing status  at the class level
 // ---------------------------------------------------------------------------

 return enabletimingglobal;
}

void
RWMutex::SetOrderCheckingGlobal (bool on)
{
 // ---------------------------------------------------------------------------
 //! Turn on/off order checking at the class level
 // ---------------------------------------------------------------------------

 enableordercheckglobal = on;
}

bool
RWMutex::GetOrderCheckingGlobal ()
{
 // ---------------------------------------------------------------------------
 //! Get the order checking status at the class level
 // ---------------------------------------------------------------------------

 return enableordercheckglobal;
}

void
RWMutex::SetDebugName (const std::string &name)
{
 // ---------------------------------------------------------------------------
 //! Set the debug name
 // ---------------------------------------------------------------------------

 debugname = name;
}

#ifdef __APPLE__

int
RWMutex::round (double number)
{
 return number < 0.0 ? ceil (number - 0.5) : floor (number + 0.5);
}
#endif

/* ---------------------------------------------------------------------------
  Enable sampling of timings
  @param $first
   turns on or off the sampling
  @param $second
   sampling between 0 and 1 (if <0, use the precomputed level for the class, see GetSamplingRateFromCPUOverhead)
   --------------------------------------------------------------------------- */

void
RWMutex::SetSampling (bool on, float rate)
{
 enablesampling = on;
 ResetTimingStatistics ();
 if (rate < 0)
   samplingModulo = samplingModulo_static;
 else
#ifdef __APPLE__
   samplingModulo = std::min (RAND_MAX, std::max (0, (int) round (1.0 / rate)));
#else
   samplingModulo = std::min (RAND_MAX, std::max (0, (int) std::round (1.0 / rate)));
#endif
}

/* ---------------------------------------------------------------------------
   Return the timing sampling rate/status
   @return the sample rate if the sampling is turned on, -1.0 if the sampling is off
   --------------------------------------------------------------------------- */

float
RWMutex::GetSampling ()
{
 if (!enablesampling) return -1.0;
 else return 1.0 / samplingModulo;
}

void
RWMutex::GetTimingStatistics (RWMutexTimingStats &stats, bool compensate)
{
 // ---------------------------------------------------------------------------
 //! Get the timing statistics at the instance level
 // ---------------------------------------------------------------------------

 size_t compensation = compensate ? timingCompensation : 0;
 stats.readLockCounterSample = AtomicGet (readLockCounterSample);
 stats.writeLockCounterSample = AtomicGet (writeLockCounterSample);

 stats.averagewaitread = 0;
 if (AtomicGet (readLockCounterSample) != 0)
 {
   double avg = (double(AtomicGet (cumulatedwaitread)) / AtomicGet (readLockCounterSample) - compensation);
   if (avg > 0)
     stats.averagewaitread = avg;
 }
 stats.averagewaitwrite = 0;
 if (AtomicGet (writeLockCounterSample) != 0)
 {
   double avg = (double(AtomicGet (cumulatedwaitwrite)) / AtomicGet (writeLockCounterSample) - compensation);
   if (avg > 0)
     stats.averagewaitwrite = avg;
 }
 if (AtomicGet (minwaitread) != std::numeric_limits<size_t>::max ())
 {
   long long compensated = AtomicGet (minwaitread) - compensation;
   if (compensated > 0)
     stats.minwaitread = compensated;
   else
     stats.minwaitread = 0;
 }
 else
   stats.minwaitread = std::numeric_limits<long long>::max ();
 if (AtomicGet (maxwaitread) != std::numeric_limits<size_t>::min ())
 {
   long long compensated = AtomicGet (maxwaitread) - compensation;
   if (compensated > 0)
     stats.maxwaitread = compensated;
   else
     stats.maxwaitread = 0;
 }
 else
   stats.maxwaitread = std::numeric_limits<size_t>::min ();
 if (AtomicGet (minwaitwrite) != std::numeric_limits<size_t>::max ())
 {
   long long compensated = AtomicGet (minwaitwrite) - compensation;
   if (compensated > 0)
     stats.minwaitwrite = compensated;
   else
     stats.minwaitwrite = 0;
 }
 else
   stats.minwaitwrite = std::numeric_limits<long long>::max ();
 if (AtomicGet (maxwaitwrite) != std::numeric_limits<size_t>::min ())
 {
   long long compensated = AtomicGet (maxwaitwrite) - compensation;
   if (compensated > 0)
     stats.maxwaitwrite = compensated;
   else
     stats.maxwaitwrite = 0;
 }
 else
   stats.maxwaitwrite = std::numeric_limits<size_t>::min ();
}

void
RWMutex::OrderViolationMessage (unsigned char rule, const std::string &message)
{
 // ---------------------------------------------------------------------------
 //! Check the orders defined by the rules and update
 // ---------------------------------------------------------------------------

 void *array[10];
 size_t size;
 unsigned long threadid = XrdSysThread::Num ();

 // get void*'s for all entries on the stack
 size = backtrace (array, 10);

 const std::string &rulename = ruleIndex2Name_static[ruleLocalIndexToGlobalIndex[rule]];
 fprintf (stderr, "RWMutex: Order Checking Error in thread %lu\n %s\n In rule %s :\nLocking Order should be:\n",
          threadid,
          message.c_str (),
          rulename.c_str ());
 std::vector<RWMutex*> order = rules_static[rulename];
 for (std::vector<RWMutex*>::iterator ito = order.begin (); ito != order.end (); ito++)
   fprintf (stderr, "\t%12s (%p)", (*ito)->debugname.c_str (), (*ito));

 fprintf (stderr, "\nThe lock states of these mutexes are (before the violating lock/unlock) :\n");
 for (unsigned char k = 0; k < order.size (); k++)
 {
   unsigned long int mask = (1 << k);
   fprintf (stderr, "\t%d", int( (ordermask_staticthread[rule] & mask) != 0));
 }
 fprintf (stderr, "\n");

 backtrace_symbols_fd (array, size, 2);
}

void
RWMutex::CheckAndLockOrder ()
{
 // ---------------------------------------------------------------------------
 //! Check the orders defined by the rules and update for a lock
 // ---------------------------------------------------------------------------

 // initialize the thread local ordermask if not already done
 if (orderCheckReset_staticthread == NULL)
 {
   ResetCheckOrder ();
 }
 if (*orderCheckReset_staticthread)
 {
   ResetCheckOrder ();
   *orderCheckReset_staticthread = false;
 }

 for (unsigned char k = 0; k < nrules; k++)
 {
   unsigned long int mask = (1 << rankinrule[k]);
   // check if following mutex is already locked in the same thread
   if (ordermask_staticthread[k] >= mask)
   {
     char strmess[1024];
     sprintf (strmess, "locking %s at address %p", debugname.c_str (), this);
     OrderViolationMessage (k, strmess);
   }
   ordermask_staticthread[k] |= mask;
 }
}

void
RWMutex::CheckAndUnlockOrder ()
{
 // ---------------------------------------------------------------------------
 //! Get the timing statistics at the instance level for an unlock
 // ---------------------------------------------------------------------------

 // initialize the thread local ordermask if not already done
 if (orderCheckReset_staticthread == NULL)
   ResetCheckOrder ();
 if (*orderCheckReset_staticthread)
 {
   ResetCheckOrder ();
   *orderCheckReset_staticthread = false;
 }

 for (unsigned char k = 0; k < nrules; k++)
 {
   unsigned long int mask = (1 << rankinrule[k]);
   // check if following mutex is already locked in the same thread
   if (ordermask_staticthread[k] >= (mask << 1))
   {
     char strmess[1024];
     sprintf (strmess, "unlocking %s at address %p", debugname.c_str (), this);
     OrderViolationMessage (k, strmess);
   }
   ordermask_staticthread[k] &= (~mask);
 }
}

void
RWMutex::GetTimingStatisticsGlobal (RWMutexTimingStats &stats, bool compensate)
{
 // ---------------------------------------------------------------------------
 //! Get the timing statistics at the class level
 // ---------------------------------------------------------------------------

 size_t compensation = compensate ? timingCompensation : 0;
 stats.readLockCounterSample = AtomicGet (readLockCounterSample_static);
 stats.writeLockCounterSample = AtomicGet (writeLockCounterSample_static);

 stats.averagewaitread = 0;
 if (AtomicGet (readLockCounterSample_static) != 0)
 {
   double avg = (double(AtomicGet (cumulatedwaitread_static)) / AtomicGet (readLockCounterSample_static) - compensation);
   if (avg > 0)
     stats.averagewaitread = avg;
 }
 stats.averagewaitwrite = 0;
 if (AtomicGet (writeLockCounterSample_static) != 0)
 {
   double avg = (double(AtomicGet (cumulatedwaitwrite_static)) / AtomicGet (writeLockCounterSample_static) - compensation);
   if (avg > 0)
     stats.averagewaitwrite = avg;
 }
 if (AtomicGet (minwaitread_static) != std::numeric_limits<size_t>::max ())
 {
   long long compensated = AtomicGet (minwaitread_static) - compensation;
   if (compensated > 0)
     stats.minwaitread = compensated;
   else
     stats.minwaitread = 0;
 }
 else
   stats.minwaitread = std::numeric_limits<long long>::max ();
 if (AtomicGet (maxwaitread_static) != std::numeric_limits<size_t>::min ())
 {
   long long compensated = AtomicGet (maxwaitread_static) - compensation;
   if (compensated > 0)
     stats.maxwaitread = compensated;
   else
     stats.maxwaitread = 0;
 }
 else
   stats.maxwaitread = std::numeric_limits<size_t>::min ();
 if (AtomicGet (minwaitwrite_static) != std::numeric_limits<size_t>::max ())
 {
   long long compensated = AtomicGet (minwaitwrite_static) - compensation;
   if (compensated > 0)
     stats.minwaitwrite = compensated;
   else
     stats.minwaitwrite = 0;
 }
 else
   stats.minwaitwrite = std::numeric_limits<long long>::max ();
 if (AtomicGet (maxwaitwrite_static) != std::numeric_limits<size_t>::min ())
 {
   long long compensated = AtomicGet (maxwaitwrite_static) - compensation;
   if (compensated > 0)
     stats.maxwaitwrite = compensated;
   else
     stats.maxwaitwrite = 0;
 }
 else
   stats.maxwaitwrite = std::numeric_limits<size_t>::min ();
}

// ---------------------------------------------------------------------------
/**
 * Compute the SamplingRate corresponding to a given CPU overhead
 *  @param $first
 *  the ratio between the timing cost and the mutexing cost
 *  @return
 *   sampling rate (the ratio of mutex to time so that the argument value is not violated)
 */
// ---------------------------------------------------------------------------

float
RWMutex::GetSamplingRateFromCPUOverhead (const double &overhead)
{
 RWMutex mutex;
 bool entimglobbak = enabletimingglobal;

 mutex.SetTiming (true);
 mutex.SetSampling (true, 1.0);
 RWMutex::SetTimingGlobal (true);
 size_t monitoredTiming = NowInt ();
 for (int k = 0; k < 1e6; k++)
 {
   mutex.LockWrite ();
   mutex.UnLockWrite ();
 }
 monitoredTiming = NowInt () - monitoredTiming;

 mutex.SetTiming (false);
 mutex.SetSampling (false);
 RWMutex::SetTimingGlobal (false);
 size_t unmonitoredTiming = NowInt ();
 for (int k = 0; k < 1e6; k++)
 {
   mutex.LockWrite ();
   mutex.UnLockWrite ();
 }
 unmonitoredTiming = NowInt () - unmonitoredTiming;

 RWMutex::SetTimingGlobal (entimglobbak);

 float mutexShare = unmonitoredTiming;
 float timingShare = monitoredTiming - unmonitoredTiming;
 float samplingRate = std::min (1.0, std::max (0.0, overhead * mutexShare / timingShare));
 samplingModulo_static = (int) (1.0 / samplingRate);

 return samplingRate;
}

// ---------------------------------------------------------------------------
/**
 * @param $first
 *   the size of the loop to estimate the compensation (default 1e7)
 * @return
 *   the compensation in nanoseconds
 */
// ---------------------------------------------------------------------------

size_t
RWMutex::EstimateTimingCompensation (size_t loopsize)
{
 size_t t = NowInt ();
 for (unsigned long k = 0; k < loopsize; k++)
 {
   struct timespec ts;
   eos::common::Timing::GetTimeSpec (ts);
 }
 t = NowInt () - t;

 return size_t (double(t) / loopsize);
}


// ---------------------------------------------------------------------------
/**
 *  Compute the speed for lock/unlock cycle
 *  @param $first
 *    the size of the loop to estimate the compensation (default 1e6)
 *  @return
 *    the duration of the cycle in nanoseconds 
 */
// ---------------------------------------------------------------------------

size_t
RWMutex::EstimateLockUnlockDuration (size_t loopsize)
{
 RWMutex mutex;

 bool sav = enabletimingglobal;
 bool sav2 = enableordercheckglobal;
 RWMutex::SetTimingGlobal (false);
 RWMutex::SetOrderCheckingGlobal (false);
 mutex.SetTiming (false);
 mutex.SetSampling (false);
 size_t t = NowInt ();
 for (size_t k = 0; k < loopsize; k++)
 {
   mutex.LockWrite ();
   mutex.UnLockWrite ();
 }
 t = NowInt () - t;
 enabletimingglobal = sav;
 enableordercheckglobal = sav2;

 return size_t (double(t) / loopsize);
}

// ---------------------------------------------------------------------------
/**
 * Compute the latency introduced by taking timings
 * @param $first
 *   the size of the loop to estimate the compensation (default 1e6)
 * @param $second
 *   enable global timing in the estimation (default false)
 * @return
 *   the latency in nanoseconds
 */
// ---------------------------------------------------------------------------

size_t
RWMutex::EstimateTimingAddedLatency (size_t loopsize, bool globaltiming)
{
 RWMutex mutex;

 bool sav = enabletimingglobal;
 bool sav2 = enableordercheckglobal;
 RWMutex::SetTimingGlobal (globaltiming);
 RWMutex::SetOrderCheckingGlobal (false);
 mutex.SetTiming (true);
 mutex.SetSampling (true, 1.0);
 size_t t = NowInt ();
 for (size_t k = 0; k < loopsize; k++)
 {
   mutex.LockWrite ();
   mutex.UnLockWrite ();
 }
 size_t s = NowInt () - t;

 RWMutex::SetTimingGlobal (false);
 mutex.SetTiming (false);
 mutex.SetSampling (false);
 t = NowInt ();
 for (size_t k = 0; k < loopsize; k++)
 {
   mutex.LockWrite ();
   mutex.UnLockWrite ();
 }
 t = NowInt () - t;
 enabletimingglobal = sav;
 enableordercheckglobal = sav2;

 return size_t (double(s - t) / loopsize);
}

// ---------------------------------------------------------------------------
/**
 * Compute the latency introduced by checking the mutexes locking orders
 * @param $first
 *   the number of nested mutexes in the loop
 * @param $second
 *   the size of the loop to estimate the compensation (default 1e6)
 * @return
 *   the latency in nanoseconds
 */
// ---------------------------------------------------------------------------

size_t
RWMutex::EstimateOrderCheckingAddedLatency (size_t nmutexes, size_t loopsize)
{
 std::vector<RWMutex> mutexes;
 mutexes.resize (nmutexes);
 std::vector<RWMutex*> order;
 order.resize (nmutexes);

 int count = 0;
 for (std::vector<RWMutex>::iterator it = mutexes.begin (); it != mutexes.end (); it++)
 {
   it->SetTiming (false);
   it->SetSampling (false);
   order[count++] = &(*it);
 }
 RWMutex::AddOrderRule ("estimaterule", order);

 bool sav = enabletimingglobal;
 bool sav2 = enableordercheckglobal;
 RWMutex::SetTimingGlobal (false);
 RWMutex::SetOrderCheckingGlobal (true);

 std::vector<RWMutex>::iterator it;
 std::vector<RWMutex>::reverse_iterator rit;
 size_t t = NowInt ();
 for (size_t k = 0; k < loopsize; k++)
 {
   for (it = mutexes.begin (); it != mutexes.end (); it++)
   {
     it->LockWrite ();
   }
   for (rit = mutexes.rbegin (); rit != mutexes.rend (); rit++)
   {
     rit->UnLockWrite ();
   }
 }
 size_t s = NowInt () - t;

 RWMutex::SetOrderCheckingGlobal (false);
 t = NowInt ();
 for (size_t k = 0; k < loopsize; k++)
 {
   for (it = mutexes.begin (); it != mutexes.end (); it++)
   {
     it->LockWrite ();
   }
   for (rit = mutexes.rbegin (); rit != mutexes.rend (); rit++)
   {
     rit->UnLockWrite ();
   }
 }
 t = NowInt () - t;
 enabletimingglobal = sav;
 enableordercheckglobal = sav2;

 RemoveOrderRule ("estimaterule");

 return size_t (double(s - t) / (loopsize * nmutexes));
}

void
RWMutex::InitializeClass ()
{
 // ---------------------------------------------------------------------------
 //! Performs the initialization of the class
 // ---------------------------------------------------------------------------

 if (pthread_rwlock_init (&orderChkMgmLock, NULL))
 {
   throw "pthread_orderChkMgmLock_init failed";
 }
}

void
RWMutex::EstimateLatenciesAndCompensation (size_t loopsize)
{
 timingCompensation = EstimateTimingCompensation (loopsize);
 timingLatency = EstimateTimingAddedLatency (loopsize);
 orderCheckingLatency = EstimateOrderCheckingAddedLatency (3, loopsize);
 lockUnlockDuration = EstimateLockUnlockDuration (loopsize);
 //std::cerr<< " timing compensation = "<<timingCompensation<<std::endl;
 //std::cerr<< " timing latency = "<<timingLatency<<std::endl;
 //std::cerr<< " order  latency = "<<orderCheckingLatency<<std::endl;
 //std::cerr<< " lock/unlock duration = "<<lockUnlockDuration<<std::endl;
}

size_t
RWMutex::GetTimingCompensation ()
{
 return timingCompensation; // in nsec
}

size_t
RWMutex::GetOrderCheckingLatency ()
{
 return orderCheckingLatency; // in nsec
}

size_t
RWMutex::GetTimingLatency ()
{
 return timingLatency; // in nsec
}

size_t
RWMutex::GetLockUnlockDuration ()
{
 return lockUnlockDuration; // in nsec
}


// ---------------------------------------------------------------------------
/**
 * Add or overwrite an order checking rule
 * @param $first
 *   name of the rule
 * @param $second
 *   a vector contaning the adress of the RWMutex instances in the locking order
 * @return
 */
// ---------------------------------------------------------------------------

int
RWMutex::AddOrderRule (const std::string &rulename, const std::vector<RWMutex*> &order)
{
 bool sav = enableordercheckglobal;
 enableordercheckglobal = false;
 usleep (100000); // let the time to all the threads to finish their bookkeeping activity regarding order checking

 pthread_rwlock_wrlock (&orderChkMgmLock);

 // if we reached the max number of rules, ignore
 if (rules_static.size () == EOS_RWMUTEX_ORDER_NRULES || order.size () > 63)
 {
   enableordercheckglobal = sav;
   pthread_rwlock_unlock (&orderChkMgmLock);
   return -1;
 }

 rules_static[rulename] = order;
 int ruleIdx = rules_static.size () - 1;

 // update the maps
 ruleName2Index_static[rulename] = ruleIdx;
 ruleIndex2Name_static[ruleIdx] = rulename;

 // update each object
 unsigned char count = 0;
 for (std::vector<RWMutex*>::const_iterator it = order.begin (); it != order.end (); it++)
 {
   //ruleIdx begin at 0
   (*it)->rankinrule[(*it)->nrules] = count; // each RWMutex has hits own number of rules, they are all <= EOS_RWMUTEX_ORDER_NRULES
   (*it)->ruleLocalIndexToGlobalIndex[(*it)->nrules++] = ruleIdx;
   count++;
 }
 pthread_rwlock_unlock (&orderChkMgmLock);

 enableordercheckglobal = sav;

 return 0;
}

void
RWMutex::ResetOrderRule ()
{
 // ---------------------------------------------------------------------------
 //! Reset order checking rules
 // ---------------------------------------------------------------------------

 bool sav = enableordercheckglobal;
 enableordercheckglobal = false;
 usleep (100000); // let some time to all the threads to finish their book keeping activity regarding order checking

 pthread_rwlock_wrlock (&orderChkMgmLock);
 // remove all the dead threads from the map
 // #### !!!!!!! THIS DOESN'T WORK SO DEAD THREADS ARE NOT REMOVED FROM THE MAP ##### //
 // #### !!!!!!! THIS IS BECAUSE THERE IS NO RELIABLE WAY TO CHECK IF A THREAD IS STILL RUNNING ##### //
 // #### !!!!!!! THIS IS NOT A PROBLEM FOR EOS BECAUSE XROOTD REUSES ITS THREADS ##### //
 // #### !!!!!!! SO THE MAP DOESN'T GO INTO AN INFINITE GROWTH ####################### //
#if 0
 for (auto it = threadOrderCheckResetFlags_static.begin (); it != threadOrderCheckResetFlags_static.end (); it++)
 {
   if (XrdSysThread::Signal (it->first, 0))
   { // this line crashes when the threads is no more valid.
     threadOrderCheckResetFlags_static.erase (it);
     it = threadOrderCheckResetFlags_static.begin ();
   }
 }
#endif

 // tell the threads to reset the states of the order mask (because it's thread-local)
 for (auto it = threadOrderCheckResetFlags_static.begin (); it != threadOrderCheckResetFlags_static.end (); it++)
   it->second = true;

 // tell all the RWMutex that they are not involved in any order checking anymore
 for (auto rit = rules_static.begin (); rit != rules_static.end (); rit++)
 { // for each rule
   for (auto it = rit->second.begin (); it != rit->second.end (); it++)
   { // for each RWMutex involved in that rule
     (*it)->nrules = 0; // no rule involved
   }
 }

 // clean the manager side.
 ruleName2Index_static.clear ();
 ruleIndex2Name_static.clear ();
 rules_static.clear ();

 pthread_rwlock_unlock (&orderChkMgmLock);

 enableordercheckglobal = sav;
}

// ---------------------------------------------------------------------------
/**
 * Remove an order checking rule
 * @param $first
 *  name of the rule
 * @return
 *  the number of rules removed (0 or 1)
 */
// ---------------------------------------------------------------------------

int
RWMutex::RemoveOrderRule (const std::string &rulename)
{
 // make a local copy of the rules and remove the rule to remove
 std::map<std::string, std::vector<RWMutex*> > rules = rules_static;
 if (!rules.erase (rulename)) return 0;

 // reset the rules
 ResetOrderRule ();

 // add all the rules but the removed one
 for (auto it = rules.begin (); it != rules.end (); it++)
   AddOrderRule (it->first, it->second);

 // one rule was removed
 return 1;
}

void
RWMutex::ResetCheckOrder ()
{
 // ---------------------------------------------------------------------------
 //! Reset the order checking mechanism for the current thread
 // ---------------------------------------------------------------------------

 // reset the order mask
 for (int k = 0; k < EOS_RWMUTEX_ORDER_NRULES; k++) ordermask_staticthread[k] = 0;

 // update orderCheckReset_staticthread, this memory should be specific to this thread
 pthread_t tid = XrdSysThread::ID ();
 pthread_rwlock_rdlock (&orderChkMgmLock);
 if (threadOrderCheckResetFlags_static.find (tid) == threadOrderCheckResetFlags_static.end ())
 {
   pthread_rwlock_unlock (&orderChkMgmLock);
   pthread_rwlock_wrlock (&orderChkMgmLock);
   threadOrderCheckResetFlags_static[tid] = false;
 }
 orderCheckReset_staticthread = &threadOrderCheckResetFlags_static[tid];
 pthread_rwlock_unlock (&orderChkMgmLock);
}

#endif

void
RWMutex::LockRead ()
{
 // ---------------------------------------------------------------------------
 //! Lock for read
 // ---------------------------------------------------------------------------

 EOS_RWMUTEX_CHECKORDER_LOCK;
 EOS_RWMUTEX_TIMER_START;
 if (pthread_rwlock_rdlock (&rwlock))
 {
   throw "pthread_rwlock_rdlock failed";
 }
 EOS_RWMUTEX_TIMER_STOP_AND_UPDATE (read);
}

void
RWMutex::LockReadCancel ()
{
 // ---------------------------------------------------------------------------
 //! Lock for read allowing to be canceled waiting for a lock
 // ---------------------------------------------------------------------------

 EOS_RWMUTEX_CHECKORDER_LOCK;
 EOS_RWMUTEX_TIMER_START;
#ifndef __APPLE__
 while (1)
 {
   int rc = pthread_rwlock_timedrdlock (&rwlock, &rlocktime);
   if (rc)
   {
     if (rc == ETIMEDOUT)
     {
       fprintf (stderr, "=== READ LOCK CANCEL POINT == TID=%llu OBJECT=%llx\n", (unsigned long long) XrdSysThread::ID (), (unsigned long long) this);
       XrdSysThread::SetCancelOn ();
       XrdSysThread::CancelPoint ();
       XrdSysTimer msSleep;
       msSleep.Wait (100);
       XrdSysThread::SetCancelOff ();
     }
     else
     {
       fprintf (stderr, "=== READ LOCK EXCEPTION == TID=%llu OBJECT=%llx rc=%d\n", (unsigned long long) XrdSysThread::ID (), (unsigned long long) this, rc);
       throw "pthread_rwlock_timedrdlock failed";
     }
   }
   else
   {
     break;
   }
 }
#else
 LockRead ();
#endif
 EOS_RWMUTEX_TIMER_STOP_AND_UPDATE (read);
}

void
RWMutex::UnLockRead ()
{
 // ---------------------------------------------------------------------------
 //! Unlock a read lock
 // ---------------------------------------------------------------------------

 EOS_RWMUTEX_CHECKORDER_UNLOCK;
 if (pthread_rwlock_unlock (&rwlock))
 {
   throw "pthread_rwlock_unlock failed";
 }
}

void
RWMutex::LockWrite ()
{
 // ---------------------------------------------------------------------------
 //! Lock for write
 // ---------------------------------------------------------------------------

 //AtomicInc(writeLockCounter);  // not needed anymore because of the macro EOS_RWMUTEX_TIMER_STOP_AND_UPDATE
 EOS_RWMUTEX_CHECKORDER_LOCK;
 EOS_RWMUTEX_TIMER_START;
 if (blocking)
 {
   // a blocking mutex is just a normal lock for write
   if (pthread_rwlock_wrlock (&rwlock))
   {
     throw "pthread_rwlock_rdlock failed";
   }
 }
 else
 {
#ifdef __APPLE__
   // -------------------------------------------------
   // Mac does not support timed mutexes
   // -------------------------------------------------
   if (pthread_rwlock_wrlock (&rwlock))
   {
     throw "pthread_rwlock_rdlock failed";
   }
#else
   // a non-blocking mutex tries for few seconds to write lock, then releases
   // this has the side effect, that it allows dead locked readers to jump ahead the lock queue
   while (1)
   {
     int rc = pthread_rwlock_timedwrlock (&rwlock, &wlocktime);
     if (rc)
     {
       if (rc != ETIMEDOUT)
       {
         fprintf (stderr, "=== WRITE LOCK EXCEPTION == TID=%llu OBJECT=%llx rc=%d\n", (unsigned long long) XrdSysThread::ID (), (unsigned long long) this, rc);
         throw "pthread_rwlock_wrlock failed";
       }
       else
       {
         //fprintf(stderr,"==== WRITE LOCK PENDING ==== TID=%llu OBJECT=%llx\n",(unsigned long long)XrdSysThread::ID(), (unsigned long long)this);
         XrdSysTimer msSleep;
         msSleep.Wait (500);
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
 EOS_RWMUTEX_TIMER_STOP_AND_UPDATE (write);
}

void
RWMutex::UnLockWrite ()
{
 // ---------------------------------------------------------------------------
 //! Unlock a write lock
 // ---------------------------------------------------------------------------

 EOS_RWMUTEX_CHECKORDER_UNLOCK;
 if (pthread_rwlock_unlock (&rwlock))
 {
   throw "pthread_rwlock_unlock failed";
 }
 //    fprintf(stderr,"*** WRITE LOCK RELEASED  **** TID=%llu OBJECT=%llx\n",(unsigned long long)XrdSysThread::ID(), (unsigned long long)this);

}

int
RWMutex::TimeoutLockWrite ()
{
 // ---------------------------------------------------------------------------
 //! Lock for write but give up after wlocktime
 // ---------------------------------------------------------------------------

 EOS_RWMUTEX_CHECKORDER_LOCK;
#ifdef __APPLE__
 return pthread_rwlock_wrlock (&rwlock);
#else
 return pthread_rwlock_timedwrlock (&rwlock, &wlocktime);
#endif
}

size_t
RWMutex::GetReadLockCounter ()
{
 // ---------------------------------------------------------------------------
 //! Get Readlock Counter
 // ---------------------------------------------------------------------------

 return AtomicGet (readLockCounter);
}

size_t
RWMutex::GetWriteLockCounter ()
{
 // ---------------------------------------------------------------------------
 //! Get Writelock Counter
 // ---------------------------------------------------------------------------

 return AtomicGet (writeLockCounter);
}

RWMutexWriteLock::RWMutexWriteLock (RWMutex &mutex)
{
 // ---------------------------------------------------------------------------
 //! Constructor
 // ---------------------------------------------------------------------------

 Mutex = &mutex;
 Mutex->LockWrite ();
}

RWMutexWriteLock::~RWMutexWriteLock ()
{
 // ---------------------------------------------------------------------------
 //! Destructor
 // ---------------------------------------------------------------------------

 Mutex->UnLockWrite ();
}

RWMutexReadLock::RWMutexReadLock (RWMutex &mutex)
{
 // ---------------------------------------------------------------------------
 //! Constructor
 // ---------------------------------------------------------------------------

 Mutex = &mutex;
 Mutex->LockRead ();
}

RWMutexReadLock::RWMutexReadLock (RWMutex &mutex, bool allowcancel)
{
 // ---------------------------------------------------------------------------
 //! Constructor
 // ---------------------------------------------------------------------------

 if (allowcancel)
 {
   Mutex = &mutex;
   Mutex->LockReadCancel ();
 }
 else
 {
   Mutex = &mutex;
   Mutex->LockRead ();
 }
}

RWMutexReadLock::~RWMutexReadLock ()
{
 // ---------------------------------------------------------------------------
 //! Destructor
 // ---------------------------------------------------------------------------

 Mutex->UnLockRead ();
}

EOSCOMMONNAMESPACE_END
