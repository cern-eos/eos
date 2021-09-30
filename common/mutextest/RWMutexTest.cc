// ----------------------------------------------------------------------
// File: RWMutexTest.cc
// Author: Geoffray Adde - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2012 CERN/Switzerland                                  *
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

//----------------------------------------------------------------------------
// Program demonstrating the timing facilities of the RWMutex class
//----------------------------------------------------------------------------
#include "common/RWMutex.hh"
#include "common/Timing.hh"
#include <iostream>
#include <thread>

using namespace eos::common;
const int loopsize = 10e6;
const unsigned long int NNUM_THREADS = 10;
pthread_t threads[NNUM_THREADS];
unsigned long int NUM_THREADS = NNUM_THREADS;
RWMutex globmutex;
RWMutex gm1, gm2, gm3;

//----------------------------------------------------------------------------
//! Output to stream operator for TimingStats structure
//!
//! @param os output strream
//! @param stats structure to serialize
//!
//! @return reference to output stream
//----------------------------------------------------------------------------
inline std::ostream& operator << (std::ostream& os,
                                  const RWMutex::TimingStats& stats)
{
  os << "\t" << "RWMutex Read  Wait (number : min , avg , max)" << " = "
     << stats.readLockCounterSample << " : " << stats.minwaitread << " , "
     << stats.averagewaitread << " , " << stats.maxwaitread << std::endl
     << "\t" << "RWMutex Write Wait (number : min , avg , max)" << " = "
     << stats.writeLockCounterSample << " : " << stats.minwaitwrite << " , "
     << stats.averagewaitwrite << " , " << stats.maxwaitwrite << std::endl;
  return os;
}

typedef void* (*TestFuncT)(void*);

//----------------------------------------------------------------------------
// Test function ran by a thread
//----------------------------------------------------------------------------
void*
TestThread(void* threadid)
{
  unsigned long int tid = *(unsigned long int*)threadid;

  if (tid % 2) {
    for (int k = 0; k < loopsize / (int) NUM_THREADS; k++) {
      globmutex.LockWrite();
      globmutex.UnLockWrite();
    }
  } else {
    for (int k = 0; k < loopsize / (int) NUM_THREADS; k++) {
      globmutex.LockRead();
      globmutex.UnLockRead();
    }
  }

  pthread_exit(NULL);
  return NULL;
}

//----------------------------------------------------------------------------
// Function to run all threads
//----------------------------------------------------------------------------
void
RunThreads(TestFuncT func)
{
  for (unsigned long int t = 0; t < NUM_THREADS; t++) {
    int rc = pthread_create(&threads[t], NULL, func, (void*) &t);

    if (rc) {
      printf("ERROR; return code from pthread_create() is %d\n", rc);
      exit(-1);
    }
  }

  void* ret;

  for (unsigned long int t = 0; t < NUM_THREADS; t++) {
    pthread_join(threads[t], &ret);
  }
}

//----------------------------------------------------------------------------
// Test function 2 ran by a thread
//----------------------------------------------------------------------------
void*
TestThread2(void* threadid)
{
  int tid = (int) XrdSysThread::Num();

  for (int k = 0; k < loopsize / (int) NUM_THREADS; ++k) {
    if (k == tid) {
      std::cout << "!!!!!!!! Thread " << tid << " triggers an incorrect "
                << "lock/unlock order ON PURPOSE at iteration " << k
                << " !!!!!!!!" << std::endl;
      gm1.LockWrite();
      gm3.LockWrite();
      gm2.LockWrite();
      gm2.UnLockWrite();
      gm3.UnLockWrite(); // swapped with previous one
      gm1.UnLockWrite();
    } else {
      gm1.LockWrite();
      gm2.LockWrite();
      gm3.LockWrite();
      gm3.UnLockWrite();
      gm2.UnLockWrite();
      gm1.UnLockWrite();
    }
  }

  return NULL;
}

//----------------------------------------------------------------------------
// Main function
//----------------------------------------------------------------------------
int
main()
{
  RWMutex::SetOrderCheckingGlobal(false);
  std::cout << " Using Instrumented Version of RWMutex class" << std::endl;
  RWMutex::EstimateLatenciesAndCompensation();
  size_t t = Timing::GetNowInNs();

  for (int k = 0; k < loopsize; ++k) {
    struct timespec ts;
    eos::common::Timing::GetTimeSpec(ts);
  }

  t = Timing::GetNowInNs() - t;
  std::cout << " ------------------------- " << std::endl;
  std::cout << " Measuring speed of function clock_gettime() " << std::endl;
  std::cout << " Monothreaded Loop of size " << double(loopsize) << " took " <<
            t /
            1.0e9 << " sec" << " (" << double(loopsize) / (t / 1.0e9) << "Hz" << ")" <<
            std::endl;
  std::cout << " ------------------------- " << std::endl << std::endl;
  RWMutex::SetTimingGlobal(true);
  RWMutex mutex, mutex2;
  mutex.SetTiming(true);
  t = Timing::GetNowInNs();

  for (int k = 0; k < loopsize; ++k) {
    mutex.LockWrite();
    mutex.UnLockWrite();
  }

  t = Timing::GetNowInNs() - t;
  RWMutex::TimingStats stats;
  mutex.GetTimingStatistics(stats);
  std::cout << " ------------------------- " << std::endl;
  std::cout << " Monothreaded Loop of size " << double(loopsize) << " took " <<
            t /
            1.0e9 << " sec" << " (" << double(loopsize) / (t / 1.0e9) << "Hz" << ")" <<
            std::endl;
  std::cout << stats;
  std::cout << " ------------------------- " << std::endl << std::endl;
  float rate = RWMutex::GetSamplingRateFromCPUOverhead(0.033);
  std::cout << " suggested sample rate is " << rate << std::endl << std::endl;
  mutex2.SetTiming(true);
  mutex2.SetSampling(true);
  t = Timing::GetNowInNs();

  for (int k = 0; k < loopsize; ++k) {
    mutex2.LockWrite();
    mutex2.UnLockWrite();
  }

  t = Timing::GetNowInNs() - t;
  mutex2.GetTimingStatistics(stats);
  std::cout << " ------------------------- " << std::endl;
  std::cout << " Monothreaded Loop of size " << double(loopsize) <<
            " with a sample rate of " << rate << " took " << t / 1.0e9 << " sec" << " (" <<
            double(loopsize) / (t / 1.0e9) << "Hz" << ")" << std::endl;
  std::cout << stats;
  std::cout << " ------------------------- " << std::endl << std::endl;
  RWMutex mutex3;
  // By default no local timing, but global timing with samplerate of 1
  RWMutex::SetTimingGlobal(false);
  t = Timing::GetNowInNs();

  for (int k = 0; k < loopsize; ++k) {
    mutex3.LockWrite();
    mutex3.UnLockWrite();
  }

  t = Timing::GetNowInNs() - t;
  std::cout << " ------------------------- " << std::endl;
  std::cout << " Monothreaded Loop of size " << double(loopsize) <<
            " without stats took " << t / 1.0e9 << " sec" << " (" << double(loopsize) /
            (t / 1.0e9) << "Hz" << ")" << std::endl;
  std::cout << " no stats available" << std::endl;
  std::cout << " ------------------------- " << std::endl << std::endl;
  globmutex.SetBlocking(true);
  RWMutex::SetTimingGlobal(false);
  t = Timing::GetNowInNs();
  RunThreads(&TestThread);
  t = Timing::GetNowInNs() - t;
  std::cout << " ------------------------- " << std::endl;
  std::cout << " Multithreaded Loop (" << NUM_THREADS <<
            " threads half reading/half writing, blocking mutex) of size " << double(
              loopsize) / (int) NUM_THREADS
            << " without stats took " << t / 1.0e9 << " sec" << " (" << double(loopsize) /
            (t / 1.0e9) << "Hz" << ")" << std::endl;
  std::cout << " no stats available" << std::endl;
  std::cout << " ------------------------- " << std::endl << std::endl;
  sleep(1);
  //----------------------------------------------------------------------------
  globmutex.SetBlocking(false);
  RWMutex::SetTimingGlobal(false);
  t = Timing::GetNowInNs();
  RunThreads(&TestThread);
  t = Timing::GetNowInNs() - t;
  std::cout << " ------------------------- " << std::endl;
  std::cout << " Multithreaded Loop (" << NUM_THREADS <<
            " threads half reading/half writing, NON-blocking mutex) of size " << double(
              loopsize) / (int) NUM_THREADS
            << " without stats took " << t / 1.0e9 << " sec" << " (" << double(loopsize) /
            (t / 1.0e9) << "Hz" << ")" << std::endl;
  std::cout << " no stats available" << std::endl;
  std::cout << " ------------------------- " << std::endl << std::endl;
  sleep(1);
  //----------------------------------------------------------------------------
  globmutex.SetBlocking(true);
  globmutex.SetDeadlockCheck(true);
  RWMutex::SetTimingGlobal(false);
  t = Timing::GetNowInNs();
  RunThreads(&TestThread);
  t = Timing::GetNowInNs() - t;
  std::cout << " ------------------------- " << std::endl;
  std::cout << " Multithreaded Loop (" << NUM_THREADS
            << " threads half reading/half writing, blocking mutex, with "
            << "deadlock check) of size "
            << double(loopsize) / (int) NUM_THREADS
            << " without stats took " << t / 1.0e9 << " sec" << " (" << double(loopsize) /
            (t / 1.0e9) << "Hz" << ")" << std::endl;
  std::cout << " no stats available" << std::endl;
  std::cout << " ------------------------- " << std::endl << std::endl;
  globmutex.SetDeadlockCheck(false);
  //----------------------------------------------------------------------------
  std::cout << " ------------------------- " << std::endl;
  std::cout << " Native statistics for global mutex" << std::endl;
  std::cout << " ReadLockCount = " << globmutex.GetReadLockCounter() << std::endl;
  std::cout << " WriteLockCount = " << globmutex.GetWriteLockCounter() <<
            std::endl;
  std::cout << " ------------------------- " << std::endl << std::endl;
  globmutex.SetBlocking(true);
  globmutex.SetTiming(true);
  globmutex.SetSampling(true);
  globmutex.ResetTimingStatistics();
  RWMutex::SetTimingGlobal(true);
  t = Timing::GetNowInNs();
  RunThreads(&TestThread);
  t = Timing::GetNowInNs() - t;
  std::cout << " ------------------------- " << std::endl;
  std::cout << " Multithreaded Loop (" << NUM_THREADS <<
            " threads half reading/half writing, blocking mutex) of size " << double(
              loopsize) / (int) NUM_THREADS
            << " with a sample rate of " << rate << " took " << t / 1.0e9 << " sec" << " ("
            << double(loopsize) / (t / 1.0e9) << "Hz" << ")" << std::endl;
  globmutex.GetTimingStatistics(stats);
  std::cout << stats;
  std::cout << " ------------------------- " << std::endl << std::endl;
  sleep(1);
  globmutex.SetBlocking(false);
  globmutex.SetTiming(true);
  globmutex.SetSampling(true);
  globmutex.ResetTimingStatistics();
  RWMutex::SetTimingGlobal(true);
  t = Timing::GetNowInNs();
  RunThreads(&TestThread);
  t = Timing::GetNowInNs() - t;
  std::cout << " ------------------------- " << std::endl;
  std::cout << " Multithreaded Loop (" << NUM_THREADS <<
            " threads half reading/half writing, NON-blocking mutex) of size " << double(
              loopsize) / (int) NUM_THREADS
            << " with a sample rate of " << rate << " took " << t / 1.0e9 << " sec" << " ("
            << double(loopsize) / (t / 1.0e9) << "Hz" << ")" << std::endl;
  globmutex.GetTimingStatistics(stats);
  std::cout << stats;
  std::cout << " ------------------------- " << std::endl << std::endl;
  std::cout << " ------------------------- " << std::endl;
  std::cout << " Global statistics" << std::endl;
  RWMutex::GetTimingStatisticsGlobal(stats);
  std::cout << stats;
  std::cout << " ------------------------- " << std::endl << std::endl;
  std::cout << "#################################################" << std::endl;
  std::cout << "######## MONOTHREADED ORDER CHECKING TESTS ######" << std::endl;
  std::cout << "#################################################" << std::endl;
  RWMutex::SetTimingGlobal(false);
  RWMutex::SetOrderCheckingGlobal(true);
  vector<RWMutex*> order;
  order.push_back(&gm1);
  gm1.SetDebugName("mutex1");
  order.push_back(&gm2);
  gm2.SetDebugName("mutex2");
  order.push_back(&gm3);
  gm3.SetDebugName("mutex3");
  RWMutex::AddOrderRule("rule1", order);
  order.clear();
  order.push_back(&gm2);
  order.push_back(&gm3);
  RWMutex::AddOrderRule("rule2", order);
  std::cout << "==== Trying lock/unlock mutex in proper order... ====" <<
            std::endl;
  std::cout.flush();
  gm1.LockWrite();
  gm2.LockWrite();
  gm3.LockWrite();
  gm3.UnLockWrite();
  gm2.UnLockWrite();
  gm1.UnLockWrite();
  std::cout << "======== ... done ========" << std::endl << std::endl;
  std::cout.flush();
  std::cout << "=== Trying lock/unlock mutex in an improper order... ===" <<
            std::endl;
  std::cout.flush();
  gm1.LockWrite();
  gm3.LockWrite();
  gm2.LockWrite();
  gm2.UnLockWrite();
  gm3.UnLockWrite();
  gm1.UnLockWrite();
  std::cout << "======== ... done ========" << std::endl << std::endl;
  std::cout.flush();
  RWMutex::SetOrderCheckingGlobal(false);
  t = Timing::GetNowInNs();

  for (int k = 0; k < loopsize; ++k) {
    gm1.LockWrite();
    gm2.LockWrite();
    gm3.LockWrite();
    gm3.UnLockWrite();
    gm2.UnLockWrite();
    gm1.UnLockWrite();
  }

  t = Timing::GetNowInNs() - t;
  std::cout << " ------------------------- " << std::endl;
  std::cout << " Monothreaded Loop of size " << double(loopsize)
            << " WITHOUT order check took " << t / 1.0e9 << " sec" << " ("
            << double(loopsize) / (t / 1.0e9) << "Hz" << ")" << std::endl;
  std::cout << " no stats available" << std::endl;
  std::cout << " ------------------------- " << std::endl << std::endl;
  RWMutex::SetOrderCheckingGlobal(true);
  t = Timing::GetNowInNs();

  for (int k = 0; k < loopsize; ++k) {
    gm1.LockWrite();
    gm2.LockWrite();
    gm3.LockWrite();
    gm3.UnLockWrite();
    gm2.UnLockWrite();
    gm1.UnLockWrite();
  }

  t = Timing::GetNowInNs() - t;
  std::cout << " ------------------------- " << std::endl;
  std::cout << " Monothreaded Loop of size " << double(loopsize)
            << " WITH order check took " << t / 1.0e9 << " sec" << " ("
            <<  double(loopsize) / (t / 1.0e9) << "Hz" << ")" << std::endl;
  std::cout << " no stats available" << std::endl;
  std::cout << " ------------------------- " << std::endl << std::endl;
  std::cout << "#################################################" << std::endl;
  std::cout << "####### MULTITHREADED ORDER CHECKING TESTS ######" << std::endl;
  std::cout << "#################################################" << std::endl;
  RunThreads(&TestThread2);
  return 0;
}

/*
int
main()
{
  std::cout << " Using NON-Instrumented Version of RWMutex class" << std::endl;
  RWMutex mutex3;
  size_t t = Timing::GetNowInNs();

  for (int k = 0; k < loopsize; ++k) {
    mutex3.LockWrite();
    mutex3.UnLockWrite();
  }

  t = Timing::GetNowInNs() - t;
  std::cout << " ------------------------- " << std::endl;
  std::cout << " Monothreaded Loop of size " << double(loopsize) <<
       " without stats took " << t / 1.0e9 << " sec" << " (" << double(loopsize) /
       (t / 1.0e9) << "Hz" << ")" << std::endl;
  std::cout << " no stats available" << std::endl;
  std::cout << " ------------------------- " << std::endl << std::endl;
  globmutex.SetBlocking(true);
  t = Timing::GetNowInNs();
  RunThreads(&TestThread);
  t = Timing::GetNowInNs() - t;
  std::cout << " ------------------------- " << std::endl;
  std::cout << " Multithreaded Loop (" << NUM_THREADS <<
       " threads half reading/half writing, blocking mutex) of size " << double(
         loopsize) / (int) NUM_THREADS
       << " without stats took " << t / 1.0e9 << " sec" << " (" << double(loopsize) /
       (t / 1.0e9) << "Hz" << ")" << std::endl;
  std::cout << " no stats available" << std::endl;
  std::cout << " ------------------------- " << std::endl << std::endl;
  globmutex.SetBlocking(false);
  t = Timing::GetNowInNs();
  RunThreads(&TestThread);
  t = Timing::GetNowInNs() - t;
  std::cout << " ------------------------- " << std::endl;
  std::cout << " Multithreaded Loop (" << NUM_THREADS <<
       " threads half reading/half writing, NON-blocking mutex) of size " << double(
         loopsize) / (int) NUM_THREADS
       << " without stats took " << t / 1.0e9 << " sec" << " (" << double(loopsize) /
       (t / 1.0e9) << "Hz" << ")" << std::endl;
  std::cout << " no stats available" << std::endl;
  std::cout << " ------------------------- " << std::endl << std::endl;
  std::cout << " ------------------------- " << std::endl;
  std::cout << " Native statistics for global mutex" << std::endl;
  std::cout << " ReadLockCount = " << globmutex.GetReadLockCounter() << std::endl;
  std::cout << " WriteLockCount = " << globmutex.GetWriteLockCounter() << std::endl;
  std::cout << " ------------------------- " << std::endl << std::endl;
  std::cout << "###################################################################" <<
       std::endl;
  std::cout << "################ MONOTHREADED ORDER CHECKING TESTS ################" <<
       std::endl;
  std::cout << "###################################################################" <<
       std::endl;
  t = Timing::GetNowInNs();

  for (int k = 0; k < loopsize; ++k) {
    gm1.LockWrite();
    gm2.LockWrite();
    gm3.LockWrite();
    gm3.UnLockWrite();
    gm2.UnLockWrite();
    gm1.UnLockWrite();
  }

  t = Timing::GetNowInNs() - t;
  std::cout << " ------------------------- " << std::endl;
  std::cout << " Monothreaded Loop of size " << double(loopsize) <<
       " WITHOUT order check took " << t / 1.0e9 << " sec" << " (" << double(
         loopsize) / (t / 1.0e9) << "Hz" << ")" << std::endl;
  std::cout << " no stats available" << std::endl;
  std::cout << " ------------------------- " << std::endl << std::endl;
}
*/
