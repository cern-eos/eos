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
/**
 * @file   RWMutexTest.cc
 *
 * @brief  This program demonstrates the timing facilities of the RWMutex class.
 *
 */

#include <iostream>
#include "common/RWMutex.hh"

using namespace eos::common;
using namespace std;

const unsigned long int NNUM_THREADS = 10;
pthread_t threads[NNUM_THREADS];
unsigned long int NUM_THREADS = NNUM_THREADS;
int writecount;

RWMutex globmutex;
const int loopsize = 10e6;

void*
TestThread(void *threadid)
{
  unsigned long int tid=* (unsigned long int*)threadid;
  if (tid % 2)
  {
    for (int k = 0; k < loopsize / (int) NUM_THREADS; k++)
    {
      globmutex.LockWrite();
      globmutex.UnLockWrite();
    }
  }
  else
  {
    for (int k = 0; k < loopsize / (int) NUM_THREADS; k++)
    {
      globmutex.LockRead();
      globmutex.UnLockRead();
    }
  }
  pthread_exit(NULL);
  return NULL;
}

void
RunThreads()
{
  void *ret;
  for (unsigned long int t = 0; t < NUM_THREADS; t++)
  {
    int rc = pthread_create(&threads[t], NULL, TestThread, (void *) &t);
    if (rc)
    {
      printf("ERROR; return code from pthread_create() is %d\n", rc);
      exit(-1);
    }
  }
  for (unsigned long int t = 0; t < NUM_THREADS; t++)
    pthread_join(threads[t], &ret);
}

RWMutex gm1,gm2,gm3;

#ifdef EOS_INSTRUMENTED_RWMUTEX

void*
TestThread2(void *threadid)
{
  unsigned long int tid=XrdSysThread::Num();
  int itid=(int)tid;
  for (int k = 0; k < loopsize / (int) NUM_THREADS; k++)
  {
    if(k==itid) {
      cout<<"!!!!!!!! thread "<<tid<<" triggers an incorrect lock/unlock order ON PURPOSE at iteration "<<k<<" !!!!!!!!"<<endl;
      gm1.LockWrite();
      gm3.LockWrite();
      gm2.LockWrite();
      gm2.UnLockWrite();
      gm3.UnLockWrite();
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

void
RunThreads2()
{
  void *ret;
  for (unsigned long int t = 0; t < NUM_THREADS; t++)
  {
    int rc = pthread_create(&threads[t], NULL, TestThread2, (void *) &t);
    if (rc)
    {
      printf("ERROR; return code from pthread_create() is %d\n", rc);
      exit(-1);
    }
  }
  for (unsigned long int t = 0; t < NUM_THREADS; t++)
    pthread_join(threads[t], &ret);
}


int
main()
{
  RWMutex::SetOrderCheckingGlobal(false);
  cout<<" Using Instrumented Version of RWMutex class"<<endl;
  RWMutex::EstimateLatenciesAndCompensation();

  size_t t = NowInt();
  for (int k = 0; k < loopsize; k++)
  {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
  }
  t = NowInt() - t;
  cout << " ------------------------- " << endl;
  cout << " Measuring speed of function clock_gettime() " << endl;
  cout << " Monothreaded Loop of size " << double(loopsize) << " took " << t / 1.0e9 << " sec"<<" ("<<double(loopsize)/(t / 1.0e9)<<"Hz"<<")"<< endl;
  cout << " ------------------------- " << endl << endl;


  RWMutex::SetTimingGlobal(true);

  RWMutex mutex, mutex2;
  mutex.SetTiming(true);

  t = NowInt();
  for (int k = 0; k < loopsize; k++)
  {
    mutex.LockWrite();
    mutex.UnLockWrite();
  }
  t = NowInt() - t;

  RWMutexTimingStats stats;
  mutex.GetTimingStatistics(stats);
  cout << " ------------------------- " << endl;
  cout << " Monothreaded Loop of size " << double(loopsize) << " took " << t / 1.0e9 << " sec"<<" ("<<double(loopsize)/(t / 1.0e9)<<"Hz"<<")"<< endl;
  cout << stats;
  cout << " ------------------------- " << endl << endl;

  float rate = RWMutex::GetSamplingRateFromCPUOverhead(0.033);
  cout << " suggested sample rate is " << rate << endl << endl;
  mutex2.SetTiming(true);
  mutex2.SetSampling(true);

  t = NowInt();
  for (int k = 0; k < loopsize; k++)
  {
    mutex2.LockWrite();
    mutex2.UnLockWrite();
  }
  t = NowInt() - t;
  mutex2.GetTimingStatistics(stats);
  cout << " ------------------------- " << endl;
  cout << " Monothreaded Loop of size " << double(loopsize) << " with a sample rate of " << rate << " took " << t / 1.0e9 << " sec"<<" ("<<double(loopsize)/(t / 1.0e9)<<"Hz"<<")" << endl;
  cout << stats;
  cout << " ------------------------- " << endl << endl;

  RWMutex mutex3;
  RWMutex::SetTimingGlobal(false); // by default no local timing, but global timing with samplerate of 1
  t = NowInt();
  for (int k = 0; k < loopsize; k++)
  {
    mutex3.LockWrite();
    mutex3.UnLockWrite();
  }
  t = NowInt() - t;
  cout << " ------------------------- " << endl;
  cout << " Monothreaded Loop of size " << double(loopsize) << " without stats took " << t / 1.0e9 << " sec"<<" ("<<double(loopsize)/(t / 1.0e9)<<"Hz"<<")" << endl;
  cout << " no stats available" << endl;
  cout << " ------------------------- " << endl << endl;

  globmutex.SetBlocking(true);
  RWMutex::SetTimingGlobal(false);
  t = NowInt();
  RunThreads();
  t = NowInt() - t;
  cout << " ------------------------- " << endl;
  cout << " Multithreaded Loop (" << NUM_THREADS << " threads half reading/half writing, blocking mutex) of size " << double(loopsize) / (int) NUM_THREADS
      << " without stats took " << t / 1.0e9 << " sec"<<" ("<<double(loopsize)/(t / 1.0e9)<<"Hz"<<")" << endl;
  cout << " no stats available" << endl;
  cout << " ------------------------- " << endl << endl;
  sleep(1);

  globmutex.SetBlocking(false);
  RWMutex::SetTimingGlobal(false);
  t = NowInt();
  RunThreads();
  t = NowInt() - t;
  cout << " ------------------------- " << endl;
  cout << " Multithreaded Loop (" << NUM_THREADS << " threads half reading/half writing, NON-blocking mutex) of size " << double(loopsize) / (int) NUM_THREADS
      << " without stats took " << t / 1.0e9 << " sec"<<" ("<<double(loopsize)/(t / 1.0e9)<<"Hz"<<")" << endl;
  cout << " no stats available" << endl;
  cout << " ------------------------- " << endl << endl;
  sleep(1);

  cout << " ------------------------- " << endl;
  cout << " Native statistics for global mutex" << endl;
  cout << " ReadLockCount = " << globmutex.GetReadLockCounter() <<endl;
  cout << " WriteLockCount = " << globmutex.GetWriteLockCounter() <<endl;
  cout << " ------------------------- " << endl << endl;

  globmutex.SetBlocking(true);
  globmutex.SetTiming(true);
  globmutex.SetSampling(true);
  globmutex.ResetTimingStatistics();
  RWMutex::SetTimingGlobal(true);
  t = NowInt();
  RunThreads();
  t = NowInt() - t;
  cout << " ------------------------- " << endl;
  cout << " Multithreaded Loop (" << NUM_THREADS << " threads half reading/half writing, blocking mutex) of size " << double(loopsize) / (int) NUM_THREADS
      << " with a sample rate of " << rate << " took " << t / 1.0e9 << " sec"<<" ("<<double(loopsize)/(t / 1.0e9)<<"Hz"<<")" << endl;
  globmutex.GetTimingStatistics(stats);
  cout << stats;
  cout << " ------------------------- " << endl << endl;
  sleep(1);

  globmutex.SetBlocking(false);
  globmutex.SetTiming(true);
  globmutex.SetSampling(true);
  globmutex.ResetTimingStatistics();
  RWMutex::SetTimingGlobal(true);
  t = NowInt();
  RunThreads();
  t = NowInt() - t;
  cout << " ------------------------- " << endl;
  cout << " Multithreaded Loop (" << NUM_THREADS << " threads half reading/half writing, NON-blocking mutex) of size " << double(loopsize) / (int) NUM_THREADS
      << " with a sample rate of " << rate << " took " << t / 1.0e9 << " sec"<<" ("<<double(loopsize)/(t / 1.0e9)<<"Hz"<<")" << endl;
  globmutex.GetTimingStatistics(stats);
  cout << stats;
  cout << " ------------------------- " << endl << endl;

  cout << " ------------------------- " << endl;
  cout << " Global statistics" << endl;
  RWMutex::GetTimingStatisticsGlobal(stats);
  cout << stats;
  cout << " ------------------------- " << endl << endl;

  cout<<"###################################################################"<<endl;
  cout<<"################ MONOTHREADED ORDER CHECKING TESTS ################"<<endl;
  cout<<"###################################################################"<<endl;
  RWMutex::SetTimingGlobal(false);
  RWMutex::SetOrderCheckingGlobal(true);
  vector<RWMutex*> order;
  order.push_back(&gm1); gm1.SetDebugName("mutex1");
  order.push_back(&gm2); gm2.SetDebugName("mutex2");
  order.push_back(&gm3); gm3.SetDebugName("mutex3");
  RWMutex::AddOrderRule("rule1",order);
  order.clear();
  order.push_back(&gm2);order.push_back(&gm3);
  RWMutex::AddOrderRule("rule2",order);

  cout<<"======== Trying lock/unlock mutex in proper order... ========"<<std::endl; cout.flush();
  gm1.LockWrite();
  gm2.LockWrite();
  gm3.LockWrite();
  gm3.UnLockWrite();
  gm2.UnLockWrite();
  gm1.UnLockWrite();
  cout<<"======== ... done ========"<<std::endl<<std::endl; cout.flush();

  cout<<"======== Trying lock/unlock mutex in an improper order... ========"<<std::endl; cout.flush();
  gm1.LockWrite();
  gm3.LockWrite();
  gm2.LockWrite();
  gm2.UnLockWrite();
  gm3.UnLockWrite();
  gm1.UnLockWrite();
  cout<<"======== ... done ========"<<std::endl<<std::endl; cout.flush();

  RWMutex::SetOrderCheckingGlobal(false);
  t = NowInt();
  for (int k = 0; k < loopsize; k++)
  {
    gm1.LockWrite();
    gm2.LockWrite();
    gm3.LockWrite();
    gm3.UnLockWrite();
    gm2.UnLockWrite();
    gm1.UnLockWrite();
  }
  t = NowInt() - t;
  cout << " ------------------------- " << endl;
  cout << " Monothreaded Loop of size " << double(loopsize) << " WITHOUT order check took " << t / 1.0e9 << " sec"<<" ("<<double(loopsize)/(t / 1.0e9)<<"Hz"<<")" << endl;
  cout << " no stats available" << endl;
  cout << " ------------------------- " << endl << endl;

  RWMutex::SetOrderCheckingGlobal(true);
  t = NowInt();
  for (int k = 0; k < loopsize; k++)
  {
    gm1.LockWrite();
    gm2.LockWrite();
    gm3.LockWrite();
    gm3.UnLockWrite();
    gm2.UnLockWrite();
    gm1.UnLockWrite();
  }
  t = NowInt() - t;
  cout << " ------------------------- " << endl;
  cout << " Monothreaded Loop of size " << double(loopsize) << " WITH order check took " << t / 1.0e9 << " sec"<<" ("<<double(loopsize)/(t / 1.0e9)<<"Hz"<<")" << endl;
  cout << " no stats available" << endl;
  cout << " ------------------------- " << endl << endl;

  cout<<"###################################################################"<<endl;
  cout<<"############### MULTITHREADED ORDER CHECKING TESTS ################"<<endl;
  cout<<"###################################################################"<<endl;

  RunThreads2();


  return 0;
}
#else

int
main()
{
  cout << " Using NON-Instrumented Version of RWMutex class" << endl;
  RWMutex mutex3;
  size_t t = NowInt();
  for (int k = 0; k < loopsize; k++)
  {
    mutex3.LockWrite();
    mutex3.UnLockWrite();
  }
  t = NowInt() - t;
  cout << " ------------------------- " << endl;
  cout << " Monothreaded Loop of size " << double(loopsize) << " without stats took " << t / 1.0e9 << " sec"<<" ("<<double(loopsize)/(t / 1.0e9)<<"Hz"<<")" << endl;
  cout << " no stats available" << endl;
  cout << " ------------------------- " << endl << endl;

  globmutex.SetBlocking(true);
  t = NowInt();
  RunThreads();
  t = NowInt() - t;
  cout << " ------------------------- " << endl;
  cout << " Multithreaded Loop (" << NUM_THREADS << " threads half reading/half writing, blocking mutex) of size " << double(loopsize) / (int) NUM_THREADS
      << " without stats took " << t / 1.0e9 << " sec"<<" ("<<double(loopsize)/(t / 1.0e9)<<"Hz"<<")" << endl;
  cout << " no stats available" << endl;
  cout << " ------------------------- " << endl << endl;

  globmutex.SetBlocking(false);
  t = NowInt();
  RunThreads();
  t = NowInt() - t;
  cout << " ------------------------- " << endl;
  cout << " Multithreaded Loop (" << NUM_THREADS << " threads half reading/half writing, NON-blocking mutex) of size " << double(loopsize) / (int) NUM_THREADS
      << " without stats took " << t / 1.0e9 << " sec"<<" ("<<double(loopsize)/(t / 1.0e9)<<"Hz"<<")" << endl;
  cout << " no stats available" << endl;
  cout << " ------------------------- " << endl << endl;

  cout << " ------------------------- " << endl;
  cout << " Native statistics for global mutex" << endl;
  cout << " ReadLockCount = " << globmutex.GetReadLockCounter() <<endl;
  cout << " WriteLockCount = " << globmutex.GetWriteLockCounter() <<endl;
  cout << " ------------------------- " << endl << endl;

  cout<<"###################################################################"<<endl;
  cout<<"################ MONOTHREADED ORDER CHECKING TESTS ################"<<endl;
  cout<<"###################################################################"<<endl;
  t = NowInt();
  for (int k = 0; k < loopsize; k++)
  {
    gm1.LockWrite();
    gm2.LockWrite();
    gm3.LockWrite();
    gm3.UnLockWrite();
    gm2.UnLockWrite();
    gm1.UnLockWrite();
  }
  t = NowInt() - t;
  cout << " ------------------------- " << endl;
  cout << " Monothreaded Loop of size " << double(loopsize) << " WITHOUT order check took " << t / 1.0e9 << " sec"<<" ("<<double(loopsize)/(t / 1.0e9)<<"Hz"<<")" << endl;
  cout << " no stats available" << endl;
  cout << " ------------------------- " << endl << endl;
}
#endif


