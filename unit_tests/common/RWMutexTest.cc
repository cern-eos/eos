//------------------------------------------------------------------------------
// File: RWMutexTests.cc
// Author: Elvin Sindrilaru <esindril at cern dot ch>
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

#include "gtest/gtest.h"
#include "common/RWMutex.hh"
#include "common/StacktraceHere.hh"
#include <cstdio>

//------------------------------------------------------------------------------
// Check stacktrace generation
//------------------------------------------------------------------------------
TEST(StacktraceHere, BasicSanity)
{
  std::cout << eos::common::getStacktrace() << std::endl;
}

//------------------------------------------------------------------------------
// Double write lock
//------------------------------------------------------------------------------
TEST(RWMutex, WriteDeadlockTest)
{
  eos::common::RWMutex mutex;
  mutex.SetBlocking(true);
  mutex.SetDeadlockCheck(true);
  mutex.LockWrite();
  ASSERT_THROW(mutex.LockWrite(), std::runtime_error);
}

//------------------------------------------------------------------------------
// Interleaved write lock with re-entrant read lock with a mutex that doesn't
// give preference to the readers.
//------------------------------------------------------------------------------
TEST(RWMutex, RdWrRdDeadlockTest)
{
  eos::common::RWMutex mutex(false);
  mutex.SetBlocking(true);
  mutex.SetDeadlockCheck(true);
  mutex.LockRead();
  std::thread t([&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    mutex.LockWrite();
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    mutex.UnLockWrite();
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  ASSERT_THROW(mutex.LockRead(), std::runtime_error);
  mutex.UnLockRead();
  t.join();
}

//------------------------------------------------------------------------------
// As above but with preference given to the readers. Writers are starved.
//------------------------------------------------------------------------------
TEST(RWMutex, RdWrRdNoDeadlockTest)
{
  eos::common::RWMutex mutex(true);
  mutex.SetBlocking(true);
  mutex.SetDeadlockCheck(true);
  mutex.LockRead();
  std::thread t([&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    mutex.LockWrite();
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    mutex.UnLockWrite();
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  //  ASSERT_NO_THROW(mutex.LockRead());
  // mutex.UnLockRead();
  mutex.UnLockRead();
  t.join();
}

//------------------------------------------------------------------------------
// Multiple reads from different threads should never deadlock
//------------------------------------------------------------------------------
TEST(RWMutex, MultiRdLockTest)
{
  eos::common::RWMutex mutex(true);
  mutex.SetBlocking(true);
  mutex.SetDeadlockCheck(true);
  mutex.LockRead();
  std::thread t([&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    mutex.LockRead();
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    mutex.UnLockRead();
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  ASSERT_NO_THROW(mutex.LockRead());
  ASSERT_NO_THROW(mutex.UnLockRead());
  ASSERT_NO_THROW(mutex.UnLockRead());
  t.join();
}

//------------------------------------------------------------------------------
// Write locks from different threads should never deadlock
//------------------------------------------------------------------------------
TEST(RWMutex, MultiWrLockTest)
{
  eos::common::RWMutex mutex(true);
  mutex.SetBlocking(true);
  mutex.SetDeadlockCheck(true);
  mutex.LockWrite();
  std::thread t([&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    mutex.LockWrite();
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    mutex.UnLockWrite();
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  ASSERT_NO_THROW(mutex.UnLockWrite());
  t.join();
}

//------------------------------------------------------------------------------
// Failed timed lock should not trigger a lock violation
//------------------------------------------------------------------------------
TEST(RWMutex, LockOrder)
{
  using eos::common::RWMutex;
  struct capture_out {
    capture_out(FILE* file):
      mFile(file)
    {
      fflush(mFile);
      mOldFd = fileno(mFile);
      mNewFd = dup(mOldFd);
      freopen("/dev/null", "a", mFile);
      setvbuf(mFile, mBuffer, _IOFBF, 1024 * 1024);
    }

    ~capture_out()
    {
      freopen("/dev/null", "a", mFile);
      dup2(mNewFd, mOldFd);
      setvbuf(mFile, nullptr, _IONBF, 0);
    }

    FILE* mFile;
    int mNewFd;
    int mOldFd;
    char mBuffer[1024 * 1024];
  };
  RWMutex mutex1;
  RWMutex mutex2;
  RWMutex::SetOrderCheckingGlobal(true);
  std::vector<RWMutex*> order;
  order.push_back(&mutex1);
  order.push_back(&mutex2);
  RWMutex::AddOrderRule("rule1", order);
  auto correct_lock_order = [&]() {
    mutex1.LockRead();
    mutex2.LockRead();
    ASSERT_NO_THROW(mutex2.UnLockRead());
    ASSERT_NO_THROW(mutex1.UnLockRead());
  };
  auto lock_order_violation = [&]() {
    capture_out cap_err(stderr);
    mutex2.LockRead();
    mutex1.LockRead();
    std::string output = cap_err.mBuffer;
    ASSERT_TRUE(output.find("Order Checking Error") != std::string::npos);
    ASSERT_NO_THROW(mutex2.UnLockRead());
    ASSERT_NO_THROW(mutex1.UnLockRead());
  };
  auto failed_timed_no_order_violation = [&]() {
    capture_out cap_err(stderr);
    std::thread t([&]() {
      mutex1.LockWrite();
      std::this_thread::sleep_for(std::chrono::seconds(1));
      mutex1.UnLockWrite();
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    ASSERT_FALSE(mutex1.TimedRdLock(100000));
    t.join();
    std::this_thread::sleep_for(std::chrono::seconds(1));
    ASSERT_TRUE(mutex1.TimedRdLock(100000));
    std::string output = cap_err.mBuffer;
    ASSERT_TRUE(output.find("Order Checking Error") == std::string::npos);
    mutex1.UnLockRead();
  };
  correct_lock_order();
  failed_timed_no_order_violation();
  lock_order_violation();
}
