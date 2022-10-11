// /************************************************************************
//  * EOS - the CERN Disk Storage System                                   *
//  * Copyright (C) 2022 CERN/Switzerland                           *
//  *                                                                      *
//  * This program is free software: you can redistribute it and/or modify *
//  * it under the terms of the GNU General Public License as published by *
//  * the Free Software Foundation, either version 3 of the License, or    *
//  * (at your option) any later version.                                  *
//  *                                                                      *
//  * This program is distributed in the hope that it will be useful,      *
//  * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
//  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
//  * GNU General Public License for more details.                         *
//  *                                                                      *
//  * You should have received a copy of the GNU General Public License    *
//  * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
//  ************************************************************************
//

#include "common/async/ExecutorMgr.hh"
#include "unit_tests/common/async/FollyExecutorFixture.hh"
#include <gtest/gtest.h>


TEST(ExecutorMgr, Construction)
{
  eos::common::ExecutorMgr mgr("std", 2);
  ASSERT_TRUE(mgr.IsThreadPool());
  ASSERT_FALSE(mgr.IsFollyExecutor());
  eos::common::ExecutorMgr mgr2("folly", 2);
  ASSERT_FALSE(mgr2.IsThreadPool());
  ASSERT_TRUE(mgr2.IsFollyExecutor());
}


TEST(ExecutorMgr, ThreadPool)
{
  eos::common::ExecutorMgr mgr("std", 3, 3);
  ASSERT_TRUE(mgr.IsThreadPool());
  std::vector<eos::common::OpaqueFuture<std::thread::id>> futures;

  for (int i = 0; i < 10; i++) {
    auto future = mgr.PushTask(
        [] {
          std::this_thread::sleep_for(std::chrono::milliseconds(20));
          return std::this_thread::get_id();
        }
    );
    futures.emplace_back(std::move(future));
  }

  std::set<std::thread::id> threadIds;

  for (auto && future : futures) {
    threadIds.insert(future.getValue());
  }

  // Check if we have exactly 3 different thread ids
  ASSERT_EQ(3, threadIds.size());
}

TEST_F(FollyExecutor_F, IOThreadPoolExecutorTests)
{
  eos::common::ExecutorMgr mgr(folly_io_executor);
  ASSERT_TRUE(mgr.IsFollyExecutor());
  std::vector<eos::common::OpaqueFuture<std::thread::id>> futures;

  for (int i = 0; i < 10; i++) {
    auto future = mgr.PushTask(
        [] {
          std::this_thread::sleep_for(std::chrono::milliseconds(20));
          return std::this_thread::get_id();
        }
    );
    futures.emplace_back(std::move(future));
  }

  std::set<std::thread::id> threadIds;

  for (auto && future : futures) {
    threadIds.insert(future.getValue());
  }

  // Check if we have exactly 4 different thread ids
  ASSERT_EQ(kNumThreads, threadIds.size());
}

TEST_F(FollyExecutor_F, CPUThreadPoolExecutorTests)
{
  eos::common::ExecutorMgr mgr(folly_cpu_executor);
  ASSERT_TRUE(mgr.IsFollyExecutor());
  std::vector<eos::common::OpaqueFuture<std::thread::id>> futures;

  for (int i = 0; i < 10; i++) {
    auto future = mgr.PushTask(
        [] {
          std::this_thread::sleep_for(std::chrono::milliseconds(20));
          return std::this_thread::get_id();
        }
    );
    futures.emplace_back(std::move(future));
  }

  std::set<std::thread::id> threadIds;

  for (auto && future : futures) {
    threadIds.insert(future.getValue());
  }

  // Check if we have exactly 4 different thread ids
  ASSERT_EQ(kNumThreads, threadIds.size());
}

TEST(ExecutorMgr, ThreadPoolShutdown)
{
  eos::common::ExecutorMgr mgr("std",2,4);
  ASSERT_TRUE(mgr.IsThreadPool());
  std::atomic<int> counter{0};

  for (int i = 0; i < 100; i++) {
    mgr.PushTask(
                 [&counter] {
                   std::this_thread::sleep_for(std::chrono::milliseconds(20));
                   counter++;
                 }
                 );
  }
  // currently the tasks shouldn't complete just yet!
  ASSERT_GT(100, counter);
  mgr.Shutdown();
  ASSERT_EQ(100, counter);
  std::cout << "common::ThreadPool executed " << counter << " tasks" << std::endl;
}

TEST_F(FollyExecutor_F, IOThreadPoolShutdown)
{
  eos::common::ExecutorMgr mgr(folly_io_executor);
  ASSERT_TRUE(mgr.IsFollyExecutor());
  std::atomic<int> counter{0};

  for (int i = 0; i < 100; i++) {
    mgr.PushTask(
                 [&counter] {
                   std::this_thread::sleep_for(std::chrono::milliseconds(20));
                   counter++;
                 }
                 );
  }
  ASSERT_GT(100, counter);
  mgr.Shutdown();
  // There is no stopping the IOThreadPoolExecutor!!!
  ASSERT_EQ(100, counter); // 100 tasks should have been executed
  std::cout << "folly::IOThreadPoolExecutor executed " << counter << " tasks" << std::endl;
}


TEST_F(FollyExecutor_F, CPUThreadPoolShutdown)
{
  eos::common::ExecutorMgr mgr(folly_cpu_executor);
  ASSERT_TRUE(mgr.IsFollyExecutor());
  std::atomic<int> counter{0};
  for (int i = 0; i < 100; i++) {
    mgr.PushTask(
                 [&counter] {
                   std::this_thread::sleep_for(std::chrono::milliseconds(20));
                   counter++;
                 }
                 );
  }
  ASSERT_GT(100, counter);
  mgr.Shutdown();
  // CPU ThreadPool supports true cancellation!
  ASSERT_GT(100, counter);
  std::cout << "folly::CPUThreadPoolExecutor executed " << counter << " tasks" << std::endl;
}

