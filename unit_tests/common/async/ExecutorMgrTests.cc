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

//
// Created by Abhishek Lekshmanan on 29/09/2022.
//
#include "common/async/ExecutorMgr.hh"
#include "unit_tests/common/async/FollyExecutorFixture.hh"
#include <gtest/gtest.h>
#include <folly/executors/CPUThreadPoolExecutor.h>


TEST(ExecutorMgr, Construction)
{
  eos::common::ExecutorMgr mgr("std",2);
  ASSERT_TRUE(mgr.IsThreadPool());
  ASSERT_FALSE(mgr.IsFollyExecutor());
  eos::common::ExecutorMgr mgr2("folly",2);
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
        []
        {
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
  eos::common::ExecutorMgr mgr(folly_executor);
  ASSERT_TRUE(mgr.IsFollyExecutor());
  std::vector<eos::common::OpaqueFuture<std::thread::id>> futures;

  for (int i = 0; i < 10; i++) {
    auto future = mgr.PushTask(
        []
        {
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