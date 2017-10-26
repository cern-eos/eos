//------------------------------------------------------------------------------
// File: ThreadPoolTest.cc
// Author: Jozsef Makai - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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
#include "common/ThreadPool.hh"

using namespace eos::common;

TEST(ThreadPoolTest, PoolSizeTest)
{
  ThreadPool pool(3, 3);

  std::vector<std::future<std::thread::id>> futures;
  for(int i = 0; i < 10; i++) {
    auto future = pool.PushTask<std::thread::id>(
      [] {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        return std::this_thread::get_id();
      }
    );

    futures.emplace_back(std::move(future));
  }

  std::set<std::thread::id> threadIds;
  for(auto&& future : futures) {
    threadIds.insert(future.get());
  }

  // Check if we have exactly 3 different thread ids
  ASSERT_EQ(3, threadIds.size());
}

TEST(ThreadPoolTest, ScaleUpAndDownTest)
{
  ThreadPool pool(2, 4, 2, 1, 1);

  std::vector<std::future<std::thread::id>> futures;
  for(int i = 0; i < 500; i++) {
    auto future = pool.PushTask<std::thread::id>(
      [] {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        return std::this_thread::get_id();
      }
    );

    futures.emplace_back(std::move(future));
  }

  std::set<std::thread::id> threadIds;
  for(auto&& future : futures) {
    threadIds.insert(future.get());
  }

  // Check if we have scaled up to 4 threads
  ASSERT_EQ(4, threadIds.size());

  std::this_thread::sleep_for(std::chrono::seconds(2));
  futures.clear();
  threadIds.clear();

  for(int i = 0; i < 10; i++) {
    auto future = pool.PushTask<std::thread::id>(
      [] {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        return std::this_thread::get_id();
      }
    );

    futures.emplace_back(std::move(future));
  }

  for(auto&& future : futures) {
    threadIds.insert(future.get());
  }

  // Check if we have scaled down to 2 threads
  ASSERT_EQ(2, threadIds.size());
}