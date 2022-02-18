//------------------------------------------------------------------------------
// File: ResponseCollectorTests.hh
// Author: Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2021 CERN/Switzerland                                  *
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
#include "fst/io/xrd/ResponseCollector.hh"
#include <thread>
//------------------------------------------------------------------------------
// Test successful run
//------------------------------------------------------------------------------
TEST(ResponseCollector, SuccessfulRun)
{
  eos::fst::ResponseCollector collector;
  std::list<std::promise<XrdCl::XRootDStatus>> lst_promises;

  for (int i = 0; i < 100; ++i) {
    auto& promise = lst_promises.emplace_back();
    collector.CollectFuture(promise.get_future());
  }

  for (auto& promise : lst_promises) {
    promise.set_value(XrdCl::XRootDStatus());
  }

  ASSERT_TRUE(collector.CheckResponses(true));
}

//------------------------------------------------------------------------------
// Test run with failures
//------------------------------------------------------------------------------
TEST(ResponseCollector, FailedRun)
{
  int count = 0;
  eos::fst::ResponseCollector collector;
  std::list<std::promise<XrdCl::XRootDStatus>> lst_promises;

  for (int i = 0; i < 100; ++i) {
    auto& promise = lst_promises.emplace_back();
    collector.CollectFuture(promise.get_future());
  }

  for (auto& promise : lst_promises) {
    ++count;

    if (count % 10 == 0) {
      promise.set_value(XrdCl::XRootDStatus(XrdCl::stError,
                                            XrdCl::errUnknown, EINVAL));
    } else {
      promise.set_value(XrdCl::XRootDStatus());
    }
  }

  ASSERT_FALSE(collector.CheckResponses(true));
}

//------------------------------------------------------------------------------
// Test collection of partial successful responses
//------------------------------------------------------------------------------
TEST(ResponseCollector, PartialSuccessfulRun)
{
  eos::fst::ResponseCollector collector;
  uint32_t num_requests = 100;
  std::list<std::promise<XrdCl::XRootDStatus>> lst_promises;

  for (unsigned int i = 0; i < num_requests; ++i) {
    auto& promise = lst_promises.emplace_back();
    collector.CollectFuture(promise.get_future());
  }

  // Set reponses for the first half
  unsigned int count = 0;

  for (auto& promise : lst_promises) {
    ++count;

    if (count < lst_promises.size() / 2) {
      promise.set_value(XrdCl::XRootDStatus());
    }
  }

  std::thread t([&]() {
    unsigned int c = 0;
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    for (auto& promise : lst_promises) {
      ++c;

      if (c >= lst_promises.size() / 2) {
        promise.set_value(XrdCl::XRootDStatus());
      }
    }
  });
  // First half should all be successful, no waiting
  ASSERT_TRUE(collector.CheckResponses(false));
  // Second half successful, wait for all
  ASSERT_TRUE(collector.CheckResponses(true));
  t.join();
}

//------------------------------------------------------------------------------
// Test collection of partial failed responses
//----------------------------------------=--------------------------------------
TEST(ResponseCollector, PartialFailedRun)
{
  eos::fst::ResponseCollector collector;
  uint32_t num_requests = 100;
  std::list<std::promise<XrdCl::XRootDStatus>> lst_promises;

  for (unsigned int i = 0; i < num_requests; ++i) {
    auto& promise = lst_promises.emplace_back();
    collector.CollectFuture(promise.get_future());
  }

  // Set reponses for the first half
  unsigned int count = 0;

  for (auto& promise : lst_promises) {
    ++count;

    if (count < lst_promises.size() / 2) {
      promise.set_value(XrdCl::XRootDStatus());
    }
  }

  std::thread t([&]() {
    unsigned int c = 0;
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    for (auto& promise : lst_promises) {
      ++c;

      if (c >= lst_promises.size() / 2) {
        if (c % 2 == 0) {
          promise.set_value(XrdCl::XRootDStatus(XrdCl::stError,
                                                XrdCl::errUnknown, EINVAL));
        } else {
          promise.set_value(XrdCl::XRootDStatus());
        }
      }
    }
  });
  // First half should all be successful, no waiting
  ASSERT_TRUE(collector.CheckResponses(false, num_requests));
  // Second half has errors, wait for all
  ASSERT_FALSE(collector.CheckResponses(true, num_requests));
  t.join();
}

//------------------------------------------------------------------------------
// Test collection when max_pending applies
//----------------------------------------=--------------------------------------
TEST(ResponseCollector, MaxPending)
{
  eos::fst::ResponseCollector collector;
  uint32_t num_requests = 50;
  uint32_t max_pending = 10;
  std::list<std::promise<XrdCl::XRootDStatus>> lst_promises;

  for (unsigned int i = 0; i < num_requests; ++i) {
    auto& promise = lst_promises.emplace_back();
    collector.CollectFuture(promise.get_future());
  }

  std::thread t([&]() {
    unsigned int c = 0;
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    for (auto& promise : lst_promises) {
      ++c;

      if (c <= (num_requests - (max_pending / 2))) {
        promise.set_value(XrdCl::XRootDStatus());
      } else {
        static bool one_off = true;

        if (one_off) {
          std::this_thread::sleep_for(std::chrono::milliseconds(500));
          one_off = false;
        }

        if (c != num_requests) {
          promise.set_value(XrdCl::XRootDStatus());
        } else {
          promise.set_value(XrdCl::XRootDStatus(XrdCl::stError,
                                                XrdCl::errUnknown, EINVAL));
        }
      }
    }
  });
  // First batch should all be successful - 45 replies
  ASSERT_TRUE(collector.CheckResponses(false, max_pending));
  ASSERT_EQ(max_pending / 2, collector.GetNumResponses());
  // Second batch has an error at the last reply
  ASSERT_FALSE(collector.CheckResponses(true, max_pending));
  t.join();
}
