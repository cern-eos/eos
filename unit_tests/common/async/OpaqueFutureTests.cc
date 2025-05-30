/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2022 CERN/Switzerland                           *
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


#include "common/async/OpaqueFuture.hh"
#include "unit_tests/common/async/FollyExecutorFixture.hh"
#include <gtest/gtest.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
using eos::common::OpaqueFuture;

TEST(OpaqueFuture, BasicStdFuture)
{
  std::promise<int> p;
  auto f = p.get_future();
  eos::common::OpaqueFuture<int> of(std::move(f));
  ASSERT_TRUE(of.valid());
  EXPECT_FALSE(of.ready());
  p.set_value(42);
  EXPECT_TRUE(of.ready());
  EXPECT_EQ(of.getValue(), 42);
}

TEST(OpaqueFuture, VoidStdFuture)
{
  std::promise<void> p;
  auto f = p.get_future();
  eos::common::OpaqueFuture<void> of(std::move(f));
  ASSERT_TRUE(of.valid());
  EXPECT_FALSE(of.ready());
  p.set_value();
  EXPECT_TRUE(of.ready());
  of.getValue();
}

// We sneak in a folly::Unit as a void future!
TEST(OpaqueFuture, VoidFollyFuture)
{
  auto f = folly::makeFuture();
  static_assert(std::is_same_v<decltype(f),
                               folly::Future<folly::Unit>>);
  eos::common::OpaqueFuture<void> of(std::move(f));
  ASSERT_TRUE(of.valid());
  EXPECT_TRUE(of.ready());  // Future is already fulfilled since we used makeFuture
  of.getValue();
}

TEST(OpaqueFuture, BasicfollyFuture)
{
  folly::Promise<int> p;
  auto f = p.getFuture();
  static_assert(std::is_same_v<decltype(f), folly::Future<int>>);
  eos::common::OpaqueFuture<int> of(std::move(f));
  ASSERT_TRUE(of.valid());
  EXPECT_FALSE(of.ready());
  p.setValue(42);
  EXPECT_TRUE(of.ready());
  EXPECT_EQ(of.getValue(), 42);
}

TEST(OpaqueFuture, BasicfollySemiFuture)
{
  folly::Promise<int> p;
  auto f = p.getSemiFuture();
  static_assert(std::is_same_v<decltype(f), folly::SemiFuture<int>>);
  eos::common::OpaqueFuture<int> of(std::move(f));
  ASSERT_TRUE(of.valid());
  EXPECT_FALSE(of.ready());
  p.setValue(42);
  EXPECT_TRUE(of.ready());
  EXPECT_EQ(of.getValue(), 42);
}

// Shamelessly borrowed from FutureWrapperTests
TEST(OpaqueFuture, stdExceptions)
{
  std::promise<int> promise;
  OpaqueFuture<int> fut(promise.get_future());
  ASSERT_FALSE(fut.ready());
  promise.set_exception(std::make_exception_ptr(
                          std::runtime_error("something terrible happened")));
  ASSERT_TRUE(fut.ready());

  try {
    fut.getValue();
    FAIL(); // should never reach here
  } catch (const std::runtime_error&
             exc) { // yes, you can use strings as exceptions
    ASSERT_STREQ(exc.what(), "something terrible happened");
  }
}

TEST(OpaqueFuture, follyExceptions)
{
  folly::Promise<int> promise;
  OpaqueFuture<int> fut(promise.getFuture());
  ASSERT_FALSE(fut.ready());
  promise.setException(folly::exception_wrapper(
                         std::runtime_error("something terrible happened")));
  ASSERT_TRUE(fut.ready());

  try {
    fut.getValue();
    FAIL(); // should never reach here
  } catch (const std::runtime_error&
             exc) { // yes, you can use strings as exceptions
    ASSERT_STREQ(exc.what(), "something terrible happened");
  }
}


int fib(int n)
{
  if (n < 3) {
    return 1;
  } else {
    return fib(n - 1) + fib(n - 2);
  }
}

TEST(OpaqueFuture, StdFutureWait)
{
  // This executes a std:future asynchronously in a new thread and we wrap the
  // resulting std::future in our OpaqueFuture. Since this takes a few 100ms
  // the result shouldn't be immediately seen as ready.
  OpaqueFuture<int> f(std::async(std::launch::async, []() {
    return fib(40);
  }));
  // This usually takes a few 100 msec.
  EXPECT_FALSE(f.ready());
  f.wait();
  EXPECT_TRUE(f.ready());
  // GetValue actually does a wait call so the f.ready() calls are redundant, however
  // this is just to demonstrate wait() functionality that makes a future value "ready"
  EXPECT_EQ(f.getValue(), 102334155);
}

TEST_F(FollyExecutor_F, follyOpaqueFutureWait)
{
  auto f = folly::makeFuture().via(folly_io_executor.get()).then([](auto&&) {
    return fib(40);
  });
  OpaqueFuture<int> of(std::move(f));
  EXPECT_FALSE(of.ready());
  of.wait();
  EXPECT_TRUE(of.ready());
  EXPECT_EQ(of.getValue(), 102334155);
}
