// ----------------------------------------------------------------------
// File: AssistedThreadTests.cc
// Author: EOS developers
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2026 CERN/Switzerland                                  *
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

#include "common/AssistedThread.hh"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <stdexcept>
#include <type_traits>
#include <vector>

namespace {

using namespace std::chrono_literals;

constexpr auto TestTimeout = 5s;
constexpr auto LongWait = 1h;

struct ThrowWhenCopied {
  ThrowWhenCopied() = default;
  ThrowWhenCopied(const ThrowWhenCopied&)
  {
    throw std::runtime_error("cannot start thread");
  }
  ThrowWhenCopied(ThrowWhenCopied&&) noexcept = default;

  void
  operator()(ThreadAssistant&) const
  {
  }
};

static_assert(!std::is_copy_constructible<AssistedThread>::value,
              "AssistedThread must not be copyable");
static_assert(std::is_nothrow_move_constructible<AssistedThread>::value,
              "AssistedThread moves must not invalidate the worker state");
static_assert(!std::is_constructible<ThreadAssistant, bool>::value,
              "Only AssistedThread may construct a ThreadAssistant");

TEST(AssistedThread, DefaultConstructedThreadIsInertAndReusable)
{
  AssistedThread thread;
  thread.stop();
  thread.join();
  thread.blockUntilThreadJoins();

  std::atomic<bool> ran{false};
  thread.reset([&](ThreadAssistant&) { ran = true; });
  thread.blockUntilThreadJoins();

  EXPECT_TRUE(ran);
}

TEST(AssistedThread, StopInterruptsWaitFor)
{
  std::promise<void> started;
  const auto startedFuture = started.get_future();
  std::promise<void> finished;
  const auto finishedFuture = finished.get_future();
  std::atomic<bool> terminationObserved{false};

  AssistedThread thread([&](ThreadAssistant& assistant) {
    started.set_value();
    assistant.wait_for(LongWait);
    terminationObserved = assistant.terminationRequested();
    finished.set_value();
  });

  ASSERT_EQ(startedFuture.wait_for(TestTimeout), std::future_status::ready);
  thread.stop();
  EXPECT_EQ(finishedFuture.wait_for(TestTimeout), std::future_status::ready);
  thread.blockUntilThreadJoins();

  EXPECT_TRUE(terminationObserved);
}

TEST(AssistedThread, StopInterruptsWaitUntil)
{
  std::promise<void> started;
  const auto startedFuture = started.get_future();
  std::promise<void> finished;
  const auto finishedFuture = finished.get_future();

  AssistedThread thread([&](ThreadAssistant& assistant) {
    started.set_value();
    assistant.wait_until(std::chrono::steady_clock::now() + LongWait);
    finished.set_value();
  });

  ASSERT_EQ(startedFuture.wait_for(TestTimeout), std::future_status::ready);
  thread.stop();
  EXPECT_EQ(finishedFuture.wait_for(TestTimeout), std::future_status::ready);
  thread.blockUntilThreadJoins();
}

TEST(AssistedThread, BlockingJoinDoesNotRequestTermination)
{
  std::atomic<bool> terminationObserved{true};
  AssistedThread thread([&](ThreadAssistant& assistant) {
    terminationObserved = assistant.terminationRequested();
  });

  thread.blockUntilThreadJoins();

  EXPECT_FALSE(terminationObserved);
}

TEST(AssistedThread, DestructorRequestsTerminationAndJoins)
{
  std::promise<void> finished;
  const auto finishedFuture = finished.get_future();

  {
    const AssistedThread thread([&](ThreadAssistant& assistant) {
      assistant.wait_for(LongWait);
      finished.set_value();
    });
  }

  EXPECT_EQ(finishedFuture.wait_for(TestTimeout), std::future_status::ready);
}

TEST(AssistedThread, MoveKeepsAssistantAlive)
{
  std::promise<void> started;
  const auto startedFuture = started.get_future();
  std::atomic<bool> terminationObserved{false};

  AssistedThread source([&](ThreadAssistant& assistant) {
    started.set_value();
    assistant.wait_for(10s);
    terminationObserved = assistant.terminationRequested();
  });

  ASSERT_EQ(startedFuture.wait_for(TestTimeout), std::future_status::ready);
  AssistedThread destination(std::move(source));
  destination.join();

  EXPECT_TRUE(terminationObserved);
}

TEST(AssistedThread, MovedFromObjectCanBeReset)
{
  AssistedThread source;
  const AssistedThread destination(std::move(source));
  std::atomic<bool> ran{false};

  source.reset([&](ThreadAssistant&) { ran = true; });
  source.blockUntilThreadJoins();

  EXPECT_TRUE(ran);
}

TEST(AssistedThread, FailedResetLeavesObjectReusable)
{
  AssistedThread thread;
  const ThrowWhenCopied callable;
  EXPECT_THROW(thread.reset(callable), std::runtime_error);

  std::atomic<bool> ran{false};
  thread.reset([&](ThreadAssistant&) { ran = true; });
  thread.blockUntilThreadJoins();

  EXPECT_TRUE(ran);
}

TEST(AssistedThread, StopCanRaceWithBlockingJoin)
{
  AssistedThread thread([](ThreadAssistant& assistant) { assistant.wait_for(LongWait); });

  std::thread stopper([&] { thread.stop(); });
  thread.blockUntilThreadJoins();
  stopper.join();
}

TEST(AssistedThread, RepeatedStopsInvokeCallbacksOnce)
{
  AssistedThread thread([](ThreadAssistant& assistant) { assistant.wait_for(LongWait); });
  std::atomic<unsigned int> callbackCount{0};
  thread.registerCallback([&] { ++callbackCount; });

  std::vector<std::thread> stoppers;

  for (unsigned int i = 0; i < 4; ++i) {
    stoppers.emplace_back([&] { thread.stop(); });
  }

  for (auto& stopper : stoppers) {
    stopper.join();
  }

  thread.blockUntilThreadJoins();
  EXPECT_EQ(callbackCount, 1u);
}

TEST(AssistedThread, CallbackRegisteredAfterStopRunsImmediately)
{
  AssistedThread thread([](ThreadAssistant& assistant) { assistant.wait_for(LongWait); });
  thread.stop();

  std::atomic<bool> callbackRan{false};
  thread.registerCallback([&] { callbackRan = true; });
  thread.blockUntilThreadJoins();

  EXPECT_TRUE(callbackRan);
}

TEST(AssistedThread, DroppedCallbacksAreNotInvoked)
{
  AssistedThread thread([](ThreadAssistant& assistant) { assistant.wait_for(LongWait); });
  std::atomic<bool> callbackRan{false};
  thread.registerCallback([&] { callbackRan = true; });

  thread.dropCallbacks();
  thread.join();

  EXPECT_FALSE(callbackRan);
}

TEST(AssistedThread, ResetClearsCallbacksAndTerminationState)
{
  AssistedThread thread([](ThreadAssistant&) {});
  std::atomic<bool> staleCallbackRan{false};
  thread.registerCallback([&] { staleCallbackRan = true; });
  thread.blockUntilThreadJoins();

  std::promise<void> started;
  const auto startedFuture = started.get_future();
  std::atomic<bool> initiallyTerminated{true};
  thread.reset([&](ThreadAssistant& assistant) {
    initiallyTerminated = assistant.terminationRequested();
    started.set_value();
    assistant.wait_for(LongWait);
  });

  ASSERT_EQ(startedFuture.wait_for(TestTimeout), std::future_status::ready);
  thread.join();

  EXPECT_FALSE(initiallyTerminated);
  EXPECT_FALSE(staleCallbackRan);
}

TEST(AssistedThread, ConcurrentRegistrationAndStopInvokeEveryCallbackOnce)
{
  AssistedThread thread([](ThreadAssistant& assistant) { assistant.wait_for(LongWait); });
  constexpr unsigned int CallbackCount = 128;
  std::atomic<unsigned int> callbackCount{0};

  std::thread registrar([&] {
    for (unsigned int i = 0; i < CallbackCount; ++i) {
      thread.registerCallback([&] { ++callbackCount; });
    }
  });
  thread.stop();
  registrar.join();
  thread.blockUntilThreadJoins();

  EXPECT_EQ(callbackCount, CallbackCount);
}

TEST(AssistedThread, PropagatesTerminationToBlockedChild)
{
  std::promise<void> propagationRegistered;
  const auto propagationRegisteredFuture = propagationRegistered.get_future();
  std::atomic<bool> childObservedTermination{false};

  AssistedThread child([&](ThreadAssistant& assistant) {
    assistant.wait_for(LongWait);
    childObservedTermination = assistant.terminationRequested();
  });
  AssistedThread parent([&](ThreadAssistant& assistant) {
    assistant.propagateTerminationSignal(child);
    propagationRegistered.set_value();
    child.blockUntilThreadJoins();
  });

  ASSERT_EQ(propagationRegisteredFuture.wait_for(TestTimeout), std::future_status::ready);
  parent.join();

  EXPECT_TRUE(childObservedTermination);
}

TEST(AssistedThread, TerminationCallbacksCanReenterAssistant)
{
  AssistedThread thread([](ThreadAssistant& assistant) { assistant.wait_for(LongWait); });

  std::atomic<bool> callbackRan{false};
  thread.registerCallback([&] {
    thread.dropCallbacks();
    callbackRan = true;
  });
  thread.join();

  EXPECT_TRUE(callbackRan);
}

} // namespace
