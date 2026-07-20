//------------------------------------------------------------------------------
// File: CrashHandlerTests.cc
// Author: Cedric Caffy - CERN
//------------------------------------------------------------------------------

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

#include "common/CrashHandler.hh"
#include <csignal>
#include <cstdlib>
#include <gtest/gtest.h>
#include <pthread.h>
#include <unistd.h>

//------------------------------------------------------------------------------
// These tests reproduce the failure mode of the historical fork()+gdb crash
// handler: a fatal signal delivered while the faulting thread holds an
// allocator lock (e.g. inside jemalloc's free path) must still terminate the
// process. The old handler forked gdb from inside the signal handler; fork()
// runs the allocator's pthread_atfork() prefork handlers, which try to acquire
// the very lock the interrupted thread already holds, deadlocking the whole
// process (observed in production as a hung MGM with thousands of threads
// stuck on jemalloc mutexes).
//
// The allocator is simulated with a plain mutex acquired both by the crashing
// thread and by a pthread_atfork() prefork handler, which is exactly how
// jemalloc protects its arenas across fork(). If the crash handler ever calls
// fork() again, the test child deadlocks, the alarm() below fires and the
// death test fails because the child dies with SIGALRM instead of the
// expected termination.
//------------------------------------------------------------------------------

namespace {
pthread_mutex_t sFakeAllocatorLock = PTHREAD_MUTEX_INITIALIZER;

void
CrashWhileHoldingAllocatorLock(int sig)
{
  pthread_atfork([] { pthread_mutex_lock(&sFakeAllocatorLock); }, nullptr, nullptr);
  pthread_mutex_lock(&sFakeAllocatorLock);
  // Hang-breaker: if the handler deadlocks, SIGALRM terminates the child
  // with the wrong signal and the death test fails instead of hanging
  alarm(10);
  raise(sig);
}

// The termination policy must not depend on the environment the test runs in
void
ClearCrashHandlerEnv()
{
  unsetenv("EOS_CORE_DUMP");
  unsetenv("EOS_RAISE_SIGNAL_AFTER_SIGV");
}
} // namespace

TEST(CrashHandlerDeathTest, SegvReRaisedWithoutForking)
{
  // The "fast" style forks the death-test child directly instead of
  // re-executing argv[0], so the tests work no matter from which directory
  // or through which PATH lookup the test binary was started
  testing::FLAGS_gtest_death_test_style = "fast";
  // With the re-raise policy the process must die from the original signal
  // (giving the kernel the chance to write a core), after printing the trace
  EXPECT_EXIT(
      {
        ClearCrashHandlerEnv();
        eos::common::CrashHandler::Install(true);
        CrashWhileHoldingAllocatorLock(SIGSEGV);
      },
      testing::KilledBySignal(SIGSEGV), "received signal");
}

TEST(CrashHandlerDeathTest, AbortReRaisedWithoutForking)
{
  testing::FLAGS_gtest_death_test_style = "fast";
  EXPECT_EXIT(
      {
        ClearCrashHandlerEnv();
        eos::common::CrashHandler::Install(true);
        CrashWhileHoldingAllocatorLock(SIGABRT);
      },
      testing::KilledBySignal(SIGABRT), "received signal");
}

TEST(CrashHandlerDeathTest, QuietExitWhenCoreDumpDisabled)
{
  testing::FLAGS_gtest_death_test_style = "fast";
  // Without the re-raise policy (MGM default: avoid multi-GB core files) the
  // process must exit with the conventional 128+signal status
  EXPECT_EXIT(
      {
        ClearCrashHandlerEnv();
        eos::common::CrashHandler::Install(false);
        CrashWhileHoldingAllocatorLock(SIGSEGV);
      },
      testing::ExitedWithCode(128 + SIGSEGV), "received signal");
}

TEST(CrashHandlerDeathTest, CoreDumpEnvEnablesReRaise)
{
  testing::FLAGS_gtest_death_test_style = "fast";
  // EOS_CORE_DUMP keeps its historical meaning: terminate in a way that can
  // produce a core, even when the daemon default is the quiet exit
  EXPECT_EXIT(
      {
        ClearCrashHandlerEnv();
        setenv("EOS_CORE_DUMP", "1", 1);
        eos::common::CrashHandler::Install(false);
        CrashWhileHoldingAllocatorLock(SIGSEGV);
      },
      testing::KilledBySignal(SIGSEGV), "received signal");
}
