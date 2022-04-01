//------------------------------------------------------------------------------
// File: TimingTest.cc
// Author: Elvin-Alin Sindrilaru <esindril at cern dot ch>
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
#include "Namespace.hh"
#include "common/Timing.hh"
#include "common/SteadyClock.hh"
#include "common/IntervalStopwatch.hh"

EOSCOMMONTESTING_BEGIN

TEST(Timing, LsFormat)
{
  using eos::common::Timing;
  time_t now = time(0);
  struct tm utc;
  struct tm* tm = gmtime_r(&now, &utc);
  std::string output;
  output = Timing::ToLsFormat(tm);
  // Should contain the hour:minute
  ASSERT_TRUE(output.find(':') != std::string::npos);
  // 1 year ago
  tm->tm_year--;
  output = Timing::ToLsFormat(tm);
  // Should contain only the year at the end
  ASSERT_TRUE(output.find(':') == std::string::npos);
}

TEST(Timing, TimespecFromTimespecString)
{
  using eos::common::Timing;
  struct timespec ts;
  int rc;
  // Extract timespec from predefined timespec string
  rc = Timing::Timespec_from_TimespecStr("1550061572.9528439045", ts);
  ASSERT_EQ(rc, 0);
  ASSERT_EQ(ts.tv_sec, 1550061572);
  ASSERT_EQ(ts.tv_nsec, 952843904);
  rc = Timing::Timespec_from_TimespecStr("1550061572", ts);
  ASSERT_EQ(rc, 0);
  ASSERT_EQ(ts.tv_sec, 1550061572);
  ASSERT_EQ(ts.tv_nsec, 0);
  // Convert current time into timespec string
  // Extract timespec from previously generated string
  struct timespec now;
  char buff[64];
  Timing::GetTimeSpec(now);
  sprintf(buff, "%ld.%ld", now.tv_sec, now.tv_nsec);
  Timing::Timespec_from_TimespecStr(buff, ts);
  ASSERT_EQ(ts.tv_sec, now.tv_sec);
  ASSERT_EQ(ts.tv_nsec, now.tv_nsec);
  // Incomplete strings
  ASSERT_EQ(Timing::Timespec_from_TimespecStr("100.no_digits", ts), -1);
  ASSERT_EQ(ts.tv_sec, 0);
  ASSERT_EQ(ts.tv_nsec, 0);
  ASSERT_EQ(Timing::Timespec_from_TimespecStr("no_digits.100", ts), -1);
  ASSERT_EQ(ts.tv_sec, 0);
  ASSERT_EQ(ts.tv_nsec, 0);
  // Invalid strings
  ASSERT_EQ(Timing::Timespec_from_TimespecStr("no digits", ts), -1);
  ASSERT_EQ(Timing::Timespec_from_TimespecStr("...", ts), -1);
  ASSERT_EQ(Timing::Timespec_from_TimespecStr("", ts), -1);
  ASSERT_EQ(ts.tv_sec, 0);
  ASSERT_EQ(ts.tv_nsec, 0);
}

TEST(Timing, NsFromTimespecString)
{
  using eos::common::Timing;
  long long nanoseconds;
  // Extract nanoseconds from predefined timespec string
  nanoseconds = Timing::Ns_from_TimespecStr("1550061572.9528439045");
  ASSERT_EQ(nanoseconds, 1550061572952843904ULL);
  nanoseconds = Timing::Ns_from_TimespecStr("1550061572");
  ASSERT_EQ(nanoseconds, 1550061572000000000ULL);
  // Convert current time into timespec string
  // Extract nanoseconds from previously generated string
  struct timespec now;
  char buff[64];
  Timing::GetTimeSpec(now);
  sprintf(buff, "%ld.%ld", now.tv_sec, now.tv_nsec);
  nanoseconds = Timing::Ns_from_TimespecStr(buff);
  ASSERT_EQ(nanoseconds, Timing::GetAgeInNs(0LL, &now));
  // Invalid strings
  ASSERT_EQ(Timing::Ns_from_TimespecStr("no digits"), -1);
  ASSERT_EQ(Timing::Ns_from_TimespecStr("..."), -1);
  ASSERT_EQ(Timing::Ns_from_TimespecStr(""), -1);
}

TEST(SteadyClock, FakeTests)
{
  eos::common::SteadyClock sc(true);
  ASSERT_EQ(sc.getTime(), std::chrono::steady_clock::time_point());
  std::chrono::steady_clock::time_point startOfTime;
  startOfTime += std::chrono::seconds(5);
  sc.advance(std::chrono::seconds(5));
  ASSERT_EQ(sc.getTime(), startOfTime);
}

TEST(SteadyClock, constructor)
{
  // Yes the 50 different ways to initialize!
  auto clock1 = eos::common::SteadyClock();
  ASSERT_FALSE(clock1.IsFake());
  auto clock2 = eos::common::SteadyClock(false);
  ASSERT_FALSE(clock2.IsFake());
  eos::common::SteadyClock default_clock;
  ASSERT_FALSE(default_clock.IsFake());
  eos::common::SteadyClock default_clock2 {};
  ASSERT_FALSE(default_clock2.IsFake());
  eos::common::SteadyClock clock3(false);
  ASSERT_FALSE(clock3.IsFake());
  eos::common::SteadyClock clock4{false};
  ASSERT_FALSE(clock4.IsFake());
  auto fake_clock = eos::common::SteadyClock(true);
  ASSERT_TRUE(fake_clock.IsFake());
}

TEST(Timing, TimespecToString)
{
  struct timespec ts;
  ts.tv_sec = 11111111;
  ts.tv_nsec = 111111111;
  ASSERT_STREQ(eos::common::Timing::TimespecToString(ts).c_str(),
               "11111111.111111111");
  ts.tv_sec = 121212;
  ts.tv_nsec = 87654321;
  ASSERT_STREQ(eos::common::Timing::TimespecToString(ts).c_str(),
               "121212.087654321");
  ts.tv_sec = 123;
  ts.tv_nsec = 321;
  ASSERT_STREQ(eos::common::Timing::TimespecToString(ts).c_str(),
               "123.000000321");
}

TEST(IntervalStopwatch, BasicSanity)
{
  common::SteadyClock sc(true);
  IntervalStopwatch stopwatch(std::chrono::milliseconds(0), &sc);
  ASSERT_EQ(stopwatch.timeIntoCycle(), std::chrono::milliseconds(0));
  ASSERT_EQ(stopwatch.timeRemainingInCycle(), std::chrono::milliseconds(0));
  ASSERT_EQ(stopwatch.getCycleStart(),
            std::chrono::steady_clock::time_point());
  sc.advance(std::chrono::milliseconds(999));
  ASSERT_EQ(stopwatch.timeIntoCycle(), std::chrono::milliseconds(999));
  ASSERT_EQ(stopwatch.timeRemainingInCycle(), std::chrono::milliseconds(0));
  ASSERT_EQ(stopwatch.getCycleStart(),
            std::chrono::steady_clock::time_point());
  stopwatch = IntervalStopwatch(std::chrono::milliseconds(3), &sc);
  ASSERT_EQ(stopwatch.timeIntoCycle(), std::chrono::milliseconds(0));
  ASSERT_EQ(stopwatch.timeRemainingInCycle(), std::chrono::milliseconds(3));
  ASSERT_EQ(stopwatch.getCycleStart(),
            std::chrono::steady_clock::time_point() + std::chrono::milliseconds(999));
  sc.advance(std::chrono::milliseconds(1));
  ASSERT_EQ(stopwatch.timeIntoCycle(), std::chrono::milliseconds(1));
  ASSERT_EQ(stopwatch.timeRemainingInCycle(), std::chrono::milliseconds(2));
  ASSERT_EQ(stopwatch.getCycleStart(),
            std::chrono::steady_clock::time_point() + std::chrono::milliseconds(999));
  stopwatch.startCycle(std::chrono::milliseconds(10));
  ASSERT_EQ(stopwatch.timeIntoCycle(), std::chrono::milliseconds(0));
  ASSERT_EQ(stopwatch.timeRemainingInCycle(), std::chrono::milliseconds(10));
  ASSERT_EQ(stopwatch.getCycleStart(),
            std::chrono::steady_clock::time_point() + std::chrono::milliseconds(1000));
  sc.advance(std::chrono::milliseconds(1));
  ASSERT_EQ(stopwatch.timeIntoCycle(), std::chrono::milliseconds(1));
  ASSERT_EQ(stopwatch.timeRemainingInCycle(), std::chrono::milliseconds(9));
  sc.advance(std::chrono::milliseconds(1));
  ASSERT_EQ(stopwatch.timeIntoCycle(), std::chrono::milliseconds(2));
  ASSERT_EQ(stopwatch.timeRemainingInCycle(), std::chrono::milliseconds(8));
  sc.advance(std::chrono::milliseconds(7));
  ASSERT_EQ(stopwatch.timeIntoCycle(), std::chrono::milliseconds(9));
  ASSERT_EQ(stopwatch.timeRemainingInCycle(), std::chrono::milliseconds(1));
  sc.advance(std::chrono::milliseconds(1));
  ASSERT_EQ(stopwatch.timeIntoCycle(), std::chrono::milliseconds(10));
  ASSERT_EQ(stopwatch.timeRemainingInCycle(), std::chrono::milliseconds(0));
  sc.advance(std::chrono::milliseconds(10));
  ASSERT_EQ(stopwatch.timeIntoCycle(), std::chrono::milliseconds(20));
  ASSERT_EQ(stopwatch.timeRemainingInCycle(), std::chrono::milliseconds(0));
}

TEST(IntervalStopwatch, RestartIfExpired)
{
  common::SteadyClock sc(true);
  IntervalStopwatch stopwatch(std::chrono::milliseconds(100), &sc);
  ASSERT_EQ(stopwatch.timeIntoCycle(), std::chrono::milliseconds(0));
  ASSERT_FALSE(stopwatch.restartIfExpired());
  sc.advance(std::chrono::milliseconds(99));
  ASSERT_EQ(stopwatch.timeIntoCycle(), std::chrono::milliseconds(99));
  ASSERT_EQ(stopwatch.timeRemainingInCycle(), std::chrono::milliseconds(1));
  ASSERT_FALSE(stopwatch.restartIfExpired());
  sc.advance(std::chrono::milliseconds(2));
  ASSERT_EQ(stopwatch.timeIntoCycle(), std::chrono::milliseconds(101));
  ASSERT_EQ(stopwatch.timeRemainingInCycle(), std::chrono::milliseconds(0));
  ASSERT_TRUE(stopwatch.restartIfExpired());
  ASSERT_EQ(stopwatch.timeIntoCycle(), std::chrono::milliseconds(0));
  ASSERT_EQ(stopwatch.timeRemainingInCycle(), std::chrono::milliseconds(100));
  ASSERT_FALSE(stopwatch.restartIfExpired());
  sc.advance(std::chrono::milliseconds(50));
  ASSERT_EQ(stopwatch.timeIntoCycle(), std::chrono::milliseconds(50));
  ASSERT_EQ(stopwatch.timeRemainingInCycle(), std::chrono::milliseconds(50));
  ASSERT_FALSE(stopwatch.restartIfExpired());
}

TEST(Timing, gtime)
{
  time_t t = 0;
  ASSERT_STREQ("Thu Jan  1 00:00:00 1970", eos::common::Timing::gtime(t).c_str());
  t = 1613646201;
  ASSERT_STREQ("Thu Feb 18 11:03:21 2021", eos::common::Timing::gtime(t).c_str());
}

EOSCOMMONTESTING_END
