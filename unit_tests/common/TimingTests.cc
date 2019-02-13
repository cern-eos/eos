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

EOSCOMMONTESTING_BEGIN

TEST(Timing, LsFormat)
{
  using namespace eos::common;
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

TEST(Timing, TimespecStringToTimespec)
{
  using namespace eos::common;
  struct timespec ts;
  int rc;

  // Extract timespec from predefined timespec string
  rc = Timing::TimespecString_to_Timespec("1550061572.9528439045", ts);
  ASSERT_EQ(rc, 0);
  ASSERT_EQ(ts.tv_sec, 1550061572);
  ASSERT_EQ(ts.tv_nsec, 952843904);

  rc = Timing::TimespecString_to_Timespec("1550061572", ts);
  ASSERT_EQ(rc, 0);
  ASSERT_EQ(ts.tv_sec, 1550061572);
  ASSERT_EQ(ts.tv_nsec, 0);

  // Convert current time into timespec string
  // Extract timespec from previously generated string
  struct timespec now;
  char buff[64];

  Timing::GetTimeSpec(now);
  sprintf(buff, "%ld.%ld", now.tv_sec, now.tv_nsec);

  Timing::TimespecString_to_Timespec(buff, ts);
  ASSERT_EQ(ts.tv_sec, now.tv_sec);
  ASSERT_EQ(ts.tv_nsec, now.tv_nsec);

  // Invalid strings
  ASSERT_EQ(Timing::TimespecString_to_Timespec("no digits", ts), -1);
  ASSERT_EQ(Timing::TimespecString_to_Timespec("...", ts), -1);
}

TEST(Timing, TimespecStringToNs)
{
  using namespace eos::common;
  long long nanoseconds;

  // Extract nanoseconds from predefined timespec string
  nanoseconds = Timing::TimespecString_to_Ns("1550061572.9528439045");
  ASSERT_EQ(nanoseconds, 1550061572952843904ULL);
  nanoseconds = Timing::TimespecString_to_Ns("1550061572");
  ASSERT_EQ(nanoseconds, 1550061572000000000ULL);

  // Convert current time into timespec string
  // Extract nanoseconds from previously generated string
  struct timespec now;
  char buff[64];

  Timing::GetTimeSpec(now);
  sprintf(buff, "%ld.%ld", now.tv_sec, now.tv_nsec);

  nanoseconds = Timing::TimespecString_to_Ns(buff);
  ASSERT_EQ(nanoseconds, Timing::GetAgeInNs(0LL, &now));

  // Invalid strings
  ASSERT_EQ(Timing::TimespecString_to_Ns("no digits"), -1);
  ASSERT_EQ(Timing::TimespecString_to_Ns("..."), -1);
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

EOSCOMMONTESTING_END
