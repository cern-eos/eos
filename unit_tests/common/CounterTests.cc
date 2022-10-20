// ----------------------------------------------------------------------
// File: CounterTests.cc
// Author: Abhishek Lekshmanan - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2022 CERN/Switzerland                                  *
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

#include "common/Counter.hh"
#include <gtest/gtest.h>

TEST(Counter, Init) {
  using namespace std::chrono;

  eos::common::Counter counter;
  counter.Init();
  auto now = eos::common::SteadyClock::now(nullptr);
  auto diff = duration_cast<seconds>(counter.GetStartTime() - now).count();
  EXPECT_EQ(diff,0);
  EXPECT_EQ(counter.GetFrequency(), 0);
  EXPECT_EQ(counter.GetLastFrequency(), 0);
}

TEST(Counter, GetFrequency)
{
  eos::common::SteadyClock fake_clock(true);
  eos::common::Counter counter(&fake_clock);
  counter.Init();
  fake_clock.advance(std::chrono::seconds(1));
  counter.Increment(100);
  EXPECT_EQ(counter.GetFrequency(), 100);
  EXPECT_EQ(counter.GetLastFrequency(), 100);
}

TEST(Counter, GetLastFrequency)
{
  eos::common::SteadyClock fake_clock(true);
  eos::common::Counter counter(&fake_clock);
  counter.Init();
  fake_clock.advance(std::chrono::seconds(1));
  counter.Increment(100);
  EXPECT_EQ(counter.GetLastFrequency(), 100);
  EXPECT_EQ(counter.GetFrequency(),100);

  fake_clock.advance(std::chrono::seconds(1));
  counter.Increment(300);
  EXPECT_EQ(counter.GetLastFrequency(), 300);
  EXPECT_EQ(counter.GetFrequency(), 200);
}
