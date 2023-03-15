//------------------------------------------------------------------------------
//! @file BackOffInvokerTests.cc
//! @author Abhishek Lekshmanan <abhishek.lekshmanan@cern.ch>
//-----------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2023 CERN/Switzerland                           *
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

#include <gtest/gtest.h>
#include "common/utils/BackOffInvoker.hh"

TEST(BackOffInvoker, simple)
{
  eos::common::BackOffInvoker backoff;
  int counter = 0;
  auto fn = [&counter]() { counter++; };
  for (int i = 0; i < 10; i++) {
    backoff.invoke(fn);
  }
  ASSERT_EQ(counter, 4);
}

TEST(BackOffInvoker, uint8_lt_half_limit)
{
  eos::common::BackOffInvoker<uint8_t> backoff;
  int counter = 0;
  auto fn = [&counter]() { counter++; };
  for (int i = 0; i < 127; i++) {
    backoff.invoke(fn);
  }
  ASSERT_EQ(counter, 7);
}

TEST(BackOffInvoker, uint8_half_limit)
{
  eos::common::BackOffInvoker<uint8_t> backoff;
  int counter = 0;
  auto fn = [&counter]() { counter++; };
  for (int i = 0; i < 128; i++) {
    backoff.invoke(fn);
  }
  ASSERT_EQ(counter, 8);
}

TEST(BackOffInvoker, uint8_full_limit)
{
  eos::common::BackOffInvoker<uint8_t,true> backoff;
  int counter = 0;
  auto fn = [&counter]() { counter++; };
  for (int i = 0; i < 256; i++) {
    backoff.invoke(fn);
  }
  EXPECT_EQ(counter, 8);
  backoff.invoke(fn);
  EXPECT_EQ(counter, 9);
}

TEST(BackOffInvoker, uint8_wrap_around)
{
  eos::common::BackOffInvoker<uint8_t> backoff;
  int counter = 0;
  auto fn = [&counter]() { counter++; };
  for (int i = 0; i < 512; i++) {
    backoff.invoke(fn);
  }
  EXPECT_EQ(counter, 16);
}

TEST(BackOffInvoker, uint8_no_wrap_around)
{
  eos::common::BackOffInvoker<uint8_t,false> backoff;
  int counter = 0;
  auto fn = [&counter]() { counter++; };
  for (int i = 0; i < 256; i++) {
    backoff.invoke(fn);
  }
  // When you don't wrap around; we continue at 256 intervals;
  EXPECT_EQ(counter, 9);
}


TEST(BackOffInvoker, uint8_no_wrap_around_twice)
{
  eos::common::BackOffInvoker<uint8_t,false> backoff;
  int counter = 0;
  auto fn = [&counter]() { counter++; };
  for (int i = 0; i < 511; i++) {
    backoff.invoke(fn);
  }
  // When you don't wrap around; we continue at 256 intervals;
  EXPECT_EQ(counter, 9);
  backoff.invoke(fn);
  EXPECT_EQ(counter, 10);
}



TEST(BackOffInvoker, uint16_t_full_limit)
{
  eos::common::BackOffInvoker<uint16_t> backoff;
  int counter = 0;
  auto fn = [&counter]() { counter++; };
  for (int i = 0; i < 65536; i++) {
    backoff.invoke(fn);
  }
  EXPECT_EQ(counter, 16);
}

TEST(BackOffInvoker, uint16_t_wrap_around)
{
  eos::common::BackOffInvoker<uint16_t> backoff;
  int counter = 0;
  auto fn = [&counter]() { counter++; };
  for (int i = 0; i < 65536*2; i++) {
    backoff.invoke(fn);
  }
  EXPECT_EQ(counter, 32);
}
