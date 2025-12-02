//------------------------------------------------------------------------------
// File: RecycleTests.cc
// Author: Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2025 CERN/Switzerland                                  *
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

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#define IN_TEST_HARNESS
#include "mgm/recycle/Recycle.hh"
#undef IN_TEST_HARNESS

using ::testing::Return;
using ::testing::NiceMock;
using namespace eos::mgm;

TEST(Recycle, ComputeCutOffDate)
{
  Recycle recycle(true);
  auto& clock = recycle.mClock;
  // Set the clock to Tue Sep 30 03:46:40 PM CEST 2025
  clock.advance(std::chrono::seconds(1759240000));
  recycle.mPolicy.mKeepTimeSec = 6 * 31 * 24 * 3600; // 6 months
  ASSERT_STREQ("2025/03/27", recycle.GetCutOffDate().c_str());
  recycle.mPolicy.mKeepTimeSec =  31 * 24 * 3600; // 1 month
  ASSERT_STREQ("2025/08/29", recycle.GetCutOffDate().c_str());
  recycle.mPolicy.mKeepTimeSec =  7 * 24 * 3600; // 1 week
  ASSERT_STREQ("2025/09/22", recycle.GetCutOffDate().c_str());
}

TEST(Recycle, DemangleTest)
{
  // Recycle path should never contain '/'
  ASSERT_STREQ("", Recycle::DemanglePath("/some/real/path/").c_str());
  ASSERT_STREQ("", Recycle::DemanglePath("").c_str());
  ASSERT_STREQ("/eos/top/dir/path",
               Recycle::DemanglePath("#:#eos#:#top#:#dir#:#path.000000000000000a").c_str());
  ASSERT_STREQ("/eos/top/with_funny_chars!#?/file",
               Recycle::DemanglePath("#:#eos#:#top#:#with_funny_chars!#?#:#file.000000000000000b").c_str());
}
