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
#include <cerrno>
#define IN_TEST_HARNESS
#include "common/Path.hh"
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

TEST(Recycle, EmptyParentCandidates)
{
  const std::string old_prefix = Recycle::gRecyclingPrefix;
  Recycle::gRecyclingPrefix = "/eos/test/proc/recycle/";
  // Bin directory rid:42/ carries the project ACLs and must be kept
  EXPECT_EQ(
      (std::vector<std::string>{"/eos/test/proc/recycle/rid:42/2026/10/",
                                "/eos/test/proc/recycle/rid:42/2026/"}),
      Recycle::GetEmptyParentCandidates("/eos/test/proc/recycle/rid:42/2026/10/08"));
  EXPECT_EQ((std::vector<std::string>{"/eos/test/proc/recycle/uid:1000/2026/10/08/0/",
                                      "/eos/test/proc/recycle/uid:1000/2026/10/08/",
                                      "/eos/test/proc/recycle/uid:1000/2026/10/",
                                      "/eos/test/proc/recycle/uid:1000/2026/"}),
            Recycle::GetEmptyParentCandidates("/eos/test/proc/recycle/uid:1000/2026/10/"
                                              "08/0/#:#eos#:#dir.000000000000000a.d/"));
  EXPECT_TRUE(Recycle::GetEmptyParentCandidates("/eos/test/proc/recycle/rid:42").empty());
  EXPECT_TRUE(Recycle::GetEmptyParentCandidates("/eos/test/proc/recycle/").empty());
  EXPECT_TRUE(Recycle::GetEmptyParentCandidates("/eos/test/other/dir/").empty());
  // Purge must skip the recycle root and bins, only date directories go
  EXPECT_EQ(5u, Recycle::GetBinLevel());
  EXPECT_EQ(5u, eos::common::Path("/eos/test/proc/recycle/rid:42/").GetSubPathSize());
  EXPECT_EQ(6u,
            eos::common::Path("/eos/test/proc/recycle/rid:42/2026/").GetSubPathSize());
  Recycle::gRecyclingPrefix = old_prefix;
}

TEST(Recycle, IsSafeRecyclePath)
{
  const std::string old_prefix = Recycle::gRecyclingPrefix;
  Recycle::gRecyclingPrefix = "/eos/test/proc/recycle/";
  // Paths escaping the recycle bin via ".." must never be removed
  EXPECT_TRUE(Recycle::IsSafeRecyclePath("/eos/test/proc/recycle/"));
  EXPECT_TRUE(Recycle::IsSafeRecyclePath("/eos/test/proc/recycle/rid:42/2026"));
  EXPECT_FALSE(Recycle::IsSafeRecyclePath("/eos/test/proc/recycle/../../project/a"));
  EXPECT_FALSE(
      Recycle::IsSafeRecyclePath("/eos/test/proc/recycle/rid:../../../a/b/c/d/e/f"));
  EXPECT_FALSE(Recycle::IsSafeRecyclePath("/eos/test/proc/recyclebin/"));
  EXPECT_FALSE(Recycle::IsSafeRecyclePath("eos/test/proc/recycle/rid:42/"));
  EXPECT_TRUE(
      Recycle::GetEmptyParentCandidates("/eos/test/proc/recycle/rid:../../../a/b/c/d/e/f")
          .empty());
  EXPECT_TRUE(Recycle::IsSafeRecyclePath("/eos/test/proc/recycle/rid:42/../rid:43"));
  EXPECT_FALSE(Recycle::IsSafeRecyclePath("/eos/test/proc/recycle/.."));
  // Recycle ids containing '/' are rejected before touching the namespace
  std::string std_out, std_err;
  auto vid = eos::common::VirtualIdentity::Root();
  EXPECT_EQ(EINVAL, Recycle::Purge(std_out, std_err, vid, "", "2026", "rid",
                                   "../../../eos/project"));
  Recycle::gRecyclingPrefix = old_prefix;
}
