//------------------------------------------------------------------------------
// File: AppTagsTests.cc
// Author: Elvin Sindrilaru - CERN
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

#include "common/Constants.hh"
#include "gtest/gtest.h"

using eos::common::EOS_APP_BALANCER;
using eos::common::EOS_APP_CONVERTER;
using eos::common::EOS_APP_DRAIN;
using eos::common::EOS_APP_FSCK;
using eos::common::EOS_APP_FSCK_SCAN;
using eos::common::EOS_APP_GEO_BALANCER;
using eos::common::EOS_APP_GROUP_BALANCER;
using eos::common::EOS_APP_GROUP_DRAINER;
using eos::common::InternalAppTag;
using eos::common::IsInternalApp;

//------------------------------------------------------------------------------
// Every subsystem EOS schedules for itself is named the same way
//------------------------------------------------------------------------------
TEST(AppTags, EverySubsystemIsPrefixed)
{
  EXPECT_EQ(InternalAppTag(EOS_APP_DRAIN), "eos/drain");
  EXPECT_EQ(InternalAppTag(EOS_APP_BALANCER), "eos/balancer");
  EXPECT_EQ(InternalAppTag(EOS_APP_GROUP_BALANCER), "eos/groupbalancer");
  EXPECT_EQ(InternalAppTag(EOS_APP_GEO_BALANCER), "eos/geobalancer");
  EXPECT_EQ(InternalAppTag(EOS_APP_GROUP_DRAINER), "eos/groupdrainer");
  EXPECT_EQ(InternalAppTag(EOS_APP_CONVERTER), "eos/converter");
  EXPECT_EQ(InternalAppTag(EOS_APP_FSCK), "eos/fsck");
  EXPECT_EQ(InternalAppTag(EOS_APP_FSCK_SCAN), "eos/fsck-scan");
}

//------------------------------------------------------------------------------
// A caller handing over a tag that already carries the prefix gets it back as
// it is - what used to produce an "eos/eos/fsck" row in "eos io stat -x"
//------------------------------------------------------------------------------
TEST(AppTags, PrefixingIsIdempotent)
{
  EXPECT_EQ(InternalAppTag("eos/fsck"), "eos/fsck");
  EXPECT_EQ(InternalAppTag(InternalAppTag(EOS_APP_DRAIN)), "eos/drain");
  // only the prefix itself is recognised, not a subsystem that merely starts
  // with the same letters
  EXPECT_EQ(InternalAppTag("eoscp"), "eos/eoscp");
  EXPECT_EQ(InternalAppTag(""), "eos/");
}

//------------------------------------------------------------------------------
// Telling EOS's own traffic apart from a client application
//------------------------------------------------------------------------------
TEST(AppTags, InternalIsTheOnesWithThePrefix)
{
  EXPECT_TRUE(IsInternalApp(InternalAppTag(EOS_APP_DRAIN)));
  EXPECT_TRUE(IsInternalApp(InternalAppTag(EOS_APP_CONVERTER)));
  EXPECT_TRUE(IsInternalApp("eos/whatever-comes-next"));
  // client applications, including the ones that look close
  EXPECT_FALSE(IsInternalApp("other"));
  EXPECT_FALSE(IsInternalApp(""));
  EXPECT_FALSE(IsInternalApp("eos"));
  EXPECT_FALSE(IsInternalApp("eoscp"));
  EXPECT_FALSE(IsInternalApp("fuse"));
  EXPECT_FALSE(IsInternalApp("http/tpcpull"));
  // the bare subsystem names are not internal by themselves - they are what a
  // call site passes in, never what goes out
  EXPECT_FALSE(IsInternalApp(EOS_APP_FSCK));
}
