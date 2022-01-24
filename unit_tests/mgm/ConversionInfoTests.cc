//------------------------------------------------------------------------------
//! @file ConversionInfoTests.cc
//! @author Elvin-Alin Sindrilaru <esindril@cern.ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2020 CERN/Switzerland                                  *
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
#include "mgm/convert/ConversionInfo.hh"

//------------------------------------------------------------------------------
// Test object construction
//------------------------------------------------------------------------------
TEST(ConversionInfo, Construction)
{
  using namespace eos::mgm;
  eos::common::GroupLocator grp_loc;
  ASSERT_TRUE(eos::common::GroupLocator::parseGroup("default.3", grp_loc));
  ASSERT_EQ(nullptr, ConversionInfo::parseConversionString(""));
  std::string input = "000000000000000a:default.3#00100002";
  ASSERT_EQ(input, ConversionInfo::parseConversionString(input)->ToString());
  input = "000000000000000b:default.3#00100002~gathered:tag1";
  ASSERT_EQ(input, ConversionInfo::parseConversionString(input)->ToString());
  input = "000000000000000c:default.3#00100002~scattered:tag1::tag2";
  ASSERT_EQ(input, ConversionInfo::parseConversionString(input)->ToString());
  input = "000000000000000d:default.3#00100002!";
  ASSERT_EQ(input, ConversionInfo::parseConversionString(input)->ToString());
  input = "000000000000000d:default.3#00100002~hybrid:tag1::tag3!";
  ASSERT_EQ(input, ConversionInfo::parseConversionString(input)->ToString());
  input = "000000000000000d:default.3#00100002~hybrid:tag1::tag3^someapp^!";
  ASSERT_EQ(input, ConversionInfo::parseConversionString(input)->ToString());
  // Test false conditions
  input = "dummy0000000000d:default.3#00100002~hybrid:tag1::tag3!";
  ASSERT_EQ(nullptr, ConversionInfo::parseConversionString(input));
  input = "000000000000000d:default.3#00xyz02~hybrid:tag1::tag3!";
  ASSERT_EQ(nullptr, ConversionInfo::parseConversionString(input));
  input = "000000000000000d:default.3#00100002~hybrid:tag1::tag3^someapp!";
  ASSERT_EQ(nullptr, ConversionInfo::parseConversionString(input));
  input = "000000000000000d:default.3#00100002^someapp~hybrid:tag1::tag3!";
  ASSERT_EQ(nullptr, ConversionInfo::parseConversionString(input));
}

TEST(ConversionInfo, OptionalMembers)
{
  using namespace eos::mgm;
  std::string input;
  {
    // make sure that we didn't bring in any reserved chars!
    input = "000000000000000d:default.3#00100002~hybrid:tag1::tag3^eos/someapp^!";
    auto info = ConversionInfo::parseConversionString(input);
    EXPECT_EQ(1048578, info->mLid); // 100002 x to d
    EXPECT_EQ("eos/someapp",info->mAppTag);
    EXPECT_EQ("hybrid:tag1::tag3", info->mPlctPolicy);
    EXPECT_EQ(input, info->ToString());
    EXPECT_TRUE(info->mUpdateCtime);
  }

  {
    // AppTag need not be in the tail position however output will always align to tail
    input = "000000000000000d:default.3#00100002^eos/someapp^~hybrid:tag1::tag3!";
    std::string expected = "000000000000000d:default.3#00100002~hybrid:tag1::tag3^eos/someapp^!";
    auto info = ConversionInfo::parseConversionString(input);
    EXPECT_EQ(1048578, info->mLid); // 100002 x to d
    EXPECT_EQ(expected, info->ToString());
    EXPECT_EQ("eos/someapp",info->mAppTag);
    EXPECT_EQ("hybrid:tag1::tag3", info->mPlctPolicy);
  }

  {
    // Have only placement tag at tail
    input = "000000000000000d:default.3#00100002^eos/someapp^!";
    auto info = ConversionInfo::parseConversionString(input);
    EXPECT_EQ(1048578, info->mLid); // 100002 x to d
    EXPECT_EQ(input, info->ToString());
    EXPECT_EQ("eos/someapp",info->mAppTag);
    EXPECT_EQ("", info->mPlctPolicy);
    EXPECT_TRUE(info->mUpdateCtime);
  }

  {
    // Have only placement tag at tail
    input = "000000000000000d:default.3#00100002~hybrid::tag1::tag3";
    auto info = ConversionInfo::parseConversionString(input);
    EXPECT_EQ(1048578, info->mLid); // 100002 x to d
    EXPECT_EQ(input, info->ToString());
    EXPECT_EQ("",info->mAppTag);
    EXPECT_EQ("hybrid::tag1::tag3", info->mPlctPolicy);
    EXPECT_FALSE(info->mUpdateCtime);
  }

}
