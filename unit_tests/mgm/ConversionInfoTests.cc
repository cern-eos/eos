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
  input = "dummy0000000000d:default.3#00100002~hybrid:tag1::tag3!";
  ASSERT_EQ(nullptr, ConversionInfo::parseConversionString(input));
  input = "000000000000000d:default.3#00xyz02~hybrid:tag1::tag3!";
  ASSERT_EQ(nullptr, ConversionInfo::parseConversionString(input));
}
