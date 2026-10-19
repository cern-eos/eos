//------------------------------------------------------------------------------
// File: ClayLayoutIdTests.cc
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

#include "common/LayoutId.hh"
#include "gtest/gtest.h"

TEST(LayoutId, ClayProfilesAreFixed)
{
  using eos::common::LayoutId;
  // A caller-supplied stripe count is replaced by the profile geometry.
  // clay15 is 15 data + 3 parity, not 12+3.
  const unsigned long clay10 = LayoutId::GetId(
                                 LayoutId::kClay10, LayoutId::kAdler, 6, LayoutId::k1M);
  EXPECT_EQ(LayoutId::GetLayoutType(clay10),
            static_cast<unsigned long>(LayoutId::kClay10));
  EXPECT_STREQ(LayoutId::GetLayoutTypeString(clay10), "clay10");
  EXPECT_EQ(LayoutId::GetStripeNumber(clay10) + 1, 12ul);
  EXPECT_EQ(LayoutId::GetRedundancyStripeNumber(clay10), 2ul);
  EXPECT_EQ(LayoutId::GetLayoutFromString("clay10"), LayoutId::kClay10);
  EXPECT_NEAR(LayoutId::GetSizeFactor(clay10), 12.0 / 10.0, 1e-9);

  const unsigned long clay15 = LayoutId::GetId(
                                 LayoutId::kClay15, LayoutId::kAdler, 12, LayoutId::k1M,
                                 LayoutId::kCRC32C, 0, 4);
  EXPECT_EQ(LayoutId::GetLayoutType(clay15),
            static_cast<unsigned long>(LayoutId::kClay15));
  EXPECT_STREQ(LayoutId::GetLayoutTypeString(clay15), "clay15");
  EXPECT_EQ(LayoutId::GetStripeNumber(clay15) + 1, 18ul);
  EXPECT_EQ(LayoutId::GetRedundancyStripeNumber(clay15), 3ul);
  EXPECT_EQ(LayoutId::GetLayoutFromString("clay15"), LayoutId::kClay15);
  EXPECT_EQ(LayoutId::GetFixedStripeCount(LayoutId::kClay15), 18ul);
  EXPECT_EQ(LayoutId::GetClayMinBlockType(LayoutId::kClay15), LayoutId::k4M);
  EXPECT_EQ(LayoutId::GetClayDefaultBlockType(LayoutId::kClay10), LayoutId::k1M);
  EXPECT_TRUE(LayoutId::IsRain(clay15));
  EXPECT_NEAR(LayoutId::GetSizeFactor(clay15), 18.0 / 15.0, 1e-9);
}
