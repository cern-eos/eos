//------------------------------------------------------------------------------
//! @file PlacementStrategy.hh
//! @author Abhishek Lekshmanan <abhishek.lekshmanan@cern.ch>
//-----------------------------------------------------------------------------

/************************************************************************
  * EOS - the CERN Disk Storage System                                   *
  * Copyright (C) 2024 CERN/Switzerland                           *
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


#include "mgm/placement/PlacementStrategy.hh"
#include "gtest/gtest.h"

TEST(PlacementResult, default)
{
  eos::mgm::placement::PlacementResult result;
  EXPECT_EQ(result.ret_code, -1);
  EXPECT_EQ(result.error_string(), "");
  EXPECT_FALSE(result.is_valid_placement(2));
}

TEST(PlacementResult, is_valid_placement)
{
  eos::mgm::placement::PlacementResult result(2);
  result.ids =  {1,2};
  EXPECT_TRUE(result.is_valid_placement(2));

  eos::mgm::placement::PlacementResult result2(2);
  result2.ids = {1,-1};
  EXPECT_FALSE(result2.is_valid_placement(2));
}

TEST(PlacementResult, contains)
{
  eos::mgm::placement::PlacementResult result(2);
  result.ids = {1,2};
  EXPECT_TRUE(result.contains(1));
  EXPECT_TRUE(result.contains(2));
  EXPECT_FALSE(result.contains(3));
}

TEST(PlacementResult, contains_invalid)
{
  eos::mgm::placement::PlacementResult result(2);
  result.ids = {4,3,2,1}; // anything beyond 2nd slot is irrelevant
  EXPECT_FALSE(result.contains(2));
  EXPECT_FALSE(result.contains(1));
  EXPECT_TRUE(result.contains(4));
  EXPECT_TRUE(result.contains(3));
}