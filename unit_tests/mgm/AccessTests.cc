//------------------------------------------------------------------------------
// File: RoutingTests.cc
// Author: Elvin-Alin Sindrilaru <esindril at cern dot ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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
#include "mgm/Access.hh"

//------------------------------------------------------------------------------
// Test basic access functionality
//------------------------------------------------------------------------------
TEST(Access, SetRule)
{
  using namespace eos::mgm;
  Access::StallInfo old_stall;
  Access::StallInfo new_stall("*", "60", "test stall", true);
  ASSERT_EQ(false, Access::gStallGlobal);
  // Set new stall state
  Access::SetStallRule(new_stall, old_stall);
  // Do checks without taking the lock as this is just for test purposes
  ASSERT_STREQ("60", Access::gStallRules[new_stall.mType].c_str());
  ASSERT_STREQ("test stall", Access::gStallComment[new_stall.mType].c_str());
  ASSERT_EQ(new_stall.mIsGlobal, Access::gStallGlobal);
  Access::StallInfo empty_stall;
  // Setting an empty stall should not change anything
  Access::SetStallRule(empty_stall, old_stall);
  ASSERT_STREQ("60", Access::gStallRules[new_stall.mType].c_str());
  ASSERT_STREQ("test stall", Access::gStallComment[new_stall.mType].c_str());
  ASSERT_EQ(new_stall.mIsGlobal, Access::gStallGlobal);
  // Revert to initial state
  Access::StallInfo tmp_stall;
  Access::SetStallRule(old_stall, tmp_stall);
  ASSERT_TRUE(Access::gStallRules.count(old_stall.mType) == 0);
  ASSERT_TRUE(Access::gStallComment.count(old_stall.mType) == 0);
  ASSERT_EQ(old_stall.mIsGlobal, Access::gStallGlobal);
}
