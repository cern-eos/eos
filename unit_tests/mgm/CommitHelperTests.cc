//------------------------------------------------------------------------------
// File: CommitHelperTests.cc
// Author: Elvin-Alin Sindrilaru <esindril at cern dot ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2024 CERN/Switzerland                                  *
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
#define IN_TEST_HARNESS
#include "mgm/ofs/fsctl/CommitHelper.hh"
#undef IN_TEST_HARNESS

using eos::mgm::CommitHelper;

//------------------------------------------------------------------------------
// Test incrementing the timestamp for versioned files
//------------------------------------------------------------------------------
TEST(CommitHelperTest, IncTsVerFn)
{
  std::string fn = "1724758410.00001111";
  ASSERT_STREQ("1724758411.00001111", CommitHelper::IncrementTsForVersionFn(fn).c_str());
  fn = "dummy.test";
  ASSERT_STREQ(fn.c_str(), CommitHelper::IncrementTsForVersionFn(fn).c_str());
  fn = "dummy";
  ASSERT_STREQ(fn.c_str(), CommitHelper::IncrementTsForVersionFn(fn).c_str());
  fn = "1724758410.";
  ASSERT_STREQ(fn.c_str(), CommitHelper::IncrementTsForVersionFn(fn).c_str());
  fn = "abcf.0001111";
  ASSERT_STREQ(fn.c_str(), CommitHelper::IncrementTsForVersionFn(fn).c_str());
  fn = "1724758420.aabbccdd";
  ASSERT_STREQ("1724758421.aabbccdd", CommitHelper::IncrementTsForVersionFn(fn).c_str());
}


