//------------------------------------------------------------------------------
// File: UtilsTest.cc
// Author: Georgios Bitzes - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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

#include "fst/tests/TestEnv.hh"
#include "fst/utils/OpenFileTracker.hh"
#include "gtest/gtest.h"

TEST(OpenFileTracker, BasicSanity) {
  eos::fst::OpenFileTracker oft;

  ASSERT_FALSE(oft.isAnyOpen());

  // fsid=1, fid=99
  ASSERT_FALSE(oft.isOpen(1, 99));
  oft.up(1, 99);
  ASSERT_TRUE(oft.isAnyOpen());
  ASSERT_TRUE(oft.isOpen(1, 99));
  ASSERT_EQ(oft.getUseCount(1, 99), 1);

  oft.down(1, 99);
  ASSERT_FALSE(oft.isOpen(1, 99));
  ASSERT_EQ(oft.getUseCount(1, 99), 0);
  ASSERT_FALSE(oft.isAnyOpen());


  oft.up(2, 100); // fsid=2, fid=100
  ASSERT_TRUE(oft.isAnyOpen());

  oft.up(3, 101); // fsid=3, fid=101
  oft.up(3, 101);
  oft.up(3, 101);

  oft.up(9, 102); // fsid=9, fid=102

  ASSERT_FALSE(oft.isOpen(2, 101));
  ASSERT_TRUE(oft.isOpen(2, 100));
  ASSERT_TRUE(oft.isOpen(3, 101));
  ASSERT_TRUE(oft.isOpen(9, 102));

  ASSERT_EQ(oft.getUseCount(2, 100), 1);
  ASSERT_EQ(oft.getUseCount(3, 101), 3);
  ASSERT_EQ(oft.getUseCount(9, 102), 1);


  oft.down(3, 101);
  ASSERT_EQ(oft.getUseCount(3, 101), 2);

  oft.down(3, 101);
  ASSERT_EQ(oft.getUseCount(3, 101), 1);

  oft.down(3, 101);
  ASSERT_EQ(oft.getUseCount(3, 101), 0);
  ASSERT_FALSE(oft.isOpen(3, 101));

  // invalid operation, as (3, 101) is already at 0
  // prints error in the log
  oft.down(3, 101);
  ASSERT_FALSE(oft.isOpen(3, 101));
  ASSERT_EQ(oft.getUseCount(3, 101), 0);

  ASSERT_TRUE(oft.isOpen(9, 102));
}

TEST(OpenFileTracker, SortedByUseCount) {
  eos::fst::OpenFileTracker oft;

  auto sorted = oft.getSortedByUsecount(3);
  ASSERT_TRUE(sorted.empty());

  oft.up(3, 101);
  oft.up(3, 101);
  oft.up(3, 101);

  oft.up(3, 102);
  oft.up(3, 102);

  oft.up(3, 103);

  oft.up(3, 104);
  oft.up(3, 104);
  oft.up(3, 104);

  oft.up(3, 105);

  sorted = oft.getSortedByUsecount(3);
  ASSERT_EQ(sorted.size(), 3);

  std::set<uint64_t> contents;
  contents = {101, 104};
  ASSERT_EQ(sorted[3], contents);

  contents = {102};
  ASSERT_EQ(sorted[2], contents);

  contents = {103, 105};
  ASSERT_EQ(sorted[1], contents);

  for(size_t i = 0; i < 5; i++) {
    oft.up(3, 100);
  }

  sorted = oft.getSortedByUsecount(3);
  ASSERT_EQ(sorted.size(), 4);

  contents = {100};
  ASSERT_EQ(sorted[5], contents);
}
