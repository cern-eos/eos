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

  // fsid=1, fid=99
  ASSERT_FALSE(oft.isOpen(1, 99));
  oft.up(1, 99);
  ASSERT_TRUE(oft.isOpen(1, 99));
  ASSERT_EQ(oft.getUseCount(1, 99), 1);

  oft.down(1, 99);
  ASSERT_FALSE(oft.isOpen(1, 99));
  ASSERT_EQ(oft.getUseCount(1, 99), 0);


  oft.up(2, 100); // fsid=2, fid=100

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
}
