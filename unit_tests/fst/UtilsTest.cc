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

#include "TestEnv.hh"
#include "fst/utils/OpenFileTracker.hh"
#include "gtest/gtest.h"

TEST(OpenFileTracker, BasicSanity)
{
  eos::fst::OpenFileTracker oft;
  ASSERT_FALSE(oft.isAnyOpen());
  // fsid=1, fid=99
  ASSERT_FALSE(oft.isOpen(1, 99));
  oft.up(1, 99);
  ASSERT_TRUE(oft.isAnyOpen());
  ASSERT_TRUE(oft.isOpen(1, 99));
  ASSERT_EQ(oft.getUseCount(1, 99), 1);
  ASSERT_EQ(oft.getOpenOnFilesystem(1), 1);
  ASSERT_EQ(oft.getOpenOnFilesystem(2), 0);
  oft.down(1, 99);
  ASSERT_FALSE(oft.isOpen(1, 99));
  ASSERT_EQ(oft.getUseCount(1, 99), 0);
  ASSERT_FALSE(oft.isAnyOpen());
  ASSERT_EQ(oft.getOpenOnFilesystem(1), 0);
  ASSERT_EQ(oft.getOpenOnFilesystem(2), 0);
  oft.up(2, 100); // fsid=2, fid=100
  ASSERT_TRUE(oft.isAnyOpen());
  oft.up(3, 101); // fsid=3, fid=101
  oft.up(3, 101);
  oft.up(3, 101);
  oft.up(9, 102); // fsid=9, fid=102
  ASSERT_EQ(oft.getOpenOnFilesystem(2), 1);
  ASSERT_EQ(oft.getOpenOnFilesystem(3), 1);
  ASSERT_EQ(oft.getOpenOnFilesystem(9), 1);
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
  ASSERT_EQ(oft.getOpenOnFilesystem(3), 0);
  // invalid operation, as (3, 101) is already at 0
  // prints error in the log
  oft.down(3, 101);
  ASSERT_FALSE(oft.isOpen(3, 101));
  ASSERT_EQ(oft.getUseCount(3, 101), 0);
  ASSERT_TRUE(oft.isOpen(9, 102));
}

TEST(OpenFileTracker, SortedByUseCount)
{
  eos::fst::OpenFileTracker oft;
  auto sorted = oft.getSortedByUsecount(3);
  ASSERT_TRUE(sorted.empty());
  oft.up(3, 101);
  oft.up(3, 101);
  oft.up(3, 101);
  ASSERT_EQ(oft.getOpenOnFilesystem(3), 1);
  oft.up(3, 102);
  oft.up(3, 102);
  ASSERT_EQ(oft.getOpenOnFilesystem(3), 2);
  oft.up(3, 103);
  ASSERT_EQ(oft.getOpenOnFilesystem(3), 3);
  oft.up(3, 104);
  oft.up(3, 104);
  oft.up(3, 104);
  ASSERT_EQ(oft.getOpenOnFilesystem(3), 4);
  oft.up(3, 105);
  ASSERT_EQ(oft.getOpenOnFilesystem(3), 5);
  sorted = oft.getSortedByUsecount(3);
  ASSERT_EQ(sorted.size(), 3);
  std::set<uint64_t> contents;
  contents = {101, 104};
  ASSERT_EQ(sorted[3], contents);
  contents = {102};
  ASSERT_EQ(sorted[2], contents);
  contents = {103, 105};
  ASSERT_EQ(sorted[1], contents);

  for (size_t i = 0; i < 5; i++) {
    oft.up(3, 100);
  }

  sorted = oft.getSortedByUsecount(3);
  ASSERT_EQ(sorted.size(), 4);
  contents = {100};
  ASSERT_EQ(sorted[5], contents);
  auto hotFiles = oft.getHotFiles(3, 1);
  ASSERT_EQ(hotFiles.size(), 1u);
  eos::fst::OpenFileTracker::HotEntry entry;
  entry = {3, 100, 5};
  ASSERT_EQ(hotFiles[0], entry);
  hotFiles = oft.getHotFiles(3, 2);
  ASSERT_EQ(hotFiles.size(), 2u);
  entry = {3, 100, 5};
  ASSERT_EQ(hotFiles[0], entry);
  entry = {3, 101, 3};
  ASSERT_EQ(hotFiles[1], entry);
  hotFiles = oft.getHotFiles(3, 3);
  ASSERT_EQ(hotFiles.size(), 3u);
  entry = {3, 100, 5};
  ASSERT_EQ(hotFiles[0], entry);
  entry = {3, 101, 3};
  ASSERT_EQ(hotFiles[1], entry);
  entry = {3, 104, 3};
  ASSERT_EQ(hotFiles[2], entry);
  hotFiles = oft.getHotFiles(3, 4);
  ASSERT_EQ(hotFiles.size(), 4u);
  entry = {3, 100, 5};
  ASSERT_EQ(hotFiles[0], entry);
  entry = {3, 101, 3};
  ASSERT_EQ(hotFiles[1], entry);
  entry = {3, 104, 3};
  ASSERT_EQ(hotFiles[2], entry);
  ASSERT_NE(hotFiles[3], entry);
  entry = {3, 102, 2};
  ASSERT_EQ(hotFiles[3], entry);
  hotFiles = oft.getHotFiles(3, 5);
  ASSERT_EQ(hotFiles.size(), 5u);
  entry = {3, 100, 5};
  ASSERT_EQ(hotFiles[0], entry);
  entry = {3, 101, 3};
  ASSERT_EQ(hotFiles[1], entry);
  entry = {3, 104, 3};
  ASSERT_EQ(hotFiles[2], entry);
  ASSERT_NE(hotFiles[3], entry);
  entry = {3, 102, 2};
  ASSERT_EQ(hotFiles[3], entry);
  entry = {3, 103, 1};
  ASSERT_EQ(hotFiles[4], entry);
  hotFiles = oft.getHotFiles(3, 6);
  ASSERT_EQ(hotFiles.size(), 6u);
  entry = {3, 100, 5};
  ASSERT_EQ(hotFiles[0], entry);
  entry = {3, 101, 3};
  ASSERT_EQ(hotFiles[1], entry);
  entry = {3, 104, 3};
  ASSERT_EQ(hotFiles[2], entry);
  ASSERT_NE(hotFiles[3], entry);
  entry = {3, 102, 2};
  ASSERT_EQ(hotFiles[3], entry);
  entry = {3, 103, 1};
  ASSERT_EQ(hotFiles[4], entry);
  entry = {3, 105, 1};
  ASSERT_EQ(hotFiles[5], entry);
  // Only have 6 items in total
  auto hotFiles2 = oft.getHotFiles(3, 7);
  ASSERT_EQ(hotFiles, hotFiles2);
  hotFiles2 = oft.getHotFiles(3, 1000000);
  ASSERT_EQ(hotFiles, hotFiles2);
  auto hotFiles3 = oft.getHotFiles(3, 0);
  ASSERT_TRUE(hotFiles3.empty());
}
