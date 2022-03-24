// ----------------------------------------------------------------------
// File: WalkDirTreeTests
// Author: Abhishek Lekshmanan - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2022 CERN/Switzerland                                  *
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

#include <gtest/gtest.h>
#include <fstream>
#include <unordered_set>
#include "fst/utils/StdFSWalkTree.hh"
#include "fst/utils/FTSWalkTree.hh"
#include "TmpDirTree.hh"

using eos::fst::stdfs::IsRegularFile;

TEST_F(TmpDirTree, WalkFSTree)
{
  std::unordered_set<std::string> files;
  auto filter_fn = [](const auto& p)  {
    return IsRegularFile(p) && p->path().extension() != ".xsmap";
  };

  auto process_fn  = [&files](const fs::path& p, int) {
    files.emplace(p);
  };
  auto count = eos::fst::stdfs::WalkFSTree("/tmp/fstest", filter_fn, process_fn);
  ASSERT_EQ(count, 12);
  EXPECT_EQ(files, expected_files);
}


TEST_F(TmpDirTree, FTSWalkTree)
{
  std::unordered_set<std::string> files;
  auto process_fn  = [&files](const char* p) {
    //std::cout << "working on " << p << std::endl;
    files.emplace(p);
  };
  auto ret = eos::fst::WalkFSTree("/tmp/fstest", process_fn);
  ASSERT_TRUE(ret.status);
  ASSERT_EQ(ret.count, 12);
  EXPECT_EQ(files, expected_files);
}