// ----------------------------------------------------------------------
// File: TmpDirTree
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
#pragma once


#if defined(__clang__) && __clang_major__ < 6
#include <experimental/filesystem>
#else
#include <filesystem>
#endif
#include <unordered_set>


#if defined(__clang__) && __clang_major__ < 6
namespace fs = std::experimental::filesystem::v1;
#else
namespace fs = std::filesystem;
#endif



static const std::string BASE_DIR="fstest";

// This file has a useful test fixture TmpDirTree that
// creates the following tree
// fstest
//        a0
//           a1
//              file0
//              [..] file5
// ...

static void create_files(std::string path, int count){
  for (int i=0; i < count; ++i) {
    std::string filename = path + "/file" + std::to_string(i);
    std::ofstream f(filename.c_str());
  }
}

static std::string TMP_DIR_ROOT = fs::temp_directory_path().native() + "/fstest";

class TmpDirTree: public ::testing::Test
{
protected:
  void SetUp() override {
    fs::current_path(fs::temp_directory_path());
    fs::create_directories("fstest/a0/a1");
    fs::create_directories("fstest/b0/b1");
    fs::create_directories("fstest/c0/c1");

    create_files("fstest/a0/a1", 3);
    create_files("fstest/b0/b1", 3);
    create_files("fstest/c0/c1", 3);
    create_files("fstest",3);
    std::ofstream f(TMP_DIR_ROOT + "/test.xsmap");

    fs::create_directories("fstest/.hidden/hidden0");
    create_files("fstest/.hidden/hidden0",3);
    create_files("fstest/.hidden", 3);
  }

  void TearDown() override {
    fs::remove_all("fstest");
  }
};

static std::unordered_set<std::string> expected_files = {
    "/tmp/fstest/a0/a1/file0",
    "/tmp/fstest/a0/a1/file1",
    "/tmp/fstest/a0/a1/file2",
    "/tmp/fstest/b0/b1/file0",
    "/tmp/fstest/b0/b1/file1",
    "/tmp/fstest/b0/b1/file2",
    "/tmp/fstest/c0/c1/file0",
    "/tmp/fstest/c0/c1/file1",
    "/tmp/fstest/c0/c1/file2",
    "/tmp/fstest/file0",
    "/tmp/fstest/file1",
    "/tmp/fstest/file2"
};

