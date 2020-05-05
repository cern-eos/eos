//------------------------------------------------------------------------------
//! @file XrdIoTests.cc
//! @author Elvin Sindrilaru - CERN
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

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#define IN_TEST_HARNESS
#include "fst/io/xrd/XrdIo.hh"
#undef IN_TEST_HARNESS
#include "common/StringConversion.hh"
#include <memory>

TEST(XrdIo, BasicPrefetch)
{
  // auto& logging = eos::common::Logging::GetInstance();
  // logging.SetLogPriority(LOG_DEBUG);
  using namespace eos::common;
  std::set<int64_t> read_sizes {4 * KB, 1 * MB};
  std::string url =
    "root://esdss000.cern.ch:1089//tmp/xrootd0/test_file.dat?fst.readahead=true";
  std::unique_ptr<eos::fst::XrdIo> file {new eos::fst::XrdIo(url)};
  struct stat info;
  ASSERT_EQ(file->fileOpen(SFS_O_RDONLY), 0);
  ASSERT_EQ(file->fileStat(&info), 0);
  uint64_t offset {0ull};
  std::unique_ptr<char> buffer {new char[1 * MB]};

  for (const auto length : read_sizes) {
    while (offset < info.st_size) {
      if (offset + length > info.st_size) {
        break;
      }

      ASSERT_EQ(file->fileReadAsync(offset, buffer.get(), length, true), length);
      offset += length;
    }

    if (offset != info.st_size) {
      ASSERT_EQ(file->fileReadAsync(offset, buffer.get(), length, true),
                info.st_size - offset);
    }

    offset = 0ull;
    std::cout << "Read block size: " << length << std::endl
              << "Prefetched blocks: " << file->mPrefetchBlocks << std::endl
              << "Prefech hits: " << file->mPrefetchHits << std::endl;
  }
}
