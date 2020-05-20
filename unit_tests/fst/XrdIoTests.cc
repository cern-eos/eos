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

#include "gtest/gtest.h"
#define IN_TEST_HARNESS
#include "fst/io/xrd/XrdIo.hh"
#undef IN_TEST_HARNESS
#include "TestEnv.hh"

TEST(XrdIo, BasicPrefetch)
{
  //auto& logging = eos::common::Logging::GetInstance();
  //logging.SetLogPriority(LOG_DEBUG);
  using namespace eos::common;
  std::set<int64_t> read_sizes {4 * KB, 1 * MB};
  std::string address = "root://root@" + gEnv->GetMapping("server");
  std::string file_path = gEnv->GetMapping("replica_file");
  // Validate URL
  XrdCl::URL url(address);
  ASSERT_TRUE(url.IsValid());
  std::string file_url = address + "/" + file_path + "?fst.readahead=true";
  std::unique_ptr<eos::fst::XrdIo> file {new eos::fst::XrdIo(file_url)};
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
    GLOG << "Read block size: " << length << std::endl;
    GLOG << "Prefetched blocks: " << file->mPrefetchBlocks << std::endl;
    GLOG << "Prefech hits: " << file->mPrefetchHits << std::endl;
    ASSERT_EQ(file->mPrefetchBlocks,
              std::ceil((info.st_size - length) * 1.0 / file->mBlocksize));
    ASSERT_EQ(file->mPrefetchHits, (int)(info.st_size - length) / length);
    // Reset prefetch counters
    file->mPrefetchHits = 0ull;
    file->mPrefetchBlocks = 0ull;
  }
}
