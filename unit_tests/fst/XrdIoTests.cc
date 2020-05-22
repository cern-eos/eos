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
#include "fst/checksum/Adler.hh"
#include "TestEnv.hh"
#include <string.h>

//------------------------------------------------------------------------------
// MockSimpleHandler that can throw errors at different offsets
//------------------------------------------------------------------------------
class MockSimpleHandler: public eos::fst::SimpleHandler
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  MockSimpleHandler(uint64_t fail_offset = 0, uint64_t offset = 0,
                    int32_t length = 0, bool isWrite = false):
    SimpleHandler(offset, length, isWrite),
    mFailOffset(fail_offset)
  {}

  //----------------------------------------------------------------------------
  //! Handle response
  //!
  //! @param pStatus status of the response
  //! @param pResponse object containing extra info about the response
  //----------------------------------------------------------------------------
  virtual void HandleResponse(XrdCl::XRootDStatus* pStatus,
                              XrdCl::AnyObject* pResponse)
  {
    if (mFailOffset &&
        (mOffset <= mFailOffset) &&
        (mOffset + mLength > mFailOffset)) {
      // Do some extra check for the read case
      if ((mIsWrite == false) && (pResponse)) {
        XrdCl::ChunkInfo* chunk = 0;
        pResponse->Get(chunk);
        mRespLength = chunk->length;
      }

      GLOG << "Failing at offset " << mOffset << " and length: " << mLength
           << " fail_offset: " << mFailOffset << std::endl;
      mCond.Lock();
      // Note: we return false on purpose
      mRespOK = false;
      mReqDone = true;
      mCond.Signal(); //signal
      mCond.UnLock();
      delete pStatus;

      if (pResponse) {
        delete pResponse;
      }

      return;
    } else {
      return SimpleHandler::HandleResponse(pStatus, pResponse);
    }
  }

  uint64_t mFailOffset {0ull};
};

TEST(XrdIo, BasicPrefetch)
{
  //auto& logging = eos::common::Logging::GetInstance();
  //logging.SetLogPriority(LOG_DEBUG);
  using namespace eos::common;
  std::set<int64_t> read_sizes {11, 23, 4 * KB, 99999, 1 * MB};
  std::string address = "root://root@" + gEnv->GetMapping("server");
  std::string file_path = gEnv->GetMapping("prefetch_file");
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
  std::unique_ptr<char> file_in_mem {new char[info.st_size]};

  for (const auto length : read_sizes) {
    while (offset < info.st_size) {
      if (offset + length > info.st_size) {
        break;
      }

      ASSERT_EQ(file->fileReadPrefetch(offset, buffer.get(), length), length);
      memcpy(file_in_mem.get() + offset, buffer.get(), length);
      offset += length;
    }

    if (offset < info.st_size) {
      ASSERT_EQ(file->fileReadPrefetch(offset, buffer.get(), length),
                info.st_size - offset);
      memcpy(file_in_mem.get() + offset, buffer.get(), info.st_size - offset);
    }

    eos::fst::Adler checksum;
    checksum.Add(file_in_mem.get(), info.st_size, 0);
    checksum.Finalize();
    memset(file_in_mem.get(), 0, info.st_size);
    offset = 0ull;
    GLOG << "Read block size: " << length << std::endl;
    GLOG << "Prefetched blocks: " << file->mPrefetchBlocks << std::endl;
    GLOG << "Prefech hits: " << file->mPrefetchHits << std::endl;
    GLOG << "Checksum: " << checksum.GetHexChecksum() << std::endl;
    ASSERT_EQ(file->mPrefetchBlocks,
              std::ceil((info.st_size - length + 1) * 1.0 / file->mBlocksize));
    ASSERT_EQ(file->mPrefetchHits,
              std::ceil((info.st_size - length + 1) * 1.0 / length));
    ASSERT_STREQ(checksum.GetHexChecksum(), "b25bae07");
    ASSERT_TRUE(file->mDoReadahead);
    // Reset prefetch counters
    file->mPrefetchHits = 0ull;
    file->mPrefetchBlocks = 0ull;
    ASSERT_EQ(file->fileWaitAsyncIO(), 0);
  }
}

TEST(XrdIo, FailPrefetchInFlight)
{
  //auto& logging = eos::common::Logging::GetInstance();
  //logging.SetLogPriority(LOG_DEBUG);
  using namespace eos::common;
  std::set<int64_t> read_sizes {1171, 4 * KB, 99999};
  std::string address = "root://root@" + gEnv->GetMapping("server");
  std::string file_path = gEnv->GetMapping("prefetch_file");
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
  std::unique_ptr<char> file_in_mem {new char[info.st_size]};
  // Pre-fill the prefetch block with a custom handler that returns an error
  // for the given offset
  std::list<int64_t> err_offsets {8 * MB, 9 * MB + 123, 14 * MB};

  for (const auto err_off : err_offsets) {
    // Clear any readahead blocks
    while (!file->mQueueBlocks.empty()) {
      delete file->mQueueBlocks.front();
      file->mQueueBlocks.pop();
    }

    // Add new readahead blocks with custom error at offset
    for (unsigned int i = 0; i < file->mNumRdAheadBlocks; i++) {
      file->mQueueBlocks.push(new eos::fst::ReadaheadBlock(file->mBlocksize,
                              nullptr, new MockSimpleHandler(err_off)));
    }

    // Run test with different read size requests
    for (const auto length : read_sizes) {
      while (offset < info.st_size) {
        if (offset + length > info.st_size) {
          break;
        }

        ASSERT_EQ(file->fileReadPrefetch(offset, buffer.get(), length), length);
        memcpy(file_in_mem.get() + offset, buffer.get(), length);
        offset += length;
      }

      if (offset < info.st_size) {
        ASSERT_EQ(file->fileReadPrefetch(offset, buffer.get(), length),
                  info.st_size - offset);
        memcpy(file_in_mem.get() + offset, buffer.get(), info.st_size - offset);
      }

      ASSERT_EQ(file->fileWaitAsyncIO(), 0);
      eos::fst::Adler checksum;
      checksum.Add(file_in_mem.get(), info.st_size, 0);
      checksum.Finalize();
      memset(file_in_mem.get(), 0, info.st_size);
      offset = 0ull;
      GLOG << "Read block size: " << length << std::endl;
      GLOG << "Prefetched blocks: " << file->mPrefetchBlocks << std::endl;
      GLOG << "Prefech hits: " << file->mPrefetchHits << std::endl;
      GLOG << "Checksum: " << checksum.GetHexChecksum() << std::endl;
      ASSERT_EQ(file->mPrefetchBlocks,
                std::ceil((err_off - length + 1) * 1.0 / file->mBlocksize));
      ASSERT_EQ(file->mPrefetchHits,
                std::ceil((err_off - length - file->mBlocksize + 1) * 1.0 / length));
      ASSERT_STREQ(checksum.GetHexChecksum(), "b25bae07");
      ASSERT_FALSE(file->mDoReadahead);
      // Reset prefetch counters and prefetch flag
      file->mPrefetchHits = 0ull;
      file->mPrefetchBlocks = 0ull;
      file->mDoReadahead = true;
    }
  }
}
