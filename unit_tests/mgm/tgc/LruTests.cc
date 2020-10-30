//------------------------------------------------------------------------------
// File: LruTests.cc
// Author: Steven Murray <smurray at cern dot ch>
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

#include "mgm/tgc/Lru.hh"
#include "mgm/tgc/MaxLenExceeded.hh"

#include <gtest/gtest.h>

class TgcLruTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcLruTest, Construction_maxQueueSize_greater_than_zero)
{
  using namespace eos::mgm::tgc;

  const Lru::FidQueue::size_type maxQueueSize = 5;
  Lru lru(maxQueueSize);
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcLruTest, Construction_maxQueueSize_zero)
{
  using namespace eos::mgm::tgc;

  const Lru::FidQueue::size_type maxQueueSize = 0;
  ASSERT_THROW(Lru lru(maxQueueSize),
    Lru::MaxQueueSizeIsZero);
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcLruTest, getAndPopFidOfLeastUsedFile_empty_queue)
{
  using namespace eos::mgm::tgc;

  const Lru::FidQueue::size_type maxQueueSize = 5;
  Lru lru(maxQueueSize);
  ASSERT_THROW(lru.getAndPopFidOfLeastUsedFile(), Lru::QueueIsEmpty);
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcLruTest, fids_1_2_3_4_5)
{ 
  using namespace eos;
  using namespace eos::mgm::tgc;

  const std::list<IFileMD::id_t> fidsIn = {1, 2, 3, 4, 5};

  const std::list<IFileMD::id_t> fidsOut = fidsIn;

  const Lru::FidQueue::size_type maxQueueSize = fidsOut.size();
  Lru lru(maxQueueSize);

  for(const auto fid: fidsIn) {
    lru.fileAccessed(fid);
  }

  ASSERT_EQ(fidsOut.size(), lru.size());

  for(const auto fid: fidsOut) {
    ASSERT_FALSE(lru.empty());
    ASSERT_EQ(fid, lru.getAndPopFidOfLeastUsedFile());
  }

  ASSERT_TRUE(lru.empty());
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcLruTest, fids_1_2_3_4_5_2)
{ 
  using namespace eos;
  using namespace eos::mgm::tgc;

  const std::list<IFileMD::id_t> fidsIn = {1, 2, 3, 4, 5, 2};

  const std::list<IFileMD::id_t> fidsOut = {1, 3, 4, 5, 2};

  const Lru::FidQueue::size_type maxQueueSize = fidsOut.size();
  Lru lru(maxQueueSize);

  for(const auto fid: fidsIn) {
    lru.fileAccessed(fid);
  }

  ASSERT_EQ(fidsOut.size(), lru.size());

  for(const auto fid: fidsOut) {
    ASSERT_FALSE(lru.empty());
    ASSERT_EQ(fid, lru.getAndPopFidOfLeastUsedFile());
  }

  ASSERT_TRUE(lru.empty());
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcLruTest, fileDeletedFromNamespace)
{ 
  using namespace eos;
  using namespace eos::mgm::tgc;

  // Emulate deleting the file with ID 4 from the EOS namespace
  const std::list<IFileMD::id_t> fidsIn = {1, 2, 3, 4, 5};
  const std::list<IFileMD::id_t> fidsOut = {1, 2, 3, 5};

  const Lru::FidQueue::size_type maxQueueSize = fidsIn.size();
  Lru lru(maxQueueSize);

  for(const auto fid: fidsIn) {
    lru.fileAccessed(fid);
  }

  ASSERT_EQ(fidsIn.size(), lru.size());

  lru.fileDeletedFromNamespace(4);

  ASSERT_EQ(fidsOut.size(), lru.size());

  for(const auto fid: fidsOut) {
    ASSERT_FALSE(lru.empty());
    ASSERT_EQ(fid, lru.getAndPopFidOfLeastUsedFile());
  }

  ASSERT_TRUE(lru.empty());
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcLruTest, exceed_maxQueueSize_max_size_1)
{
  using namespace eos;
  using namespace eos::mgm::tgc;

  const Lru::FidQueue::size_type maxQueueSize = 1;
  Lru lru(maxQueueSize);

  ASSERT_TRUE(lru.empty());
  ASSERT_EQ(0, lru.size());
  ASSERT_FALSE(lru.maxQueueSizeExceeded());

  lru.fileAccessed(1);

  ASSERT_FALSE(lru.empty());
  ASSERT_EQ(1, lru.size());
  ASSERT_FALSE(lru.maxQueueSizeExceeded());

  lru.fileAccessed(2);

  ASSERT_FALSE(lru.empty());
  ASSERT_EQ(1, lru.size());
  ASSERT_TRUE(lru.maxQueueSizeExceeded());

  ASSERT_EQ(1, lru.getAndPopFidOfLeastUsedFile());

  ASSERT_TRUE(lru.empty());
  ASSERT_EQ(0, lru.size());
  ASSERT_FALSE(lru.maxQueueSizeExceeded());
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcLruTest, exceed_maxQueueSize_5_fids_vs_max_size_2)
{
  using namespace eos;
  using namespace eos::mgm::tgc;
  const std::list<IFileMD::id_t> fidsIn = {1, 2, 3, 4, 5};

  const std::list<IFileMD::id_t> fidsOut = {1, 2};

  const Lru::FidQueue::size_type maxQueueSize = fidsOut.size();
  Lru lru(maxQueueSize);

  ASSERT_TRUE(lru.empty());
  ASSERT_EQ(0, lru.size());
  ASSERT_FALSE(lru.maxQueueSizeExceeded());

  for(const auto fid: fidsIn) {
    lru.fileAccessed(fid);

    ASSERT_FALSE(lru.empty());

    if(fid <= maxQueueSize) {
      ASSERT_EQ(fid, lru.size());
      ASSERT_FALSE(lru.maxQueueSizeExceeded());
    } else {
      ASSERT_EQ(maxQueueSize, lru.size());
      ASSERT_TRUE(lru.maxQueueSizeExceeded());
    }
  }

  ASSERT_EQ(maxQueueSize, lru.size());

  for(const auto fid: fidsOut) {
    ASSERT_FALSE(lru.empty());
    ASSERT_EQ(fid, lru.getAndPopFidOfLeastUsedFile());
    ASSERT_FALSE(lru.maxQueueSizeExceeded());
  }

  ASSERT_TRUE(lru.empty());
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcLruTest, DISABLED_performance_500000_files) {
  using namespace eos;
  using namespace eos::mgm::tgc;

  const Lru::FidQueue::size_type maxQueueSize = 500000;
  Lru lru(maxQueueSize);

  for(Lru::FidQueue::size_type fid = 0; fid < maxQueueSize; fid++) {
    const auto start = std::chrono::high_resolution_clock::now();
    lru.fileAccessed(fid);
    const auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end - start;
    std::cout << "Time elapsed for fid " << fid << " = " << elapsed.count() << " seconds" << std::endl;
  }
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcLruTest, toJson)
{
  using namespace eos;
  using namespace eos::mgm::tgc;

  const std::list<IFileMD::id_t> fidsIn = {1, 2};

  const Lru::FidQueue::size_type maxQueueSize = fidsIn.size();
  Lru lru(maxQueueSize);

  for(const auto fid: fidsIn) {
    lru.fileAccessed(fid);
  }

  ASSERT_EQ(fidsIn.size(), lru.size());

  const std::string expectedJson =
    "{\"size\":\"2\",\"fids_from_MRU_to_LRU\":[\"0x0000000000000002\",\"0x0000000000000001\"]}";

  std::ostringstream json;
  lru.toJson(json);
  ASSERT_EQ(expectedJson, json.str());
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcLruTest, toJson_exceed_maxLen)
{
  using namespace eos;
  using namespace eos::mgm::tgc;

  const Lru::FidQueue::size_type maxQueueSize = 1;
  Lru lru(maxQueueSize);

  std::ostringstream json;
  const std::string::size_type maxLen = 1;
  ASSERT_THROW(lru.toJson(json, maxLen), MaxLenExceeded);
}
