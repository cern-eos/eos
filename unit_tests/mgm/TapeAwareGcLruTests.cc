//------------------------------------------------------------------------------
// File: TapeAwareLruTests.cc
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

#include "mgm/TapeAwareGcLru.hh"

#include <gtest/gtest.h>

class TapeAwareGcLruTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TapeAwareGcLruTest, Construction_maxQueueSize_greater_than_zero)
{
  using namespace eos::mgm;

  const TapeAwareGcLru::FidQueue::size_type maxQueueSize = 5;
  TapeAwareGcLru lru(maxQueueSize);
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TapeAwareGcLruTest, Construction_maxQueueSize_zero)
{
  using namespace eos::mgm;

  const TapeAwareGcLru::FidQueue::size_type maxQueueSize = 0;
  ASSERT_THROW(TapeAwareGcLru lru(maxQueueSize),
    TapeAwareGcLru::MaxQueueSizeIsZero);
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TapeAwareGcLruTest, getAndPopFidOfLeastUsedFile_empty_queue)
{
  using namespace eos::mgm;

  const TapeAwareGcLru::FidQueue::size_type maxQueueSize = 5;
  TapeAwareGcLru lru(maxQueueSize);
  ASSERT_THROW(lru.getAndPopFidOfLeastUsedFile(), TapeAwareGcLru::QueueIsEmpty);
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TapeAwareGcLruTest, fids_1_2_3_4_5)
{ 
  using namespace eos;
  using namespace eos::mgm;

  const std::list<FileIdentifier> fidsIn = {FileIdentifier(1),
    FileIdentifier(2), FileIdentifier(3), FileIdentifier(4), FileIdentifier(5)};

  const std::list<FileIdentifier> fidsOut = fidsIn;

  const TapeAwareGcLru::FidQueue::size_type maxQueueSize = fidsOut.size();
  TapeAwareGcLru lru(maxQueueSize);

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
TEST_F(TapeAwareGcLruTest, fids_1_2_3_4_5_2)
{ 
  using namespace eos;
  using namespace eos::mgm;

  const std::list<FileIdentifier> fidsIn = {FileIdentifier(1),
    FileIdentifier(2), FileIdentifier(3), FileIdentifier(4), FileIdentifier(5),
    FileIdentifier(2)};

  const std::list<FileIdentifier> fidsOut = {FileIdentifier(1),
    FileIdentifier(3), FileIdentifier(4), FileIdentifier(5), FileIdentifier(2)};

  const TapeAwareGcLru::FidQueue::size_type maxQueueSize = fidsOut.size();
  TapeAwareGcLru lru(maxQueueSize);

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
TEST_F(TapeAwareGcLruTest, exceed_maxQueueSize_max_size_1)
{
  using namespace eos;
  using namespace eos::mgm;

  const TapeAwareGcLru::FidQueue::size_type maxQueueSize = 1;
  TapeAwareGcLru lru(maxQueueSize);

  ASSERT_TRUE(lru.empty());
  ASSERT_EQ(0, lru.size());
  ASSERT_FALSE(lru.maxQueueSizeExceeded());

  lru.fileAccessed(FileIdentifier(1));

  ASSERT_FALSE(lru.empty());
  ASSERT_EQ(1, lru.size());
  ASSERT_FALSE(lru.maxQueueSizeExceeded());

  lru.fileAccessed(FileIdentifier(2));

  ASSERT_FALSE(lru.empty());
  ASSERT_EQ(1, lru.size());
  ASSERT_TRUE(lru.maxQueueSizeExceeded());

  ASSERT_EQ(FileIdentifier(1), lru.getAndPopFidOfLeastUsedFile());

  ASSERT_TRUE(lru.empty());
  ASSERT_EQ(0, lru.size());
  ASSERT_FALSE(lru.maxQueueSizeExceeded());
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TapeAwareGcLruTest, exceed_maxQueueSize_5_fids_vs_max_size_2)
{
  using namespace eos;
  using namespace eos::mgm;
  const std::list<FileIdentifier> fidsIn = {FileIdentifier(1),
    FileIdentifier(2), FileIdentifier(3), FileIdentifier(4), FileIdentifier(5)};

  const std::list<FileIdentifier> fidsOut = {FileIdentifier(1),
    FileIdentifier(2)};

  const TapeAwareGcLru::FidQueue::size_type maxQueueSize = fidsOut.size();
  TapeAwareGcLru lru(maxQueueSize);

  ASSERT_TRUE(lru.empty());
  ASSERT_EQ(0, lru.size());
  ASSERT_FALSE(lru.maxQueueSizeExceeded());

  for(const auto fid: fidsIn) {
    lru.fileAccessed(fid);

    ASSERT_FALSE(lru.empty());

    if(fid.getUnderlyingUInt64() <= maxQueueSize) {
      ASSERT_EQ(fid.getUnderlyingUInt64(), lru.size());
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
