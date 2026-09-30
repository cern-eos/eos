// ----------------------------------------------------------------------
// File: RRSeedTests
// Author: Abhishek Lekshmanan - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2023 CERN/Switzerland                           *
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

#include "mgm/placement/RRSeed.hh"
#include "gtest/gtest.h"
#include <limits>
#include <set>
#include <thread>

using eos::mgm::placement::RRSeed;

TEST(RRSeed, Construction)
{
  RRSeed seed(10);
  EXPECT_EQ(seed.GetNumSeeds(), 10);
}

TEST(RRSeed, out_of_bounds)
{
  RRSeed seed(10);
  // 0-indexing, 10 should be offby1
  EXPECT_THROW(seed.Get(10, 0), std::out_of_range);
}

TEST(RRSeed, single_thread)
{
  RRSeed seed(10);
  // Trivial no op case, reads the random starting point without moving it
  const uint64_t start = seed.Get(0, 0);
  EXPECT_EQ(seed.Get(0, 0), start);

  // Reserving one item hands out the current value and moves past it
  EXPECT_EQ(seed.Get(0, 1), start);
  EXPECT_EQ(seed.Get(0, 0), start + 1);
  EXPECT_EQ(seed.Get(0, 1), start + 1);

  // Reserving a window moves by the window size
  EXPECT_EQ(seed.Get(0, 10), start + 2);
  EXPECT_EQ(seed.Get(0, 0), start + 12);
}

TEST(RRSeed, multithread)
{
  RRSeed seed(10);
  const uint64_t start0 = seed.Get(0, 0);
  const uint64_t start1 = seed.Get(1, 0);

  auto f = [&seed]() {
    for (int i = 0; i < 1000; i++) {
      seed.Get(0, 1);
    }
  };

  std::vector<std::thread> threads;
  for (int i=0;i<16;++i){
    threads.emplace_back(f);
  }

  for (int i=0;i<16;++i){
    threads[i].join();
  }

  EXPECT_EQ(seed.Get(0, 0), start0 + 16000);
  // Only index 0 was advanced
  EXPECT_EQ(seed.Get(1, 0), start1);
}

TEST(RRSeed, CountersStartAtRandomValues)
{
  // Zero-started counters would line all the buckets up on the same items
  RRSeed seed(RRSeed::kChunkSize);
  std::set<uint64_t> starts;

  for (size_t i = 0; i < RRSeed::kChunkSize; ++i) {
    const uint64_t start = seed.Get(i, 0);
    EXPECT_LE(start, std::numeric_limits<uint32_t>::max());
    starts.insert(start);
  }

  // Draws from 2^32 values: a collision among 1024 of them is already rare
  EXPECT_GT(starts.size(), RRSeed::kChunkSize - 8);
}

TEST(RRSeed, GrowthKeepsExistingCounters)
{
  RRSeed seed(10);
  const uint64_t start = seed.Get(3, 5);
  const size_t grown = 3 * RRSeed::kChunkSize + 7;
  seed.EnsureCapacity(grown);
  EXPECT_EQ(seed.GetNumSeeds(), grown);
  EXPECT_EQ(seed.Get(3, 0), start + 5);
  EXPECT_NO_THROW(seed.Get(grown - 1, 1));
  EXPECT_THROW(seed.Get(grown, 0), std::out_of_range);

  // Never shrinks
  seed.EnsureCapacity(5);
  EXPECT_EQ(seed.GetNumSeeds(), grown);
}
