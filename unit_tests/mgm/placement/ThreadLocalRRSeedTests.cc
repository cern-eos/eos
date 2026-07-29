// ----------------------------------------------------------------------
// File: ThreadLocalRRSeedTests.cc
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

#include "mgm/placement/ThreadLocalRRSeed.hh"
#include "gtest/gtest.h"
#include <thread>

using eos::mgm::placement::kDefaultMaxRRSeeds;
using eos::mgm::placement::ThreadLocalRRSeed;

TEST(ThreadLocalRRSeed, random)
{
  if (ThreadLocalRRSeed::GetNumSeeds() < 10) {
    std::cout << "Not enough seeds, initializing with 10 seeds";
  } else {
    std::cout << "Already initialized!";
  }

   std::vector<uint64_t> seeds;
  for (auto i = 0; i < 10; i++) {
    std::cout << ThreadLocalRRSeed::gRRSeeds[i] << " ";
    seeds.push_back(ThreadLocalRRSeed::gRRSeeds[i]);
  }
  std::cout << "\n";

  EXPECT_EQ(ThreadLocalRRSeed::Get(0, 0), seeds[0]);
  EXPECT_EQ(ThreadLocalRRSeed::Get(0, 1), seeds[0]);
  EXPECT_EQ(ThreadLocalRRSeed::Get(0, 0), seeds[0] + 1);
}

// A thread other than the one that called Init() must also start with
// randomized cursors, otherwise every scheduling thread begins at bucket 0
// and the anti-thundering-herd randomization applies to one thread only.
TEST(ThreadLocalRRSeed, WorkerThreadSeedsAreRandomized)
{
  ThreadLocalRRSeed::Init(kDefaultMaxRRSeeds, true);
  size_t num_seeds = 0;
  bool all_zero = true;
  std::thread worker([&num_seeds, &all_zero]() {
    num_seeds = ThreadLocalRRSeed::GetNumSeeds();

    for (size_t i = 0; i < num_seeds; ++i) {
      // Get(i, 0) reads the cursor without advancing it
      if (ThreadLocalRRSeed::Get(i, 0) != 0) {
        all_zero = false;
        break;
      }
    }
  });
  worker.join();
  EXPECT_EQ(num_seeds, kDefaultMaxRRSeeds);
  // 1024 seeds drawn from [0, 1024] are all zero with probability ~0
  EXPECT_FALSE(all_zero);
}
