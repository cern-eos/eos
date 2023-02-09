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

using eos::mgm::placement::ThreadLocalRRSeed;

TEST(ThreadLocalRRSeed, random)
{
  if (ThreadLocalRRSeed::getNumSeeds() < 10) {
    ThreadLocalRRSeed::init(10, true);
  }

  EXPECT_EQ(ThreadLocalRRSeed::gRRSeeds.size(), 10);
  std::vector<uint64_t> seeds;
  for (auto i = 0; i < 10; i++) {
    std::cout << ThreadLocalRRSeed::gRRSeeds[i] << " ";
    seeds.push_back(ThreadLocalRRSeed::gRRSeeds[i]);
  }
  std::cout << "\n";

  EXPECT_EQ(ThreadLocalRRSeed::get(0, 0), seeds[0]);
  EXPECT_EQ(ThreadLocalRRSeed::get(0, 1), seeds[0]);
  EXPECT_EQ(ThreadLocalRRSeed::get(0, 0), seeds[0] + 1);
}
