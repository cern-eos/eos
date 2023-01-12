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
#include <thread>

using RRSeed = eos::mgm::placement::RRSeed<uint64_t>;

TEST(RRSeed, Construction)
{
  RRSeed seed(10);
  EXPECT_EQ(seed.getNumSeeds(), 10);
}

TEST(RRSeed, out_of_bounds)
{
  RRSeed seed(10);
  // 0-indexing, 10 should be offby1
  EXPECT_THROW(seed.get(10,0), std::out_of_range);
}

TEST(RRSeed, single_thread)
{
  RRSeed seed(10);
  EXPECT_EQ(seed.getNumSeeds(), 10);

  // Trivial No op case - base condition, we don't change anything here
  EXPECT_EQ(seed.get(0,0),0);

  // We ask for the next seed, we'd get the starting seed ie. 0 and internally
  // the counter is 1
  EXPECT_EQ(seed.get(0,1),0);

  // No op - check internal counter
  EXPECT_EQ(seed.get(0,0),1);
  // Repeat No op!
  EXPECT_EQ(seed.get(0,0),1);

  // Now we ask for the next seed, we'd still get 1, internal counter = 2
  EXPECT_EQ(seed.get(0,1),1);

  // No op - check internal counter
  EXPECT_EQ(seed.get(0,0),2);

  // ask for 10 seeds
  EXPECT_EQ(seed.get(0,10),2);

  // No op - check internal counter
  EXPECT_EQ(seed.get(0,0),12);
}

TEST(RRSeed, multithread)
{
  RRSeed seed(10);

  auto f = [&seed]() {
    for (int i = 0; i < 1000; i++) {
      seed.get(0, 1);
    }
  };

  std::vector<std::thread> threads;
  for (int i=0;i<16;++i){
    threads.emplace_back(f);
  }

  for (int i=0;i<16;++i){
    threads[i].join();
  }

  EXPECT_EQ(seed.get(0,0),16000);
  // Get at a different index, we only modified index 0 seed,
  // rest should all be 0
  EXPECT_EQ(seed.get(1,0),0);
}

TEST(RRSeed, wrap_around)
{
  eos::mgm::placement::RRSeed<uint8_t> seed(10);
  // no op - check initial state
  EXPECT_EQ(seed.get(0,0),0);
  for (int i = 0; i < 255; i++) {
    EXPECT_EQ(seed.get(0,1),i);
  }
  // no op - final value at 255
  EXPECT_EQ(seed.get(0,0),255);

  // now increment!
  EXPECT_EQ(seed.get(0,1),255);

  // wrap aroound! check reusability
  EXPECT_EQ(seed.get(0,1),0);
  EXPECT_EQ(seed.get(0,1),1);
  EXPECT_EQ(seed.get(0,1),2);

}