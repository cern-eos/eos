//------------------------------------------------------------------------------
//! @file ThreadLocalRRSeed.cc
//! @author Abhishek Lekshmanan - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2023 CERN/Switzerland                                  *
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

#include "ThreadLocalRRSeed.hh"
#include "common/Logging.hh"
#include "utils/RandUtils.hh"
#include <atomic>

namespace eos::mgm::placement {

namespace {

//! Configuration shared by every thread: how many seeds a thread should hold
//! and whether fresh seeds start at random values. Written by Init(), read by
//! each thread when it conforms its own seed vector.
std::atomic<size_t> gNumConfiguredSeeds{kDefaultMaxRRSeeds};
std::atomic<bool> gRandomizeSeeds{true};

//------------------------------------------------------------------------------
//! Grow the given seed vector to the configured size, randomizing the new
//! entries if so configured. This is the per-thread half of Init(): every
//! thread runs it lazily on first use, so the randomized starting cursors
//! apply to all threads and not only to the one that called Init().
//------------------------------------------------------------------------------
void
ConformSeeds(std::vector<uint64_t>& seeds)
{
  const size_t wanted = gNumConfiguredSeeds.load(std::memory_order_acquire);

  if (seeds.size() >= wanted) {
    return;
  }

  const size_t old_size = seeds.size();
  seeds.resize(wanted, 0);

  if (gRandomizeSeeds.load(std::memory_order_acquire)) {
    for (size_t i = old_size; i < wanted; ++i) {
      seeds[i] = eos::common::getRandom(0ul, wanted);
    }
  }
}

} // anonymous namespace

thread_local std::vector<uint64_t> ThreadLocalRRSeed::gRRSeeds = [] {
  std::vector<uint64_t> seeds;
  ConformSeeds(seeds);
  return seeds;
}();

//------------------------------------------------------------------------------
// Configure the number of seeds every thread holds
//------------------------------------------------------------------------------
void
ThreadLocalRRSeed::Init(size_t max_items, bool randomize)
{
  gNumConfiguredSeeds.store(max_items, std::memory_order_release);
  gRandomizeSeeds.store(randomize, std::memory_order_release);
  // Rebuild the calling thread's seeds right away, the other threads conform
  // on their next Get()
  gRRSeeds.clear();
  gRRSeeds.shrink_to_fit();
  ConformSeeds(gRRSeeds);
}

//------------------------------------------------------------------------------
// Raise the configured number of seeds
//------------------------------------------------------------------------------
void
ThreadLocalRRSeed::EnsureCapacity(size_t max_items)
{
  // Raise only, so that concurrent callers racing on a growing topology settle
  // on the largest request instead of the last one to arrive
  size_t current = gNumConfiguredSeeds.load(std::memory_order_acquire);

  while ((current < max_items) &&
         !gNumConfiguredSeeds.compare_exchange_weak(
             current, max_items, std::memory_order_release, std::memory_order_acquire)) {
  }
}

//------------------------------------------------------------------------------
// Get the number of seeds available to the calling thread
//------------------------------------------------------------------------------
size_t
ThreadLocalRRSeed::GetNumSeeds()
{
  ConformSeeds(gRRSeeds);
  return gRRSeeds.size();
}

//------------------------------------------------------------------------------
// Reserve a contiguous round-robin window for the given bucket
//------------------------------------------------------------------------------
uint64_t
ThreadLocalRRSeed::Get(size_t index, size_t n_items)
{
  ConformSeeds(gRRSeeds);

  if (index >= gRRSeeds.size()) {
    eos_static_crit("msg=\"thread local RR seed index out of range\" "
                    "index=%lu size=%lu",
                    index, gRRSeeds.size());
    return 0;
  }

  uint64_t ret = gRRSeeds[index];
  gRRSeeds[index] += n_items;
  return ret;
}

} // namespace eos::mgm::placement
