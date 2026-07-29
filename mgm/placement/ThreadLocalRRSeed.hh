//------------------------------------------------------------------------------
//! @file ThreadLocalRRSeed.hh
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

#pragma once
#include <vector>
#include <cstdint>
#include <cstddef>

namespace eos::mgm::placement {

//! Default number of round-robin seeds kept per thread
constexpr size_t kDefaultMaxRRSeeds = 1024;

//------------------------------------------------------------------------------
//! Thread local version of RRSeed. In the scheduler context we don't want the
//! seeds of all threads to start at 0, therefore they are initialized with
//! random values. Dropping the atomics makes this cheaper than RRSeed at the
//! cost of only per-thread fairness.
//!
//! Init() only stores the wanted size and randomization flag; every thread -
//! not just the one that happened to call Init() - grows its own seed vector
//! lazily to that configuration on first use, so the randomized starting
//! cursors actually apply to all scheduling threads.
//------------------------------------------------------------------------------
struct ThreadLocalRRSeed {
  //----------------------------------------------------------------------------
  //! Reserve a contiguous round-robin window for the given bucket
  //!
  //! @param index bucket index
  //! @param n_items number of items to reserve
  //!
  //! @return first value of the reserved window
  //----------------------------------------------------------------------------
  static uint64_t Get(size_t index, size_t n_items);

  //----------------------------------------------------------------------------
  //! Configure the number of seeds every thread holds. The calling thread's
  //! seeds are rebuilt immediately, other threads pick the configuration up
  //! on their next Get().
  //!
  //! @param max_items number of seeds to hold
  //! @param randomize if true seeds start at random values, otherwise at 0
  //----------------------------------------------------------------------------
  static void Init(size_t max_items, bool randomize = true);

  //----------------------------------------------------------------------------
  //! Raise the configured number of seeds so that every thread ends up holding
  //! at least max_items of them. Unlike Init() this never lowers the count and
  //! never discards the cursors already handed out, so it is what a topology
  //! that gained buckets calls. Threads grow their own vector on next use.
  //!
  //! @param max_items number of seeds the caller needs
  //----------------------------------------------------------------------------
  static void EnsureCapacity(size_t max_items);

  //----------------------------------------------------------------------------
  //! Get the number of seeds available to the calling thread
  //!
  //! @return number of seeds
  //----------------------------------------------------------------------------
  static size_t GetNumSeeds();

  static thread_local std::vector<uint64_t> gRRSeeds; ///< Per thread seeds
};

} // namespace eos::mgm::placement
