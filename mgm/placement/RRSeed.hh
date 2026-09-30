//------------------------------------------------------------------------------
//! @file RRSeed.hh
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
#include "common/utils/RandUtils.hh"
#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <stdexcept>

namespace eos::mgm::placement {

//------------------------------------------------------------------------------
//! Class RRSeed - one shared round-robin counter per bucket.
//!
//! Every counter starts at a random value. Starting them all at 0 lines the
//! buckets up: groups listing their disks in the same host order would all hand
//! their first replicas to the same hosts, and since every group advances at
//! the same pace they would keep doing so.
//!
//! Each counter sits on a cache line of its own, so that placements in
//! unrelated buckets do not contend. The counters are allocated in chunks that
//! never move once published, which lets the table grow with the topology
//! while concurrent placements keep reading it lock free.
//------------------------------------------------------------------------------
class RRSeed {
public:
  //! Counters per chunk, and the largest number of chunks that can be held
  static constexpr size_t kChunkSize = 1024;
  static constexpr size_t kMaxChunks = 1024;

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param max_items number of seeds to hold, grown later by EnsureCapacity
  //----------------------------------------------------------------------------
  explicit RRSeed(size_t max_items) { EnsureCapacity(max_items); }

  //----------------------------------------------------------------------------
  //! Get the seed at the given index and advance it by n_items
  //!
  //! @param index seed index
  //! @param n_items number of items to reserve
  //!
  //! @return seed value before the advance
  //!
  //! @throw std::out_of_range if the index is past the held seeds. The
  //!        placement path grows the table to the topology first, so this
  //!        marks a programming error rather than an oversized cluster.
  //----------------------------------------------------------------------------
  uint64_t
  Get(size_t index, size_t n_items)
  {
    // Acquire pairs with the release closing EnsureCapacity: an accepted index
    // has its chunk in place
    if (index >= mNumSeeds.load(std::memory_order_acquire)) {
      throw std::out_of_range("RRSeed index out of range");
    }

    return mChunks[index / kChunkSize][index % kChunkSize].value.fetch_add(
        n_items, std::memory_order_relaxed);
  }

  //----------------------------------------------------------------------------
  //! Get the number of seeds held
  //!
  //! @return number of seeds
  //----------------------------------------------------------------------------
  size_t
  GetNumSeeds() const
  {
    return mNumSeeds.load(std::memory_order_acquire);
  }

  //----------------------------------------------------------------------------
  //! Grow the table to hold at least the given number of seeds. Never shrinks,
  //! so concurrent callers settle on the largest request. Saturates at
  //! kChunkSize * kMaxChunks; a caller asking for more finds GetNumSeeds()
  //! short and reports the topology as out of range.
  //!
  //! @param max_items number of seeds the caller needs
  //----------------------------------------------------------------------------
  void
  EnsureCapacity(size_t max_items)
  {
    if (max_items <= GetNumSeeds()) {
      return;
    }

    max_items = std::min(max_items, kChunkSize * kMaxChunks);
    std::lock_guard<std::mutex> guard(mGrowMutex);

    if (max_items <= mNumSeeds.load(std::memory_order_relaxed)) {
      return;
    }

    // Readers only dereference chunks below the published count, so the slots
    // filled in here need no atomics: the release store below publishes them
    for (size_t i = 0; i < (max_items + kChunkSize - 1) / kChunkSize; ++i) {
      if (mChunks[i] == nullptr) {
        mChunks[i] = std::make_unique<Counter[]>(kChunkSize);

        for (size_t j = 0; j < kChunkSize; ++j) {
          // Far below the wrap-around of the counter, where the modulo over
          // the bucket items would skip a beat
          mChunks[i][j].value.store(
              eos::common::getRandom<uint64_t>(0, std::numeric_limits<uint32_t>::max()),
              std::memory_order_relaxed);
        }
      }
    }

    mNumSeeds.store(max_items, std::memory_order_release);
  }

private:
  //! Counter padded to a cache line of its own
  struct alignas(64) Counter {
    std::atomic<uint64_t> value{0};
  };

  std::array<std::unique_ptr<Counter[]>, kMaxChunks> mChunks; ///< Counter chunks
  std::atomic<size_t> mNumSeeds{0}; ///< Seeds published so far
  std::mutex mGrowMutex;            ///< Serializes the growth, never taken to read
};

} // namespace eos::mgm::placement
