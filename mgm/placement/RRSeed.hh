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
#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <stdexcept>
#include <type_traits>

namespace eos::mgm::placement {

//------------------------------------------------------------------------------
//! Class RRSeed - a simple round-robin seed generator stored as a list of
//! atomic counters. The list serves the case of a 2-D round-robin where one
//! needs to round-robin over the second dimension. Under the hood each entry is
//! nothing but a 1-D counter incremented by a given size.
//!
//! The counters are held in fixed size chunks allocated on demand rather than
//! in one contiguous array, so that the table can grow with a topology that
//! gained buckets without ever moving a counter a concurrent placement is
//! reading. Growth takes a mutex, reads are lock free.
//------------------------------------------------------------------------------
template <typename T = uint64_t>
class RRSeed {
public:
  // The counter wraps around to 0 once it reaches the maximum value of type T,
  // as is defined for unsigned integers. If you ever need negative values then
  // rewrite this carefully considering overflows!
  static_assert(std::is_integral<T>::value && std::is_unsigned<T>::value,
                "We expect only unsigned integer types, "
                "otherwise overflow would be Undefined Behaviour");

  //! Counters per chunk, and the largest number of chunks that can be held.
  //! The product bounds the table at a size no EOS topology approaches, and
  //! only the chunks actually reached are allocated.
  static constexpr size_t kChunkSize = 1024;
  static constexpr size_t kMaxChunks = 1024;

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param max_items number of seeds to hold, grown later as needed by
  //!        EnsureCapacity
  //----------------------------------------------------------------------------
  explicit RRSeed(size_t max_items) { EnsureCapacity(max_items); }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~RRSeed()
  {
    for (auto& chunk : mChunks) {
      delete[] chunk.load(std::memory_order_relaxed);
    }
  }

  RRSeed(const RRSeed&) = delete;
  RRSeed& operator=(const RRSeed&) = delete;
  RRSeed(RRSeed&&) = delete;
  RRSeed& operator=(RRSeed&&) = delete;

  //----------------------------------------------------------------------------
  //! Get the seed at the given index and reserve n_items, so that the next
  //! seed handed out is n_items away
  //!
  //! @param index seed index, must be within range
  //! @param n_items number of items to reserve
  //!
  //! @return seed value before the reservation
  //!
  //! @throw std::out_of_range if the index is past the held seeds. Callers on
  //!        the placement path grow the table to the topology first, so this
  //!        marks a programming error rather than an oversized cluster.
  //----------------------------------------------------------------------------
  T
  Get(size_t index, size_t n_items)
  {
    // Acquire pairs with the release store closing EnsureCapacity, so an index
    // below the published count is guaranteed to see its chunk pointer too
    if (index >= mNumSeeds.load(std::memory_order_acquire)) {
      throw std::out_of_range("RRSeed index out of range");
    }

    std::atomic<T>* chunk = mChunks[index / kChunkSize].load(std::memory_order_acquire);
    return chunk[index % kChunkSize].fetch_add(n_items, std::memory_order_relaxed);
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
  //! Grow the table so that it holds at least the given number of seeds. Never
  //! shrinks, so concurrent callers racing on a growing topology settle on the
  //! largest request. Saturates at kChunkSize * kMaxChunks; a caller asking for
  //! more finds GetNumSeeds() short of what it wanted and reports the topology
  //! as out of range rather than getting an exception on the placement path.
  //!
  //! @param max_items number of seeds the caller needs
  //----------------------------------------------------------------------------
  void
  EnsureCapacity(size_t max_items)
  {
    if (max_items <= mNumSeeds.load(std::memory_order_acquire)) {
      return;
    }

    max_items = std::min(max_items, kChunkSize * kMaxChunks);
    std::lock_guard<std::mutex> guard(mGrowMutex);

    if (max_items <= mNumSeeds.load(std::memory_order_relaxed)) {
      return;
    }

    const size_t n_chunks = (max_items + kChunkSize - 1) / kChunkSize;

    for (size_t i = 0; i < n_chunks; ++i) {
      if (mChunks[i].load(std::memory_order_relaxed) == nullptr) {
        // Value initialized, ie. every fresh counter starts at 0
        mChunks[i].store(new std::atomic<T>[kChunkSize](), std::memory_order_release);
      }
    }

    // Published last: a reader that accepts an index has its chunk in place
    mNumSeeds.store(max_items, std::memory_order_release);
  }

private:
  //! Round-robin counters, allocated one chunk at a time. A chunk never moves
  //! once published, which is what lets the table grow under concurrent reads.
  std::array<std::atomic<std::atomic<T>*>, kMaxChunks> mChunks{};
  std::atomic<size_t> mNumSeeds{0}; ///< Seeds published so far
  std::mutex mGrowMutex;            ///< Serializes the growth, never taken to read
};

} // namespace eos::mgm::placement
