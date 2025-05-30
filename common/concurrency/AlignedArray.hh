//------------------------------------------------------------------------------
// File: AlignedArray.hh
// Author: Abhishek Lekshmanan - CERN
//------------------------------------------------------------------------------

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

#pragma once
#include "common/concurrency/AlignMacros.hh"
#include <atomic>
#include <array>

namespace eos::common {

// Fixed cache line size for x86_64 and ARM64 architectures
// This value is chosen to match the typical L1 cache line size and the default
// value of hardware_destructive_interference_size on modern CPUs (64 bytes).
// We use a fixed value to ensure ABI stability across different compiler versions
// and CPU architectures.
constexpr std::size_t CACHE_LINE_SIZE = 64;

template <typename T>
struct alignas(CACHE_LINE_SIZE) AlignedAtomic
    : private std::atomic<T> {
  using std::atomic<T>::atomic;
  using std::atomic<T>::operator=;
  using std::atomic<T>::store;
  using std::atomic<T>::load;
  using std::atomic<T>::exchange;
  using std::atomic<T>::fetch_add;
  using std::atomic<T>::fetch_sub;
};

// Check some basic properties of AlignedAtomic; since we have this at compile
// time and asserts do not exist in actual code, compiler does the unit testing for us
static_assert(sizeof(AlignedAtomic<int>) == CACHE_LINE_SIZE,
               "AlignedAtomic should be CACHE_LINE_SIZE");
static_assert(alignof(AlignedAtomic<int>) == CACHE_LINE_SIZE,
              "AlignedAtomic should be CACHE_LINE_SIZE");

// An array where each element is aligned to CACHE_LINE_SIZE
// ie. elements do not share cache lines
template <typename T, std::size_t N>
using AlignedAtomicArray = std::array<AlignedAtomic<T>, N>;
} // namespace eos::common
