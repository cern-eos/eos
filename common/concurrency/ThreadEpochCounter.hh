//------------------------------------------------------------------------------
// File: ThreadEpochCounter.hh
// Author: Abhishek Lekshmanan - CERN
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
#include "common/concurrency/AlignMacros.hh"
#include <array>
#include <atomic>
#include <cassert>
#include <iostream>
#include <thread>

namespace eos::common {

namespace detail {

template <typename, typename = void>
struct is_state_less : std::false_type {};

template <typename T>
struct is_state_less<T,
                     std::void_t<typename T::is_state_less>> : std::true_type {};

template <typename T>
constexpr bool is_state_less_v = is_state_less<T>::value;
} // detail

template <size_t kMaxEpochs=32768>
class VersionEpochCounter {
public:
  inline uint64_t getEpochIndex(uint64_t epoch) noexcept {
    if (epoch < kMaxEpochs)
      return epoch;
    // TODO: This only works assuming that we wouldn't really have
    // readers at epoch 0 by the time kMaxEpochs is reached, which
    // is relatively safe given kMaxEpochs amount of writes don't happen
    // before the first reader finishes.
    return epoch % kMaxEpochs;

  }

  inline size_t increment(uint64_t epoch, uint16_t count=1) noexcept {
    auto index = getEpochIndex(epoch);
    mCounter[index].fetch_add(count, std::memory_order_release);
    return index;
  }

  inline void decrement(uint64_t epoch) noexcept {
    auto index = getEpochIndex(epoch);
    mCounter[index].fetch_sub(1, std::memory_order_release);
  }

  inline void decrement(uint64_t epoch, uint64_t index) noexcept {
    mCounter[index].fetch_sub(1, std::memory_order_release);
  }

  inline size_t getReaders(uint64_t epoch) noexcept {
    return mCounter[getEpochIndex(epoch)].load(std::memory_order_relaxed);
  }

  bool epochHasReaders(uint64_t epoch) noexcept {
    auto index = getEpochIndex(epoch);
    return mCounter[index].load(std::memory_order_acquire) > 0;
  }

private:
  alignas(hardware_destructive_interference_size) std::array<std::atomic<uint16_t>, kMaxEpochs> mCounter{0};
};

namespace experimental {

// The Idea of Thread local ID is borrowed from
// https://github.com/cmuparlay/concurrent_deferred_rcu
// Turning Manual Concurrent Memory Reclamation into Automatic Reference Counting
// Daniel Anderson, Guy E. Blelloch, Yuanhao Wei (PLDI 2022)
static constexpr size_t EOS_MAX_THREADS=4096;
static std::array<std::atomic<bool>, EOS_MAX_THREADS> g_thread_in_use {false};

struct ThreadID {
  ThreadID() {
    for (size_t i = 0; i < EOS_MAX_THREADS; ++i) {
      bool expected = false;
      if (!g_thread_in_use[i] &&
          g_thread_in_use[i].compare_exchange_strong(expected, true)) {
        tid = i;
        return;
      }
    }
    // COULD NOT FIND A FREE THREAD ID, PANIC!
    // assert(true); In the rare event we reach here, we can't guarantee EpochCounter
    // correctness, so we'll just assert.
    throw std::runtime_error("Could not find a free thread id");
  }

  ~ThreadID() {
    g_thread_in_use[tid].store(false, std::memory_order_release);
  }

  size_t get() {
    return tid;
  }

  size_t tid;
};

static thread_local ThreadID tlocalID;


/**
* @brief a simple epoch counter per thread that can be used to implement
* RCU-like algorithms. Basically we store a bitfield of
 * 16 bit counter and a 48 bit epoch. If we have no hash collisions, this is fairly
 * simple to implement, you'd only need a simple increment and a memory_order_release
 * store. However, if we have hash collisions, we need to store the oldest epoch
 * as we're tracking the oldest epoch.
 *
 * This counter currently is not meant to be stable as with thread_id, hash collisions are
 * expected and thus we won't be doing the epoch tracking correctly if a thread with a colliding
 * hash moves onto a different epoch. A different way of doing this is just having a thread_local
 * reader/epoch counter, and when one does a write lock, you'd have to walk through this list of
 * thread_local pointers.
 */

struct alignas(hardware_destructive_interference_size) ThreadEpoch {
  auto get(std::memory_order order = std::memory_order_acquire) {
    return epoch_counter.load(order);
  }

  auto get_counter(std::memory_order order = std::memory_order_acquire) {
    return get(order) & 0xFFFF;
  }

  std::atomic<uint64_t> epoch_counter;
};

class ThreadEpochCounter {
public:

  using is_state_less = void;

  size_t increment(uint64_t epoch, uint16_t count=1) noexcept {
    auto tid = tlocalID.get();
    // This is 2 instructions, instead of a single CAS. Given that threads
    // will not hash to the same number, we can guarantee that we'd only have one
    // epoch per thread

    auto old = mCounter[tid].get();
    assert(old && 0xFFFF == 0 || (old >> 16) == epoch);

    auto new_val = (epoch << 16) | (old & 0xFFFF) + count;
    mCounter[tid].epoch_counter.store(new_val, std::memory_order_release);
    return tid;
  }

  inline void decrement(uint64_t epoch, size_t tid) {
    // assert (old >> 16) == epoch);
    mCounter[tid].epoch_counter.fetch_sub(1, std::memory_order_release);
  }

  inline void decrement() {
    auto tid = tlocalID.get();
    mCounter[tid].epoch_counter.fetch_sub(1, std::memory_order_release);
  }

  size_t getReaders(size_t tid) noexcept {
    return mCounter[tid].get_counter();
  }


  bool epochHasReaders(uint64_t epoch) noexcept {
    for (int i=0; i < EOS_MAX_THREADS; ++i) {
      auto val = mCounter[i].get();
      if ((val >> 16) == epoch && (val & 0xFFFF) > 0) {
        return true;
      }
    }
    return false;
  }

private:
  std::array<ThreadEpoch, EOS_MAX_THREADS> mCounter{0};
};

static_assert(detail::is_state_less_v<ThreadEpochCounter>);

} // experimental
} // eos::common
