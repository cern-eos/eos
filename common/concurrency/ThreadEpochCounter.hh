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
struct is_stateful : std::false_type {};

template <typename T>
struct is_stateful<T,
                   std::void_t<typename T::is_stateful>> : std::true_type {};

template <typename T>
constexpr bool is_stateful_v = is_stateful<T>::value;

template <typename T, typename = void>
struct stateful_trait_base {};

template <typename T>
struct stateful_trait_base <T, std::void_t<typename T::is_stateful>> {
  using is_stateful = void;
};

} // detail

namespace experimental {
  // The Counters in experimental namespace are not
  // yet production ready! This is for testing only
  // currently
  template <size_t kMaxEpochs=32768>
  class VersionEpochCounter {
  public:
    using is_stateful = void;
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
} // experimental

// The Idea of Thread local ID is borrowed from
// https://github.com/cmuparlay/concurrent_deferred_rcu
// Turning Manual Concurrent Memory Reclamation into Automatic Reference Counting
// Daniel Anderson, Guy E. Blelloch, Yuanhao Wei (PLDI 2022)
static constexpr size_t EOS_MAX_THREADS=65536;
extern std::array<std::atomic<bool>, EOS_MAX_THREADS> g_thread_in_use;

struct ThreadID {
  ThreadID();

  ~ThreadID();

  size_t get() {
    return tid;
  }

  size_t tid;
};

extern thread_local ThreadID tlocalID;

/**
* @brief a simple epoch counter per thread that can be used to implement
* RCU-like algorithms. Basically we store a bitfield of
 * 16 bit counter and a 48 bit epoch. If we have no hash collisions, this is fairly
 * simple to implement, you'd only need a simple increment and a memory_order_release
 * store. However, if we have hash collisions, we need to store the oldest epoch
 * as we're tracking the oldest epoch.
 *
 * This counter is supposed to be used with a threadID that is unique
 * like the one provided by ThreadID above.
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

  //using is_state_less = void;

  size_t increment(uint64_t epoch, uint16_t count=1) noexcept {
    auto tid = tlocalID.get();
    // This is 2 instructions, instead of a single CAS. Given that threads
    // will not hash to the same number, we can guarantee that we'd only have one
    // epoch per thread

    auto old = mCounter[tid].get();
    auto new_val = (epoch << 16) | ((old & 0xFFFF) + count);
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
    for (size_t i=0; i < EOS_MAX_THREADS; ++i) {
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

// Cross check our assumptions about statefulness are true
static_assert(detail::is_stateful_v<experimental::VersionEpochCounter<>>);
static_assert(!detail::is_stateful_v<ThreadEpochCounter>);

} // eos::common
