//------------------------------------------------------------------------------
// File: RCULite.hh
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
#include "common/concurrency/ThreadEpochCounter.hh"

#include <atomic>
#include <thread>
#include <memory>


namespace eos::common
{

// CACHE_LINE_SIZE is already defined in AlignedArray.hh
constexpr size_t MAX_THREADS = 4096;

/*
  A Read Copy Update Like primitive that guarantees that is wait-free on the
  readers and guarantees that all memory is protected from deletion. This
  is similar to folly's RCU implementation, but a bit simpler to accomodate
  our use cases.

  Let's say you've a data type that is mostly a read workload with very rare
  updates, with classical RW Locks this is what you'd be doing

  void reader() {
     std::shared_lock lock(shared_mutex);
     process(myconfig);
  }

  A rather simple way to not pay the cost would be using something like
  atomic_unique_ptr

  void reader() {
     auto* config_data = myconfig.get()
     process(config_data);
  }

  void writer() {
    auto *old_config_data = myconfig.reset(new myconfig(config_data));
    // This works and is safe, however we don't know when is a good checkpoint
    // in the program to delete the old_config_data. Deleting when another reader
    // is still accessing the data is something we want to avoid

  }

  void reader() {
    RCUReadLock rlock(my_rcu_domain);
    process(myconfig.get());
  }

  void writer() {
    ConfigData* old_config_data(nullptr);
    {
      RCUWriteLock wlock(my_rcu_domain);
      old_config_data = myconfig.reset(new config(config_data));
    }

    delete (old_config_data);
  }

  // Alternatively a scopedRCUWrite will drain the readers and wait for them to
  complete before deletion

  void writer() { ScopedRCUWrite(my_rcu_domain,
  myconfig, new config(config_data)); }


 */

template <typename ListT = VersionEpochCounter<32>, size_t MaxWriters = 1>
class RCUDomain
{
public:

  RCUDomain() = default;

  inline uint64_t get_current_epoch(std::memory_order order
                                    = std::memory_order_acquire) noexcept
  {
    return mEpoch.load(order);
  }


  inline size_t rcu_read_lock(uint64_t epoch) noexcept
  {
    return mReadersCounter.increment(epoch);
  }

  inline size_t rcu_read_lock() noexcept
  {
    return rcu_read_lock(mEpoch.load(std::memory_order_acquire));
  }


  inline void rcu_read_unlock(uint64_t epoch, uint64_t tag) noexcept
  {
    mReadersCounter.decrement(epoch, tag);
  }

  // rcu_read_unlock for a stateless list, which doesn't depend on return from
  // the lock call
  template <typename T = ListT>
  inline auto
  rcu_read_unlock() noexcept
  -> std::enable_if_t<detail::is_state_less_v<T>, void>
  {
    mReadersCounter.decrement();
  }

  inline void rcu_read_unlock(uint64_t tag) noexcept
  {
    mReadersCounter.decrement(mEpoch.load(std::memory_order_acquire), tag);
  }

  inline void rcu_write_lock() noexcept
  {
    uint64_t expected_writers = MaxWriters - 1;
    uint64_t counter{0};

    while (!mWritersCount.compare_exchange_strong(expected_writers,
           expected_writers + 1,
           std::memory_order_acq_rel)) {
      if (expected_writers >= MaxWriters) {
        expected_writers = MaxWriters - 1;
      }

      if (counter % 20 == 0) {
        std::this_thread::yield();
      }
    }
  }

  inline void rcu_synchronize() noexcept
  {
    auto curr_epoch = mEpoch.load(std::memory_order_acquire);

    while (!mEpoch.compare_exchange_strong(curr_epoch, curr_epoch + 1,
                                           std::memory_order_acq_rel)) ;

    int i = 0;

    while (mReadersCounter.epochHasReaders(curr_epoch)) {
      if (i++ % 20 == 0) {
        std::this_thread::yield();
      }
    }

    mWritersCount.fetch_sub(1, std::memory_order_release);
  }

  inline void rcu_write_unlock() noexcept
  {
    rcu_synchronize();
  }


private:
  ListT mReadersCounter;
  alignas(CACHE_LINE_SIZE) std::atomic<uint64_t> mEpoch{0};
  alignas(CACHE_LINE_SIZE) std::atomic<uint64_t> mWritersCount{0};
};

template <typename RCUDomain>
struct RCUReadLock {
  RCUReadLock(RCUDomain& _rcu_domain) : rcu_domain(_rcu_domain)
  {
    epoch = rcu_domain.get_current_epoch();
    tag = rcu_domain.rcu_read_lock(epoch);
  }

  ~RCUReadLock()
  {
    rcu_domain.rcu_read_unlock(epoch, tag);
  }

  uint64_t tag;
  uint64_t epoch;
  RCUDomain& rcu_domain;
};

template <typename RCUDomain>
struct RCUWriteLock {
  RCUWriteLock(RCUDomain& _rcu_domain): rcu_domain(_rcu_domain)
  {
    rcu_domain.rcu_write_lock();
  }

  ~RCUWriteLock()
  {
    rcu_domain.rcu_synchronize();
  }

  RCUDomain& rcu_domain;
};

template <typename RCUDomain, typename Ptr>
struct ScopedRCUWrite {
  ScopedRCUWrite(RCUDomain& _rcu_domain,
                 Ptr& ptr,
                 typename Ptr::pointer new_val) : rcu_domain(_rcu_domain)
  {
    rcu_domain.rcu_write_lock();
    old_val = ptr.reset(new_val);
  }

  ~ScopedRCUWrite()
  {
    rcu_domain.rcu_synchronize();
    delete old_val;
  }

  RCUDomain& rcu_domain;
  typename Ptr::pointer old_val;
};

using VersionedRCUDomain = RCUDomain<VersionEpochCounter<32>, 1>;
using EpochRCUDomain = RCUDomain<experimental::ThreadEpochCounter, 1>;
} // eos::common
