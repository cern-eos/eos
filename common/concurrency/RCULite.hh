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
#include "common/concurrency/AlignMacros.hh"
#include "common/concurrency/ThreadEpochCounter.hh"
#include <atomic>
#include <thread>


namespace eos::common
{

constexpr size_t MAX_THREADS = 4096;
// A simple ticket spin lock implementation

class TicketLock {
public:
  void lock() noexcept {
    auto my_ticket = ticket.fetch_add(1, std::memory_order_acquire);

    uint32_t spin_count = 0;
    while (serving.load(std::memory_order_acquire) != my_ticket) {
      if (spin_count < 100) {
        ++spin_count;
      } else if (spin_count < 1000) {
        if (++spin_count % 20 == 0) {
          std::this_thread::yield();
        }
      } else {
        std::this_thread::sleep_for(std::chrono::microseconds(10));
      }
    }
  }

  void unlock() noexcept {
    serving.fetch_add(1, std::memory_order_release);
  }
private:
  alignas(hardware_destructive_interference_size)  std::atomic<uint32_t> ticket {0};
  alignas(hardware_destructive_interference_size) std::atomic<uint32_t> serving {0};
};

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

template <typename ListT = ThreadEpochCounter>
class RCUDomain
{
public:
  using is_state_less = detail::is_state_less<ListT>;
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
    mWriterLock.lock();
  }



  inline void rcu_write_unlock() noexcept
  {
    rcu_synchronize();
    mWriterLock.unlock();
  }


private:

  inline void rcu_synchronize() noexcept
  {
    auto old_epoch = mEpoch.fetch_add(1, std::memory_order_acq_rel);
    uint32_t spin_count = 0;
    while (mReadersCounter.epochHasReaders(old_epoch)) {
      if (++spin_count % 1000 == 0) {
        std::this_thread::sleep_for(std::chrono::microseconds(100));
      } else if (spin_count % 20 == 0) {
        std::this_thread::yield();
      }
    }
  }

  ListT mReadersCounter;
  alignas(hardware_destructive_interference_size) std::atomic<uint64_t> mEpoch{0};
  TicketLock mWriterLock;
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
    rcu_domain.rcu_write_unlock();
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
    rcu_domain.rcu_write_unlock();
    delete old_val;

  }

  RCUDomain& rcu_domain;
  typename Ptr::pointer old_val;
};

using VersionedRCUDomain = RCUDomain<experimental::VersionEpochCounter<32>>;
using EpochRCUDomain = RCUDomain<ThreadEpochCounter>;

// An adapter to use RCUDomain as a std::shared_mutex like object
template <typename RCUDomainT = EpochRCUDomain>
class RCUMutexT {
  static_assert(detail::is_state_less_v<RCUDomainT>,
                "RCUMutex needs to be stateless to confirm to std::shared_mutex api");
public:
  // implement here the std::shared_lock and unique_lock api
  void lock_shared() {
    rcu_domain.rcu_read_lock();
  }

  void unlock_shared() {
    rcu_domain.rcu_read_unlock();
  }

  void lock() {
    rcu_domain.rcu_write_lock();
  }

  void unlock() {
    rcu_domain.rcu_write_unlock();
  }
private:
  RCUDomainT rcu_domain;
};

// Specialization of ScopedRCUWrite for RCUMutexT which is
// compatible with std::shared_mutex/unique/shared_lock apis
template <typename Ptr>
class ScopedRCUWrite<RCUMutexT<>, Ptr> {
public:
    ScopedRCUWrite(RCUMutexT<>& _rcu_mutex,
                   Ptr& ptr,
                   typename Ptr::pointer new_val) : rcu_mutex(_rcu_mutex)
    {
      rcu_mutex.lock();
      old_val = ptr.reset(new_val);
    }

  ~ScopedRCUWrite()
    {
      rcu_mutex.unlock();
      delete old_val;
    }
    private:
    RCUMutexT<>& rcu_mutex;
    typename Ptr::pointer old_val;
};

  // Specialization for RCUMutexT, in the future replace these calls with
  // std::shared_lock where possible. This is just for backward compatibility
template <>
class RCUReadLock <RCUMutexT<>> {
public:
  RCUReadLock(RCUMutexT<>& _rcu_mutex) : rcu_mutex(_rcu_mutex) {
    rcu_mutex.lock_shared();
  }

  ~RCUReadLock() {
    rcu_mutex.unlock_shared();
  }
private:
  RCUMutexT<>& rcu_mutex;
};
} // eos::common
