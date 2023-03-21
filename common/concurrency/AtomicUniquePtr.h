//------------------------------------------------------------------------------
// File: AtomicUniquePtr.hh
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
#include <atomic>
#include <cassert>

namespace eos::common {

/*
 * A thread safe unique_ptr - The main use case of this is when you have data
 * that is rarely changing, read often and written rarely. In this case
 * with a classic RWLock, even though the data is rarely changing, readers always
 * have to pay the cost of acquiring the lock. With this class the data load itself
 * is wait-free costing only a single atomic load. While the rest of the API closely
 * matches unique_ptr, reset() is different. We return the old value of data,
 * and the caller is responsible for deleting it. This is because we cannot make
 * any assumptions on how many readers are using the old value. So the writer has
 * to copy the old value and delete it after a sufficient point of synchronization.
 *
 * For really small data, you could get away with not bothering to delete and just
 * storing in some global list.
 */
template <typename T>
class atomic_unique_ptr
{
public:
  using pointer = T*;
  using element_type = T;
  atomic_unique_ptr() = default;
  atomic_unique_ptr(T* p) : p_(p) {}

  atomic_unique_ptr(const atomic_unique_ptr&) = delete;
  atomic_unique_ptr& operator=(const atomic_unique_ptr&) = delete;

  atomic_unique_ptr(atomic_unique_ptr&& other) {
    T* p = other.p_.exchange(nullptr, std::memory_order_acq_rel);
    publish(p);
  }

  // No move assignment operator, as to do this safely we have no idea how many
  // readers could be potentially accessing both the old and new values!

  ~atomic_unique_ptr() {;
    delete p_.load(std::memory_order_relaxed);
  }

  T* get() const noexcept {
    return p_.load(std::memory_order_acquire);
  }

  T* release() {
    return p_.exchange(nullptr, std::memory_order_acq_rel);
  }

  /*!
   * reset- the old pointer is returned instead of deleted
   * This is because we cannot make sure that the pointer is not being used
   * by another thread, so it is upto the caller to ensure a sufficient point
   * of synchronization where it is safe to delete the old value. When using
   * reset as a way to initialize the pointer, it is safe to use reset_from_null
   *
   * An atomic exchange is used over a get/publish as 2 transactions would no longer
   * be atomic, though calling reset from multiple threads is generally not a good
   * idea
   *  @param p: pointer to be stored in the atomic_unique_ptr
   *  @return: the old pointer
   * */
  [[nodiscard]] T* reset(T* p)
  {
    return p_.exchange(p, std::memory_order_acq_rel);
  }

  // not TS! spinning in an atomic compare exchange can be used to make it so,
  // but reset_from_null is a construction routine and just like construction of
  // the AtomicPtr itself isn't threadsafe, this shouldn't be!
  void reset_from_null(T* p) {
    assert(p_.load(std::memory_order_acquire) == nullptr);
    publish(p);
  }

  T* operator->() const {
    return p_.load(std::memory_order_acquire);
  }

  T& operator*() const {
    return *this->get();
  }

  explicit operator bool() const {
    return p_.load(std::memory_order_acquire)  != nullptr;
  }
private:
  void publish(T* p) noexcept {
    p_.store(p, std::memory_order_release);
  }

  std::atomic<T*> p_ {nullptr};
};


} // eos::common


