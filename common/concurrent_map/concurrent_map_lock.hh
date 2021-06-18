//------------------------------------------------------------------------------
// File: concurrent_map_lock.hh
// Author: Abhishek Lekshmanan <abhishek.lekshmanan@cern.ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2021 CERN/Switzerland                                  *
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

#include <type_traits>
#include <mutex>
#include <shared_mutex>
#include "common/RWMutex.hh"

namespace eos::common {
namespace detail {
  // Test for Lockable named req
  template <typename T, typename = void>
  struct is_lockable_t : std::false_type {};


  // A type checker for checking a lockable concept, essentially a compile time
  // check for C++ BasicLockable named requirement
  // (https://en.cppreference.com/w/cpp/named_req/Lockable). The ret type
  // signature deduction is not exact, atm anything convertible to the type will
  // still pass the type checker, as we're just evaluating for eg. bool b =
  // mtx.try_lock(), since C++ will cast anything else that can be bool
  // assignable this will pass for eg. if you do std::declval<size_t&>() etc.
  template <typename T>
  struct is_lockable_t<T, std::void_t<decltype(std::declval<T&>().lock(),
                                               std::declval<T&>().unlock(),
                                               std::declval<bool&>() = std::declval<T>().try_lock())>>
    : std::integral_constant<bool, !std::is_copy_constructible<T>::value &&
                             !std::is_copy_assignable<T>::value> {};


  // SharedMutex named requirement
  template <typename T, typename = void>
  struct is_shared_mtx : std::false_type {};

  template <typename T>
  struct is_shared_mtx<T, std::void_t<decltype(std::declval<T&>().lock_shared(),
                                               std::declval<T&>().unlock_shared(),
                                               std::declval<bool&>() = std::declval<T&>().try_lock_shared())>>
    : std::integral_constant<bool, is_lockable_t<T>::value> {};

}

// An exemplar NullMutex which does nothing is shown below and can be used in
// case external sync. is guaranteed. To some extent this can be used to verify
// for eg concurrent_map<MapType<K,V>, NullMutex> should be equal in performance to a
// a MapType<K,V>
struct NullMutex {
  NullMutex() = default;
  ~NullMutex() = default;

  NullMutex(NullMutex const&) = delete;
  NullMutex& operator=(NullMutex const&) = delete;

  void lock() {}
  void lock_shared() {}
  bool try_lock() { return true; }
  bool try_lock_shared() { return true; }

  void unlock() {}
  void unlock_shared() {}
};

template <typename Mtx, typename is_shared_mtx_t = void>
struct LockImpl
{
  using mutex_type = Mtx;
  using SharedLock = std::lock_guard<mutex_type>;
  using UniqueLock = std::lock_guard<mutex_type>;
};

template <typename Mtx>
struct LockImpl<Mtx, typename std::enable_if_t<detail::is_shared_mtx<Mtx>::value>>
{
  using mutex_type = Mtx;
  using SharedLock = std::shared_lock<mutex_type>;
  using UniqueLock = std::unique_lock<mutex_type>;
};

template <>
class LockImpl<eos::common::RWMutex>: eos::common::RWMutex
{
public:
  using mutex_type = eos::common::RWMutex;
  using SharedLock = eos::common::RWMutexReadLock;
  using UniqueLock = eos::common::RWMutexWriteLock;
};

} // namespace eos::common
