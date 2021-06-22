//------------------------------------------------------------------------------
// File: concurrent_map_adapter.hh
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

#include "concurrent_map_lock.hh"
#include <thread>
#include <utility>

namespace eos::common {

template <class MapType,
          class Mtx = std::mutex>
class concurrent_map_adapter {
public:
  using key_type =typename MapType::key_type;
  using mapped_type = typename MapType::mapped_type;
  using size_type = typename MapType::size_type;
  using difference_type = typename MapType::difference_type;
  using allocator_type = typename MapType::allocator_type;
  using value_type = typename MapType::value_type;

  // Export the mutex & Lock typenames, can be useful to inspect that the
  // correct specialization is used
  using mutex_type = Mtx;
  using MapLock = LockImpl<mutex_type>;


  class const_iterator {
    friend class concurrent_map_adapter;
  public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = typename concurrent_map_adapter::value_type;
    using reference = const value_type&; // right?
    using pointer = typename MapType::const_pointer;
    using difference_type = typename concurrent_map_adapter::difference_type;
    // user-defined types not a part of a std:: iterator definition
    // should we expose this?
    using UnderlyingIterator = typename MapType::const_iterator;

    const_iterator& operator++() {
      // TODO implement me: a first impl could be that we initialize an
      // atomic_gen_nr at initialization time so that we can pull off
      // current_gen_nr == prev_gen_nr to check for rehashes. A fully safe
      // version will mean that we'll have to hold somethign like a set of seen
      // values which can be expensive for large hashmaps A cheap version could
      // be to find the current iterator in case of a rehash, but a rehash +
      // deletion of the current iterator would mean tracking with a cond_var or
      // so
      it++;
      return *this;
    }

    const_iterator operator++(int) {
      UnderlyingIterator it2 = it;
      it2++;
      return const_iterator(it2);
    }

    friend bool operator==(const const_iterator& a, const const_iterator& b) {
      return a.it ==b.it;
    }

    friend bool operator!=(const const_iterator& a, const const_iterator& b) {
      return !(a==b);
    }

    reference operator*() const { return *(it);}
    pointer operator->() const {  return it.operator->(); }
  private:
    const_iterator (const UnderlyingIterator& _it): it(_it) {}
    const_iterator (UnderlyingIterator&& _it): it(std::move(_it)) {}
    UnderlyingIterator it;
  };

  const_iterator begin() const {
    const_iterator it(hashmap.cbegin());
    return it;
  }

  const_iterator end() const {
    const_iterator it(hashmap.cend());
    return it;
  }
  template <typename... Args>
  auto emplace(Args&&... args)
  {
    typename MapLock::UniqueLock wlock(mtx);
    return hashmap.emplace(std::forward<Args>(args)...);
  }

  // FIXME: The auto types are a placeolder atm, move to concrete or atleast
  // provide type hints on the return type
  // insert actually returns a pair of <iterator,bool>
  // we could deviate from the standard and return a pair <const_iterator,bool> instead
  // Most call sites seem to only care about the bool return value at best
  auto insert(value_type&& val) //-> decltype(hashmap).insert(value_type&&)
  {
    typename MapLock::UniqueLock wlock(mtx);
    return hashmap.insert(std::forward<value_type>(val));
  }

  // Warning- this is a deviation from standard, we don't return a mutable iterator, as we can't guarantee
  // updates, if you want to update a found key, use the insert overload with the const_iterator
  // TODO-future: Can we pull of a C++20 style no-construction
  // find for a transparent key type
  const_iterator find(const key_type& key) {
    typename MapLock::SharedLock rlock(mtx);
    return const_iterator(hashmap.find(key));
  }

  // We can't fixate on bool ret_type as some maps for eg. abseil have a void
  // interface as well
  auto erase(const key_type& key) -> decltype(std::declval<MapType&>().erase(key))
  {
    typename MapLock::UniqueLock wlock(mtx);
    return hashmap.erase(key);
  }


  // This API from std::unordered_map actually is rarely used this way, ie.
  // reuse the next iterator from return, so maybe we should just return void
  // like densemap and many others do so this whole snifae madness can go away
  template <typename It, typename = std::enable_if_t<std::is_same_v<decltype(std::declval<MapType&>().erase(std::declval<typename It::UnderlyingIterator>())),
                                                       typename MapType::iterator>>>
  const_iterator erase(It pos) {
    typename MapLock::UniqueLock wlock(mtx);
    return const_iterator(hashmap.erase(pos.it));
  }

  // TODO: Value Modification not thread safe as it could be outside of the map,
  // prefer insert_or_assign or the like
  template <typename K = key_type>
  value_type& operator[](key_type&& key) {
    typename MapLock::UniqueLock wlock(mtx);
    return hashmap.try_emplace(std::forward<K>(key)).first;
  }

  size_type size() const {
    return hashmap.size();
  }

private:
  mutable Mtx mtx;
  MapType hashmap;
};

} // namespace eos::common
