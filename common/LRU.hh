/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Adapted by Andreas-Joachim Peters <andreas.joachim.peters@cern.ch>   *
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

/************************************************************************/
/* Github: https://github.com/mohaps/lrucache11                         */
/* Copyright (c) 2012-22 SAURAV MOHAPATRA <mohaps@gmail.com>            */
/************************************************************************/

#pragma once
#include "common/Namespace.hh"
#include <algorithm>
#include <cstdint>
#include <list>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <unordered_map>


EOSCOMMONNAMESPACE_BEGIN

namespace LRU {

class NullLock {
 public:
  void lock() {}
  void unlock() {}
  bool try_lock() { return true; }
};

/**
 * error raised when a key not in cache is passed to get()
 */
class KeyNotFound : public std::invalid_argument {
 public:
  KeyNotFound() : std::invalid_argument("key_not_found") {}
};

template <typename K, typename V>
struct KeyValuePair {
 public:
  K key;
  V value;

  KeyValuePair(const K& k, const V& v) : key(k), value(v) {}
};

/**
 *	The LRU Cache class templated by
 *		Key - key type
 *		Value - value type
 *		MapType - an associative container like std::unordered_map
 *		LockType - a lock type derived from the Lock class (default:
 *NullLock = no synchronization)
 *
 *	The default NullLock based template is not thread-safe, however passing
 *Lock=std::mutex will make it
 *	thread-safe
 */
template <class Key, class Value, class Lock = NullLock,
          class Map = std::unordered_map<
              Key, typename std::list<KeyValuePair<Key, Value>>::iterator>>
class Cache {
 public:
  typedef KeyValuePair<Key, Value> node_type;
  typedef std::list<KeyValuePair<Key, Value>> list_type;
  typedef Map map_type;
  typedef Lock lock_type;
  using Guard = std::lock_guard<lock_type>;
  /**
   * the maxSize is the soft limit of keys and (maxSize + elasticity) is the
   * hard limit
   * the cache is allowed to grow till (maxSize + elasticity) and is pruned back
   * to maxSize keys
   * set maxSize = 0 for an unbounded cache (but in that case, you're better off
   * using a std::unordered_map
   * directly anyway! :)
   */
  explicit Cache(size_t maxSize = 64, size_t elasticity = 10)
      : maxSize_(maxSize), elasticity_(elasticity) {}
  virtual ~Cache() = default;
  size_t size() const {
    Guard g(lock_);
    return cache_.size();
  }
  bool empty() const {
    Guard g(lock_);
    return cache_.empty();
  }
  void clear() {
    Guard g(lock_);
    cache_.clear();
    keys_.clear();
  }
  void insert(const Key& k, const Value& v) {
    Guard g(lock_);
    const auto iter = cache_.find(k);
    if (iter != cache_.end()) {
      iter->second->value = v;
      keys_.splice(keys_.begin(), keys_, iter->second);
      return;
    }

    keys_.emplace_front(k, v);
    cache_[k] = keys_.begin();
    prune();
  }
  bool tryGet(const Key& kIn, Value& vOut) {
    Guard g(lock_);
    const auto iter = cache_.find(kIn);
    if (iter == cache_.end()) {
      return false;
    }
    keys_.splice(keys_.begin(), keys_, iter->second);
    vOut = iter->second->value;
    return true;
  }
  /**
   *	The const reference returned here is only
   *    guaranteed to be valid till the next insert/delete
   */
  const Value& get(const Key& k) {
    Guard g(lock_);
    const auto iter = cache_.find(k);
    if (iter == cache_.end()) {
      throw KeyNotFound();
    }
    keys_.splice(keys_.begin(), keys_, iter->second);
    return iter->second->value;
  }
  /**
   * returns a copy of the stored object (if found)
   */
  Value getCopy(const Key& k) {
   return get(k);
  }
  bool remove(const Key& k) {
    Guard g(lock_);
    auto iter = cache_.find(k);
    if (iter == cache_.end()) {
      return false;
    }
    keys_.erase(iter->second);
    cache_.erase(iter);
    return true;
  }
  bool contains(const Key& k) const {
    Guard g(lock_);
    return cache_.find(k) != cache_.end();
  }

  size_t getMaxSize() const { return maxSize_; }
  size_t getElasticity() const { return elasticity_; }
  size_t getMaxAllowedSize() const { return maxSize_ + elasticity_; }
  template <typename F>
  void cwalk(F& f) const {
    Guard g(lock_);
    std::for_each(keys_.begin(), keys_.end(), f);
  }

 protected:
  size_t prune() {
    size_t maxAllowed = maxSize_ + elasticity_;
    if (maxSize_ == 0 || cache_.size() < maxAllowed) {
      return 0;
    }
    size_t count = 0;
    while (cache_.size() > maxSize_) {
      cache_.erase(keys_.back().key);
      keys_.pop_back();
      ++count;
    }
    return count;
  }

 private:
  // Dissallow copying.
  Cache(const Cache&) = delete;
  Cache& operator=(const Cache&) = delete;

  mutable Lock lock_;
  Map cache_;
  list_type keys_;
  size_t maxSize_;
  size_t elasticity_;
};

} // end namespace LRU

EOSCOMMONNAMESPACE_END
