//----------------------------------------------------------------------
// File: ShardedCache.hh
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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

#ifndef SHARDED_CACHE__HH__
#define SHARDED_CACHE__HH__

#include "common/AssistedThread.hh"
#include <memory>
#include <vector>
#include <mutex>
#include <map>
#include <unordered_map>
#include <numeric>

using Milliseconds = int64_t;

// A generic copy-on-write sharded cache with configurable hash function, and
// automatic garbage collection.
//
// That's a lot of buzzwords.
// 1. Sharding: Concurrent clients can perform operations at the same time
//    without blocking each other, as long as they're hitting different shards.
// 2. Copy-on-write: Clients always get an immutable snapshot of the data in the
//    form of a shared pointer. No need to worry about locks or races after
//    acquiring such a snapshot.
// 3. Hashing: You can specify a custom hashing function to map from Key -> shard id.
// 4. Garbage collection: Thanks to shared pointers, we can keep track of how many
//    references currently exist for each element in the cache by calling use_count.
//
//    Garbage collection is done in two passes.
//    - Every N seconds, we go through the entire contents. If an element exists
//      only in our cache, we mark it as unused, but we don't remove it yet.
//    - If this element is retrieved after that, we unset the mark.
//    - If during the next pass the mark is still there, it means it hasn't been
//      used for at least N seconds, so we evict it.
//     In case a ttl is not supplied at start, the GC thread is not started. This
//     way this can just function as regular non expiring concurrent map
//
//     By default, we choose the underlying map to be unordered_map, but you can
//     force std::map by passing the 4th template parameter to true.

template<typename Key>
struct IdentityHash {
  static uint64_t hash(const Key& key)
  {
    return key;
  }
};

template <typename Key>
struct DefaultHash {
  static uint64_t hash(const Key& key)
  {
    return std::hash<Key>()(key);
  }
};

template<typename Key, typename Value, typename Hash=DefaultHash<Key>,
         bool isUnordered=true>
class ShardedCache
{
private:
  class ShardGuard
  {
  public:
    ShardGuard(ShardedCache *cache, const Key &key) {
      shardId = cache->calculateShard(key);
      mtx = &cache->mutexes[shardId];

      mtx->lock();
    }

    int64_t getShard() const
    {
      return shardId;
    }

    ~ShardGuard() {
      mtx->unlock();
    }
  private:
    std::mutex *mtx;
    int64_t shardId;
  };

  struct CacheEntry {
    std::shared_ptr<Value> value;
    bool marked;
  };

public:

  template <typename... Args>
  using MapT = typename std::conditional_t<isUnordered,
      std::unordered_map<Args...>,
      std::map<Args...>>;

  using shard_map_t = MapT<Key, CacheEntry>;
  using key_type = Key;
  using value_type = CacheEntry;

  int64_t calculateShard(const Key &key) const {
    return Hash::hash(key) % shards;
  }

  // ShardedCache without a GC thread!
  explicit ShardedCache(uint8_t shardBits_) : shards(1UL << shardBits_),
                                              mutexes(shards),
                                              contents(shards) {
  }

  // TTL is approximate. An element can stay while unused from [ttl, 2*ttl]
  ShardedCache(uint8_t shardBits_, Milliseconds ttl_,
               std::string name_= "ShardedCacheGC")
  :  shards(1UL << shardBits_), ttl(ttl_),
     mutexes(shards), contents(shards),
     threadName(std::move(name_)) {
    cleanupThread.reset(&ShardedCache::garbageCollector, this);
  }

  void reset_cleanup_thread(Milliseconds ttl_,
                            std::string name_ = "ShardedCacheGC") {
    ttl = ttl_;
    threadName = std::move(name_);
    cleanupThread.reset(&ShardedCache::garbageCollector, this);
  }

  ~ShardedCache() { }

  // Retrieves an item from the cache. If there isn't any, return a null shared_ptr.
  std::shared_ptr<Value> retrieve(const Key& key) {
    ShardGuard guard(this, key);
    auto it = contents[guard.getShard()].find(key);

    if (it == contents[guard.getShard()].end()) {
      return std::shared_ptr<Value>();
    }

    // if(it->first == 4) std::cerr << "erasing " << it->first << std::endl;
    it->second.marked = false;
    return it->second.value;
  }

  bool contains(const Key& key) {
    ShardGuard guard(this, key);
    auto it = contents[guard.getShard()].find(key);
    return it != contents[guard.getShard()].end();
  }

  // Calling this function means giving up ownership of the pointer.
  // Don't use it anymore and don't call delete on it!
  // Return value: whether insertion was successful.
  bool store(const Key& key, std::unique_ptr<Value> value, std::shared_ptr<Value>
    &retval, bool replace = true)
  {
    CacheEntry entry;
    entry.marked = false;
    entry.value = std::move(value);
    ShardGuard guard(this, key);

    if (replace) {
      contents[guard.getShard()][key] = entry;
      retval = entry.value;
      return true;
    }


    auto status = contents[guard.getShard()].insert(std::pair<Key, CacheEntry>(key,
             entry));
    retval = status.first->second.value;
    return status.second;
  }

  // store overload without retval
  bool store(const Key& key, std::unique_ptr<Value> value, bool replace = true)
  {
    std::shared_ptr<Value> val;
    return store(key, std::move(value), val, replace);
  }

  // store overload with const retval.
  bool store(const Key& key, std::unique_ptr<Value> value,
    std::shared_ptr<const Value> &retval, bool replace = true)
  {
    std::shared_ptr<Value> val;
    bool status = store(key, std::move(value), val, replace);
    retval = val;
    return status;
  }

  // Removes an element from the cache. Return value is whether the key existed.
  // If you want to replace an entry, just call store with replace set to false.
  bool invalidate(const Key& key) {
    ShardGuard guard(this, key);
    auto it = contents[guard.getShard()].find(key);
    contents[guard.getShard()].erase(it);
    return true;
  }

  void clear() {
    for (size_t i = 0; i < contents.size(); ++i) {
      std::lock_guard guard(mutexes[i]);
      contents[i].clear();
    }
  }

  // Some observer functions for validation, and in cases where we need to know
  // cache sizes
  size_t num_shards() const {
    return shards;
  }

  size_t num_entries() const {
    size_t count = 0;
    for (size_t i = 0; i < contents.size(); ++i) {
      std::lock_guard guard(mutexes[i]);
      count += contents[i].size();
    }

    return count;
  }

  size_t num_content_shards() const {
    return contents.size();
  }

  /**
   * @brief      Get a copy of contents of a given shard
   * @param[in]  shard number
   * @return     A map with the values copied out of their shared_ptr, so lifetimes
   * will not be affected! The map type is the same as the underlying map type,
   * which could be unordered or std::map depending on the template parameter.
   */
  MapT<Key,Value> get_shard(size_t shard) const {
    if (shard >= contents.size()) {
      throw std::out_of_range("trying to access non-existent shard");
    }
    MapT<Key,Value> ret;
    std::lock_guard guard(mutexes[shard]);
    std::transform(contents[shard].begin(), contents[shard].end(),
                   std::inserter(ret, ret.end()),
                   [](const auto& pair) {
                     return std::make_pair(pair.first, *pair.second.value);
                   });
    return ret;
  }

private:
  size_t shards;
  Milliseconds ttl;

  mutable std::vector<std::mutex> mutexes;
  std::vector<MapT<Key, CacheEntry>> contents;

  AssistedThread cleanupThread;
  std::string threadName;
  // Sweep through all entries in all shards to either mark them as unused or
  // remove them
  void collectorPass() {
    for(size_t i = 0; i < shards; i++) {
      std::lock_guard<std::mutex> lock(mutexes[i]);

      for (auto iterator = contents[i].begin();
           iterator != contents[i].end(); /* no increment */) {
        if (iterator->second.marked) {
          iterator = contents[i].erase(iterator);
          continue;
        }

        if (iterator->second.value.use_count() == 1) {
          iterator->second.marked = true;
        }

        iterator++;
      }
    }
  }

  void garbageCollector(ThreadAssistant &assistant) {
    ThreadAssistant::setSelfThreadName(threadName);
    while(!assistant.terminationRequested()) {
      assistant.wait_for(std::chrono::milliseconds(ttl));
      if(assistant.terminationRequested()) return;

      collectorPass();
    }
  }
};

#endif
