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
#include <algorithm>
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

template<typename Key, typename Value, typename Hash = DefaultHash<Key>,
         bool isUnordered = true>
class ShardedCache
{
private:
  class ShardGuard
  {
  public:
    ShardGuard(ShardedCache* cache, const Key& key)
    {
      shardId = cache->calculateShard(key);
      mtx = &cache->mMutexes[shardId];
      mtx->lock();
    }

    int64_t getShard() const
    {
      return shardId;
    }

    ~ShardGuard()
    {
      mtx->unlock();
    }
  private:
    std::mutex* mtx;
    int64_t shardId;
  };

  struct CacheEntry {
    std::shared_ptr<Value> value;
    bool marked;

    CacheEntry() = default;

    explicit CacheEntry(Value value_) : value(std::make_shared<Value>(value_)),
                                        marked(false) {}
  };

public:

  template <typename... Args>
  using MapT = typename std::conditional_t<isUnordered,
        std::unordered_map<Args...>,
        std::map<Args...>>;

  using shard_map_t = MapT<Key, CacheEntry>;
  using key_type = Key;
  using value_type = CacheEntry;

  int64_t calculateShard(const Key& key) const
  {
    return Hash::hash(key) % mNumShards;
  }

  // ShardedCache without a GC thread!
  explicit ShardedCache(uint8_t shardBits_) : mNumShards(1UL << shardBits_),
    mMutexes(mNumShards),
    mContents(mNumShards)
  {
  }

  // TTL is approximate. An element can stay while unused from [ttl, 2*ttl]
  ShardedCache(uint8_t shardBits_, Milliseconds ttl_,
               std::string_view name_ = "ShardedCacheGC")
    :  mNumShards(1UL << shardBits_), mTTL(ttl_),
       mMutexes(mNumShards), mContents(mNumShards),
       mThreadName(name_.substr(0, 15))
  {
    mCleanupThread.reset(&ShardedCache::garbageCollector, this);
  }

  void reset_cleanup_thread(Milliseconds ttl_,
                            std::string_view name_ = "ShardedCacheGC")
  {
    mTTL = ttl_;
    mThreadName = name_.substr(0, 15);
    mCleanupThread.reset(&ShardedCache::garbageCollector, this);
  }

  ~ShardedCache()
  {
    mCleanupThread.join();
  }

  // Retrieves an item from the cache. If there isn't any, return a null shared_ptr.
  std::shared_ptr<Value> retrieve(const Key& key)
  {
    ShardGuard guard(this, key);
    auto it = mContents[guard.getShard()].find(key);

    if (it == mContents[guard.getShard()].end()) {
      return std::shared_ptr<Value>();
    }

    // if(it->first == 4) std::cerr << "erasing " << it->first << std::endl;
    it->second.marked = false;
    return it->second.value;
  }

  bool contains(const Key& key)
  {
    ShardGuard guard(this, key);
    auto it = mContents[guard.getShard()].find(key);
    return it != mContents[guard.getShard()].end();
  }

  // Calling this function means giving up ownership of the pointer.
  // Don't use it anymore and don't call delete on it!
  // Return value: whether insertion was successful.
  bool store(const Key& key, std::unique_ptr<Value> value, std::shared_ptr<Value>
             & retval, bool replace = true)
  {
    CacheEntry entry;
    entry.marked = false;
    entry.value = std::move(value);
    ShardGuard guard(this, key);

    if (replace) {
      mContents[guard.getShard()][key] = entry;
      retval = entry.value;
      return true;
    }

    auto status = mContents[guard.getShard()].insert(std::pair<Key, CacheEntry>(key,
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
             std::shared_ptr<const Value>& retval, bool replace = true)
  {
    std::shared_ptr<Value> val;
    bool status = store(key, std::move(value), val, replace);
    retval = val;
    return status;
  }

  /**
   * @brief      Increment the value by a given argument safely. In case
   * the key exists, we increment by the given argument, otherwise
   * we create a key with supplied value.
   * @param[in]  key: key to retrieve value from.
   * @param[in]  inc_val: incremental value to add to the existing value or
   *             the value in case key doesn't exist. We assume the value type
   *             supporting + would be a simple type to copy, so only the value
   *             overload is provided atm instead of the reference overloads.
   * @return     The old value before increment
   */
  Value fetch_add(const Key& key, Value inc_val)
  {
    ShardGuard guard(this, key);
    auto shard = guard.getShard();
    Value old_val{};
    auto it = mContents[shard].find(key);
    if (it != mContents[shard].end()) {
      Value* value = it->second.value.get();
      old_val = *value;
      *value += inc_val;
    } else {
      mContents[shard].emplace(key, CacheEntry(inc_val));
    }
    return old_val;
  }

  // Removes an element from the cache. Return value is whether the key existed.
  // If you want to replace an entry, just call store with replace set to false.
  bool invalidate(const Key& key)
  {
    ShardGuard guard(this, key);
    auto it = mContents[guard.getShard()].find(key);

    if (it == mContents[guard.getShard()].end()) {
      return false;
    }

    mContents[guard.getShard()].erase(it);
    return true;
  }

  void clear()
  {
    for (size_t i = 0; i < mContents.size(); ++i) {
      std::lock_guard guard(mMutexes[i]);
      mContents[i].clear();
    }
  }

  // Some observer functions for validation, and in cases where we need to know
  // cache sizes
  size_t num_shards() const
  {
    return mNumShards;
  }

  size_t num_entries() const
  {
    size_t count = 0;

    for (size_t i = 0; i < mContents.size(); ++i) {
      std::lock_guard guard(mMutexes[i]);
      count += mContents[i].size();
    }

    return count;
  }

  size_t num_content_shards() const
  {
    return mContents.size();
  }

  /**
   * @brief      Get a copy of contents of a given shard
   * @param[in]  shard number
   * @return     A map with the values copied out of their shared_ptr, so lifetimes
   * will not be affected! The map type is the same as the underlying map type,
   * which could be unordered or std::map depending on the template parameter.
   */
  MapT<Key, Value> get_shard(size_t shard) const
  {
    if (shard >= mContents.size()) {
      throw std::out_of_range("trying to access non-existent shard");
    }

    MapT<Key, Value> ret;
    std::lock_guard guard(mMutexes[shard]);
    std::transform(mContents[shard].begin(), mContents[shard].end(),
                   std::inserter(ret, ret.end()),
    [](const auto & pair) {
      return std::make_pair(pair.first, *pair.second.value);
    });
    return ret;
  }

private:
  size_t mNumShards;
  Milliseconds mTTL;
  mutable std::vector<std::mutex> mMutexes;
  std::vector<MapT<Key, CacheEntry>> mContents;
  std::string mThreadName;
  AssistedThread mCleanupThread;

  // Sweep through all entries in all shards to either mark them as unused or
  // remove them
  void collectorPass()
  {
    for (size_t i = 0; i < mNumShards; i++) {
      std::lock_guard<std::mutex> lock(mMutexes[i]);

      for (auto iterator = mContents[i].begin();
           iterator != mContents[i].end(); /* no increment */) {
        if (iterator->second.marked) {
          iterator = mContents[i].erase(iterator);
          continue;
        }

        if (iterator->second.value.use_count() == 1) {
          iterator->second.marked = true;
        }

        iterator++;
      }
    }
  }

  void garbageCollector(ThreadAssistant& assistant)
  {
    ThreadAssistant::setSelfThreadName(mThreadName);

    while (!assistant.terminationRequested()) {
      assistant.wait_for(std::chrono::milliseconds(mTTL));

      if (assistant.terminationRequested()) {
        return;
      }

      collectorPass();
    }
  }
};

#endif
