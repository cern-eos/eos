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

#ifndef __SHARDED_CACHE__HH__
#define __SHARDED_CACHE__HH__

#include "Utils.hh"
#include "common/RWMutex.hh"
#include "misc/AssistedThread.hh"
#include <memory>
#include <vector>
#include <mutex>
#include <map>

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
// 3. Hashing: You can specify a custom hasing function to map from Key -> shard id.
// 4. Garbage collection: Thanks to shared pointers, we can keep track of how many
//    references currently exist for each element in the cache by calling use_count.
//
//    Garbage collection is done in two passes.
//    - Every N seconds, we go through the entire contents. If an element exists
//      only in our cache, we mark it as unused, but we don't remove it yet.
//    - If this element is retrieved after that, we unset the mark.
//    - If during the next pass the mark is still there, it means it hasn't been
//      used for at least N seconds, so we evict it.

template<typename Key>
struct IdentityHash {
  static uint64_t hash(const Key& key) {
    return key;
  }
};

template<typename Key, typename Value, typename Hash>
class ShardedCache {
private:
  class ShardGuard {
  public:
    ShardGuard(ShardedCache *cache, const Key &key) {
      shardId = cache->calculateShard(key);
      mtx = &cache->mutexes[shardId];

      mtx->lock();
    }

    int64_t getShard() const {
      return shardId;
    }

    ~ShardGuard() {
      mtx->unlock();
    }
  private:
    std::mutex *mtx;
    int64_t shardId;
  };

  int64_t calculateShard(const Key &key) {
    return Hash::hash(key) >> shardBits;
  }

public:
  // TTL is approximate. An element can stay while unused from [ttl, 2*ttl]
  ShardedCache(size_t shardBits_, Milliseconds ttl_)
  : shardBits(shardBits_), shards(pow(2, shardBits)), ttl(ttl_), mutexes(shards), contents(shards) {
    cleanupThread.reset(&ShardedCache<Key, Value, Hash>::garbageCollector, this);
  }

  ~ShardedCache() { }

  // Retrieves an item from the cache. If there isn't any, return a null shared_ptr.
  std::shared_ptr<Value> retrieve(const Key& key) {
    ShardGuard guard(this, key);

    typename std::map<Key, CacheEntry>::iterator it = contents[guard.getShard()].find(key);
    if(it == contents[guard.getShard()].end()) {
      return std::shared_ptr<Value>();
    }

    // if(it->first == 4) std::cerr << "erasing " << it->first << std::endl;
    it->second.marked = false;
    return it->second.value;
  }

  // Calling this function means giving up ownership of the pointer.
  // Don't use it anymore and don't call delete on it!
  // Return value: whether insertion was successful.
  bool store(const Key& key, Value* const value, bool replace = true) {
    CacheEntry entry;
    entry.marked = false;
    entry.value = std::shared_ptr<Value>(value);

    ShardGuard guard(this, key);

    if(replace) {
      contents[guard.getShard()][key] = entry;
      return true;
    }

    std::pair<typename std::map<Key, CacheEntry>::iterator, bool> status;
    status = contents[guard.getShard()].insert(std::pair<Key, CacheEntry>(key, entry));
    return status.second;
  }

  // Removes an element from the cache. Return value is whether the key existed.
  // If you want to replace an entry, just call store with replace set to false.
  bool invalidate(const Key& key) {
    ShardGuard guard(this, key);

    typename std::map<Key, CacheEntry>::iterator it = contents[guard.getShard()].find(key);
    if(it == contents[guard.getShard()].end()) return false;
    contents[guard.getShard()].erase(it);
    return true;
  }

private:
  size_t shardBits;
  size_t shards;
  Milliseconds ttl;

  struct CacheEntry {
    std::shared_ptr<Value> value;
    bool marked;
  };

  std::vector<std::mutex> mutexes;
  std::vector<std::map<Key, CacheEntry>> contents;

  AssistedThread cleanupThread;

  // Sweep through all entries in all shards to either mark them as unused or
  // remove them
  void collectorPass() {
    for(size_t i = 0; i < shards; i++) {
      std::lock_guard<std::mutex> lock(mutexes[i]);

      typename std::map<Key, CacheEntry>::iterator iterator;
      for(iterator = contents[i].begin(); iterator != contents[i].end(); /* no increment */) {
        if(iterator->second.marked) {
          iterator = contents[i].erase(iterator);
          continue;
        }

        if(iterator->second.value.use_count() == 1) {
          iterator->second.marked = true;
        }

        iterator++;
      }
    }
  }

  void garbageCollector(ThreadAssistant &assistant) {
    while(!assistant.terminationRequested()) {
      assistant.wait_for(std::chrono::milliseconds(ttl));
      if(assistant.terminationRequested()) return;

      collectorPass();
    }
  }
};

#endif
