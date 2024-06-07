//------------------------------------------------------------------------------
// File: ShardedCacheTests.cc
// Author: Abhishek Lekshmanan - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2022 CERN/Switzerland                           *
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

#include <gtest/gtest.h>
#include "common/ShardedCache.hh"

TEST(ShardedCache, Construction)
{
  ShardedCache<std::string, int> cache(8, 100);
  ASSERT_EQ(cache.num_shards(), 256);
  ASSERT_EQ(cache.num_content_shards(), 256);
  EXPECT_EQ(cache.num_entries(), 0);
}

TEST(ShardedCache, CalculateShard)
{
  ShardedCache<std::string, int> cache(8, 100);
  std::hash<std::string> hasher{};
  ASSERT_GE(cache.calculateShard("hello"), 0);
  ASSERT_LT(cache.calculateShard("hello"), 256);
  EXPECT_EQ(cache.calculateShard("hello"),
            hasher("hello") % 256);
}

TEST(ShardedCache, NoGC)
{
  ShardedCache<std::string, int> cache(8);
  ASSERT_EQ(cache.num_shards(), 256);
  ASSERT_EQ(cache.num_content_shards(), 256);
  EXPECT_EQ(cache.num_entries(), 0);
  std::hash<std::string> hasher{};
  ASSERT_GE(cache.calculateShard("hello"), 0);
  ASSERT_LT(cache.calculateShard("hello"), 256);
  EXPECT_EQ(cache.calculateShard("hello"),
            hasher("hello") % 256);
}

TEST(ShardedCache, EmptyRetrieve)
{
  ShardedCache<std::string, int> cache(8, 100);
  auto result = cache.retrieve("hello");
  EXPECT_EQ(result, nullptr);
}

TEST(ShardedCache, ValueRetrieve)
{
  ShardedCache<std::string, int> cache(8, 100);
  auto val = std::make_unique<int>(5);
  ASSERT_TRUE(cache.store("hello", std::move(val)));
  auto result = cache.retrieve("hello");
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(*result, 5);
}

TEST(ShardedCache, ValueNonExpiry)
{
  ShardedCache<std::string, int> cache(8, 10);
  ASSERT_TRUE(cache.store("hello", std::make_unique<int>(5)));
  auto result = cache.retrieve("hello");
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(*result, 5);
  // Since we hold a valid reference to the entry it never expires
  std::this_thread::sleep_for(std::chrono::milliseconds(30));
  auto result2 = cache.retrieve("hello");
  EXPECT_EQ(result2, result);
  // now release ownership. The entry should expire
  result.reset();
  result2.reset();
  std::this_thread::sleep_for(std::chrono::milliseconds(30));
  auto result3 = cache.retrieve("hello");
  EXPECT_EQ(result3, nullptr);
}

TEST(ShardedCache, ValueExpiry)
{
  ShardedCache<std::string, int> cache(8, 10);
  ASSERT_TRUE(cache.store("hello", std::make_unique<int>(5)));
  auto result = cache.retrieve("hello");
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(*result, 5);
  result.reset();
  // The entry takes between 2*ttl and 3*ttl to completely expire
  // as the first round just marks the entry as expired and the second round
  // actually deletes it.
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  auto result2 = cache.retrieve("hello");
  EXPECT_EQ(result2, nullptr);
}

TEST(ShardedCache, NoGCRetrieve)
{
  ShardedCache<std::string, int> cache(8);
  ASSERT_TRUE(cache.store("hello", std::make_unique<int>(5)));
  auto result = cache.retrieve("hello");
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(*result, 5);
}

TEST(ShardedCache, LateGCRun)
{
  ShardedCache<std::string, int> cache(8);
  ASSERT_TRUE(cache.store("hello", std::make_unique<int>(5)));
  auto result = cache.retrieve("hello");
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(*result, 5);
  result.reset();
  cache.reset_cleanup_thread(10);
  std::this_thread::sleep_for(std::chrono::milliseconds(30));
  ASSERT_EQ(cache.num_entries(), 0);
  auto result2 = cache.retrieve("hello");
  EXPECT_EQ(result2, nullptr);
}

TEST(ShardedCache, get_shard_except)
{
  ShardedCache<std::string, int> cache(8, 10);
  std::unordered_map<std::string, int> empty_map;

  for (unsigned int i = 0; i < cache.num_shards(); i++) {
    ASSERT_EQ(cache.get_shard(i), empty_map);
  }

  ASSERT_THROW(cache.get_shard(cache.num_shards()), std::out_of_range);
  ASSERT_THROW(cache.get_shard(cache.num_shards() + 1), std::out_of_range);
  ASSERT_THROW(cache.get_shard(-1), std::out_of_range);
}

TEST(ShardedCache, get_shard)
{
  ShardedCache<std::string, int> cache(8, 10);
  ASSERT_TRUE(cache.store("hello", std::make_unique<int>(5)));
  ASSERT_EQ(cache.num_entries(), 1);
  auto shard_copy = cache.get_shard(cache.calculateShard("hello"));
  ASSERT_EQ(shard_copy.size(), 1);
  ASSERT_EQ(shard_copy["hello"], 5);
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  ASSERT_EQ(cache.num_entries(), 0);
}

TEST(ShardedCache, clear)
{
  ShardedCache<std::string, int> cache(8, 10);
  ASSERT_TRUE(cache.store("hello", std::make_unique<int>(5)));
  ASSERT_EQ(cache.num_entries(), 1);
  cache.clear();
  ASSERT_EQ(cache.num_entries(), 0);
  auto result = cache.retrieve("hello");
  EXPECT_EQ(result, nullptr);
}

TEST(ShardedCache, reuse_after_clear)
{
  ShardedCache<std::string, int> cache(8, 10);
  ASSERT_TRUE(cache.store("hello", std::make_unique<int>(5)));
  ASSERT_EQ(cache.num_entries(), 1);
  cache.clear();
  ASSERT_EQ(cache.num_entries(), 0);
  auto result = cache.retrieve("hello");
  EXPECT_EQ(result, nullptr);
  ASSERT_TRUE(cache.store("hello", std::make_unique<int>(5)));
  ASSERT_EQ(cache.num_entries(), 1);
  result = cache.retrieve("hello");
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(*result, 5);
}

TEST(ShardedCache, value_after_clear)
{
  ShardedCache<std::string, int> cache(8, 10);
  ASSERT_TRUE(cache.store("hello", std::make_unique<int>(5)));
  ASSERT_EQ(cache.num_entries(), 1);
  auto result = cache.retrieve("hello");
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(*result, 5);
  ASSERT_EQ(result.use_count(), 2);
  cache.clear();
  // result is a shared_ptr, so regardless it is still valid
  // however no longer present in the cache map
  ASSERT_EQ(cache.num_entries(), 0);
  ASSERT_EQ(result.use_count(), 1);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(*result, 5);
}

TEST(ShardedCache, fetch_add)
{
  ShardedCache<std::string, int> cache(8, 10);
  ASSERT_TRUE(cache.store("hello", std::make_unique<int>(5)));
  auto result = cache.retrieve("hello");
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(*result, 5);
  auto old_val = cache.fetch_add("hello", 1);
  EXPECT_EQ(old_val, 5);
  auto result2 = cache.retrieve("hello");
  ASSERT_NE(result2, nullptr);
  EXPECT_EQ(*result2, 6);

}

TEST(ShardedCache, fetch_add_empty_key)
{
  ShardedCache<std::string, int> cache(8, 10);
  auto null_result = cache.retrieve("hello");
  ASSERT_EQ(null_result, nullptr);
  cache.fetch_add("hello", 1);
  auto result3 = cache.retrieve("hello");
  ASSERT_NE(result3, nullptr);
  EXPECT_EQ(*result3, 1);
}

TEST(ShardedCache, fetch_add_multithreaded)
{
  // Give a minute of GC instead of 10ms, so that we'd not expire any caches
  // before the function completes! Otherwise with 10ms it can so happen that
  // in small env by the time all the threads are run, GC would just run before we
  // retrieve the cache.
  ShardedCache<std::string, int> cache(8, 60*1000);
  std::vector<std::thread> threads;
  for (int i=0; i<200; i++) {
    threads.emplace_back([&cache]() {
      cache.fetch_add("mykey",1);
    });
  }
  for (auto& t : threads) {
    t.join();
  }
  ASSERT_EQ(cache.num_entries(), 1);
  auto result = cache.retrieve("mykey");
  ASSERT_EQ(*result, 200);
}

TEST(ShardedCache, invalidate)
{
  ShardedCache<std::string, int> cache(8, 10);
  ASSERT_TRUE(cache.store("hello", std::make_unique<int>(5)));
  ASSERT_TRUE(cache.store("hello2", std::make_unique<int>(6)));
  ASSERT_EQ(cache.num_entries(), 2);
  bool ret = cache.invalidate("hello");
  ASSERT_EQ(ret, true);
  ASSERT_EQ(cache.num_entries(), 1);
  ret = cache.invalidate("hello");
  ASSERT_EQ(ret, false);
  auto result = cache.retrieve("hello");
  EXPECT_EQ(result, nullptr);
}

TEST(ShardedCache, force_expiry)
{
  ShardedCache<std::string, int> cache(8,10);
  ASSERT_TRUE(cache.store("hello", std::make_unique<int>(5)));
  ASSERT_TRUE(cache.store("delete-me", std::make_unique<int>(5)));
  // We hold a shared ptr, so ideally the GC shouldn't clear this!
  auto result = cache.retrieve("hello");
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(*result, 5);
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  ASSERT_TRUE(cache.contains("hello"));
  ASSERT_FALSE(cache.contains("delete-me"));
  // Now force the reset cycle!
  cache.set_force_expiry(true, 2);

  std::this_thread::sleep_for(std::chrono::milliseconds(30));
  ASSERT_FALSE(cache.contains("hello"));
}
