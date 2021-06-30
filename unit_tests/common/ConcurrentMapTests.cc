//------------------------------------------------------------------------------
// File: ConcurrentMapTests.cc
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

#include "gtest/gtest.h"
#include "common/concurrent_map/concurrent_map.hh"
#include <unordered_map>
#include <unordered_set>
#include <numeric>
#include <google/dense_hash_map>
using eos::common::std_concurrent_map;
using eos::common::dense_concurrent_map;


namespace test {
  using namespace eos::common;
// Some compile time checks... we could move this with an assert_true if we
// later find very long amount of these increase the compile time TEST that the
// aliases correctly forward the types to the underlying containers
  static_assert(std::is_same_v<std_concurrent_map<int,std::string>,
                eos::common::concurrent_map_adapter<std::unordered_map<int,std::string>,std::mutex>>);

  static_assert(std::is_same_v<std_concurrent_map<int,std::string,std::shared_mutex>,
                eos::common::concurrent_map_adapter<std::unordered_map<int,std::string>,std::shared_mutex>>);

  static_assert(std::is_same_v<std_concurrent_map<int,std::string,std::shared_mutex,std::hash<int>>,
                eos::common::concurrent_map_adapter<std::unordered_map<int,std::string,std::hash<int>>,std::shared_mutex>>);

  static_assert(std::is_same_v<dense_concurrent_map<int,std::string>,
                eos::common::concurrent_map_adapter<google::dense_hash_map<int,std::string,SPARSEHASH_HASH<int>>,std::mutex>>);

  static_assert(detail::has_try_emplace<std::unordered_map<int,int>>::value);
  static_assert(!detail::has_try_emplace<google::dense_hash_map<int,int>>::value);

}

TEST(ConcurrentMap, Basic)
{
  std_concurrent_map<int,std::string> cm;
  for (int i=0;i<100;i++){
    cm.emplace(i,"value"+std::to_string(i));
  }
  // Test iterator & std::algo basic
  std::vector <int> expected_keys, actual_keys;
  expected_keys.resize(100);
  std::iota(expected_keys.begin(), expected_keys.end(),0);
  std::transform(cm.begin(),
                 cm.end(),
                 std::back_inserter(actual_keys),
                 [](const auto& kv) { return kv.first; });
  std::sort(actual_keys.begin(),actual_keys.end());


  EXPECT_EQ(expected_keys, actual_keys);

  auto kv = cm.find(10);
  EXPECT_EQ(kv->second,"value10");
  // Can't modify!
  // kv->second = "foo"; will not compile as we return a const
  EXPECT_TRUE(cm.erase(10));
  EXPECT_EQ(cm.find(10),cm.end());
}

TEST(ConcurrentMap, BasicDense)
{
  dense_concurrent_map<int,std::string> cm;
  cm.set_empty_key(-1);
  cm.set_deleted_key(-100);
  for (int i=0;i<100;i++){
    // emplace will raise the static_assert here and fail compilation!
    auto p = std::make_pair(i,"value"+std::to_string(i));
    cm.insert(p);
  }
  // Test iterator & std::algo basic
  std::vector <int> expected_keys, actual_keys;
  expected_keys.resize(100);
  std::iota(expected_keys.begin(), expected_keys.end(),0);
  std::transform(cm.begin(),
                 cm.end(),
                 std::back_inserter(actual_keys),
                 [](const auto& kv) { return kv.first; });
  std::sort(actual_keys.begin(),actual_keys.end());


  EXPECT_EQ(expected_keys, actual_keys);

  auto kv = cm.find(10);
  EXPECT_EQ(kv->second,"value10");
  // Can't modify!
  // kv->second = "foo"; will not compile as we return a const
  EXPECT_TRUE(cm.erase(10));
  EXPECT_EQ(cm.find(10),cm.end());
}

TEST(ConcurrentMap, emplace)
{
  std_concurrent_map<std::string,std::string> cm;
  std::string f{"foo"};
  std::string f2{"foo"};

  {
    auto [it, status] = cm.try_emplace(std::move(f),"bar");
    EXPECT_TRUE(status);
    EXPECT_FALSE(f.size());
    EXPECT_EQ(it->second,"bar");
  }
  {
    auto [it, status] = cm.try_emplace(std::move(f2), "bar2");
    EXPECT_FALSE(status);
    EXPECT_EQ(it->second,"bar");
  }
}

TEST(ConcurrentMap, erase_it)
{
  std_concurrent_map<int,std::string> cm;
  for (int i=0;i<100;i++){
    cm.emplace(i,"value"+std::to_string(i));
  }

  auto kv = cm.find(10);
  auto it2 = kv;
  ++it2;
  auto result_it = cm.erase(kv);
  EXPECT_EQ(cm.find(10),cm.end());
  EXPECT_EQ(result_it, it2);
}
