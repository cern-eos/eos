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
  EXPECT_TRUE(cm.erase(10));
  const auto k = cm.find(10);
  EXPECT_EQ(k, cm.end());


}
