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
#include "common/concurrent_map/concurrent_map_adapter.hh"
#include <unordered_map>

TEST(ConcurrentMap, Basic)
{
  eos::common::concurrent_map_adapter<std::unordered_map<int,std::string>> cm;
  for (int i=0;i<100;i++){
    cm.emplace(i,"value"+std::to_string(i));
  }

  auto kv = cm.find(10);
  EXPECT_EQ(kv->second,"value10");
  EXPECT_TRUE(cm.erase(10));
  const auto k = cm.find(10);
  EXPECT_EQ(k, cm.end());
}
