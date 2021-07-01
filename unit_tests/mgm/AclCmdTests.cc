//------------------------------------------------------------------------------
// File: AclTests.cc
// Author: Elvin-Alin Sindrilaru <esindril at cern dot ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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
#include "mgm/proc/user/AclCmd.hh"

using namespace eos::mgm;

TEST(AclCmd, RuleMap)
{
  RuleMap expect_map = {
    { "u:99", 0b011111111111}, { "u:0", 0b01010101010}
  };
  RuleMap result_map;
  const std::string acl = "u:99:rwxm!m!d+d!u+uqc,u:0:wm!d!uq";
  AclCmd::GenerateRuleMap(acl, result_map);
  ASSERT_EQ(result_map.size(), expect_map.size());
  ASSERT_EQ(result_map, expect_map);
}

TEST(AclCmd, key_position)
{
  RuleMap input_map {
    {"u:99", 0b011111111111},
    {"u:1001",0b01},
    {"g:123",0b101},
    {"u:100",0b11}
  };

  auto begin_it = input_map.begin();
  auto end_it = input_map.end();
  EXPECT_EQ(key_position(input_map, "u:123"),end_it);
  EXPECT_EQ(key_position(input_map, "g:123"),std::next(begin_it,2));
}

TEST(AclCmd, insert_or_assign_simple)
{
  RuleMap input_map {
    {"u:99", 0b011111111111},
    {"u:1001",0b01},
    {"g:123",0b101},
    {"u:100",0b11}
  };

  RuleMap expected_map {
    {"u:99", 0b011111111111},
    {"u:1001",0b01},
    {"g:123",0b101},
    {"u:100",0b11},
    {"u:123",0b100}
  };

  insert_or_assign(input_map, "u:123", 0b100);
  EXPECT_EQ(input_map, expected_map);

  insert_or_assign(input_map, "u:1001", 0b1001);

  expected_map = {{"u:99", 0b011111111111},
                  {"u:1001",0b1001},
                  {"g:123",0b101},
                  {"u:100",0b11},
                  {"u:123",0b100}
  };

  EXPECT_EQ(input_map, expected_map);

  {
    std::string key1 = "u:9001";
    unsigned short i = 100;
    insert_or_assign(input_map, key1, i);
    // Check that the values haven't moved
    std::string key2 = "u:9001";
    EXPECT_EQ(key1,key2);
    expected_map.emplace_back(key1,i);
    EXPECT_EQ(input_map, expected_map);
  }

  {
    std::string key1 = "u:9002";
    unsigned short val = 101;
    insert_or_assign(input_map, std::move(key1), std::move(val));
    // key was moved
    EXPECT_EQ(key1,std::string());
    EXPECT_EQ(val, 101);

    expected_map.emplace_back("u:9002",val);
    EXPECT_EQ(input_map, expected_map);
  }

  {
    std::string key1 = "u:9002";
    unsigned short val = 102;
    insert_or_assign(input_map, std::move(key1), std::move(val));
    // key will not be moved as it already exists
    EXPECT_EQ(key1, "u:9002");
    EXPECT_EQ(val, 102);

    // There is no easy way to do this than one of our functions again ;)
    expected_map.pop_back();
    expected_map.emplace_back(key1, val);
    EXPECT_EQ(input_map, expected_map);
  }
}

TEST(AclCmd, get_iterator)
{
  RuleMap input_map = {{"u:99", 0b011111111111},
                        {"u:1001",0b1001},
                        {"g:123",0b101},
                        {"u:100",0b11},
                        {"u:123",0b100}
  };

  {
    auto [it, err] = get_iterator(input_map, 1);
    EXPECT_EQ(it, input_map.begin());
  }

  {
    auto [it, err] = get_iterator(input_map, 6);
    EXPECT_EQ(err, EINVAL);
  }

  {
    auto [it, err] = get_iterator(input_map, 5);
    ASSERT_EQ(err, 0);
    EXPECT_EQ(it->first,"u:123");
  }
}

TEST(AclCmd, insert_or_assign_iter)
{
  RuleMap input_map = {{"u:99", 0b011111111111},
                       {"u:1001",0b1001},
                       {"g:123",0b101},
                       {"u:100",0b11},
                       {"u:123",0b100}
  };

  {
    auto [it, _] = get_iterator(input_map, 1);
    insert_or_assign(input_map, "u:9001",0b1010, it);

    RuleMap expected_map = {{"u:9001",0b1010},
                            {"u:99", 0b011111111111},
                            {"u:1001",0b1001},
                            {"g:123",0b101},
                            {"u:100",0b11},
                            {"u:123",0b100}
    };

    EXPECT_EQ(expected_map, input_map);
  }

  {
    // No movement of keys as we dont override move_existing
    auto [it, _] = get_iterator(input_map, 3);
    insert_or_assign(input_map, "u:9001",0b1011, it);

    RuleMap expected_map = {{"u:9001",0b1011},
                            {"u:99", 0b011111111111},
                            {"u:1001",0b1001},
                            {"g:123",0b101},
                            {"u:100",0b11},
                            {"u:123",0b100}
    };

    EXPECT_EQ(expected_map, input_map);
  }

  {
    // In this case we move the keys as we are passing true, so we're promoting
    // this val up one place
    auto [it, _] = get_iterator(input_map, 4);
    insert_or_assign(input_map, "u:100",0b11011, it, true);

    RuleMap expected_map = {{"u:9001",0b1011},
                            {"u:99", 0b011111111111},
                            {"u:1001",0b1001},
                            {"u:100",0b11011},
                            {"g:123",0b101},
                            {"u:123",0b100}
    };

    EXPECT_EQ(expected_map, input_map);
  }
  {
    // we are demoting an element
    auto [it, _] = get_iterator(input_map, 5);
    insert_or_assign(input_map, "u:99",0b11011, it, true);

    RuleMap expected_map = {{"u:9001",0b1011},
                            {"u:1001",0b1001},
                            {"u:100",0b11011},
                            {"g:123",0b101},
                            {"u:99", 0b11011},
                            {"u:123",0b100}
    };

    EXPECT_EQ(expected_map, input_map);

  }

}
