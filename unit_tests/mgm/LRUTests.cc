//------------------------------------------------------------------------------
// File: LRUTests.cc
// Author: Georgios Bitzes <georgios.bitzes@cern.ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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
#include "mgm/lru/LRU.hh"

//------------------------------------------------------------------------------
// Test "sys.lru.expire.match" policy parsing, just one entry
//------------------------------------------------------------------------------
TEST(LRUTests, ExpireMatchParsingSingle) {
  std::map<std::string, time_t> results, expected;

  ASSERT_TRUE(eos::mgm::LRU::parseExpireMatchPolicy("*:1d", results));
  expected = { {"*", 86400 } };
  ASSERT_EQ(results, expected);

  ASSERT_TRUE(eos::mgm::LRU::parseExpireMatchPolicy("*:1mo", results));
  expected = { {"*", 31 * 86400 } };
  ASSERT_EQ(results, expected);
}

//------------------------------------------------------------------------------
// Test "sys.lru.expire.match" policy parsing, multiple entries
//------------------------------------------------------------------------------
TEST(LRUTests, ExpireMatchParsingMultiple) {
  std::map<std::string, time_t> results, expected;

  ASSERT_TRUE(eos::mgm::LRU::parseExpireMatchPolicy("*.root:1mo,*.tgz:1w", results));
  expected = { {"*.root", 31 * 86400 }, {"*.tgz", 7 * 86400} };
  ASSERT_EQ(results, expected);

  ASSERT_TRUE(eos::mgm::LRU::parseExpireMatchPolicy("*.root:1mo,*.tgz:1w,*.txt:77d", results));
  expected = { {"*.root", 31 * 86400 }, {"*.tgz", 7 * 86400}, {"*.txt", 77 * 86400} };
  ASSERT_EQ(results, expected);
}

//------------------------------------------------------------------------------
// Test age and size policy parsing
//------------------------------------------------------------------------------
TEST(LRUTests, ExtractTimeSizeCriterias)
{
  std::ostringstream errMsg;
  ssize_t size;
  time_t age;
  ASSERT_FALSE(eos::mgm::LRU::extractTimeSizeCriterias("", age, size, errMsg));
  ASSERT_TRUE(eos::mgm::LRU::extractTimeSizeCriterias("1", age, size, errMsg));
  ASSERT_EQ(0, size);
  ASSERT_EQ(1, age);
  ASSERT_TRUE(eos::mgm::LRU::extractTimeSizeCriterias("1mo:", age, size, errMsg));
  ASSERT_EQ(0, size);
  ASSERT_EQ(31 * 86400, age);
  ASSERT_FALSE(eos::mgm::LRU::extractTimeSizeCriterias("1w:1G", age, size, errMsg));
  ASSERT_TRUE(eos::mgm::LRU::extractTimeSizeCriterias("1mo:>1K", age, size, errMsg));
  ASSERT_EQ(1000, size);
  ASSERT_EQ(31 * 86400, age);
  ASSERT_TRUE(eos::mgm::LRU::extractTimeSizeCriterias("1mo:<1K", age, size, errMsg));
  ASSERT_EQ(-1000, size);
  ASSERT_EQ(31 * 86400, age);
}

static inline const std::pair<std::string, eos::mgm::LRU::PolicyRules>
    expectedPolicies[] = {
        {"*:1mo:>4M", {eos::mgm::LRU::PolicyRule("*", 86400 * 31, 4000000, 0)}},
        {"*:1d:<1G", {eos::mgm::LRU::PolicyRule("*", 86400, -1000000000, 0)}},
        {"*:1d:<1M,*.root:1d",
         {eos::mgm::LRU::PolicyRule("*", 86400, -1000000, 0),
          eos::mgm::LRU::PolicyRule("*.root", 86400, 0, 0)}},
        {"*:1d:<1M,*.root:1d,",
         {eos::mgm::LRU::PolicyRule("*", 86400, -1000000, 0),
          eos::mgm::LRU::PolicyRule("*.root", 86400, 0, 0)}},
        {"*:1d:<1M,,*.root:1d,",
         {eos::mgm::LRU::PolicyRule("*", 86400, -1000000, 0),
          eos::mgm::LRU::PolicyRule("*.root", 86400, 0, 0)}},
        {"*:1d", {eos::mgm::LRU::PolicyRule("*", 86400, 0, 0)}},

};

static inline const std::string wrongPolicies[] = {"*", "*:<1d", "azewrty", ""};

//------------------------------------------------------------------------------
// Test "sys.lru.expire.match" policy parsing, multiple entries
//------------------------------------------------------------------------------
TEST(LRUTests, ParseExpireSizeMatchPolicy)
{
  eos::mgm::LRU::PolicyRules results;
  std::ostringstream errMsg;
  for (const auto& validationItem : expectedPolicies) {
    ASSERT_TRUE(
        eos::mgm::LRU::parseExpireSizeMatchPolicy(validationItem.first, results, errMsg));
    size_t i = 0;
    for (const auto& policyRule : validationItem.second) {
      ASSERT_EQ(policyRule, results[i]);
      i++;
    }
  }

  for (const auto& wrongPolicy : wrongPolicies) {
    ASSERT_FALSE(eos::mgm::LRU::parseExpireSizeMatchPolicy(wrongPolicy, results, errMsg));
  }
}
