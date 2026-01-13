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

