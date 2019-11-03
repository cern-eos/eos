//------------------------------------------------------------------------------
// File: TgcUtilsTests.cc
// Author: Steven Murray <smurray at cern dot ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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

#include "mgm/tgc/Utils.hh"

#include <gtest/gtest.h>

class TgcUtilsTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcUtilsTest, isValidUInt_unsigned_int) {
  using namespace eos::mgm::tgc;

  ASSERT_TRUE(Utils::isValidUInt("12345"));
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcUtilsTest, isValidUInt_empty_string) {
  using namespace eos::mgm::tgc;

  ASSERT_FALSE(Utils::isValidUInt(""));
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcUtilsTest, isValidUInt_signed_int) {
  using namespace eos::mgm::tgc;

  ASSERT_FALSE(Utils::isValidUInt("-12345"));
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcUtilsTest, isValidUInt_not_a_number) {
  using namespace eos::mgm::tgc;

  ASSERT_FALSE(Utils::isValidUInt("one"));
}

TEST_F(TgcUtilsTest, toUint64_unsigned_int) {
  using namespace eos::mgm::tgc;

  ASSERT_EQ((uint64_t)12345, Utils::toUint64("12345"));
  ASSERT_EQ((uint64_t)18446744073709551615ULL, Utils::toUint64("18446744073709551615"));
}

TEST_F(TgcUtilsTest, toUint64_out_of_range) {
  using namespace eos::mgm::tgc;

  ASSERT_THROW(Utils::toUint64("18446744073709551616"), Utils::OutOfRangeUint64);
}

TEST_F(TgcUtilsTest, toUint64_empty_string) {
  using namespace eos::mgm::tgc;

  ASSERT_THROW(Utils::toUint64(""), Utils::InvalidUint64);
}

TEST_F(TgcUtilsTest, toUint64_max) {
  using namespace eos::mgm::tgc;

  ASSERT_EQ((uint64_t)18446744073709551615UL, Utils::toUint64("18446744073709551615"));
}

TEST_F(TgcUtilsTest, toUint64_not_a_number) {
  using namespace eos::mgm::tgc;

  ASSERT_THROW(Utils::toUint64("one"), Utils::InvalidUint64);
}
