//------------------------------------------------------------------------------
// File: TapeAwareGcUtilsTests.cc
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

#include "mgm/TapeAwareGcUtils.hh"

#include <gtest/gtest.h>

class TapeAwareGcUtilsTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TapeAwareGcUtilsTest, isValidUInt_unsigned_int) {
  using namespace eos::mgm;

  ASSERT_TRUE(TapeAwareGcUtils::isValidUInt("12345"));
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TapeAwareGcUtilsTest, isValidUInt_empty_string) {
  using namespace eos::mgm;

  ASSERT_FALSE(TapeAwareGcUtils::isValidUInt(""));
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TapeAwareGcUtilsTest, isValidUInt_signed_int) {
  using namespace eos::mgm;

  ASSERT_FALSE(TapeAwareGcUtils::isValidUInt("-12345"));
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TapeAwareGcUtilsTest, isValidUInt_not_a_number) {
  using namespace eos::mgm;

  ASSERT_FALSE(TapeAwareGcUtils::isValidUInt("one"));
}

TEST_F(TapeAwareGcUtilsTest, toUint64_unsigned_int) {
  using namespace eos::mgm;

  ASSERT_EQ((uint64_t)12345, TapeAwareGcUtils::toUint64("12345"));
  ASSERT_EQ((uint64_t)18446744073709551615ULL, TapeAwareGcUtils::toUint64("18446744073709551615"));
}

TEST_F(TapeAwareGcUtilsTest, toUint64_out_of_range) {
  using namespace eos::mgm;

  ASSERT_THROW(TapeAwareGcUtils::toUint64("18446744073709551616"), TapeAwareGcUtils::OutOfRangeUint64);
}

TEST_F(TapeAwareGcUtilsTest, toUint64_empty_string) {
  using namespace eos::mgm;

  ASSERT_THROW(TapeAwareGcUtils::toUint64(""), TapeAwareGcUtils::InvalidUint64);
}

TEST_F(TapeAwareGcUtilsTest, toUint64_max) {
  using namespace eos::mgm;

  ASSERT_EQ((uint64_t)18446744073709551615UL, TapeAwareGcUtils::toUint64("18446744073709551615"));
}

TEST_F(TapeAwareGcUtilsTest, toUint64_not_a_number) {
  using namespace eos::mgm;

  ASSERT_THROW(TapeAwareGcUtils::toUint64("one"), TapeAwareGcUtils::InvalidUint64);
}
