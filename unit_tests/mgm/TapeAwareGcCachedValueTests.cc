//------------------------------------------------------------------------------
// File: TapeAwareGcCachedValueTests.cc
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

#include "mgm/TapeAwareGcCachedValue.hh"

#include <gtest/gtest.h>
#include <stdint.h>

class TapeAwareGcCachedValueTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TapeAwareGcCachedValueTest, noChange)
{
  using namespace eos::mgm;

  const uint64_t initialValue = 1234;
  const uint64_t nextValue = 5678;
  auto getter = [nextValue]()->uint64_t{return nextValue;};
  const time_t maxAgeSecs = 1000;
  TapeAwareGcCachedValue<uint64_t> cachedValue(initialValue, getter, maxAgeSecs);

  bool valueChanged = false;
  const uint64_t retrievedValue = cachedValue.get(valueChanged);

  ASSERT_EQ(initialValue, retrievedValue);
  ASSERT_FALSE(valueChanged);
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TapeAwareGcCachedValueTest, aChangeOccurred)
{
  using namespace eos::mgm;

  const uint64_t initialValue = 1234;
  const uint64_t nextValue = 5678;
  auto getter = [nextValue]()->uint64_t{return nextValue;};
  const time_t maxAgeSecs = 0;
  TapeAwareGcCachedValue<uint64_t> cachedValue(initialValue, getter, maxAgeSecs);

  bool valueChanged = false;
  const uint64_t retrievedValue = cachedValue.get(valueChanged);

  ASSERT_EQ(nextValue, retrievedValue);
  ASSERT_TRUE(valueChanged);
}
