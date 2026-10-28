//------------------------------------------------------------------------------
// File: CRC32CTests.cc
// Author: Luis Antonio Obis Aparicio - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2026 CERN/Switzerland                                  *
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

#include "fst/checksum/CRC32C.hh"
#include "gtest/gtest.h"
#include <string>

//------------------------------------------------------------------------------
// Standard CRC-32C check value
//------------------------------------------------------------------------------
TEST(CRC32C, CheckValue)
{
  eos::fst::CRC32C xs;
  ASSERT_TRUE(xs.Add("123456789", 9, 0));
  ASSERT_STREQ("e3069283", xs.GetHexChecksum());
}

//------------------------------------------------------------------------------
// Chunked updates, including appending after finalization, match one shot
//------------------------------------------------------------------------------
TEST(CRC32C, ChunkedAndAppend)
{
  const std::string data(100000, 'x');
  eos::fst::CRC32C whole;
  ASSERT_TRUE(whole.Add(data.data(), data.size(), 0));
  const std::string expected = whole.GetHexChecksum();
  eos::fst::CRC32C chunked;
  ASSERT_TRUE(chunked.Add(data.data(), 4096, 0));
  (void)chunked.GetHexChecksum();
  ASSERT_TRUE(chunked.Add(data.data() + 4096, data.size() - 4096, 4096));
  ASSERT_EQ(expected, chunked.GetHexChecksum());
}
