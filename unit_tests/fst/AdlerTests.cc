//------------------------------------------------------------------------------
// File: AdlerTests.cc
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

#include "fst/checksum/Adler.hh"
#include "gtest/gtest.h"
#include <cstdio>
#include <random>
#include <string>
#include <vector>
#include <zlib.h>

namespace {
//------------------------------------------------------------------------------
// Reference value computed with zlib in one shot
//------------------------------------------------------------------------------
std::string
ZlibAdlerHex(const char* data, size_t len)
{
  uLong value = adler32(0L, Z_NULL, 0);
  value = adler32(value, reinterpret_cast<const Bytef*>(data), len);
  char hex[9];
  snprintf(hex, sizeof(hex), "%08lx", value);
  return hex;
}

std::vector<char>
RandomBytes(size_t len)
{
  std::mt19937 gen(42);
  std::uniform_int_distribution<int> dist(0, 255);
  std::vector<char> data(len);

  for (auto& c : data) {
    c = static_cast<char>(dist(gen));
  }

  return data;
}
} // namespace

//------------------------------------------------------------------------------
// Standard Adler-32 check value
//------------------------------------------------------------------------------
TEST(Adler, CheckValue)
{
  eos::fst::Adler xs;
  ASSERT_TRUE(xs.Add("Wikipedia", 9, 0));
  xs.Finalize();
  ASSERT_STREQ("11e60398", xs.GetHexChecksum());
}

//------------------------------------------------------------------------------
// The accelerated implementation (ISA-L, when built with it) must match zlib
// for lengths that exercise vector tails and the zlib NMAX boundary
//------------------------------------------------------------------------------
TEST(Adler, MatchesZlibOneShot)
{
  const auto data = RandomBytes(1 << 20);

  for (size_t len : {0ul, 1ul, 15ul, 31ul, 63ul, 64ul, 65ul, 5551ul, 5552ul, 5553ul,
                     65537ul, 1048575ul, 1048576ul}) {
    eos::fst::Adler xs;
    ASSERT_TRUE(xs.Add(data.data(), len, 0));
    xs.Finalize();
    ASSERT_EQ(ZlibAdlerHex(data.data(), len), xs.GetHexChecksum()) << "len=" << len;
  }
}

//------------------------------------------------------------------------------
// Contiguous chunks of uneven size and non-overlapping out-of-order chunks
// both combine to the one-shot value
//------------------------------------------------------------------------------
TEST(Adler, ChunkedMatchesZlib)
{
  const auto data = RandomBytes(3 * 1000003);
  const std::string expected = ZlibAdlerHex(data.data(), data.size());
  const size_t chunk = 1000003;
  eos::fst::Adler in_order;

  for (size_t off = 0; off < data.size(); off += chunk) {
    ASSERT_TRUE(in_order.Add(data.data() + off, chunk, off));
  }

  in_order.Finalize();
  ASSERT_FALSE(in_order.NeedsRecalculation());
  ASSERT_EQ(expected, in_order.GetHexChecksum());
  eos::fst::Adler out_of_order;
  ASSERT_TRUE(out_of_order.Add(data.data() + 2 * chunk, chunk, 2 * chunk));
  ASSERT_TRUE(out_of_order.Add(data.data(), chunk, 0));
  ASSERT_TRUE(out_of_order.Add(data.data() + chunk, chunk, chunk));
  out_of_order.Finalize();
  ASSERT_FALSE(out_of_order.NeedsRecalculation());
  ASSERT_EQ(expected, out_of_order.GetHexChecksum());
}
