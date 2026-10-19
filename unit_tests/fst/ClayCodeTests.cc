//------------------------------------------------------------------------------
// File: ClayCodeTests.cc
// Author: Andreas-Joachim Peters - CERN
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

#include "fst/layout/clay/ClayCode.hh"
#include "gtest/gtest.h"
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <set>
#include <vector>

namespace
{

struct Buffer {
  explicit Buffer(size_t n) : bytes(n)
  {
    if (posix_memalign(&raw, 64, n) != 0) {
      raw = nullptr;
      return;
    }

    memset(raw, 0, n);
  }

  ~Buffer()
  {
    free(raw);
  }

  Buffer(const Buffer&) = delete;
  Buffer& operator=(const Buffer&) = delete;

  char* data()
  {
    return static_cast<char*>(raw);
  }

  void* raw;
  size_t bytes;
};

void
Fill(std::vector<std::unique_ptr<Buffer>>& chunks, int data_count,
     int chunk_size, uint32_t seed)
{
  uint32_t x = seed;

  for (int s = 0; s < data_count; ++s) {
    char* p = chunks[static_cast<size_t>(s)]->data();

    for (int i = 0; i < chunk_size; ++i) {
      x = x * 1664525u + 1013904223u;
      p[i] = static_cast<char>(x >> 24);
    }
  }
}

std::vector<char*>
Ptrs(std::vector<std::unique_ptr<Buffer>>& chunks)
{
  std::vector<char*> out;

  for (auto& chunk : chunks) {
    out.push_back(chunk->data());
  }

  return out;
}

std::vector<char>
Pack(const char* chunk, const std::vector<eos::fst::ClayCode::Range>& ranges,
     int sc)
{
  std::vector<char> packed;

  for (const auto& range : ranges) {
    const char* begin = chunk + static_cast<size_t>(range.index) * sc;
    const char* end = begin + static_cast<size_t>(range.count) * sc;
    packed.insert(packed.end(), begin, end);
  }

  return packed;
}

void
RoundTrip(eos::fst::ClayCode::Profile profile, int chunk_size)
{
  eos::fst::ClayCode code(profile);
  const int n = code.StripeCount();
  const int k = code.DataCount();
  const int m = code.ParityCount();
  ASSERT_GT(chunk_size, 0);
  ASSERT_EQ(chunk_size % code.SubChunkCount(), 0);
  std::vector<std::unique_ptr<Buffer>> chunks;

  for (int i = 0; i < n; ++i) {
    chunks.emplace_back(new Buffer(static_cast<size_t>(chunk_size)));
    ASSERT_NE(chunks.back()->data(), nullptr);
  }

  Fill(chunks, k, chunk_size, 0xC1A70001u);
  auto ptrs = Ptrs(chunks);
  ASSERT_EQ(code.Encode(ptrs.data(), chunk_size), 0);

  bool parity_nonzero = false;

  for (int s = k; s < n; ++s) {
    for (int i = 0; i < chunk_size; ++i) {
      if (chunks[static_cast<size_t>(s)]->data()[i] != 0) {
        parity_nonzero = true;
      }
    }
  }

  ASSERT_TRUE(parity_nonzero);
  std::vector<std::unique_ptr<Buffer>> original;

  for (int i = 0; i < n; ++i) {
    original.emplace_back(new Buffer(static_cast<size_t>(chunk_size)));
    memcpy(original.back()->data(), chunks[static_cast<size_t>(i)]->data(),
           static_cast<size_t>(chunk_size));
  }

  // Every single-stripe loss, full decode and bandwidth-reduced repair.
  for (int lost = 0; lost < n; ++lost) {
    memcpy(chunks[static_cast<size_t>(lost)]->data(),
           original[static_cast<size_t>(lost)]->data(),
           static_cast<size_t>(chunk_size));
    memset(chunks[static_cast<size_t>(lost)]->data(), 0x5a,
           static_cast<size_t>(chunk_size));
    std::set<int> erased;
    erased.insert(lost);
    ptrs = Ptrs(chunks);
    ASSERT_EQ(code.Decode(erased, ptrs.data(), chunk_size), 0) << "lost=" << lost;
    EXPECT_EQ(memcmp(chunks[static_cast<size_t>(lost)]->data(),
                     original[static_cast<size_t>(lost)]->data(),
                     static_cast<size_t>(chunk_size)),
              0) << "decode lost=" << lost;

    const int repair_bytes = code.RepairBytes(chunk_size);
    ASSERT_EQ(repair_bytes, chunk_size / code.CouplingFactor());
    auto helpers = code.RepairHelpers(lost);
    ASSERT_EQ(static_cast<int>(helpers.size()), code.HelperCount());
    auto ranges = code.RepairRanges(lost);
    int covered = 0;

    for (const auto& range : ranges) {
      covered += range.count;
    }

    EXPECT_EQ(covered, code.SubChunkCount() / code.CouplingFactor());
    std::vector<std::vector<char>> packed;
    std::vector<char*> packed_ptrs;

    for (int helper : helpers) {
      EXPECT_NE(helper, lost);
      packed.push_back(Pack(original[static_cast<size_t>(helper)]->data(),
                            ranges, chunk_size / code.SubChunkCount()));
      ASSERT_EQ(static_cast<int>(packed.back().size()), repair_bytes);
    }

    for (auto& buf : packed) {
      packed_ptrs.push_back(buf.data());
    }

    Buffer recovered(static_cast<size_t>(chunk_size));
    ASSERT_EQ(code.Repair(lost, helpers, packed_ptrs.data(), recovered.data(),
                          chunk_size),
              0) << "repair lost=" << lost;
    EXPECT_EQ(memcmp(recovered.data(), original[static_cast<size_t>(lost)]->data(),
                     static_cast<size_t>(chunk_size)),
              0) << "repair lost=" << lost;

    for (int i = 0; i < n; ++i) {
      memcpy(chunks[static_cast<size_t>(i)]->data(),
             original[static_cast<size_t>(i)]->data(),
             static_cast<size_t>(chunk_size));
    }
  }

  // Two losses, full decode only. A third loss must fail.
  {
    std::set<int> erased;
    erased.insert(0);
    erased.insert(n - 1);
    memset(chunks[0]->data(), 0, static_cast<size_t>(chunk_size));
    memset(chunks[static_cast<size_t>(n - 1)]->data(), 0,
           static_cast<size_t>(chunk_size));
    ptrs = Ptrs(chunks);
    ASSERT_EQ(code.Decode(erased, ptrs.data(), chunk_size), 0);
    EXPECT_EQ(memcmp(chunks[0]->data(), original[0]->data(),
                     static_cast<size_t>(chunk_size)), 0);
    EXPECT_EQ(memcmp(chunks[static_cast<size_t>(n - 1)]->data(),
                     original[static_cast<size_t>(n - 1)]->data(),
                     static_cast<size_t>(chunk_size)), 0);
  }

  {
    std::set<int> erased;

    for (int i = 0; i < m + 1; ++i) {
      erased.insert(i);
      memset(chunks[static_cast<size_t>(i)]->data(), 0,
             static_cast<size_t>(chunk_size));
    }

    ptrs = Ptrs(chunks);
    EXPECT_NE(code.Decode(erased, ptrs.data(), chunk_size), 0);
  }
}

}  // namespace

TEST(ClayCode, Profiles)
{
  eos::fst::ClayCode clay10(eos::fst::ClayCode::Profile::Clay10);
  EXPECT_EQ(clay10.DataCount(), 10);
  EXPECT_EQ(clay10.ParityCount(), 2);
  EXPECT_EQ(clay10.StripeCount(), 12);
  EXPECT_EQ(clay10.HelperCount(), 11);
  EXPECT_EQ(clay10.CouplingFactor(), 2);
  EXPECT_EQ(clay10.SubChunkCount(), 64);

  eos::fst::ClayCode clay15(eos::fst::ClayCode::Profile::Clay15);
  EXPECT_EQ(clay15.DataCount(), 15);
  EXPECT_EQ(clay15.ParityCount(), 3);
  EXPECT_EQ(clay15.StripeCount(), 18);
  EXPECT_EQ(clay15.HelperCount(), 16);
  EXPECT_EQ(clay15.CouplingFactor(), 2);
  EXPECT_EQ(clay15.SubChunkCount(), 512);
}

TEST(ClayCode, Clay10RoundTrip)
{
  // 64 sub-chunks of 64 bytes.
  RoundTrip(eos::fst::ClayCode::Profile::Clay10, 64 * 64);
}

TEST(ClayCode, Clay10LargeSubChunk)
{
  // Same profile with 4 KiB sub-chunks, the layout minimum.
  RoundTrip(eos::fst::ClayCode::Profile::Clay10, 64 * 4096);
}

TEST(ClayCode, Clay15RoundTrip)
{
  // 15+3. 512 sub-chunks of 64 bytes.
  RoundTrip(eos::fst::ClayCode::Profile::Clay15, 512 * 64);
}
