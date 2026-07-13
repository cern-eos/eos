//------------------------------------------------------------------------------
// File: MirageTests.cc
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

#include "common/FileId.hh"
#include "common/Mirage.hh"
#include "gtest/gtest.h"

#include <cstring>
#include <string>
#include <vector>

using eos::common::MirageSpec;
using eos::common::mirage_canonical_value;
using eos::common::mirage_disabled;
using eos::common::mirage_etag;
using eos::common::mirage_fill;
using eos::common::mirage_sequential_only;
using eos::common::normalize_mirage_cgi;
using eos::common::parse_mirage;
using eos::common::parse_mirage_with_seed;

namespace {

std::uint64_t reference_deterministic_random64(std::uint64_t seed,
                                               std::uint64_t block) {
  std::uint64_t z = seed + block * 0x9E3779B97F4A7C15ull;
  z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ull;
  z = (z ^ (z >> 27)) * 0x94D049BB133111EBull;
  return z ^ (z >> 31);
}

void reference_deterministic_read(std::uint64_t seed, std::uint64_t offset,
                                  void* dst, std::size_t count) {
  auto* out = static_cast<std::uint8_t*>(dst);
  std::uint64_t block = offset / 8;
  std::size_t skip = static_cast<std::size_t>(offset % 8);

  while (count > 0) {
    std::uint64_t value = reference_deterministic_random64(seed, block++);
    std::uint8_t bytes[8];
    std::memcpy(bytes, &value, 8);
    std::size_t n = std::min(count, 8 - skip);
    std::memcpy(out, bytes + skip, n);
    out += n;
    count -= n;
    skip = 0;
  }
}

} // namespace

TEST(Mirage, NormalizeCgiAliases) {
  EXPECT_EQ(normalize_mirage_cgi("true"), "algorithm:deterministic");
  EXPECT_EQ(normalize_mirage_cgi("1"), "algorithm:deterministic");
  EXPECT_EQ(normalize_mirage_cgi("on"), "algorithm:deterministic");
  EXPECT_EQ(normalize_mirage_cgi("pattern:abc"), "pattern:abc");
}

TEST(Mirage, DisableSentinels) {
  for (const char* value : {"disable", "off", "0", "false"}) {
    EXPECT_TRUE(mirage_disabled(value)) << value;
    EXPECT_FALSE(parse_mirage(value).has_value()) << value;
  }
  EXPECT_FALSE(mirage_disabled("algorithm:deterministic"));
  EXPECT_FALSE(mirage_disabled("pattern:abc"));
  EXPECT_FALSE(mirage_disabled(""));
}

TEST(Mirage, ParsePattern) {
  auto spec = parse_mirage("pattern:1234567890");
  ASSERT_TRUE(spec.has_value());
  EXPECT_EQ(spec->kind, MirageSpec::Kind::Pattern);
  EXPECT_EQ(spec->pattern, "1234567890");
  EXPECT_FALSE(spec->explicit_seed);
}

TEST(Mirage, ParseAlgorithms) {
  auto xoshiro = parse_mirage("algorithm:xoshiro256pp:42");
  ASSERT_TRUE(xoshiro.has_value());
  EXPECT_EQ(xoshiro->kind, MirageSpec::Kind::Xoshiro256pp);
  EXPECT_EQ(xoshiro->seed, 42u);
  EXPECT_TRUE(xoshiro->explicit_seed);

  auto deterministic = parse_mirage("algorithm:deterministic:7");
  ASSERT_TRUE(deterministic.has_value());
  EXPECT_EQ(deterministic->kind, MirageSpec::Kind::Deterministic);
  EXPECT_EQ(deterministic->seed, 7u);
  EXPECT_TRUE(deterministic->explicit_seed);

  auto no_seed = parse_mirage("algorithm:xoshiro256pp");
  ASSERT_TRUE(no_seed.has_value());
  EXPECT_EQ(no_seed->seed, 0u);
  EXPECT_FALSE(no_seed->explicit_seed);
}

TEST(Mirage, ParseRejectsInvalidValues) {
  EXPECT_FALSE(parse_mirage("").has_value());
  EXPECT_FALSE(parse_mirage("algorithm:unknown").has_value());
  EXPECT_FALSE(parse_mirage("pattern:").has_value());
  EXPECT_FALSE(parse_mirage("algorithm:xoshiro256pp:abc").has_value());
}

TEST(Mirage, ParseWithSeedUsesFileInode) {
  constexpr std::uint64_t file_id = 0x1234ull;
  const std::uint64_t inode = eos::common::FileId::FidToInode(file_id);

  auto spec = parse_mirage_with_seed("algorithm:deterministic", file_id);
  ASSERT_TRUE(spec.has_value());
  EXPECT_EQ(spec->seed, inode);
  EXPECT_EQ(spec->value, "algorithm:deterministic:" + std::to_string(inode));

  auto xoshiro = parse_mirage_with_seed("algorithm:xoshiro256pp:99", file_id);
  ASSERT_TRUE(xoshiro.has_value());
  EXPECT_EQ(xoshiro->seed, 99u);
  EXPECT_EQ(xoshiro->value, "algorithm:xoshiro256pp:99");
}

TEST(Mirage, CanonicalValueAndEtag) {
  MirageSpec pattern;
  pattern.kind = MirageSpec::Kind::Pattern;
  pattern.pattern = "xyz";
  EXPECT_EQ(mirage_canonical_value(pattern), "pattern:xyz");
  EXPECT_EQ(mirage_etag("pattern:xyz"), "\"mirage:pattern:xyz\"");

  MirageSpec deterministic;
  deterministic.kind = MirageSpec::Kind::Deterministic;
  deterministic.seed = 5;
  EXPECT_EQ(mirage_canonical_value(deterministic),
            "algorithm:deterministic:5");
}

TEST(Mirage, SequentialOnlyFlag) {
  MirageSpec xoshiro;
  xoshiro.kind = MirageSpec::Kind::Xoshiro256pp;
  MirageSpec deterministic;
  deterministic.kind = MirageSpec::Kind::Deterministic;
  MirageSpec pattern;
  pattern.kind = MirageSpec::Kind::Pattern;

  EXPECT_TRUE(mirage_sequential_only(xoshiro));
  EXPECT_FALSE(mirage_sequential_only(deterministic));
  EXPECT_FALSE(mirage_sequential_only(pattern));
}

TEST(Mirage, PatternFillRepeats) {
  MirageSpec spec;
  spec.kind = MirageSpec::Kind::Pattern;
  spec.pattern = "ab";

  char buf[7] = {};
  mirage_fill(spec, 3, buf, sizeof(buf));
  EXPECT_EQ(std::string(buf, sizeof(buf)), "bababab");
}

TEST(Mirage, DeterministicFillMatchesReference) {
  MirageSpec spec;
  spec.kind = MirageSpec::Kind::Deterministic;
  spec.seed = 42;

  for (std::uint64_t offset : {0u, 1u, 7u, 8u, 123u, 4096u}) {
    char expected[32] = {};
    char actual[32] = {};
    reference_deterministic_read(spec.seed, offset, expected, sizeof(expected));
    mirage_fill(spec, offset, actual, sizeof(actual));
    EXPECT_EQ(std::memcmp(expected, actual, sizeof(expected)), 0)
        << "offset=" << offset;
  }
}

TEST(Mirage, XoshiroFillIsStablePerOffset) {
  auto spec = parse_mirage("algorithm:xoshiro256pp:99");
  ASSERT_TRUE(spec.has_value());

  char first[64] = {};
  char second[64] = {};
  mirage_fill(*spec, 0, first, sizeof(first));
  mirage_fill(*spec, 0, second, sizeof(second));
  EXPECT_EQ(std::memcmp(first, second, sizeof(first)), 0);

  char span_a[16] = {};
  char span_b[16] = {};
  mirage_fill(*spec, 100, span_a, sizeof(span_a));
  mirage_fill(*spec, 100, span_b, sizeof(span_b));
  EXPECT_EQ(std::memcmp(span_a, span_b, sizeof(span_b)), 0);
}

TEST(Mirage, NormalizeCgiWithSeedResolution) {
  constexpr std::uint64_t file_id = 0xdeadbeefull;
  const std::uint64_t inode = eos::common::FileId::FidToInode(file_id);

  auto spec = parse_mirage_with_seed(normalize_mirage_cgi("true"), file_id);
  ASSERT_TRUE(spec.has_value());
  EXPECT_EQ(spec->kind, MirageSpec::Kind::Deterministic);
  EXPECT_EQ(spec->seed, inode);
}

TEST(Mirage, ForcedPolicyValuesParseWithoutSeed) {
  for (const char* value : {"algorithm:deterministic",
                            "algorithm:xoshiro256pp",
                            "pattern:loadtest"}) {
    auto spec = parse_mirage(value);
    ASSERT_TRUE(spec.has_value()) << value;
    EXPECT_FALSE(spec->explicit_seed) << value;
  }
}
