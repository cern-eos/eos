// ----------------------------------------------------------------------
// File: SyntheticIO.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

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

//-----------------------------------------------------------------------------
//! @brief SyntheticIO objects: synthetic data for load and network testing.
//-----------------------------------------------------------------------------
#include "common/SyntheticIO.hh"
#include "common/FileId.hh"

#include <algorithm>
#include <charconv>
#include <cstring>

EOSCOMMONNAMESPACE_BEGIN

namespace {

std::uint64_t splitmix64(std::uint64_t& x) {
  x += 0x9E3779B97F4A7C15ull;
  std::uint64_t z = x;
  z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ull;
  z = (z ^ (z >> 27)) * 0x94D049BB133111EBull;
  return z ^ (z >> 31);
}

std::uint64_t deterministic_random64(std::uint64_t seed, std::uint64_t block) {
  std::uint64_t z = seed + block * 0x9E3779B97F4A7C15ull;
  z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ull;
  z = (z ^ (z >> 27)) * 0x94D049BB133111EBull;
  return z ^ (z >> 31);
}

// xoshiro256++ (Blackman & Vigna), state seeded via splitmix64.
struct Xoshiro256pp {
  std::uint64_t s[4];

  explicit Xoshiro256pp(std::uint64_t seed) {
    for (auto& v : s) {
      v = splitmix64(seed);
    }
  }

  static std::uint64_t rotl(std::uint64_t v, int k) {
    return (v << k) | (v >> (64 - k));
  }

  std::uint64_t next() {
    const std::uint64_t result = rotl(s[0] + s[3], 23) + s[0];
    const std::uint64_t t = s[1] << 17;
    s[2] ^= s[0];
    s[3] ^= s[1];
    s[1] ^= s[2];
    s[0] ^= s[3];
    s[2] ^= t;
    s[3] = rotl(s[3], 45);
    return result;
  }
};

void fill_xoshiro_block(std::uint64_t seed, std::uint64_t block,
                        std::uint64_t in_block, char* buf, std::size_t len) {
  Xoshiro256pp rng(seed + block);
  for (std::uint64_t i = 0; i < in_block / 8; ++i) {
    rng.next();
  }
  std::uint64_t word_off = in_block % 8;
  while (len > 0) {
    std::uint64_t w = rng.next();
    unsigned char bytes[8];
    std::memcpy(bytes, &w, 8);
    std::size_t take = static_cast<std::size_t>(
        std::min<std::uint64_t>(8 - word_off, len));
    std::memcpy(buf, bytes + word_off, take);
    buf += take;
    len -= take;
    word_off = 0;
  }
}

void fill_deterministic(std::uint64_t seed, std::uint64_t offset, char* buf,
                        std::size_t len) {
  std::uint64_t block = offset / 8;
  std::size_t skip = static_cast<std::size_t>(offset % 8);
  while (len > 0) {
    std::uint64_t value = deterministic_random64(seed, block++);
    unsigned char bytes[8];
    std::memcpy(bytes, &value, 8);
    std::size_t take = std::min(len, 8 - skip);
    std::memcpy(buf, bytes + skip, take);
    buf += take;
    len -= take;
    skip = 0;
  }
}

} // namespace

std::string normalize_syntheticio_cgi(std::string_view cgi) {
  if (cgi == "true" || cgi == "1" || cgi == "on") {
    return "algorithm:deterministic";
  }
  return std::string(cgi);
}

bool syntheticio_disabled(std::string_view value) {
  return value == "disable" || value == "off" || value == "0" ||
         value == "false";
}

std::optional<SyntheticIOSpec> parse_syntheticio(std::string_view value) {
  SyntheticIOSpec spec;
  spec.value = std::string(value);

  constexpr std::string_view kAlg = "algorithm:";
  constexpr std::string_view kPat = "pattern:";
  if (value.substr(0, kAlg.size()) == kAlg) {
    std::string_view rest = value.substr(kAlg.size());
    std::string_view name = rest.substr(0, rest.find(':'));
    if (name == "xoshiro256pp") {
      spec.kind = SyntheticIOSpec::Kind::Xoshiro256pp;
    } else if (name == "deterministic") {
      spec.kind = SyntheticIOSpec::Kind::Deterministic;
    } else {
      return std::nullopt;
    }
    if (rest.size() > name.size()) {
      std::string_view s = rest.substr(name.size() + 1);
      auto [p, ec] =
          std::from_chars(s.data(), s.data() + s.size(), spec.seed);
      if (ec != std::errc{} || p != s.data() + s.size()) {
        return std::nullopt;
      }
      spec.explicit_seed = true;
    }
    return spec;
  }
  if (value.substr(0, kPat.size()) == kPat) {
    spec.kind = SyntheticIOSpec::Kind::Pattern;
    spec.pattern = std::string(value.substr(kPat.size()));
    if (spec.pattern.empty()) {
      return std::nullopt;
    }
    return spec;
  }
  return std::nullopt;
}

std::string syntheticio_canonical_value(const SyntheticIOSpec& spec) {
  switch (spec.kind) {
  case SyntheticIOSpec::Kind::Pattern:
    return "pattern:" + spec.pattern;
  case SyntheticIOSpec::Kind::Xoshiro256pp:
    return "algorithm:xoshiro256pp:" + std::to_string(spec.seed);
  case SyntheticIOSpec::Kind::Deterministic:
    return "algorithm:deterministic:" + std::to_string(spec.seed);
  }
  return spec.value;
}

std::optional<SyntheticIOSpec> parse_syntheticio_with_seed(std::string_view value,
                                                 std::uint64_t file_id) {
  auto spec = parse_syntheticio(value);
  if (!spec) {
    return std::nullopt;
  }
  if ((spec->kind == SyntheticIOSpec::Kind::Xoshiro256pp ||
       spec->kind == SyntheticIOSpec::Kind::Deterministic) &&
      !spec->explicit_seed) {
    spec->seed = FileId::FidToInode(file_id);
    spec->value = syntheticio_canonical_value(*spec);
  }
  return spec;
}

bool syntheticio_sequential_only(const SyntheticIOSpec& spec) {
  return spec.kind == SyntheticIOSpec::Kind::Xoshiro256pp;
}

std::string syntheticio_etag(std::string_view value) {
  std::string clean;
  for (char c : value) {
    if (c != '"') {
      clean += c;
    }
  }
  return "\"syntheticio:" + clean + "\"";
}

void syntheticio_fill(const SyntheticIOSpec& spec, std::uint64_t offset, char* buf,
                 std::size_t len) {
  if (spec.kind == SyntheticIOSpec::Kind::Pattern) {
    const std::string& p = spec.pattern;
    for (std::size_t i = 0; i < len; ++i) {
      buf[i] = p[static_cast<std::size_t>((offset + i) % p.size())];
    }
    return;
  }
  if (spec.kind == SyntheticIOSpec::Kind::Deterministic) {
    fill_deterministic(spec.seed, offset, buf, len);
    return;
  }
  while (len > 0) {
    std::uint64_t block = offset / kSyntheticIOBlockBytes;
    std::uint64_t in_block = offset % kSyntheticIOBlockBytes;
    std::size_t take = static_cast<std::size_t>(
        std::min<std::uint64_t>(kSyntheticIOBlockBytes - in_block, len));
    fill_xoshiro_block(spec.seed, block, in_block, buf, take);
    buf += take;
    offset += take;
    len -= take;
  }
}

EOSCOMMONNAMESPACE_END
