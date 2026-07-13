// ----------------------------------------------------------------------
// File: Mirage.hh
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
//! @brief Mirage objects: synthetic data for load and network testing.
//!
//! A mirage value describes a deterministic, offset-addressable byte stream:
//!
//!   "algorithm:xoshiro256pp[:<seed>]"
//!       Pseudo-random bytes from xoshiro256++ (seed defaults to the file
//!       inode when omitted). The stream is generated in independent 1 MiB
//!       blocks. Only sequential streaming reads are allowed.
//!
//!   "algorithm:deterministic[:<seed>]"
//!       Offset-addressable pseudo-random bytes (seed defaults to the file
//!       inode when omitted). Supports random access.
//!
//!   "pattern:<text>"
//!       The pattern repeated forever, anchored at offset 0.
//!
//! CGI aliases "true", "1", and "on" map to "algorithm:deterministic".
//! Disable sentinels "disable", "off", "0", and "false" forbid mirage
//! when set as a forced directory or space policy.
//-----------------------------------------------------------------------------
#pragma once

#include "common/Namespace.hh"
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

EOSCOMMONNAMESPACE_BEGIN

inline constexpr std::uint64_t kMirageBlockBytes = 1ull << 20;

struct MirageSpec {
  enum class Kind { Xoshiro256pp, Deterministic, Pattern };
  Kind kind = Kind::Xoshiro256pp;
  std::uint64_t seed = 0;
  std::string pattern;
  std::string value;        // canonical text form (as stored/transmitted)
  bool explicit_seed = false;
};

// Map CGI aliases (true/1/on) to a canonical mirage value.
std::string normalize_mirage_cgi(std::string_view cgi);

// True for disable sentinels (disable/off/0/false).
bool mirage_disabled(std::string_view value);

// Parses a mirage value; nullopt when malformed or the algorithm is unknown.
std::optional<MirageSpec> parse_mirage(std::string_view value);

// Parse and apply the file inode as seed when an algorithm omits ":<seed>".
std::optional<MirageSpec> parse_mirage_with_seed(std::string_view value,
                                                 std::uint64_t file_id);

// Rebuild the canonical value string from a spec.
std::string mirage_canonical_value(const MirageSpec& spec);

// True for xoshiro256pp, which only allows sequential streaming reads.
bool mirage_sequential_only(const MirageSpec& spec);

// Fills buf with the stream bytes for [offset, offset + len).
void mirage_fill(const MirageSpec& spec, std::uint64_t offset, char* buf,
                 std::size_t len);

// Strong etag for mirage objects, derived from the mirage value.
std::string mirage_etag(std::string_view value);

EOSCOMMONNAMESPACE_END
