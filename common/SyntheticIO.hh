// ----------------------------------------------------------------------
// File: SyntheticIO.hh
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
//!
//! A syntheticio value describes a deterministic, offset-addressable byte stream:
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
//! Disable sentinels "disable", "off", "0", and "false" forbid syntheticio
//! when set as a forced directory or space policy.
//-----------------------------------------------------------------------------
#pragma once

#include "common/Namespace.hh"
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

EOSCOMMONNAMESPACE_BEGIN

inline constexpr std::uint64_t kSyntheticIOBlockBytes = 1ull << 20;

struct SyntheticIOSpec {
  enum class Kind { Xoshiro256pp, Deterministic, Pattern };
  Kind kind = Kind::Xoshiro256pp;
  std::uint64_t seed = 0;
  std::string pattern;
  std::string value;        // canonical text form (as stored/transmitted)
  bool explicit_seed = false;
};

// Map CGI aliases (true/1/on) to a canonical syntheticio value.
std::string normalize_syntheticio_cgi(std::string_view cgi);

// True for disable sentinels (disable/off/0/false).
bool syntheticio_disabled(std::string_view value);

// Parses a syntheticio value; nullopt when malformed or the algorithm is unknown.
std::optional<SyntheticIOSpec> parse_syntheticio(std::string_view value);

// Parse and apply the file inode as seed when an algorithm omits ":<seed>".
std::optional<SyntheticIOSpec> parse_syntheticio_with_seed(std::string_view value,
                                                 std::uint64_t file_id);

// Rebuild the canonical value string from a spec.
std::string syntheticio_canonical_value(const SyntheticIOSpec& spec);

// True for xoshiro256pp, which only allows sequential streaming reads.
bool syntheticio_sequential_only(const SyntheticIOSpec& spec);

// Fills buf with the stream bytes for [offset, offset + len).
void syntheticio_fill(const SyntheticIOSpec& spec, std::uint64_t offset, char* buf,
                 std::size_t len);

// Strong etag for syntheticio objects, derived from the syntheticio value.
std::string syntheticio_etag(std::string_view value);

EOSCOMMONNAMESPACE_END
