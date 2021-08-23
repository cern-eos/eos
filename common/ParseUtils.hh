// ----------------------------------------------------------------------
// File: ParseUtils.hh
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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

//------------------------------------------------------------------------------
//! @author Georgios Bitzes <georgios.bitzes@cern.ch>
//! @brief Parse utilities with proper error checking
//------------------------------------------------------------------------------

#pragma once
#include "common/Namespace.hh"
#include "common/Logging.hh"
#include "common/StringTokenizer.hh"
#include <string>
#include <limits>

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Parse an int64 encoded in the given numerical base, return true if parsing
//! was successful, false otherwise.
//------------------------------------------------------------------------------
inline bool ParseInt64(const std::string& str, int64_t& ret, int base = 10)
{
  char* endptr = NULL;
  ret = std::strtoll(str.c_str(), &endptr, base);

  if (endptr != str.c_str() + str.size() ||
      ret == std::numeric_limits<long long>::min() ||
      ret == std::numeric_limits<long long>::max()) {
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
//! Parse an uint64 encoded in the given numerical base, return true if parsing
//! was successful, false otherwise.
//------------------------------------------------------------------------------
inline bool ParseUInt64(const std::string& str, uint64_t& ret, int base = 10)
{
  char* endptr = NULL;
  ret = std::strtoull(str.c_str(), &endptr, base);

  if (endptr != str.c_str() + str.size() ||
      ret == std::numeric_limits<unsigned long long>::min() ||
      ret == std::numeric_limits<unsigned long long>::max()) {
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
//! Parse a long long - behave exactly the same as old XrdMq "GetLongLong".
//------------------------------------------------------------------------------
inline long long ParseLongLong(const std::string& str)
{
  if (str.length()) {
    errno = 0;
    long long ret = strtoll(str.c_str(), 0, 10);

    if (!errno) {
      return ret;
    }
  }

  return 0;
}

//------------------------------------------------------------------------------
//! Parse a long long - behave exactly the same as old XrdMq "GetDouble".
//------------------------------------------------------------------------------
inline double ParseDouble(const std::string& str)
{
  if (str.length()) {
    return atof(str.c_str());
  }

  return 0;
}

//------------------------------------------------------------------------------
//! Parse hostname and port from input string. If no port value present then
//! default to 1094.
//!
//! @param input input string e.g. hostname.cern.ch:port
//! @param host parsed hostname
//! @parma port parsed port
//!
//! @return true if parsing succeeded, otherwise false
//------------------------------------------------------------------------------
inline bool ParseHostNamePort(const std::string& input, std::string& host,
                              int& port)
{
  if (input.empty()) {
    return false;
  }

  size_t pos = input.find(':');

  if ((pos == std::string::npos) || (pos == input.length())) {
    host = input;
    port = 1094;
  } else {
    host = input.substr(0, pos);
    int64_t ret = 0ll;

    if (!ParseInt64(input.substr(pos + 1), ret)) {
      return false;
    }

    port = ret;
  }

  return true;
}


//------------------------------------------------------------------------------
//! Check that the given string is a valid hostname or IP specification
//!
//! @param input hostname host.cern.ch or IP address
//------------------------------------------------------------------------------
inline bool ValidHostnameOrIP(const std::string& input)
{
  for (const auto& c : input) {
    if (!std::isalnum(c) && (c != '.') && (c != '-') && (c != ':')) {
      return false;
    }
  }

  return true;
}


//----------------------------------------------------------------------------
//! Sanitize input geotag
//!
//! @param geotag input value
//!
//! @return emptystring if geotag not valid, otherwise sanitized geotag
//----------------------------------------------------------------------------
inline std::string SanitizeGeoTag(const std::string& geotag)
{
  if (geotag.empty()) {
    return std::string();
  }

  std::string tmp_tag(geotag);
  // Make sure the new geotag is properly formatted and respects the contraints
  auto tokens = eos::common::StringTokenizer::split<std::vector<std::string>>
                (tmp_tag, ':');
  tmp_tag.clear();

  for (const auto& token : tokens) {
    if (token.empty()) {
      continue;
    }

    if (token.length() > 8) {
      eos_static_err("msg=\"token in geotag longer than 8 chars\" geotag=\"%s\"",
                     geotag.c_str());
      return std::string();
    }

    tmp_tag += token;
    tmp_tag += "::";
  }

  if (tmp_tag.length() <= 2) {
    eos_static_err("%s", "msg=\"empty geotag\"");
    return std::string();
  }

  tmp_tag.erase(tmp_tag.length() - 2);

  if (tmp_tag != geotag) {
    return std::string();
  }

  return tmp_tag;
}

EOSCOMMONNAMESPACE_END
