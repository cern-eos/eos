// ----------------------------------------------------------------------
// File: StringUtils.hh
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

#pragma once

#include <algorithm>
#include <iomanip>
#include <map>
#include <sstream>
#include <string>

#include "common/Namespace.hh"

EOSCOMMONNAMESPACE_BEGIN

inline bool startsWith(const std::string& str, const std::string& prefix)
{
  if (prefix.size() > str.size()) {
    return false;
  }

  for (size_t i = 0; i < prefix.size(); i++) {
    if (str[i] != prefix[i]) {
      return false;
    }
  }

  return true;
}

//------------------------------------------------------------------------------
//! Left trim string in-place
//------------------------------------------------------------------------------
static inline void ltrim(std::string& s)
{
  s.erase(s.begin(), std::find_if(s.begin(), s.end(), [](int ch) {
    return !std::isspace(ch);
  }));
}

//------------------------------------------------------------------------------
//! Rigth trim string in-place
//------------------------------------------------------------------------------
static inline void rtrim(std::string& s)
{
  s.erase(std::find_if(s.rbegin(), s.rend(), [](int ch) {
    return !std::isspace(ch);
  }).base(), s.end());
}

//------------------------------------------------------------------------------
//! Trim from both ends in-place
//------------------------------------------------------------------------------
static inline void trim(std::string& s)
{
  ltrim(s);
  rtrim(s);
}

//------------------------------------------------------------------------------
//! Bool to string
//------------------------------------------------------------------------------
static inline std::string boolToString(bool b)
{
  if (b) {
    return "true";
  }

  return "false";
}

//------------------------------------------------------------------------------
//! Join map
//------------------------------------------------------------------------------
static inline std::string joinMap(const std::map<std::string, std::string> &m, const std::string &delim) {
  std::ostringstream ss;

  auto it = m.begin();
  while(it != m.end()) {
    ss << it->first << "=" << it->second;

    it++;
    if(it != m.end()) {
      ss << delim;
    }
  }

  return ss.str();
}

static inline std::string stringToHex(const std::string& in, const char filler = '0', int width = 2, const std::string& delimiter = "") {
  std::ostringstream ss;
  ss << std::hex << std::setfill(filler);
  for (size_t i = 0; in.length() > i; ++i) {
    ss << std::setw(width > 0 ? width : 2) << static_cast<unsigned int>(static_cast<unsigned char>(in[i])) << delimiter;
  }
  return ss.str();
}

static inline std::string hexToString(const std::string& in) {
  std::string output;

  // The caller must be aware and check the pair ( output, in.lenght() )
  if ((in.length() % 2) != 0) {
    return "";
  }

  size_t cnt = in.length() / 2;

  for (size_t i = 0; cnt > i; ++i) {
    uint32_t s = 0;
    std::stringstream ss;
    ss << std::hex << in.substr(i * 2, 2);
    ss >> s;
    output.push_back(static_cast<unsigned char>(s));
  }

  return output;
}

EOSCOMMONNAMESPACE_END
