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
#include "common/Namespace.hh"
#include <string>
#include <algorithm>

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

EOSCOMMONNAMESPACE_END
