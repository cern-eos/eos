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
#include <charconv>
#include <cstdint>
#include <string_view>

#include "common/Namespace.hh"
#include "common/utils/TypeTraits.hh"

EOSCOMMONNAMESPACE_BEGIN

inline bool startsWith(std::string_view str,  std::string_view prefix)
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

inline bool endsWith(std::string_view str, std::string_view suffix)
{
  return (str.size() >= suffix.size()) &&
         (str.compare(str.size() - suffix.size(), suffix.size(), suffix) == 0);
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
static inline std::string joinMap(const std::map<std::string, std::string>& m,
                                  const std::string& delim)
{
  std::ostringstream ss;
  auto it = m.begin();

  while (it != m.end()) {
    ss << it->first << "=" << it->second;
    it++;

    if (it != m.end()) {
      ss << delim;
    }
  }

  return ss.str();
}

static inline std::string stringToHex(const std::string& in,
                                      const char filler = '0', int width = 2, const std::string& delimiter = "")
{
  std::ostringstream ss;
  ss << std::hex << std::setfill(filler);

  for (size_t i = 0; in.length() > i; ++i) {
    ss << std::setw(width > 0 ? width : 2) << static_cast<unsigned int>
       (static_cast<unsigned char>(in[i])) << delimiter;
  }

  return ss.str();
}

static inline std::string hexToString(const std::string& in)
{
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

//----------------------------------------------------------------------------
//! Get a Numeric Value from String supports all unsigned + integer types,
//! can set a default value on failure if supplied.
//! Also fills a string for log_msg if supplied, On Linux this is usually
//! faster than atoi and friends, no allocation is promised in case the StrT
//! doesn't allocate, the template arguments are inferred based on the given
//! value of value. Unfortunately gcc < 11 does not support floating point
//! conversions just yet, to save from long template compilation failures we
//! SFINAE for this and fallback to stod and friends for float, this can be
//! removed once we move to gcc11 everywhere, where std::from_chars natively
//! does the right thing and also does some neat optimisations
//!
//! @tparam StrT A string like container type, std::string/string_view or
//!              a container of chars will also work, however const char* will
//!              not as we need a start and end position (use operator ""s/sv for
//!              arguments in case you're doing strings at compile time)
//! @tparam NumT The numeric type, will be inferred from the given arguments
//! @param key the string key which needs to be converted to a number
//! @param value the value that will be filled on conversion
//! @param default_val a default in case conversion fails (Set to Integer Default)
//! @param log_msg a string log msg that can be later used for logging
//!
//! @return bool indicating successful conversion
//----------------------------------------------------------------------------
template <typename StrT, typename NumT>
auto StringToNumeric(const StrT& key, NumT& value,
                     NumT default_val = {},
                     std::string* log_msg = nullptr) noexcept
-> std::enable_if_t<detail::is_charconv_numeric_v<NumT>, bool> {
  NumT result;

  static_assert(detail::has_data_t<StrT>::value,
  "StringToNumeric requires a string like container with data(),"
  "consider wrapping a string_view or operator sv for string literals");

  auto ret = std::from_chars(key.data(), key.data() + key.size(), result);

  if (ret.ec != std::errc())
  {
    value = default_val;

    if (log_msg != nullptr) {
      auto _ec  = std::make_error_condition(ret.ec);
      // Obligatory gripe about the std; since we can not concat str_view + str
      // doing it this way so that it will work for any str like types
      log_msg->append("\"msg=Failed Numeric conversion\" key=");
      log_msg->append(key);
      log_msg->append(" error_msg=");
      log_msg->append(_ec.message());
    }

    return false;
  }

  value = result;
  return true;
}

#if __cpp_lib_to_chars < 201611
// A floating point version of StringToNumeric, that iterates through
// the various stod and friends depending on the type supplied
// Currently the str overload will only work for const str& type
// or anything convertible to std::string that stod understands
// TODO: Remove this whenever we update to gcc11!!
template <typename StrT, typename NumT>
auto StringToNumeric(const StrT& key, NumT& value,
                     NumT default_val = {},
                     std::string* log_msg = nullptr) noexcept
-> std::enable_if_t<std::is_floating_point_v<NumT>, bool> {
  // Not super nice, but gets the job done, a lazy way to iterate through
  // the different fp types, since the evaluation hapens at compile time
  // only the relevant branch will be compiled.
  try {
    if constexpr(std::is_same_v<NumT, float>)
    {
      value = std::stof(key);
    } else if constexpr(std::is_same_v<NumT, double>)
    {
      value = std::stod(key);
    } else if constexpr(std::is_same_v<NumT, long double>)
    {
      value = std::stold(key);
    }
  } catch (std::exception& ec)
  {
    value = default_val;

    if (log_msg != nullptr) {
      // Slightly tweak the error message, can be used in tests to identify
      // that no silly float -> int conversion took place and this function
      // was only selected for floats
      log_msg->append("\"msg=Failed float conversion\" key=");
      log_msg->append(key);
      log_msg->append(" error_msg=");
      log_msg->append(ec.what());
    }

    return false;
  }

  return true;
}

#endif

EOSCOMMONNAMESPACE_END
