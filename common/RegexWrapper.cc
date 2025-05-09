//------------------------------------------------------------------------------
// File: RegexWrapper.cc
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2025 CERN/Switzerland                                  *
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

#include "common/RegexWrapper.hh"
#include "common/Logging.hh"
#include <mutex>
#include <iostream>
#include <regex>

namespace
{
std::mutex sRegexMutex;

//------------------------------------------------------------------------------
//! Wrapper for std::regex_match taking regular expression as std::regex
//!
//! @param input input string
//! @param regex regular expression given as std::regex
//!
//! @return true if there is a full match, otherwise false
//------------------------------------------------------------------------------
bool eos_regex_match(const std::string& input, const std::regex& regex)
{
  std::unique_lock<std::mutex> lock(sRegexMutex);
  return std::regex_match(input, regex);
}

//------------------------------------------------------------------------------
//! Wrapper for std::regex_search taking regular expression as std::regex
//!
//! @param input input string
//! @param regex regular expression given as std::regex
//!
//! @return true if there is any match, otherwise false
//------------------------------------------------------------------------------
bool eos_regex_search(const std::string& input, const std::regex& regex)
{
  std::unique_lock<std::mutex> lock(sRegexMutex);
  return std::regex_search(input, regex);
}
}


EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Wrapper for std::regex_match taking regular expression as std::string
//------------------------------------------------------------------------------
bool eos_regex_match(const std::string& input, const std::string& regex)
{
  try {
    std::regex re(regex);
    return ::eos_regex_match(input, re);
  } catch (std::regex_error& e) {
    std::cerr << "error: invalid regex : " << e.what() << " : "
              << "CODE IS: " <<  e.code() << std::endl;
    return false;
  }
}

//------------------------------------------------------------------------------
// Wrapper for std::regex_search taking regular expression as std::string
//------------------------------------------------------------------------------
bool eos_regex_search(const std::string& input, const std::string& regex)
{
  try {
    std::regex re(regex);
    return ::eos_regex_search(input, re);
  } catch (std::regex_error& e) {
    std::cerr << "error: invalid regex : " << e.what() << " : "
              << "CODE IS: " <<  e.code() << std::endl;
    return false;
  }
}

//------------------------------------------------------------------------------
// Check if given input is a valid regex
//------------------------------------------------------------------------------
bool eos_regex_valid(const std::string& regex)
{
  // Looks like std::regex suffers from https://gcc.gnu.org/bugzilla/show_bug.cgi?id=86164#c7
  // Found by filtering like " newfind --name '*sometext' " (note the '*' prefix!),
  // which raises <std::regex_constants::error_paren> - although quite misleading.
  try {
    std::regex filter(regex, std::regex_constants::egrep);
    return true;
  } catch (const std::regex_error& e) {
    eos_static_err("msg=\"failed regex check\" regex=\"%s\" except_code=%d "
                   "except_msg=\"%s\"", regex.c_str(), e.code(),
                   e.what());
    return false;
  }
}

EOSCOMMONNAMESPACE_END
