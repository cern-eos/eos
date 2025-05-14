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
#include <memory>
#include <regex.h>

namespace
{
//! Custom deleter for regex_t std::unique_ptr object
auto regex_deleter = [](regex_t* ptr)
{
  regfree(ptr);
  free(ptr);
};
//! unique_regex_t which is a unique pointer to a regex_t object
using unique_regex_t = std::unique_ptr<regex_t, decltype(regex_deleter)>;
std::mutex sRegexMutex; ///< Mutex protecting access to the map below
//! Map string regex to regex_t objects
std::map<std::string, unique_regex_t> sMapRegex;

//------------------------------------------------------------------------------
//! Get regex expression based on the input string regex. The result is either
//! computed on the fly or it's taken from the cache
//!
//! @param in_regex input string regex
//!
//! @return compiled regex expression or std::null_ptr in case of error
//------------------------------------------------------------------------------
regex_t* eos_get_regex(const std::string& in_regex)
{
  {
    // This is the most common code path
    std::unique_lock<std::mutex> lock(sRegexMutex);
    auto it = sMapRegex.find(in_regex);

    if (it != sMapRegex.end()) {
      return it->second.get();
    }
  }
  regex_t* re = static_cast<regex_t*>(malloc(sizeof(regex_t)));

  if (regcomp(re, in_regex.c_str(), REG_EXTENDED | REG_NOSUB) != 0) {
    eos_static_err("msg=\"failed to compile regex\" sregex=\"%s\"",
                   in_regex.c_str());
    return nullptr; // error
  }

  unique_regex_t uniq_re(re, regex_deleter);
  std::unique_lock<std::mutex> lock(sRegexMutex);
  return sMapRegex.emplace(in_regex, std::move(uniq_re)).first->second.get();
}
}

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Wrapper for std::regex_match taking regular expression as std::string
//------------------------------------------------------------------------------
bool eos_regex_match(const std::string& input, const std::string& regex)
{
  if (regex.empty()) {
    return false;
  }

  std::string lregex = regex;

  // Make sure the regex pattern asks for a full match!
  if (*lregex.begin() != '^') {
    lregex.insert(0, "^");
  }

  if (*lregex.rbegin() != '$') {
    lregex.append("$");
  }

  regex_t* re = eos_get_regex(lregex);

  if ((re == nullptr) ||
      (regexec(re, input.c_str(), (size_t) 0, nullptr, 0) != 0)) {
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Wrapper for std::regex_search taking regular expression as std::string
//------------------------------------------------------------------------------
bool eos_regex_search(const std::string& input, const std::string& regex)
{
  if (regex.empty()) {
    return false;
  }

  regex_t re;

  if (regcomp(&re, regex.c_str(), REG_EXTENDED | REG_NOSUB) != 0) {
    return false;
  }

  int status = regexec(&re, input.c_str(), 0, nullptr, 0);
  regfree(&re);

  if (status != 0) {
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Check if given input is a valid regex
//------------------------------------------------------------------------------
bool eos_regex_valid(const std::string& regex)
{
  regex_t re;

  if (regcomp(&re, regex.c_str(), REG_EXTENDED | REG_NOSUB) != 0) {
    return false;
  }

  return true;
}

EOSCOMMONNAMESPACE_END
