// ----------------------------------------------------------------------
// File: Utils.hh
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

#ifndef __EOSFUSE_UTILS_HH__
#define __EOSFUSE_UTILS_HH__

#include <exception>
#include <string>
#include <vector>
#include <sstream>
#include <iostream>
#include "common/StringUtils.hh"
#include "common/StringSplit.hh"

class FatalException : public std::exception
{
public:

  FatalException(const std::string& m) : msg(m) { }

  virtual ~FatalException() { }

  virtual const char* what() const noexcept
  {
    return msg.c_str();
  }

private:
  std::string msg;
};

#define DBG(message) std::cerr << __FILE__ << ":" << __LINE__ << " -- " << #message << " = " << message << std::endl
#define SSTR(message) static_cast<std::ostringstream&>(std::ostringstream().flush() << message).str()
#define THROW(message) throw FatalException(SSTR(message))

inline std::vector<std::string> split_on_nullbyte(std::string_view data)
{
  std::vector<std::string> result;
  auto segments = eos::common::CharSplitIt(data, '\0');

  for (std::string_view segment : segments) {
    result.emplace_back(segment);
  }

  return result;
}

inline std::string join(const std::vector<std::string>& contents,
                        const std::string& delimiter)
{
  std::stringstream ss;

  for (size_t i = 0; i < contents.size(); i++) {
    ss << contents[i];

    if (i != contents.size() - 1) {
      ss << " ";
    }
  }

  return ss.str();
}

using eos::common::startsWith;
bool readFile(int fd, std::string& ret);
bool readFile(const std::string& path, std::string& ret);
bool writeFile600(const std::string& path, const std::string& contents);

bool checkCredSecurity(const struct stat& filestat, uid_t uid);
std::string chopTrailingSlashes(const std::string& path);

#endif
