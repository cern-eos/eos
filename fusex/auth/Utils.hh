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

class FatalException : public std::exception
{
public:
  FatalException(const std::string& m) : msg(m) {}
  virtual ~FatalException() {}

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

inline std::vector<std::string> split(std::string data, std::string token)
{
  std::vector<std::string> output;
  size_t pos = std::string::npos;

  do {
    pos = data.find(token);
    output.push_back(data.substr(0, pos));

    if (std::string::npos != pos) {
      data = data.substr(pos + token.size());
    }
  } while (std::string::npos != pos);

  return output;
}

inline std::vector<std::string> split_on_nullbyte(std::string data)
{
  std::string nullbyte("\0", 1);
  std::vector<std::string> ret = split(data, nullbyte);

  if (ret[ret.size() - 1].size() == 0) {
    ret.pop_back();
  }

  return ret;
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

inline bool startswith(const std::string& str, const std::string& prefix)
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

bool readFile(const std::string& path, std::string& ret);
bool checkCredSecurity(const struct stat& filestat, uid_t uid);

#endif
