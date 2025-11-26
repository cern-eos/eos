/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

/************************************************************************
 * @file  XattrSet.hh                                                   *
 * @brief Serialize/deserialize a set to allow it to be stored as an    *
 *        extended attribute value (string) but manipulated as a set    *
 ************************************************************************/

#pragma once

#include <set>
#include <string>
#include <iostream>

#include "mgm/Namespace.hh"

EOSMGMNAMESPACE_BEGIN

struct XattrSet {
  XattrSet() {}

  XattrSet(const char *str) { deserialize(str); }

  // Convert a string of space-separated values into a set
  void deserialize(const char *str) {
    values.clear();
    do {
      const char *begin;

      for(begin = str; *str != ' ' && *str != '\0'; ++str) ;

      if(str-begin > 1) {
        values.insert(std::string(begin, str));
      }
    } while(*str++ != '\0');
  }

  void deserialize(const std::string &str) { deserialize(str.c_str()); }

  // Convert a set into a string
  std::string serialize() const {
    std::string xattr_str;
    if(!values.empty()) {
      for(auto &str : values) {
        xattr_str += str + ' ';
      }
      xattr_str.resize(xattr_str.length()-1);
    }
    return xattr_str;
  }

  std::set<std::string> values;
};

EOSMGMNAMESPACE_END
