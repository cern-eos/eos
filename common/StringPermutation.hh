// ----------------------------------------------------------------------
// File: StringPermutation.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2022 CERN/Switzerland                                  *
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

/**
 * @file   StringPermutation.hh
 * @brief  Convenience class to create all possible permutations of a given string
 */

#ifndef __EOSCOMMON_STRINGPERMUTATION__
#define __EOSCOMMON_STRINGPERMUTATION__


#include "common/Namespace.hh"
#include <algorithm>
#include <string>
#include <vector>

EOSCOMMONNAMESPACE_BEGIN

class StringPermutation {
public:
  StringPermutation(const std::string& in, const std::string* secret=nullptr, size_t max=1024);

  void computeHmac(const std::string& secret);
  std::vector<std::string>& Permutations() { return mPermutations; }
  std::vector<std::string>& Hmacs() {return mHmacs; }

private:
  std::string s;
  std::vector<std::string> mPermutations;
  std::vector<std::string> mHmacs;
};

EOSCOMMONNAMESPACE_END

#endif
