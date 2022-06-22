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

EOSCOMMONNAMESPACE_BEGIN

class StringPermutation {
public:
  StringPermutation(const std::string& in) : s(in) {}

  // if the permutation has to be predictable, initialize using Seed before usage
  std::string Permutation(size_t i) {
    struct PermutationFunc {
      size_t operator() (size_t n)
      {
        return (++tSeed)%n;
      }
    };

    std::string ss(s);
    std::random_shuffle(ss.begin(), ss.end(), PermutationFunc());
    return ss;
  }

  void Seed(size_t s) { tSeed = s; }

private:
  std::string s;
  thread_local static size_t tSeed;
};

EOSCOMMONNAMESPACE_END

#endif
