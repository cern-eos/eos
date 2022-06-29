// ----------------------------------------------------------------------
// File: StringPermutation.cc
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

// Convenience class to create all possible permutations of a given string
#include "common//SymKeys.hh"
#include "common/StringPermutation.hh"

EOSCOMMONNAMESPACE_BEGIN

StringPermutation::StringPermutation(const std::string& in, const std::string* secret, size_t max) : s(in) {
  if (max==0) {
    // sort string                                                                                                                                     
    std::sort(s.begin(), s.end());
  }
  do {
    mPermutations.push_back(s);
    if ((max!=0) && (mPermutations.size() >= max))
      break;
  } while(std::next_permutation(s.begin(), s.end()));
  if (secret) {
    computeHmac((*secret));
  }
}

void 
StringPermutation::computeHmac(const std::string& secret) {
  for ( auto i:mPermutations ) {
    mHmacs.push_back(eos::common::SymKey::HmacSha256(secret, i));
  }
}


EOSCOMMONNAMESPACE_END
