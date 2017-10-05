//------------------------------------------------------------------------------
// File: SymKeysTests.cc
// Author: Elvin Sindrilaru <esindril at cern dot ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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

#include <gtest/gtest.h>
#include "common/SymKeys.hh"

//------------------------------------------------------------------------------
// Base64 test
//------------------------------------------------------------------------------
TEST(SymKeys, Base64Test)
{
  std::map<std::string, std::string> map_tests = {
    {"",  ""},
    {"f", "Zg=="},
    {"fo", "Zm8="},
    {"foo", "Zm9v"},
    {"foob", "Zm9vYg=="},
    {"fooba", "Zm9vYmE="},
    {"foobar", "Zm9vYmFy"},
    {"testtest", "dGVzdHRlc3Q="}
  };

  for (auto elem = map_tests.begin(); elem != map_tests.end(); ++elem) {
    // Check encoding
    std::string encoded;
    ASSERT_TRUE(eos::common::SymKey::Base64Encode((char*)elem->first.c_str(),
                elem->first.length(), encoded));
    ASSERT_TRUE(elem->second == encoded)
        << "Expected:" << elem->second << ", obtained:" << encoded << std::endl;
    // Check decoding
    char* decoded_bytes;
    size_t decoded_length;
    ASSERT_TRUE(eos::common::SymKey::Base64Decode(encoded.c_str(), decoded_bytes,
                decoded_length));
    ASSERT_TRUE(elem->first.length() == (size_t)decoded_length)
        << "Expected:" << elem->first.length() << ", obtained:" << decoded_length;
    ASSERT_TRUE(elem->first == decoded_bytes)
        << "Expected:" << elem->first << ", obtained:" << decoded_bytes;
    free(decoded_bytes);
  }
}
