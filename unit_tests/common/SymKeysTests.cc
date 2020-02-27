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

#include "gtest/gtest.h"
#include "common/SymKeys.hh"
#include <fstream>
#include <list>

//------------------------------------------------------------------------------
// Cipher encoding and decoding test
//------------------------------------------------------------------------------
TEST(SymKeys, CipherTest)
{
  using namespace eos::common;
  char* key = (char*)"12345678901234567890";
  std::list<ssize_t> set_lengths {1, 10, 100, 1024, 4096, 5746};

  for (auto it = set_lengths.begin(); it != set_lengths.end(); ++it) {
    std::unique_ptr<char[]> data {new char[*it]};
    // Generate random data
    std::ifstream urandom("/dev/urandom", std::ios::in | std::ios::binary);
    urandom.read(data.get(), (ssize_t)*it);
    urandom.close();
    // Encrypt data
    char* encrypted_data;
    ssize_t encrypted_length = 0;
    ASSERT_TRUE(SymKey::CipherEncrypt(data.get(), *it, encrypted_data,
                                      encrypted_length, key));
    // Decrypt data
    char* decrypted_data;
    ssize_t decrypted_length = 0;
    ASSERT_TRUE(SymKey::CipherDecrypt(encrypted_data, encrypted_length,
                                      decrypted_data, decrypted_length,
                                      key));
    ASSERT_TRUE(*it == decrypted_length)
        << "Expected:" << *it << ", obtained:" << decrypted_length << std::endl;
    ASSERT_TRUE(memcmp(data.get(), decrypted_data, decrypted_length) == 0);
    free(encrypted_data);
    free(decrypted_data);
  }
}

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
    ssize_t decoded_length;
    ASSERT_TRUE(eos::common::SymKey::Base64Decode(encoded.c_str(), decoded_bytes,
                decoded_length));
    ASSERT_TRUE(elem->first.length() == (size_t)decoded_length)
        << "Expected:" << elem->first.length() << ", obtained:" << decoded_length;
    ASSERT_TRUE(elem->first == decoded_bytes)
        << "Expected:" << elem->first << ", obtained:" << decoded_bytes;
    free(decoded_bytes);
  }
}
