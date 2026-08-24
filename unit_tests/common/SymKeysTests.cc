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

//------------------------------------------------------------------------------
// Base64 encoding of raw bytes (used for RFC 9530 Repr-Digest values)
//------------------------------------------------------------------------------
TEST(SymKeys, Base64EncodeBytes)
{
  using eos::common::SymKey;
  // Empty input gives an empty string
  ASSERT_TRUE(SymKey::Base64Encode(std::vector<uint8_t>{}).empty());
  // Known vectors, same expectations as the string based Base64 encoding
  ASSERT_EQ("Zg==", SymKey::Base64Encode(std::vector<uint8_t>{'f'}));
  ASSERT_EQ("Zm9vYmFy",
            SymKey::Base64Encode(std::vector<uint8_t>{'f', 'o', 'o', 'b', 'a', 'r'}));
  // Binary data with leading zero byte (adler 00000001 of a 0-size file)
  ASSERT_EQ("AAAAAQ==",
            SymKey::Base64Encode(std::vector<uint8_t>{0x00, 0x00, 0x00, 0x01}));
}

//------------------------------------------------------------------------------
// Validation of the keys which can be configured as a space encryption key.
// The key ends up in the opaque information of a redirection capability,
// therefore it must not carry any of its separators.
//------------------------------------------------------------------------------
TEST(SymKeys, IsValidEncryptionKey)
{
  using eos::common::SymKey;
  ASSERT_TRUE(SymKey::IsValidEncryptionKey("1234"));
  ASSERT_TRUE(SymKey::IsValidEncryptionKey("858aa9f8-545f-4b10-a823-3b7d822291a3"));
  ASSERT_TRUE(SymKey::IsValidEncryptionKey("a+b/c.d_e:f%g,h"));
  // An empty key is not a key
  ASSERT_FALSE(SymKey::IsValidEncryptionKey(""));
  // Opaque separators and quotes are refused
  ASSERT_FALSE(SymKey::IsValidEncryptionKey("bad&key"));
  ASSERT_FALSE(SymKey::IsValidEncryptionKey("bad=key"));
  ASSERT_FALSE(SymKey::IsValidEncryptionKey("bad?key"));
  ASSERT_FALSE(SymKey::IsValidEncryptionKey("bad\"key"));
  ASSERT_FALSE(SymKey::IsValidEncryptionKey("bad'key"));
  // White space of any kind is refused
  ASSERT_FALSE(SymKey::IsValidEncryptionKey("bad key"));
  ASSERT_FALSE(SymKey::IsValidEncryptionKey("bad\tkey"));
  ASSERT_FALSE(SymKey::IsValidEncryptionKey("bad\nkey"));
  ASSERT_FALSE(SymKey::IsValidEncryptionKey("bad\rkey"));
}

//------------------------------------------------------------------------------
// Low resolution fingerprint of an encryption key. It is stored next to an
// encrypted file to detect a wrong or a changed key without ever storing the
// key itself.
//------------------------------------------------------------------------------
TEST(SymKeys, KeyPrint16)
{
  using eos::common::SymKey;
  const std::string key = "858aa9f8-545f-4b10-a823-3b7d822291a3";
  const std::string cipher = "0c4f4a1e5b2d47c8a9e3f60b1d8c2a75";
  // Stable for the same input
  ASSERT_EQ(SymKey::KeyPrint16(key, cipher), SymKey::KeyPrint16(key, cipher));
  // Always within the 16 bit range
  ASSERT_LT(std::stoul(SymKey::KeyPrint16(key, cipher)), 65536ul);
  ASSERT_LT(std::stoul(SymKey::KeyPrint16("", "")), 65536ul);
  // Depends on both the key and the obfuscation key - the same key used on
  // two files gives two different fingerprints
  ASSERT_NE(SymKey::KeyPrint16(key, cipher), SymKey::KeyPrint16(key + "x", cipher));
  ASSERT_NE(SymKey::KeyPrint16(key, cipher), SymKey::KeyPrint16(key, cipher + "x"));
  // Known property: the two arguments are hashed concatenated, so a different
  // split of the same characters collides. Harmless here since the obfuscation
  // key always has a fixed length, but do not use this as a generic digest
  ASSERT_EQ(SymKey::KeyPrint16("ab", "c"), SymKey::KeyPrint16("a", "bc"));
}

//------------------------------------------------------------------------------
// Obfuscation with a secret - this is what a space encryption key does to a
// file: the per file obfuscation key is hashed with the secret of the space
// and the result is used as the cipher.
//------------------------------------------------------------------------------
TEST(SymKeys, EncryptWithSecret)
{
  using eos::common::SymKey;
  const std::string plain = "Hello World! This is a secret payload.";
  // A per file obfuscation key long enough for the given secret
  const std::string secret = "858aa9f8-545f-4b10-a823-3b7d822291a3";
  const std::string obfuscation_key = SymKey::RandomCipher(secret);
  ASSERT_FALSE(obfuscation_key.empty());
  SymKey::hmac_t hmac(obfuscation_key, secret);
  // The cipher is derived, the obfuscation key alone is not used
  ASSERT_FALSE(hmac.hmac.empty());
  ASSERT_EQ(hmac.key, hmac.hmac);
  ASSERT_NE(hmac.key, obfuscation_key);
  std::string encrypted(plain.size(), '\0');
  SymKey::ObfuscateBuffer(&encrypted[0], plain.data(), plain.size(), 0, hmac);
  ASSERT_NE(plain, encrypted);
  // Round trip with the very same secret
  std::string decrypted = encrypted;
  SymKey::UnobfuscateBuffer(&decrypted[0], decrypted.size(), 0, hmac);
  ASSERT_EQ(plain, decrypted);
  // Cryptographic destruction - a changed secret does not give the contents
  // back, not even for a single byte of the buffer
  SymKey::hmac_t rotated(obfuscation_key, secret + "-rotated");
  ASSERT_NE(hmac.key, rotated.key);
  std::string garbage = encrypted;
  SymKey::UnobfuscateBuffer(&garbage[0], garbage.size(), 0, rotated);
  ASSERT_NE(plain, garbage);
  // The same holds for the obfuscation key alone, i.e. for a client which
  // knows the file attribute but not the secret of the space
  SymKey::hmac_t no_secret;
  no_secret.set("", obfuscation_key);
  ASSERT_EQ(no_secret.key, obfuscation_key);
  garbage = encrypted;
  SymKey::UnobfuscateBuffer(&garbage[0], garbage.size(), 0, no_secret);
  ASSERT_NE(plain, garbage);
}

//------------------------------------------------------------------------------
// Obfuscation has to be offset aware, the FST and the FUSE client decrypt
// partial buffers at arbitrary offsets
//------------------------------------------------------------------------------
TEST(SymKeys, EncryptAtOffset)
{
  using eos::common::SymKey;
  const std::string secret = "1234";
  std::string plain(4096, '\0');

  for (size_t i = 0; i < plain.size(); ++i) {
    plain[i] = (char)(i % 251);
  }

  SymKey::hmac_t hmac(SymKey::RandomCipher(secret), secret);
  std::string encrypted(plain.size(), '\0');
  SymKey::ObfuscateBuffer(&encrypted[0], plain.data(), plain.size(), 0, hmac);
  // Decrypt in chunks which are neither aligned to the cipher length nor to
  // each other, exactly as a random access read would do
  const std::list<std::pair<size_t, size_t>> chunks{{0, 7},       {7, 1017}, {1024, 2048},
                                                    {3072, 1024}, {13, 3},   {4095, 1}};

  for (const auto& chunk : chunks) {
    std::string part = encrypted.substr(chunk.first, chunk.second);
    SymKey::UnobfuscateBuffer(&part[0], part.size(), chunk.first, hmac);
    ASSERT_EQ(plain.substr(chunk.first, chunk.second), part)
        << "offset=" << chunk.first << " length=" << chunk.second;
  }
}

//------------------------------------------------------------------------------
// The obfuscation key has to be long enough to cover the secret it is hashed
// with, otherwise short keys would repeat quickly
//------------------------------------------------------------------------------
TEST(SymKeys, RandomCipherLength)
{
  using eos::common::SymKey;
  // One 32 character block for anything up to 36 characters
  ASSERT_EQ(32u, SymKey::RandomCipher("").length());
  ASSERT_EQ(32u, SymKey::RandomCipher("1234").length());
  ASSERT_EQ(32u, SymKey::RandomCipher(std::string(36, 'k')).length());
  // Beyond that it grows with the length of the key
  ASSERT_EQ(64u, SymKey::RandomCipher(std::string(37, 'k')).length());
  ASSERT_EQ(96u, SymKey::RandomCipher(std::string(80, 'k')).length());
  // Two invocations never give the same key
  ASSERT_NE(SymKey::RandomCipher("1234"), SymKey::RandomCipher("1234"));
}

//------------------------------------------------------------------------------
// KeyPrint16 was factored out of fusex/md/md.hh, where it was written inline.
// The fingerprint is persisted per file in 'user.encrypted.fp', so the value
// has to stay bit for bit what the inline version produced - otherwise every
// already encrypted file would suddenly report a wrong key.
//------------------------------------------------------------------------------
TEST(SymKeys, KeyPrint16MatchesLegacyInlineVersion)
{
  using eos::common::SymKey;
  // verbatim copy of the implementation that used to live in md.hh
  auto legacy = [](const std::string& key1, const std::string& key2) {
    std::hash<std::string> secrethash;
    return std::to_string(secrethash(key1 + key2) % 65536);
  };
  const std::list<std::pair<std::string, std::string>> cases{
      {"", ""},
      {"1234", "0c4f4a1e5b2d47c8a9e3f60b1d8c2a75"},
      {"858aa9f8-545f-4b10-a823-3b7d822291a3", "deadbeef"},
      {"secret with spaces", "0123456789abcdef0123456789abcdef"},
      {std::string(1024, 'x'), std::string(32, 'y')}};

  for (const auto& c : cases) {
    ASSERT_EQ(legacy(c.first, c.second), SymKey::KeyPrint16(c.first, c.second))
        << "fingerprint drifted for key='" << c.first << "'";
  }
}
