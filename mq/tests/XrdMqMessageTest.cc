//------------------------------------------------------------------------------
// File: XrdMqMessageTest.cc
// Author: Elvin Sindrilaru <esindril@cern.ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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

#include "XrdMqMessageTest.hh"
#include "openssl/pem.h"
#include <algorithm>
#include <istream>
#include <list>

CPPUNIT_TEST_SUITE_REGISTRATION(XrdMqMessageTest);
EVP_PKEY*     XrdMqMessage::PrivateKey = 0;
XrdSysLogger* XrdMqMessage::Logger = 0;
XrdSysError   XrdMqMessage::Eroute(0);

//------------------------------------------------------------------------------
// setUp function
//------------------------------------------------------------------------------
void
XrdMqMessageTest::setUp()
{
  mEnv = new eos::mq::test::TestEnv();
  XrdMqMessage::Logger = new XrdSysLogger();
  XrdMqMessage::Eroute.logger(XrdMqMessage::Logger);
}

//------------------------------------------------------------------------------
// tearDown function
//------------------------------------------------------------------------------
void
XrdMqMessageTest::tearDown()
{
  delete mEnv;
  mEnv = 0;
  delete XrdMqMessage::Logger;
}

//----------------------------------------------------------------------------
//! Generate random data
//----------------------------------------------------------------------------
void
XrdMqMessageTest::GenerateRandomData(char* data, ssize_t length)
{
  std::ifstream urandom("/dev/urandom", std::ios::in | std::ios::binary);
  urandom.read(data, length);
  urandom.close();
}

//------------------------------------------------------------------------------
// Base64 test
//------------------------------------------------------------------------------
void
XrdMqMessageTest::Base64Test()
{
  std::map<std::string, std::string> map_tests =
  {
    {"",  ""},
    {"f", "Zg=="},
    {"fo", "Zm8="},
    {"foo", "Zm9v"},
    {"foob", "Zm9vYg=="},
    {"fooba", "Zm9vYmE="},
    {"foobar", "Zm9vYmFy"},
    {"testtest", "dGVzdHRlc3Q="}
  };

  for (auto elem = map_tests.begin(); elem != map_tests.end(); ++elem)
  {
    // Check encoding
    std::string encoded;
    CPPUNIT_ASSERT(XrdMqMessage::Base64Encode((char*)elem->first.c_str(),
                                              elem->first.length(), encoded));
    CPPUNIT_ASSERT_STREAM(
        "Expected:" << elem->second << ", obtained:" << encoded,
        elem->second == encoded);

    // Check decoding
    char* decoded_bytes;
    ssize_t decoded_length;
    CPPUNIT_ASSERT(XrdMqMessage::Base64Decode((char*)encoded.c_str(),
                                              decoded_bytes, decoded_length));
    CPPUNIT_ASSERT_STREAM(
        "Expected:" << elem->first.length() << ", obtained:" << decoded_length,
        elem->first.length() == (size_t)decoded_length);
    CPPUNIT_ASSERT_STREAM(
        "Expected:" << elem->first << ", obtained:" << decoded_bytes,
        elem->first == decoded_bytes);
    free(decoded_bytes);
  }
}

//------------------------------------------------------------------------------
// Cipher encoding and decoding test
//------------------------------------------------------------------------------
void
XrdMqMessageTest::CipherTest()
{
  char* key = (char*)"12345678901234567890";
  std::list<ssize_t> set_lengths {1, 10, 100, 1024, 4096, 5746};

  for (auto it = set_lengths.begin(); it != set_lengths.end(); ++it)
  {
    std::unique_ptr<char[]> data {new char[*it]};
    GenerateRandomData(data.get(), (ssize_t)*it);

    // Encrypt data
    char* encrypted_data;
    ssize_t encrypted_length = 0;
    CPPUNIT_ASSERT(XrdMqMessage::CipherEncrypt(data.get(), *it, encrypted_data,
                                               encrypted_length, key));

    // Decrypt data
    char* decrypted_data;
    ssize_t decrypted_length = 0;
    CPPUNIT_ASSERT(XrdMqMessage::CipherDecrypt(encrypted_data, encrypted_length,
                                               decrypted_data, decrypted_length,
                                               key));

    CPPUNIT_ASSERT_STREAM("Expected:" << *it << ", obtained:" << decrypted_length,
                          *it == decrypted_length);
    CPPUNIT_ASSERT(memcmp(data.get(), decrypted_data, decrypted_length) == 0);
    free(encrypted_data);
    free(decrypted_data);
  }
}


//------------------------------------------------------------------------------
// RSA encoding and decoding test
//------------------------------------------------------------------------------
void
XrdMqMessageTest::RSATest()
{
  XrdOucString rsa_hash = "rsa_key";
  std::string rsa_private_key = mEnv->GetMapping("rsa_private_key");
  std::string rsa_public_key = mEnv->GetMapping("rsa_public_key");
  // Read in EVP_PKEY structure the private key
  BIO* bio = BIO_new_mem_buf((void*)rsa_private_key.c_str(), -1);
  CPPUNIT_ASSERT(bio);
  XrdMqMessage::PrivateKey = PEM_read_bio_PrivateKey(bio, 0, 0, 0);
  BIO_free(bio);
  CPPUNIT_ASSERT(XrdMqMessage::PrivateKey != NULL);
  RSA* rsa_key = EVP_PKEY_get1_RSA(XrdMqMessage::PrivateKey);
  CPPUNIT_ASSERT(RSA_check_key(rsa_key));

  // Read in EVP_PKEY structure the public key
  bio = BIO_new_mem_buf((void*)rsa_public_key.c_str(), -1);
  CPPUNIT_ASSERT(bio);
  EVP_PKEY* pub_key = PEM_read_bio_PUBKEY(bio, 0, 0, 0);
  CPPUNIT_ASSERT(pub_key != NULL);
  XrdMqMessage::PublicKeyHash.Add(rsa_hash.c_str(), pub_key);

  ssize_t data_length = SHA_DIGEST_LENGTH;
  std::unique_ptr<char[]> data {new char[data_length]};
  GenerateRandomData(data.get(), (ssize_t)data_length);

  // Encrypt data
  char* encrypted_data;
  ssize_t encrypted_length;
  CPPUNIT_ASSERT(XrdMqMessage::RSAEncrypt(data.get(), data_length,
                                          encrypted_data, encrypted_length));

  // Decrypt data
  char* decrypted_data;
  ssize_t decrypted_length;
  CPPUNIT_ASSERT(XrdMqMessage::RSADecrypt(encrypted_data, encrypted_length,
                                          decrypted_data, decrypted_length,
                                          rsa_hash));
  CPPUNIT_ASSERT(decrypted_length == data_length);
  free(encrypted_data);
  free(decrypted_data);
}
