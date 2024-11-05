//------------------------------------------------------------------------------
// File: SciTokensTests.cc
// Author: Andreas-Joachim Peters <andreas.joachim.peters@cern.ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2024 CERN/Switzerland                                  *
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
#include "XrdOuc/XrdOucString.hh"

#include "Namespace.hh"
#include "common/token/SciToken.hh"
#include "common/token/scitoken.h"
#include <memory>
#include <time.h>
#include <sys/stat.h>

EOSCOMMONTESTING_BEGIN

using namespace eos::common;

TEST(SciToken, Factory)
{
  std::unique_ptr<SciToken> issuer;
  // create keys
  std::ofstream("/tmp/.eosunit.sci.cred") << "-----BEGIN PUBLIC KEY-----\nMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAE9lFITZoMpmdgPN2rRFT3EUEYXybj\nzRoTSdF6P5I9eyCj42n/OASfE+jMB2FtpV8FrwIk7D8xqWAJ9KbHTZPKag==\n-----END PUBLIC KEY-----\n";
  std::ofstream("/tmp/.eosunit.sci.key") << "-----BEGIN PRIVATE KEY-----\nMIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgRnmkbjzf5uE5INQR\n4XBA973ioI7vuAMgfV8MFcnzP36hRANCAAT2UUhNmgymZ2A83atEVPcRQRhfJuPN\nGhNJ0Xo/kj17IKPjaf84BJ8T6MwHYW2lXwWvAiTsPzGpYAn0psdNk8pq\n-----END PRIVATE KEY-----\n";
  // make key secure
  ::chmod("/tmp/.eosunit.sci.key",S_IRUSR);
  issuer.reset(eos::common::SciToken::Factory("/tmp/.eosunit.sci.cred", "/tmp/.eosunit.sci.key", "eos","localhost"));
  ASSERT_NE(issuer, nullptr);

  std::string token;
  time_t expires = time(NULL) + 3600;
  std::set<std::string> claims;
  for ( size_t i = 0 ; i< 10000; i++) {
    claims.clear();
    std::string scope = "scope=storage.read:\"/eos/";
    scope += std::to_string(i);
    scope += "\"";
    claims.insert(scope);
    int rc = issuer->CreateToken(token, expires, claims);
    ASSERT_EQ(rc, 0);
  }
  
  ::unlink("/tmp/.eosunit.sci.key");
  ::unlink("/tmp/.eosunit.sci.cred");
}

TEST(SciToken, CFactory)
{
  std::unique_ptr<SciToken> issuer;
  // create keys
  std::ofstream("/tmp/.eosunit.sci.cred") << "-----BEGIN PUBLIC KEY-----\nMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAE9lFITZoMpmdgPN2rRFT3EUEYXybj\nzRoTSdF6P5I9eyCj42n/OASfE+jMB2FtpV8FrwIk7D8xqWAJ9KbHTZPKag==\n-----END PUBLIC KEY-----\n";
  std::ofstream("/tmp/.eosunit.sci.key") << "-----BEGIN PRIVATE KEY-----\nMIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgRnmkbjzf5uE5INQR\n4XBA973ioI7vuAMgfV8MFcnzP36hRANCAAT2UUhNmgymZ2A83atEVPcRQRhfJuPN\nGhNJ0Xo/kj17IKPjaf84BJ8T6MwHYW2lXwWvAiTsPzGpYAn0psdNk8pq\n-----END PRIVATE KEY-----\n";
  // make key secure
  ::chmod("/tmp/.eosunit.sci.key",S_IRUSR);

  void* sci_ctx = c_scitoken_factory_init("/tmp/.eosunit.sci.cred", "/tmp/.eosunit.sci.key", "eos","localhost");
  ASSERT_NE((long long)sci_ctx,0);
  
  std::string token;
  time_t expires = time(NULL) + 3600;
  for ( size_t i = 0 ; i< 10000; i++) {
    token="";
    token.resize(4096);
    std::string scope = "scope=storage.read:\"/eos/";
    scope += std::to_string(i);
    scope += "\"";
    int rc = c_scitoken_create((char*)token.c_str(), token.size(), expires, scope.c_str());
    ASSERT_EQ(rc, 0);
  }
  
  ::unlink("/tmp/.eosunit.sci.key");
  ::unlink("/tmp/.eosunit.sci.cred");
}

EOSCOMMONTESTING_END
