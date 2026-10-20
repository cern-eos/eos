//------------------------------------------------------------------------------
// File: EosTokenTests.cc
// Author: Andreas-Joachim Peters <andreas.joachim.peters@cern.ch>
//------------------------------------------------------------------------------

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

#include "common/Mapping.hh"
#include "common/SymKeys.hh"
#include "common/token/EosTok.hh"
#include "common/token/GrpcQuotaScope.hh"
#include "gtest/gtest.h"
#include <XrdSec/XrdSecEntity.hh>
#include <cstdlib>

//------------------------------------------------------------------------------
// Test Tokens
//------------------------------------------------------------------------------
TEST(EosTokenTests, Tokens) {
  eos::common::EosTok token;
  std::string path = "/eos/token/test/";
  token.SetPath(path, true);
  std::string perm = "rwx";
  token.SetPermission(perm);
  std::string owner = "myuser";
  std::string group = "mygroup";
  token.SetOwner(owner);
  token.SetGroup(group);
  time_t now = time(NULL);
  token.SetExpires(now + 10);
  token.SetGeneration(0);
  std::string dump;
  token.Dump(dump);
  token.AddOrigin("*","*","*");

  //  std::cout << dump.c_str() << std::endl;
  std::string key = "1234567890";
  std::string btoken = token.Write(key);

  token.Dump(dump);
  //  std::cout << dump << btoken << std::endl;

  eos::common::EosTok reversetoken;

  // Test Token writing
  ASSERT_TRUE(reversetoken.Read(btoken, key, 0)==0);

  std::string reversedump;
  reversetoken.Dump(reversedump);
  //  std::cout << reversedump << std::endl;

  // Test Token reading
  ASSERT_EQ(dump, reversedump);

  reversetoken.Reset();
  reversetoken.Dump(reversedump);
  //  std::cout << reversedump << std::endl;

  std::string expectreversedump = "{\n"
                                  " \"signature\": \"\",\n"
                                  " \"serialized\": \"\",\n"
                                  " \"seed\": 0\n"
                                  "}\n";

  // Test Token Reset
  ASSERT_EQ(reversedump, expectreversedump);

  // Test Token Reading with new generation
  ASSERT_TRUE(reversetoken.Read(btoken, key, 1)!=0);

  // Test Token Reading with wrong key
  ASSERT_TRUE(reversetoken.Read(btoken, key+"z", 0)!=0);

  // Test Token Reading with wrong token
  std::string faultytoken = btoken.erase(10,1);
  ASSERT_TRUE(reversetoken.Read(faultytoken, key, 0)!=0);


  // Test Token expired;
  now = time(NULL);
  token.SetExpires(now+1);
  btoken = token.Write(key);
  ASSERT_TRUE(reversetoken.Read(btoken, key, 0)==0);
  sleep(2);
  ASSERT_TRUE(reversetoken.Read(btoken, key, 0)!=0);
}


TEST(EosTokenTests, Origins) {
  eos::common::EosTok token;

  ASSERT_TRUE(!token.VerifyOrigin("eos.cern.ch","admin","sss"));
  token.AddOrigin("(.*)","(.*)","(.*)");

  ASSERT_TRUE(!token.VerifyOrigin("eos.cern.ch","root","sss"));

  token.Reset();

  token.AddOrigin("host(.*)", "(.*)","(.*)");
  ASSERT_TRUE(!token.VerifyOrigin("host.cern.ch","root","sss"));
  ASSERT_TRUE(!token.VerifyOrigin("hosty.cern.ch","root","sss"));
  ASSERT_TRUE(token.VerifyOrigin("tosty.cern.ch","root","sss"));
}

TEST(EosTokenTests, QuotaScopesSignedRoundTrip)
{
  eos::common::EosTok token;
  token.SetPath("/eos/token/test/", true);
  token.SetPermission("rx");
  token.SetOwner("1234");
  token.SetGroup("4321");
  token.SetExpires(time(nullptr) + 60);
  token.SetGeneration(42);
  token.AddScope("grpc.quota.read");
  token.AddScope("grpc.quota.write");
  const std::vector<std::string> scopes = {"grpc.quota.read", "grpc.quota.write"};
  const std::string key = "quota-scope-test-key";
  const std::string encoded = token.Write(key);
  ASSERT_FALSE(encoded.empty());

  eos::common::EosTok decoded;
  ASSERT_EQ(0, decoded.Read(encoded, key, 42));
  EXPECT_TRUE(decoded.Valid());
  EXPECT_EQ(scopes, decoded.Scopes());
  EXPECT_EQ(token.Path(), decoded.Path());
  EXPECT_EQ(token.Permission(), decoded.Permission());
  EXPECT_EQ(token.Owner(), decoded.Owner());
  EXPECT_EQ(token.Group(), decoded.Group());

  std::string issued_dump;
  std::string decoded_dump;
  token.Dump(issued_dump);
  decoded.Dump(decoded_dump);
  EXPECT_EQ(issued_dump, decoded_dump);

  decoded.Reset();
  EXPECT_FALSE(decoded.Valid());
  EXPECT_TRUE(decoded.Scopes().empty());
  EXPECT_NE(0, decoded.Read(encoded, key + "wrong", 42));
  EXPECT_NE(0, decoded.Read(encoded, key, 43));
}

TEST(EosTokenTests, QuotaScopesAreSigned)
{
  const std::string key = "quota-scope-test-key";
  eos::common::EosTok token;
  token.SetExpires(time(nullptr) + 60);
  token.AddScope("grpc.quota.read");
  ASSERT_FALSE(token.Write(key).empty());
  ASSERT_EQ(0, token.Verify(key));

  // A new scope must invalidate the signature until the token is reissued.
  token.AddScope("grpc.quota.write");
  token.Serialize();
  EXPECT_NE(0, token.Verify(key));
}

TEST(EosTokenTests, QuotaScopePolicy)
{
  using eos::common::GrpcQuotaScopeAllows;
  using eos::common::IsGrpcQuotaScope;
  EXPECT_TRUE(IsGrpcQuotaScope("grpc.quota.read"));
  EXPECT_TRUE(IsGrpcQuotaScope("grpc.quota.write"));
  EXPECT_FALSE(GrpcQuotaScopeAllows({}, false));
  EXPECT_FALSE(GrpcQuotaScopeAllows({}, true));
  EXPECT_TRUE(GrpcQuotaScopeAllows({"grpc.quota.read"}, false));
  EXPECT_FALSE(GrpcQuotaScopeAllows({"grpc.quota.read"}, true));
  EXPECT_TRUE(GrpcQuotaScopeAllows({"grpc.quota.write"}, false));
  EXPECT_TRUE(GrpcQuotaScopeAllows({"grpc.quota.write"}, true));
  EXPECT_TRUE(GrpcQuotaScopeAllows({"grpc.quota.read", "grpc.quota.write"}, true));
  EXPECT_TRUE(GrpcQuotaScopeAllows({"grpc.quota.read", "grpc.quota.read"}, false));

  for (const std::string scope :
       {"", "*", "grpc.*", "grpc.quota.*", "grpc.quota.Read", "grpc.exec.quota.get",
        " grpc.quota.read", "grpc.quota.write "}) {
    SCOPED_TRACE(scope);
    EXPECT_FALSE(IsGrpcQuotaScope(scope));
    EXPECT_FALSE(GrpcQuotaScopeAllows({scope}, false));
    EXPECT_FALSE(GrpcQuotaScopeAllows({"grpc.quota.write", scope}, true));
    EXPECT_FALSE(GrpcQuotaScopeAllows({scope, "grpc.quota.write"}, false));
  }
}

TEST(EosTokenTests, ScopedTokensRequireGrpcTransport)
{
  using namespace eos::common;

  if (getenv("EOS_MGM_TOKEN_KEYFILE")) {
    GTEST_SKIP() << "requires the default in-process token key";
  }

  SymKey* symkey = gSymKeyStore.GetCurrentKey();
  const std::string key = symkey ? symkey->GetKey64() : "0123457890defaultkey";

  for (const std::string scope : {"grpc.quota.read", "grpc.quota.write"}) {
    EosTok token;
    token.SetPath("/eos/token/test/", true);
    token.SetOwner("0");
    token.SetGroup("0");
    token.SetPermission("rwx");
    token.SetExpires(time(nullptr) + 60);
    token.SetGeneration(EosTok::sTokenGeneration);
    token.AddScope(scope);
    const std::string encoded = token.Write(key);
    const std::string opaque = "authz=" + encoded;

    for (const char* protocol : {"sss", "https", "unix", "ztn", "grpc"}) {
      SCOPED_TRACE(scope + ":" + protocol);
      XrdSecEntity client(protocol);
      char name[] = "quota-scope-test";
      char host[] = "localhost";
      client.name = name;
      client.host = host;
      client.tident = "quota-scope-test@localhost";
      // Native gRPC mapping resets vid.prot while unwrapping token endorsements.
      client.endorsements = const_cast<char*>(encoded.c_str());
      VirtualIdentity vid;
      Mapping::IdMap(&client, opaque.c_str(), client.tident, vid, nullptr, AOP_Stat,
                     "/proc/user/", false);
      ASSERT_NE(nullptr, vid.token);

      if (std::string(protocol) == "grpc") {
        EXPECT_TRUE(vid.token->Valid());
        EXPECT_EQ(std::vector<std::string>{scope}, vid.token->Scopes());
      } else {
        EXPECT_FALSE(vid.token->Valid());
        EXPECT_TRUE(vid.token->Scopes().empty());
        EXPECT_TRUE(vid.IsNobody());
      }
    }
  }
}

TEST(EosTokenTests, UnscopedTokensKeepNonGrpcTransport)
{
  using namespace eos::common;

  if (getenv("EOS_MGM_TOKEN_KEYFILE")) {
    GTEST_SKIP() << "requires the default in-process token key";
  }

  SymKey* symkey = gSymKeyStore.GetCurrentKey();
  const std::string key = symkey ? symkey->GetKey64() : "0123457890defaultkey";
  EosTok token;
  token.SetPath("/eos/token/test/", true);
  token.SetExpires(time(nullptr) + 60);
  token.SetGeneration(EosTok::sTokenGeneration);
  const std::string opaque = "authz=" + token.Write(key);
  XrdSecEntity client("sss");
  char name[] = "quota-scope-test";
  char host[] = "localhost";
  client.name = name;
  client.host = host;
  client.tident = "quota-scope-test@localhost";
  VirtualIdentity vid;
  Mapping::IdMap(&client, opaque.c_str(), client.tident, vid, nullptr, AOP_Stat,
                 "/proc/user/", false);
  ASSERT_NE(nullptr, vid.token);
  EXPECT_TRUE(vid.token->Valid());
  EXPECT_TRUE(vid.token->Scopes().empty());
}
