//------------------------------------------------------------------------------
// File: process-cache.cc
// Author: Georgios Bitzes - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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

#include "auth/ProcessCache.hh"
#include "auth/UserCredentialFactory.hh"
#include "auth/Logbook.hh"
#include "test-utils.hh"
#include <gtest/gtest.h>

TEST_F(UnixAuthF, BasicSanity)
{
  injectProcess(1234, 1, 1234, 1234, 9999, 0);
  ProcessSnapshot snapshot = processCache()->retrieve(1234, 5, 6, false);
  ASSERT_EQ(snapshot->getXrdLogin(), LoginIdentifier(5, 6, 1234,
            0).getStringID());
  ProcessSnapshot snapshot2 = processCache()->retrieve(1234, 5, 6, false);
  ASSERT_EQ(snapshot2->getXrdLogin(), LoginIdentifier(5, 6, 1234,
            0).getStringID());
  ProcessSnapshot snapshot3 = processCache()->retrieve(1234, 5, 6, true);
  ASSERT_EQ(snapshot3->getXrdLogin(), LoginIdentifier(5, 6, 1234,
            1).getStringID());
  ProcessSnapshot snapshot4 = processCache()->retrieve(1234, 7, 6, false);
  ASSERT_EQ(snapshot4->getXrdLogin(), LoginIdentifier(7, 6, 1234,
            0).getStringID());
  injectProcess(1235, 1, 1235, 1235, 9999, 0);
  ProcessSnapshot snapshot5 = processCache()->retrieve(1235, 8, 6, false);
  ASSERT_EQ(snapshot5->getXrdLogin(), LoginIdentifier(8, 6, 1235,
            0).getStringID());
}

TEST_F(Krb5AuthF, BasicSanity)
{
  injectProcess(1234, 1, 1234, 1234, 9999, 0);
  securityChecker()->inject(localJail().id, "/tmp/my-creds", 1000, 0400, {1, 1});
  environmentReader()->inject(1234, createEnv("/tmp/my-creds", ""));
  ProcessSnapshot snapshot = processCache()->retrieve(1234, 1000, 1000, false);
  ASSERT_EQ(snapshot->getXrdLogin(), LoginIdentifier(1).getStringID());
  ASSERT_EQ(snapshot->getXrdCreds(),
            "xrd.k5ccname=/tmp/my-creds&xrd.wantprot=krb5,unix&xrdcl.secgid=1000&xrdcl.secuid=1000");
}

TEST_F(Krb5AuthF, UnixFallback)
{
  injectProcess(1234, 1, 1234, 1234, 9999, 0);
  ProcessSnapshot snapshot = processCache()->retrieve(1234, 1000, 1000, false);
  ASSERT_EQ(snapshot->getXrdLogin(), LoginIdentifier(1000, 1000, 1234,
            0).getStringID());
  ASSERT_EQ(snapshot->getXrdCreds(), "xrd.wantprot=unix");
}

TEST(UserCredentialFactory, BothKrb5AndX509) {
  CredentialConfig config;
  config.use_user_krb5cc = true;
  config.use_user_gsiproxy = true;
  config.tryKrb5First = true;
  config.use_user_sss = true;

  Environment env;
  env.push_back("KRB5CCNAME=/tmp/my-krb5-creds");
  env.push_back("X509_USER_PROXY=/tmp/my-x509-creds");

  JailIdentifier id = JailIdentifier::Make(5, 3);
  UserCredentialFactory factory(config);

  LogbookScope empty;
  SearchOrder searchOrder;
  std::string key;
  ASSERT_TRUE(factory.parseSingle(empty, "defaults", id, env, 9, 8, searchOrder));

  ASSERT_EQ(searchOrder.size(), 3u);
  ASSERT_EQ(searchOrder[0], UserCredentials::MakeSSS("", 9, 8, key));
  ASSERT_EQ(searchOrder[1], UserCredentials::MakeKrb5(id, "/tmp/my-krb5-creds", 9, 8, key));
  ASSERT_EQ(searchOrder[2], UserCredentials::MakeX509(id, "/tmp/my-x509-creds", 9, 8, key));

  // Now swap krb5 <-> x509 order
  config.tryKrb5First = false;
  factory = UserCredentialFactory(config);

  searchOrder.clear();
  ASSERT_TRUE(factory.parseSingle(empty, "defaults", id, env, 8, 9, searchOrder));
  // factory.addDefaultsFromEnv(id, env, 8, 9, searchOrder);

  ASSERT_EQ(searchOrder.size(), 3u);
  ASSERT_EQ(searchOrder[0], UserCredentials::MakeSSS("", 8, 9, key));
  ASSERT_EQ(searchOrder[1], UserCredentials::MakeX509(id, "/tmp/my-x509-creds", 8, 9, key));
  ASSERT_EQ(searchOrder[2], UserCredentials::MakeKrb5(id, "/tmp/my-krb5-creds", 8, 9, key));
}

TEST(UserCredentialFactory, JustKrb5) {
  CredentialConfig config;
  config.use_user_krb5cc = true;
  config.use_user_gsiproxy = false;
  config.use_user_sss = false;

  Environment env;
  env.push_back("KRB5CCNAME=FILE:/tmp/my-krb5-creds");
  env.push_back("X509_USER_PROXY=/tmp/my-x509-creds");

  JailIdentifier id = JailIdentifier::Make(5, 3);
  UserCredentialFactory factory(config);

  LogbookScope empty;
  SearchOrder searchOrder;
  std::string key;

  ASSERT_TRUE(factory.parseSingle(empty, "defaults", id, env, 12, 14, searchOrder));

  ASSERT_EQ(searchOrder.size(), 1u);
  ASSERT_EQ(searchOrder[0], UserCredentials::MakeKrb5(id, "/tmp/my-krb5-creds", 12, 14, key));
}

TEST(UserCredentialFactory, JustKrk5) {
  CredentialConfig config;
  config.use_user_krb5cc = true;
  config.use_user_gsiproxy = false;
  config.use_user_sss = false;

  Environment env;
  env.push_back("KRB5CCNAME=KEYRING:my-keyring");
  env.push_back("X509_USER_PROXY=/tmp/my-x509-creds");

  JailIdentifier id = JailIdentifier::Make(5, 3);
  UserCredentialFactory factory(config);

  LogbookScope empty;
  SearchOrder searchOrder;
  std::string key;
  ASSERT_TRUE(factory.parseSingle(empty, "defaults", id, env, 19, 15, searchOrder));

  ASSERT_EQ(searchOrder.size(), 1u);
  ASSERT_EQ(searchOrder[0], UserCredentials::MakeKrk5("KEYRING:my-keyring", 19, 15, key));
}

TEST(UserCredentialFactory, ParseSingleKrb5) {
  CredentialConfig config;
  config.use_user_krb5cc = true;

  JailIdentifier id = JailIdentifier::Make(2, 3);
  UserCredentialFactory factory(config);

  Environment env;
  LogbookScope empty;
  SearchOrder searchOrder;
  std::string key;

  ASSERT_TRUE(factory.parseSingle(empty, "krb:FILE:/some-file", id, env, 100, 101, searchOrder));
  ASSERT_EQ(searchOrder.size(), 1u);
  ASSERT_EQ(searchOrder[0], UserCredentials::MakeKrb5(id, "/some-file", 100, 101, key));

  searchOrder.clear();
  ASSERT_TRUE(factory.parseSingle(empty, "krb:/some-file-2", id, env, 100, 101, searchOrder));
  ASSERT_EQ(searchOrder.size(), 1u);
  ASSERT_EQ(searchOrder[0], UserCredentials::MakeKrb5(id, "/some-file-2", 100, 101, key));

  config.use_user_krb5cc = false;
  factory = UserCredentialFactory(config);

  searchOrder.clear();
  ASSERT_TRUE(factory.parseSingle(empty, "krb:FILE:/some-file", id, env, 100, 101, searchOrder));
  ASSERT_EQ(searchOrder.size(), 0u);
}

TEST(UserCredentialFactory, ParseSingleKrk5) {
  CredentialConfig config;
  config.use_user_krb5cc = true;

  JailIdentifier id = JailIdentifier::Make(2, 3);
  UserCredentialFactory factory(config);

  Environment env;
  LogbookScope empty;
  SearchOrder searchOrder;
  std::string key;
  ASSERT_TRUE(factory.parseSingle(empty, "krb:KEYRING:my-keyring", id, env, 100, 100, searchOrder));

  ASSERT_EQ(searchOrder.size(), 1u);
  ASSERT_EQ(searchOrder[0], UserCredentials::MakeKrk5("KEYRING:my-keyring", 100, 100, key));
}

TEST(UserCredentialFactory, ParseSingleX509) {
  CredentialConfig config;
  config.use_user_gsiproxy = true;

  JailIdentifier id = JailIdentifier::Make(2, 3);
  UserCredentialFactory factory(config);

  Environment env;
  LogbookScope empty;
  SearchOrder searchOrder;
  std::string key;

  ASSERT_TRUE(factory.parseSingle(empty, "x509:/tmp/my-gsi-creds", id, env, 200, 201, searchOrder));

  ASSERT_EQ(searchOrder.size(), 1u);
  ASSERT_EQ(searchOrder[0], UserCredentials::MakeX509(id, "/tmp/my-gsi-creds", 200, 201, key));
}

TEST(UserCredentialFactory, ParseEnv) {
  CredentialConfig config;
  config.use_user_krb5cc = true;

  JailIdentifier id = JailIdentifier::Make(2, 3);
  UserCredentialFactory factory(config);

  Environment env;
  env.push_back("KRB5CCNAME=/tmp-krbccname");
  env.push_back("EOS_FUSE_CREDS=krb:/tmp/first,krb:/tmp/second,defaults");

  LogbookScope empty;
  SearchOrder searchOrder = factory.parse(empty, id, env, 100, 100);
  std::string key;

  ASSERT_EQ(searchOrder.size(), 3u);
  ASSERT_EQ(searchOrder[0], UserCredentials::MakeKrb5(id, "/tmp/first", 100, 100, key));
  ASSERT_EQ(searchOrder[1], UserCredentials::MakeKrb5(id, "/tmp/second", 100, 100, key));
  ASSERT_EQ(searchOrder[2], UserCredentials::MakeKrb5(id, "/tmp-krbccname", 100, 100, key));

  env = {};
  env.push_back("KRB5CCNAME=/tmp-krbccname");
  env.push_back("EOS_FUSE_CREDS=krb:/tmp/first,krb:/tmp/second");
  searchOrder = factory.parse(empty, id, env, 100, 100);

  ASSERT_EQ(searchOrder.size(), 2u);
  ASSERT_EQ(searchOrder[0], UserCredentials::MakeKrb5(id, "/tmp/first", 100, 100, key));
  ASSERT_EQ(searchOrder[1], UserCredentials::MakeKrb5(id, "/tmp/second", 100, 100, key));
}
