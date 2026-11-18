//------------------------------------------------------------------------------
// File: TokenAuthzChainTests.cc
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2026 CERN/Switzerland                                  *
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
#include "common/StringConversion.hh"
#include "common/SymKeys.hh"
#include "common/token/EosTok.hh"
#include "unit_tests/mgm/FakeSciTokensAuthz.hh"
#include "gtest/gtest.h"
#include <XrdSec/XrdSecEntity.hh>
#include <ctime>
#include <pwd.h>
#include <string>

using eos::common::Mapping;
using eos::common::VirtualIdentity;

//------------------------------------------------------------------------------
// Mapping::IdMap with the token authorization chain of the MGM: XrdSciTokens,
// then XrdMgmAuthz. The entity of a client lives as long as its connection,
// and XrdSciTokens writes the user of a token in it. A request must map only
// with its own credentials: with a token, the token's user or nobody, never
// the user that a token gave to an earlier request on the same connection,
// nor the client's own name.
//
// The token "valid-token" covers the paths under /eos/in/ and maps to daemon.
// The clients that have a name of their own are named bin.
//------------------------------------------------------------------------------
class TokenAuthzChainF : public ::testing::Test {
protected:
  void
  SetUp() override
  {
    Mapping::Init();
    mUids.swap(Mapping::gVirtualUidMap);
    mGids.swap(Mapping::gVirtualGidMap);

    // Map the clients to the local user of their name, like
    // "eos vid enable https" and "eos vid enable ztn"
    for (const std::string prot : {"https", "ztn", "unix"}) {
      Mapping::gVirtualUidMap[prot + ":\"<pwd>\":uid"] = 0;
      Mapping::gVirtualGidMap[prot + ":\"<pwd>\":gid"] = 0;
    }

    const struct passwd* pw = getpwnam("daemon");
    ASSERT_NE(nullptr, pw);
    mTokenUid = pw->pw_uid;
    mTokenGid = pw->pw_gid;
    pw = getpwnam("lp");
    ASSERT_NE(nullptr, pw);
    mForcedUid = pw->pw_uid;
    mForcedGid = pw->pw_gid;
  }

  //! Map every unix client to lp, like
  //! eos vid set map -unix "<pwd>" vuid:<lp> vgid:<lp>
  void
  ForceUnixMapping()
  {
    Mapping::gVirtualUidMap["unix:\"<pwd>\":uid"] = mForcedUid;
    Mapping::gVirtualGidMap["unix:\"<pwd>\":gid"] = mForcedGid;
  }

  void
  TearDown() override
  {
    mUids.swap(Mapping::gVirtualUidMap);
    mGids.swap(Mapping::gVirtualGidMap);
    Mapping::Reset();
  }

  VirtualIdentity
  Map(XrdSecEntity& client, const std::string& env, const std::string& path,
      XrdAccAuthorize* authz = nullptr)
  {
    VirtualIdentity vid;
    Mapping::IdMap(&client, env.c_str(), mTident.c_str(), vid, authz ? authz : &mAuthz,
                   AOP_Read, path, false);
    return vid;
  }

  //! An EOS token for the paths under /eos/in/, signed with the key that
  //! IdMap verifies it with. An empty owner gives a token without an owner,
  //! which only an admin can create.
  static std::string
  EosToken(const std::string& owner)
  {
    eos::common::EosTok token;
    token.SetPath("/eos/in/", true);
    token.SetPermission("rx");

    if (!owner.empty()) {
      token.SetOwner(owner);
      token.SetGroup(owner);
    }

    token.SetExpires(time(nullptr) + 300);
    token.SetGeneration(eos::common::EosTok::sTokenGeneration);
    eos::common::SymKey* symkey = eos::common::gSymKeyStore.GetCurrentKey();
    return token.Write(symkey ? symkey->GetKey64() : "0123457890defaultkey");
  }

  void
  ExpectToken(const VirtualIdentity& vid)
  {
    EXPECT_EQ(mTokenUid, vid.uid);
    EXPECT_EQ(mTokenGid, vid.gid);
  }

  void
  ExpectNobody(const VirtualIdentity& vid)
  {
    EXPECT_EQ(VirtualIdentity::kNobodyUid, vid.uid);
    EXPECT_EQ(VirtualIdentity::kNobodyGid, vid.gid);
  }

  void
  ExpectForced(const VirtualIdentity& vid)
  {
    EXPECT_EQ(mForcedUid, vid.uid);
    EXPECT_EQ(mForcedGid, vid.gid);
  }

  Mapping::VirtualUserMap_t mUids;
  Mapping::VirtualGroupMap_t mGids;
  FakeSciTokensAuthz mAuthz{"valid-token", "/eos/in/", "daemon"};
  uid_t mTokenUid{0};
  gid_t mTokenGid{0};
  uid_t mForcedUid{0};
  gid_t mForcedGid{0};
  std::string mName{"bin"};
  std::string mTident{"client.1:2@client.example.org"};
  const std::string mIn{"/eos/in/f"};
  const std::string mOut{"/eos/out/f"};
  const std::string mValid{"authz=Bearer%20valid-token"};
  const std::string mInvalid{"authz=Bearer%20invalid-token"};
};

TEST_F(TokenAuthzChainF, HttpsInvalidTokenAfterAValidTokenIsNobody)
{
  XrdSecEntity client("https");
  client.tident = mTident.c_str();
  ExpectToken(Map(client, mValid, mIn));
  ExpectNobody(Map(client, mInvalid, mOut));
}

// XrdSciTokens passes a token on a path outside its scope with a new entity
TEST_F(TokenAuthzChainF, HttpsTokenOutsideItsScopeAfterAValidTokenIsNobody)
{
  XrdSecEntity client("https");
  client.tident = mTident.c_str();
  ExpectToken(Map(client, mValid, mIn));
  ExpectNobody(Map(client, mValid, mOut));
}

// XrdMgmAuthz allows a client that has a name of its own, e.g. from its
// certificate, but with a token only the token gives the identity.
TEST_F(TokenAuthzChainF, HttpsInvalidTokenOfAClientWithANameIsNobody)
{
  XrdSecEntity client("https");
  client.name = mName.data();
  client.tident = mTident.c_str();
  ExpectNobody(Map(client, mInvalid, mIn));
}

// With a token, a unix client maps to the token's user. Without a user from
// the token, it maps as "nobody" by the unix rules, which may force every
// unix client to one local user.
TEST_F(TokenAuthzChainF, UnixInvalidTokenAfterAValidTokenIsNobody)
{
  XrdSecEntity client("unix");
  client.name = mName.data();
  client.tident = mTident.c_str();
  ExpectToken(Map(client, mValid, mIn));
  ExpectNobody(Map(client, mInvalid, mIn));
}

TEST_F(TokenAuthzChainF, UnixInvalidTokenGetsTheForcedUnixMapping)
{
  ForceUnixMapping();
  XrdSecEntity client("unix");
  client.name = mName.data();
  client.tident = mTident.c_str();
  ExpectForced(Map(client, mInvalid, mIn));
}

// The user of a token is not subject to the forced unix mapping
TEST_F(TokenAuthzChainF, UnixInvalidTokenAfterAValidTokenGetsTheForcedUnixMapping)
{
  ForceUnixMapping();
  XrdSecEntity client("unix");
  client.name = mName.data();
  client.tident = mTident.c_str();
  ExpectToken(Map(client, mValid, mIn));
  ExpectForced(Map(client, mInvalid, mIn));
}

// A token that authorizes the path but maps to no user gives no user name
TEST_F(TokenAuthzChainF, UnixTokenWithoutUserGetsTheForcedUnixMapping)
{
  ForceUnixMapping();
  FakeSciTokensAuthz no_user("valid-token", "/eos/in/", "");
  XrdSecEntity client("unix");
  client.name = mName.data();
  client.tident = mTident.c_str();
  ExpectForced(Map(client, mValid, mIn, &no_user));
}

// The token of a ztn connection stops validating, e.g. when it expires
TEST_F(TokenAuthzChainF, ZtnTokenThatNoLongerValidatesIsNobody)
{
  XrdSecEntity client("ztn");
  std::string creds{"valid-token"};
  client.name = mName.data();
  client.tident = mTident.c_str();
  client.creds = creds.data();
  client.credslen = creds.size();
  ExpectToken(Map(client, "", mIn));
  creds = "invalid-token";
  client.creds = creds.data();
  client.credslen = creds.size();
  ExpectNobody(Map(client, "", mIn));
}

// XrdMgmAuthz allows every EOS token, and the token's owner is the identity
TEST_F(TokenAuthzChainF, HttpsEosTokenMapsToItsOwner)
{
  XrdSecEntity client("https");
  client.name = mName.data();
  client.tident = mTident.c_str();
  const std::string env =
      "authz=Bearer%20" +
      eos::common::StringConversion::curl_default_escaped(EosToken("daemon"));
  ExpectToken(Map(client, env, mIn));
}

TEST_F(TokenAuthzChainF, HttpsEosTokenWithoutOwnerIsNobody)
{
  XrdSecEntity client("https");
  client.name = mName.data();
  client.tident = mTident.c_str();
  const std::string env =
      "authz=Bearer%20" +
      eos::common::StringConversion::curl_default_escaped(EosToken(""));
  ExpectNobody(Map(client, env, mIn));
}
