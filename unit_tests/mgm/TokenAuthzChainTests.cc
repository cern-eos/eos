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
#include <XrdAcc/XrdAccAuthorize.hh>
#include <XrdSec/XrdSecEntity.hh>
#include <ctime>
#include <pwd.h>
#include <string>

using eos::common::Mapping;
using eos::common::VirtualIdentity;

namespace {
//------------------------------------------------------------------------------
//! A token authorization chain that refuses every request, like XrdSciTokens
//! with "onmissing = deny" in its configuration. It counts the requests.
//------------------------------------------------------------------------------
class RefusingAuthz : public XrdAccAuthorize {
public:
  using XrdAccAuthorize::Access;

  XrdAccPrivs
  Access(const XrdSecEntity*, const char*, const Access_Operation, XrdOucEnv*) override
  {
    ++mCalls;
    return XrdAccPriv_None;
  }

  int
  Audit(const int, const XrdSecEntity*, const char*, const Access_Operation,
        XrdOucEnv*) override
  {
    return 0;
  }

  int
  Test(const XrdAccPrivs, const Access_Operation) override
  {
    return 0;
  }

  int
  Calls() const
  {
    return mCalls;
  }

private:
  int mCalls{0};
};
} // namespace

//------------------------------------------------------------------------------
// Mapping::IdMap with the token authorization chain of the MGM: XrdSciTokens,
// then XrdMgmAuthz. The entity of a client lives as long as its connection,
// and XrdSciTokens writes the user of a token in it. A request must map only
// with its own credentials: with a token, the token's user or nobody, never
// the user that a token gave to an earlier request on the same connection,
// nor the client's own name. An EOS token without an owner is the exception:
// it keeps the client's own identity. An EOS token never goes to the chain:
// IdMap checks it itself.
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
    pw = getpwnam("bin");
    ASSERT_NE(nullptr, pw);
    mClientUid = pw->pw_uid;
    mClientGid = pw->pw_gid;
    pw = getpwnam("adm");
    ASSERT_NE(nullptr, pw);
    mVomsUid = pw->pw_uid;
    mVomsGid = pw->pw_gid;
    pw = getpwnam("mail");
    ASSERT_NE(nullptr, pw);
    mKeyUid = pw->pw_uid;
    mKeyGid = pw->pw_gid;
  }

  //! Map the key of a gateway to mail, like
  //! eos vid set map -<prot> key:<key> vuid:<mail> vgid:<mail>
  void
  SetKey(const std::string& prot)
  {
    Mapping::gVirtualUidMap[prot + ":\"key:" + mKey + "\":uid"] = mKeyUid;
    Mapping::gVirtualGidMap[prot + ":\"key:" + mKey + "\":gid"] = mKeyGid;
  }

  //! Give the client the VOMS group /cms with the role production, and map it
  //! to adm, like eos vid set map -voms "/cms:production" vuid:<adm> vgid:<adm>
  void
  SetVoms(XrdSecEntity& client)
  {
    client.grps = mGrps.data();
    client.role = mRole.data();
    Mapping::gVirtualUidMap["voms:\"/cms:production\":uid"] = mVomsUid;
    Mapping::gVirtualGidMap["voms:\"/cms:production\":gid"] = mVomsGid;
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

  //! The client's own name, bin
  void
  ExpectClient(const VirtualIdentity& vid)
  {
    EXPECT_EQ(mClientUid, vid.uid);
    EXPECT_EQ(mClientGid, vid.gid);
  }

  void
  ExpectVoms(const VirtualIdentity& vid)
  {
    EXPECT_EQ(mVomsUid, vid.uid);
    EXPECT_EQ(mVomsGid, vid.gid);
  }

  //! The user of the gateway key, mail
  void
  ExpectKey(const VirtualIdentity& vid)
  {
    EXPECT_EQ(mKeyUid, vid.uid);
    EXPECT_EQ(mKeyGid, vid.gid);
    EXPECT_TRUE(vid.gateway);
  }

  //! The "authz" CGI of an HTTPS request with this token in its Authorization
  //! header, as HttpServer passes it to IdMap
  static std::string
  BearerEnv(const std::string& token)
  {
    return "authz=Bearer%20" + eos::common::StringConversion::curl_default_escaped(token);
  }

  Mapping::VirtualUserMap_t mUids;
  Mapping::VirtualGroupMap_t mGids;
  FakeSciTokensAuthz mAuthz{"valid-token", "/eos/in/", "daemon"};
  uid_t mTokenUid{0};
  gid_t mTokenGid{0};
  uid_t mForcedUid{0};
  gid_t mForcedGid{0};
  uid_t mClientUid{0};
  gid_t mClientGid{0};
  uid_t mVomsUid{0};
  gid_t mVomsGid{0};
  uid_t mKeyUid{0};
  gid_t mKeyGid{0};
  std::string mKey{"gateway-key"};
  std::string mGrps{"/cms"};
  std::string mRole{"production"};
  std::string mName{"bin"};
  std::string mTident{"client.1:2@client.example.org"};
  const std::string mIn{"/eos/in/f"};
  const std::string mOut{"/eos/out/f"};
  const std::string mValid{"authz=Bearer%20valid-token"};
  const std::string mInvalid{"authz=Bearer%20invalid-token"};
  //! The same token in the URL: HttpServer passes the decoded query to IdMap
  const std::string mUrlValid{"authz=valid-token"};
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

// The owner of an EOS token is the identity
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

// An EOS token without an owner keeps the identity of the client that presents
// it, on every transport: the token only gives its permission on its paths
// (doc/diopside/manual/using.rst, "dynamic" user tokens).
TEST_F(TokenAuthzChainF, HttpsEosTokenWithoutOwnerMapsTheClientName)
{
  XrdSecEntity client("https");
  client.name = mName.data();
  client.tident = mTident.c_str();
  ExpectClient(Map(client, BearerEnv(EosToken("")), mIn));
}

TEST_F(TokenAuthzChainF, HttpsEosTokenWithoutOwnerMapsTheVomsUser)
{
  XrdSecEntity client("https");
  client.name = mName.data();
  client.tident = mTident.c_str();
  SetVoms(client);
  ExpectVoms(Map(client, BearerEnv(EosToken("")), mIn));
}

// Without a name of its own, e.g. without a client certificate, the client is
// nobody
TEST_F(TokenAuthzChainF, HttpsEosTokenWithoutOwnerOfAClientWithoutANameIsNobody)
{
  XrdSecEntity client("https");
  client.tident = mTident.c_str();
  ExpectNobody(Map(client, BearerEnv(EosToken("")), mIn));
}

//------------------------------------------------------------------------------
// An EOS token is EOS's own: IdMap checks it, and the authz plugins never see
// it. A chain that refuses every request does not change how it maps. A WLCG
// token or a macaroon still goes to the plugins.
//------------------------------------------------------------------------------
TEST_F(TokenAuthzChainF, HttpsEosTokenWithoutOwnerIsNotGivenToTheAuthzPlugins)
{
  RefusingAuthz refusing;
  XrdSecEntity client("https");
  client.name = mName.data();
  client.tident = mTident.c_str();
  ExpectClient(Map(client, BearerEnv(EosToken("")), mIn, &refusing));
  EXPECT_EQ(0, refusing.Calls());
}

TEST_F(TokenAuthzChainF, HttpsEosTokenWithAnOwnerIsNotGivenToTheAuthzPlugins)
{
  RefusingAuthz refusing;
  XrdSecEntity client("https");
  client.name = mName.data();
  client.tident = mTident.c_str();
  ExpectToken(Map(client, BearerEnv(EosToken("daemon")), mIn, &refusing));
  EXPECT_EQ(0, refusing.Calls());
}

TEST_F(TokenAuthzChainF, HttpsTokenThatTheAuthzPluginsRefuseIsNobody)
{
  RefusingAuthz refusing;
  XrdSecEntity client("https");
  client.name = mName.data();
  client.tident = mTident.c_str();
  ExpectNobody(Map(client, mValid, mIn, &refusing));
  EXPECT_EQ(1, refusing.Calls());
}

//------------------------------------------------------------------------------
// Over HTTPS, a token can also come in the URL, as "authz=<token>" without the
// "Bearer%20" prefix. It maps like a token in the Authorization header: the
// token's user or nobody, never the name or the VOMS groups of the client's
// certificate. An EOS token keeps the client's own identity there too.
//------------------------------------------------------------------------------
TEST_F(TokenAuthzChainF, HttpsTokenInTheUrlMapsToItsUser)
{
  XrdSecEntity client("https");
  client.name = mName.data();
  client.tident = mTident.c_str();
  ExpectToken(Map(client, mUrlValid, mIn));
}

TEST_F(TokenAuthzChainF, HttpsTokenInTheUrlOfAClientWithoutANameMapsToItsUser)
{
  XrdSecEntity client("https");
  client.tident = mTident.c_str();
  ExpectToken(Map(client, mUrlValid, mIn));
}

TEST_F(TokenAuthzChainF, HttpsTokenInTheUrlOutsideItsScopeIsNobody)
{
  XrdSecEntity client("https");
  client.name = mName.data();
  client.tident = mTident.c_str();
  ExpectNobody(Map(client, mUrlValid, mOut));
}

TEST_F(TokenAuthzChainF, HttpsTokenInTheUrlIsNotReplacedByTheVomsUser)
{
  XrdSecEntity client("https");
  client.name = mName.data();
  client.tident = mTident.c_str();
  SetVoms(client);
  ExpectToken(Map(client, mUrlValid, mIn));
}

TEST_F(TokenAuthzChainF, HttpsEosTokenWithoutOwnerInTheUrlMapsTheClientName)
{
  XrdSecEntity client("https");
  client.name = mName.data();
  client.tident = mTident.c_str();
  ExpectClient(Map(client, "authz=" + EosToken(""), mIn));
}

TEST_F(TokenAuthzChainF, HttpsEosTokenInTheUrlIsNotGivenToTheAuthzPlugins)
{
  RefusingAuthz refusing;
  XrdSecEntity client("https");
  client.name = mName.data();
  client.tident = mTident.c_str();
  ExpectClient(Map(client, "authz=" + EosToken(""), mIn, &refusing));
  EXPECT_EQ(0, refusing.Calls());
}

// An EOS token as the path of the request: HttpServer gives IdMap the token
// and no path
TEST_F(TokenAuthzChainF, HttpsEosTokenPathIsNotGivenToTheAuthzPlugins)
{
  RefusingAuthz refusing;
  XrdSecEntity client("https");
  client.name = mName.data();
  client.tident = mTident.c_str();
  const VirtualIdentity vid = Map(client, "authz=" + EosToken(""), "", &refusing);
  ExpectClient(vid);
  ASSERT_NE(nullptr, vid.token);
  EXPECT_TRUE(vid.token->Valid());
  EXPECT_EQ(0, refusing.Calls());
}

//------------------------------------------------------------------------------
// Gateways keep their key mappings. HttpServer maps the key of an HTTPS
// gateway, from its x-gateway-authorization header, without the opaque data
// of the request. Over sss, a token in the request does not replace the
// mapping of the key.
//------------------------------------------------------------------------------
TEST_F(TokenAuthzChainF, HttpsGatewayKeyMapsToTheUserOfTheKey)
{
  SetKey("https");
  XrdSecEntity client("https");
  client.name = mName.data();
  client.tident = mTident.c_str();
  client.endorsements = mKey.data();
  VirtualIdentity vid;
  Mapping::IdMap(&client, "", "https.0:0@client.example.org", vid);
  ExpectKey(vid);
}

TEST_F(TokenAuthzChainF, SssGatewayKeyWithATokenMapsToTheUserOfTheKey)
{
  // eos vid add gateway client.example.org sss
  Mapping::gVirtualUidMap["tident:\"sss@client.example.org\":uid"] = 0;
  Mapping::gVirtualGidMap["tident:\"sss@client.example.org\":gid"] = 0;
  SetKey("sss");
  XrdSecEntity client("sss");
  client.name = mName.data();
  client.tident = mTident.c_str();
  client.endorsements = mKey.data();
  ExpectKey(Map(client, mUrlValid, mIn));
  ExpectKey(Map(client, mValid, mIn));
}
