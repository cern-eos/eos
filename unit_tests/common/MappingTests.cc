//------------------------------------------------------------------------------
// File: MappingTests.cc
// Author: Elvin-Alin Sindrilaru <esindril at cern dot ch>
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
#include "gmock/gmock.h"
#include "Namespace.hh"

#define IN_TEST_HARNESS
#include "common/Mapping.hh"
#undef IN_TEST_HARNESS

#include "common/UnixGroupsFetcher.hh"
#include "unit_tests/common/MappingTestFixture.hh"
#include <XrdAcc/XrdAccAuthorize.hh>
#include <XrdSec/XrdSecEntity.hh>
#include <XrdSec/XrdSecEntityAttr.hh>
#include <memory>
#include <pthread.h>
#include <pwd.h>
#include <unistd.h>

EOSCOMMONTESTING_BEGIN

namespace {
struct IdentityLookupResult {
  bool success{false};
};

void*
RunIdentityLookups(void* opaque)
{
  using namespace eos::common;
  auto* result = static_cast<IdentityLookupResult*>(opaque);
  int errc = 0;
  const std::string username = Mapping::UidToUserName(getuid(), errc);

  if (errc || username.empty()) {
    return nullptr;
  }

  const std::string groupname = Mapping::GidToGroupName(getgid(), errc);

  if (errc || groupname.empty()) {
    return nullptr;
  }

  const std::string missing_user = "eos-mapping-stack-regression-user";

  if (Mapping::UserNameToUid(missing_user, errc) != VirtualIdentity::kNobodyUid ||
      errc != EINVAL) {
    return nullptr;
  }

  const std::string missing_group = "eos-mapping-stack-regression-group";

  if (Mapping::GroupNameToGid(missing_group, errc) != VirtualIdentity::kNobodyGid ||
      errc != EINVAL) {
    return nullptr;
  }

  result->success = true;
  return nullptr;
}
} // namespace

TEST(Mapping, IdentityLookupsFitWorkerStack)
{
  using namespace eos::common;
  Mapping::Reset();
  pthread_attr_t attributes;
  ASSERT_EQ(0, pthread_attr_init(&attributes));
  ASSERT_EQ(0, pthread_attr_setstacksize(&attributes, 256 * 1024));
  pthread_t thread;
  IdentityLookupResult result;
  ASSERT_EQ(0, pthread_create(&thread, &attributes, RunIdentityLookups, &result));
  ASSERT_EQ(0, pthread_attr_destroy(&attributes));
  ASSERT_EQ(0, pthread_join(thread, nullptr));
  EXPECT_TRUE(result.success);
}

TEST(Mapping, CachedGidLookupFailurePreservesError)
{
  using namespace eos::common;
  Mapping::Reset();
  const gid_t gid = 123456789;
  const std::string fallback = std::to_string(gid);
  Mapping::gShardedNegativeGroupNameCache.store(gid,
                                                std::make_unique<std::string>(fallback));

  for (int attempt = 0; attempt < 2; ++attempt) {
    int errc = 0;
    EXPECT_EQ(fallback, Mapping::GidToGroupName(gid, errc));
    EXPECT_EQ(EINVAL, errc);
  }

  Mapping::Reset();
}

void FreeXrdSecEntity(XrdSecEntity* client)
{
  free(client->name);
  free(client->host);
  free(client->vorg);
  free(client->role);
  free(client->grps);
  free(client->caps);
  free(client->endorsements);
  free(client->moninfo);
  free(client->creds);
  free((char*)client->tident);
}

TEST(Mapping, ReduceTidentLargeUidLoginPrefix)
{
  using namespace eos::common;
  std::string wildcardtident;
  std::string myhost;
  const std::string tident = ".AAAPQkA.3372354:472@vl-ns-04.cern.ch";
  const std::string mytident = Mapping::ReduceTident(tident, wildcardtident, myhost);
  ASSERT_EQ(mytident, ".AAAPQkA@vl-ns-04.cern.ch");
  ASSERT_EQ(wildcardtident, "*@vl-ns-04.cern.ch");
  ASSERT_EQ(myhost, "vl-ns-04.cern.ch");
}

TEST(Mapping, ReduceTidentCombinedUidLoginPrefix)
{
  using namespace eos::common;
  std::string wildcardtident;
  std::string myhost;
  const std::string tident = "*AAPQk+g.3372354:472@vl-ns-04.cern.ch";
  const std::string mytident = Mapping::ReduceTident(tident, wildcardtident, myhost);
  ASSERT_EQ(mytident, "*AAPQk+g@vl-ns-04.cern.ch");
  ASSERT_EQ(wildcardtident, "*@vl-ns-04.cern.ch");
  ASSERT_EQ(myhost, "vl-ns-04.cern.ch");
}

TEST(Mapping, ReduceTidentLegacyLoginWithEarlyDot)
{
  using namespace eos::common;
  std::string wildcardtident;
  std::string myhost;
  const std::string tident = "foo.bar.3372354:472@vl-ns-04.cern.ch";
  const std::string mytident = Mapping::ReduceTident(tident, wildcardtident, myhost);
  ASSERT_EQ(mytident, "foo@vl-ns-04.cern.ch");
  ASSERT_EQ(wildcardtident, "*@vl-ns-04.cern.ch");
  ASSERT_EQ(myhost, "vl-ns-04.cern.ch");
}

TEST(Mapping, VidAssignOperator)
{
  using namespace eos::common;
  VirtualIdentity vid;
  vid.uid = 99;
  vid.gid = 99;
  vid.uid_string = "99";
  vid.gid_string = "99";
  vid.allowed_uids = {2, 3, 99};
  vid.allowed_gids = {2, 4, 99};
  vid.tident = "client:process_id:sockd_fd";
  vid.name = "dummy_user";
  vid.prot = "xrootd";
  vid.host = "localhost.localdomain";
  vid.grps = "some_random_grps";
  vid.role = "some_random_role";
  vid.dn = "some_random_dn";
  vid.geolocation = "some_random_geoloacation";
  vid.app = "some_random_app";
  vid.sudoer = true;
  VirtualIdentity copy_vid = vid;
  ASSERT_EQ(vid.uid, copy_vid.uid);
  ASSERT_EQ(vid.gid, copy_vid.gid);
  ASSERT_EQ(vid.uid_string, copy_vid.uid_string);
  ASSERT_EQ(vid.gid_string, copy_vid.gid_string);
  ASSERT_EQ(vid.allowed_uids, copy_vid.allowed_uids);
  ASSERT_EQ(vid.allowed_gids, copy_vid.allowed_gids);
  ASSERT_STREQ(vid.tident.c_str(), copy_vid.tident.c_str());
  ASSERT_STREQ(vid.name.c_str(),  copy_vid.name.c_str());
  ASSERT_STREQ(vid.prot.c_str(), copy_vid.prot.c_str());
  ASSERT_EQ(vid.host, copy_vid.host);
  ASSERT_EQ(vid.grps, copy_vid.grps);
  ASSERT_EQ(vid.role, copy_vid.role);
  ASSERT_EQ(vid.dn, copy_vid.dn);
  ASSERT_EQ(vid.geolocation, copy_vid.geolocation);
  ASSERT_EQ(vid.app, copy_vid.app);
  ASSERT_EQ(vid.sudoer, copy_vid.sudoer);
  ASSERT_TRUE(vid.HasUid(2));
  ASSERT_TRUE(copy_vid.HasUid(2));
  ASSERT_TRUE(vid.HasUid(3));
  ASSERT_TRUE(copy_vid.HasUid(3));
  ASSERT_TRUE(vid.HasUid(99));
  ASSERT_TRUE(copy_vid.HasUid(99));
  ASSERT_FALSE(vid.HasUid(4));
  ASSERT_FALSE(copy_vid.HasUid(4));
  ASSERT_TRUE(vid.HasGid(4));
  ASSERT_TRUE(copy_vid.HasGid(4));
  ASSERT_FALSE(vid.HasGid(3));
  ASSERT_FALSE(copy_vid.HasGid(3));
}

TEST(VirtualIdentity, IsLocalhost)
{
  VirtualIdentity vid;
  vid.host = "localhost";
  ASSERT_TRUE(vid.IsLocalhost());
  vid.host = "localhost6";
  ASSERT_TRUE(vid.IsLocalhost());
  vid.host = "localhost.localdomain";
  ASSERT_TRUE(vid.IsLocalhost());
  vid.host = "localhost6.localdomain6";
  ASSERT_TRUE(vid.IsLocalhost());
  vid.host = "pickles";
  ASSERT_FALSE(vid.IsLocalhost());
  vid.host = "asdf";
  ASSERT_FALSE(vid.IsLocalhost());
}


TEST(VirtualIdentity, HandleKEYS)
{
  using namespace eos::common;
  struct ScopedMappings {
    decltype(Mapping::gVirtualUidMap) uids;
    decltype(Mapping::gVirtualGidMap) gids;
    ScopedMappings()
    {
      uids.swap(Mapping::gVirtualUidMap);
      gids.swap(Mapping::gVirtualGidMap);
    }
    ~ScopedMappings()
    {
      uids.swap(Mapping::gVirtualUidMap);
      gids.swap(Mapping::gVirtualGidMap);
    }
  } scoped_mappings;
  const std::string secret_key = "xyz_my_secret_key_xyz";
  VirtualIdentity vid;
  auto EntityDeleter = [](XrdSecEntity * client) {
    FreeXrdSecEntity(client);
    delete client;
  };
  std::unique_ptr<XrdSecEntity, decltype(EntityDeleter)>
  client(new XrdSecEntity("gsi"), EntityDeleter);
  client->name = strdup("random");
  client->host = strdup("[::ffff:172.24.76.44]");
  client->vorg = strdup("cms cms cms");
  client->role = strdup("production NULL NULL NULL");
  client->grps = strdup("/cms /cms /cms/country /cms/country/us /cms/uscms");
  client->caps = nullptr;
  client->endorsements = strdup(secret_key.c_str());
  client->moninfo = nullptr;
  client->creds = nullptr;
  client->credslen = 0;
  client->ueid = 0xdead;
  client->addrInfo = nullptr;
  client->tident = strdup("http");
  client->pident = nullptr;
  client->sessvar = nullptr;
  client->uid = 0;
  client->gid = 0;
  // This is happenndin in the IdMap for sss/grpc/https
  vid.key = client->endorsements;
  // Add VOMS mapping
  const std::string_view uid_voms = "voms:\"/cms:production\":uid";
  const std::string_view gid_voms = "voms:\"/cms:production\":gid";
  // VOMS resolves the mapped UID through the host's password database.
  const uid_t voms_uid = getuid();
  const gid_t voms_gid = getgid();
  Mapping::gVirtualUidMap.emplace(uid_voms, voms_uid);
  Mapping::gVirtualGidMap.emplace(gid_voms, voms_gid);
  // Add specific EOS KEY mapping
  std::string uid_key = "https:\"key:abbabeefdeadabba\":uid";
  std::string gid_key = "https:\"key:abbabeefdeadabba\":gid";
  const uid_t mapped_uid = voms_uid == 32 ? 33 : 32;
  const gid_t mapped_gid = voms_gid == 32 ? 33 : 32;
  Mapping::gVirtualUidMap.emplace(uid_key, mapped_uid);
  Mapping::gVirtualGidMap.emplace(gid_key, mapped_gid);
  Mapping::HandleVOMS(client.get(), vid);
  ASSERT_EQ(voms_uid, vid.uid);
  ASSERT_EQ(voms_gid, vid.gid);
  // The key should not match so there should be no change
  Mapping::HandleKEYS(client.get(), vid);
  ASSERT_EQ(voms_uid, vid.uid);
  ASSERT_EQ(voms_gid, vid.gid);
  // Add a new key mapping to match the given endorsements
  uid_key = "https:\"key:";
  uid_key += secret_key;
  uid_key += "\":uid";
  gid_key = "https:\"key:";
  gid_key += secret_key;
  gid_key += "\":gid";
  Mapping::gVirtualUidMap.emplace(uid_key, mapped_uid);
  Mapping::gVirtualGidMap.emplace(gid_key, mapped_gid);
  // The key matches so the mapped identity should also match
  Mapping::HandleKEYS(client.get(), vid);
  ASSERT_EQ(mapped_uid, vid.uid);
  ASSERT_EQ(mapped_gid, vid.gid);
}

//------------------------------------------------------------------------------
// The token plugins, e.g. XrdSciTokens, put the user they map a token to in the
// "request.name" attribute of the entity. With a token, an empty value means
// that the token maps to no user: the client is nobody, even if it has a name
// of its own. Without a token, an empty value is no user name.
//------------------------------------------------------------------------------

//! Authorizes everything, like a token plugin that accepted the token
class AllowAllAuthz : public XrdAccAuthorize {
public:
  using XrdAccAuthorize::Access;

  XrdAccPrivs
  Access(const XrdSecEntity*, const char*, const Access_Operation, XrdOucEnv*) override
  {
    return XrdAccPriv_All;
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
};

//! Maps the https and ztn clients to the local user of their name, like
//! "eos vid enable https" and "eos vid enable ztn". The clients are named
//! "daemon", and the tokens map to "bin".
class TokenUserNameF : public MappingTestF {
protected:
  void
  SetUp() override
  {
    MappingTestF::SetUp();
    mUids.swap(Mapping::gVirtualUidMap);
    mGids.swap(Mapping::gVirtualGidMap);

    for (const std::string prot : {"https", "ztn"}) {
      Mapping::gVirtualUidMap[prot + ":\"<pwd>\":uid"] = 0;
      Mapping::gVirtualGidMap[prot + ":\"<pwd>\":gid"] = 0;
    }

    const struct passwd* pw = getpwnam("daemon");
    ASSERT_NE(nullptr, pw);
    mDaemonUid = pw->pw_uid;
    mDaemonGid = pw->pw_gid;
    pw = getpwnam("bin");
    ASSERT_NE(nullptr, pw);
    mBinUid = pw->pw_uid;
    mBinGid = pw->pw_gid;
  }

  void
  TearDown() override
  {
    mUids.swap(Mapping::gVirtualUidMap);
    mGids.swap(Mapping::gVirtualGidMap);
    MappingTestF::TearDown();
  }

  Mapping::VirtualUserMap_t mUids;
  Mapping::VirtualGroupMap_t mGids;
  uid_t mDaemonUid{0};
  gid_t mDaemonGid{0};
  uid_t mBinUid{0};
  gid_t mBinGid{0};
  std::string mName{"daemon"};
  std::string mTident{"daemon.1:2@client.example.org"};
};

TEST_F(TokenUserNameF, HttpsWithoutTokenEmptyUserNameMapsTheClientName)
{
  XrdSecEntity client("https");
  client.name = mName.data();
  client.tident = mTident.c_str();
  VirtualIdentity vid;
  // Without the attribute, the client maps with its own name
  Mapping::IdMap(&client, "", mTident.c_str(), vid, nullptr, AOP_Stat, "", false);
  ASSERT_EQ(mDaemonUid, vid.uid);
  ASSERT_EQ(mDaemonGid, vid.gid);
  ASSERT_TRUE(client.eaAPI->Add("request.name", "", true));
  Mapping::IdMap(&client, "", mTident.c_str(), vid, nullptr, AOP_Stat, "", false);
  EXPECT_EQ(mDaemonUid, vid.uid);
  EXPECT_EQ(mDaemonGid, vid.gid);
}

TEST_F(TokenUserNameF, HttpsTokenWithEmptyUserNameMapsToNobody)
{
  XrdSecEntity client("https");
  client.name = mName.data();
  client.tident = mTident.c_str();
  AllowAllAuthz authz;
  const char* env = "authz=Bearer%20token";
  VirtualIdentity vid;
  // The client maps to the user of the token, not to its own name
  ASSERT_TRUE(client.eaAPI->Add("request.name", "bin", true));
  Mapping::IdMap(&client, env, mTident.c_str(), vid, &authz, AOP_Stat, "", false);
  ASSERT_EQ(mBinUid, vid.uid);
  ASSERT_EQ(mBinGid, vid.gid);
  ASSERT_TRUE(client.eaAPI->Add("request.name", "", true));
  Mapping::IdMap(&client, env, mTident.c_str(), vid, &authz, AOP_Stat, "", false);
  EXPECT_EQ(VirtualIdentity::kNobodyUid, vid.uid);
  EXPECT_EQ(VirtualIdentity::kNobodyGid, vid.gid);
}

TEST_F(TokenUserNameF, ZtnTokenWithEmptyUserNameMapsToNobody)
{
  XrdSecEntity client("ztn");
  std::string creds{"token"};
  client.name = mName.data();
  client.tident = mTident.c_str();
  client.creds = creds.data();
  client.credslen = creds.size();
  AllowAllAuthz authz;
  VirtualIdentity vid;
  // The client maps to the user of the token, not to its own name
  ASSERT_TRUE(client.eaAPI->Add("request.name", "bin", true));
  Mapping::IdMap(&client, "", mTident.c_str(), vid, &authz, AOP_Stat, "", false);
  ASSERT_EQ(mBinUid, vid.uid);
  ASSERT_EQ(mBinGid, vid.gid);
  ASSERT_TRUE(client.eaAPI->Add("request.name", "", true));
  Mapping::IdMap(&client, "", mTident.c_str(), vid, &authz, AOP_Stat, "", false);
  EXPECT_EQ(VirtualIdentity::kNobodyUid, vid.uid);
  EXPECT_EQ(VirtualIdentity::kNobodyGid, vid.gid);
}

//------------------------------------------------------------------------------
// With a token, only the token gives the identity: the VOMS groups of the
// client's certificate must not change it. Without a token, they map the
// client as before.
//------------------------------------------------------------------------------
class TokenVomsF : public TokenUserNameF {
protected:
  void
  SetUp() override
  {
    TokenUserNameF::SetUp();
    const struct passwd* pw = getpwnam("lp");
    ASSERT_NE(nullptr, pw);
    mVomsUid = pw->pw_uid;
    mVomsGid = pw->pw_gid;
    // Restored by TokenUserNameF::TearDown
    Mapping::gVirtualUidMap["voms:\"/cms:production\":uid"] = mVomsUid;
    Mapping::gVirtualGidMap["voms:\"/cms:production\":gid"] = mVomsGid;
  }

  //! Client "daemon" with the VOMS group /cms and the role production
  void
  SetVomsClient(XrdSecEntity& client)
  {
    client.name = mName.data();
    client.tident = mTident.c_str();
    client.grps = mGrps.data();
    client.role = mRole.data();
  }

  uid_t mVomsUid{0};
  gid_t mVomsGid{0};
  std::string mGrps{"/cms"};
  std::string mRole{"production"};
  const char* mBearer = "authz=Bearer%20token";
};

TEST_F(TokenVomsF, HttpsWithoutTokenMapsTheVomsUser)
{
  XrdSecEntity client("https");
  SetVomsClient(client);
  VirtualIdentity vid;
  Mapping::IdMap(&client, "", mTident.c_str(), vid, nullptr, AOP_Stat, "", false);
  EXPECT_EQ(mVomsUid, vid.uid);
  EXPECT_EQ(mVomsGid, vid.gid);
}

TEST_F(TokenVomsF, HttpsTokenUserIsNotReplacedByTheVomsUser)
{
  XrdSecEntity client("https");
  SetVomsClient(client);
  AllowAllAuthz authz;
  ASSERT_TRUE(client.eaAPI->Add("request.name", "bin", true));
  VirtualIdentity vid;
  Mapping::IdMap(&client, mBearer, mTident.c_str(), vid, &authz, AOP_Stat, "", false);
  EXPECT_EQ(mBinUid, vid.uid);
  EXPECT_EQ(mBinGid, vid.gid);
}

TEST_F(TokenVomsF, HttpsTokenWithEmptyUserNameIsNotTheVomsUser)
{
  XrdSecEntity client("https");
  SetVomsClient(client);
  AllowAllAuthz authz;
  ASSERT_TRUE(client.eaAPI->Add("request.name", "", true));
  VirtualIdentity vid;
  Mapping::IdMap(&client, mBearer, mTident.c_str(), vid, &authz, AOP_Stat, "", false);
  EXPECT_EQ(VirtualIdentity::kNobodyUid, vid.uid);
  EXPECT_EQ(VirtualIdentity::kNobodyGid, vid.gid);
}

struct MockGroupListProvider : UnixGroupListFetcher {
  MOCK_METHOD(std::vector<gid_t>, getGroups, (const std::string&, gid_t), (override));
};

class SecondaryGroupF: public ::testing::Test {
protected:
  void SetUp() override {
    Mapping::gSecondaryGroups = true;
    Mapping::gGroupsFetcher = std::make_unique<MockGroupListProvider>();
  }
  void TearDown() override {
    Mapping::gGroupsFetcher.reset();
  }
};

void printvid(const VirtualIdentity& vid) {
  std::cerr << vid.getTrace() << "\nallowed gids: ";
  for (const auto& gid: vid.allowed_gids) {
    std::cerr << gid << ",";
  }
}

TEST_F(SecondaryGroupF, AddSecondaryGroups)
{
  VirtualIdentity vid;
  const std::string name = "dummy_user";
  const gid_t gid = 99;
  ASSERT_NE(Mapping::gGroupsFetcher, nullptr);
  MockGroupListProvider* mock = dynamic_cast<MockGroupListProvider*>(Mapping::gGroupsFetcher.get());
  ASSERT_NE(mock, nullptr);

  EXPECT_CALL(*mock, getGroups(name, gid))
  .WillOnce(::testing::Return(std::vector<gid_t>{2, 3, 4}));
  Mapping::addSecondaryGroups(vid, name, gid);
  printvid(vid);
  EXPECT_TRUE(vid.HasGid(2));
  EXPECT_TRUE(vid.HasGid(3));
  EXPECT_TRUE(vid.HasGid(4));
  EXPECT_FALSE(vid.HasGid(5));
}

EOSCOMMONTESTING_END
