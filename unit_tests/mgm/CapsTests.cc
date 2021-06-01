//------------------------------------------------------------------------------
// File: CapsTests.cc
// Author: Abhishek Lekshmanan <abhishek.lekshmanan@cern.ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2021 CERN/Switzerland                                  *
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
#include "mgm/FuseServer/Caps.hh"
#include "mgm/XrdMgmOfs.hh"

using namespace eos::mgm::FuseServer;

class FakeStats {
public:
  template <typename... Args>
  void Add(Args&&... args) {
  }
};

class FakeXrdMgmOFS: public XrdMgmOfs
{
public:
  FakeXrdMgmOFS(XrdSysError* lp) : XrdMgmOfs(lp)
  {
  }
  ~FakeXrdMgmOFS() {
    mDoneOrderlyShutdown = true;
  }
  // TODO shadow + mock?
  FakeStats MgmStats;
};

class EnvMgr {
  std::unordered_map<std::string, char*> env;
  std::vector<std::string> keys;
public:
  void save_env(const char* key) {
    env[key]=getenv(key);
  }

  void load_env(const char* key) {
    if(auto kv = env.find(key);
       kv != env.end()) {
      kv->second ? setenv(key, kv->second, 1) : unsetenv(key);
    }
  }

  void modify_env(const char* key, const char* val = "0", int force = 1)
  {
    setenv(key, val, force);
  }

  EnvMgr(std::vector<std::string>&& _keys) : keys(std::move(_keys)) {
    std::for_each(keys.begin(), keys.end(), [this](const std::string& key) { save_env(key.c_str()); });
    std::for_each(keys.begin(), keys.end(), [this](const std::string& key) { modify_env(key.c_str()); });
  }

  ~EnvMgr() {
    std::for_each(keys.begin(), keys.end(), [this](const std::string& key) { load_env(key.c_str()); });
  }
};

// use a fixture, we'll retain the same Caps across the various tests
class CapsTest : public ::testing::Test
{
protected:
  // TODO: When you update gtest remember to change this to SetUpTestSuite
  // We need these resources shared across all the test suites!
  static void SetUpTestCase() {
    // We're doing this because initializing the public vars like the http port
    // won't yet help our case because the base class constructor is passed which already
    // sets defaults for these. We want to avoid construction of the service objects which
    // happens for 0 values.
    test_env = new EnvMgr({"EOS_MGM_HTTP_PORT", "EOS_MGM_GRPC_PORT"});
    fake_sys_error = new XrdSysError(nullptr, "fake");
    fake_ofs = new FakeXrdMgmOFS(fake_sys_error);
    gOFS = fake_ofs;
  }

  static void TearDownTestCase() {
    delete fake_sys_error;
    delete fake_ofs;
    delete test_env;
  }

  static XrdSysError* fake_sys_error;
  static XrdMgmOfs* fake_ofs;
  static EnvMgr* test_env;
  Caps mCaps;
};

XrdSysError* CapsTest::fake_sys_error = nullptr;
XrdMgmOfs* CapsTest::fake_ofs = nullptr;
EnvMgr* CapsTest::test_env = nullptr;


eos::fusex::cap make_cap(int id,
                         std::string clientid,
                         std::string authid,
                         uint64_t vtime = 0)
{
  eos::fusex::cap c;
  c.set_id(id);
  c.set_clientid(std::move(clientid));
  c.set_authid(std::move(authid));
  vtime ? c.set_vtime(vtime) :
          c.set_vtime(static_cast<uint64_t>(time(nullptr)));
  return c;
}

eos::common::VirtualIdentity make_vid(uid_t uid, gid_t gid)
{
  eos::common::VirtualIdentity vid;
  vid.uid = uid;
  vid.gid = gid;
  return vid;
}

TEST_F(CapsTest, EmptyCapsInit) {
  EXPECT_EQ(mCaps.ncaps(), 0);
}

TEST_F(CapsTest, StoreBasic) {
  eos::common::VirtualIdentity vid;
  eos::fusex::cap c;
  mCaps.Store(c,&vid);
  EXPECT_EQ(mCaps.ncaps(), 1);
}

TEST_F(CapsTest, StoreUpdate) {
  auto vid1 = make_vid(1234, 1234);
  auto c1 = make_cap(123,"cid1","authid1");
  mCaps.Store(c1,&vid1);
  EXPECT_EQ(mCaps.ncaps(), 1);
  std::string authid {"authid1"};
  auto k = mCaps.Get(authid);
  EXPECT_EQ(k->id(),123);
  EXPECT_EQ(k->clientid(), "cid1");

  // now update this cap
  c1.set_clientid("clientid_1");
  mCaps.Store(c1,&vid1);
  EXPECT_EQ(mCaps.ncaps(), 1);

  auto k2 = mCaps.Get(authid);
  EXPECT_EQ(k2->id(),123);
  EXPECT_EQ(k2->clientid(), "clientid_1");
}

TEST_F(CapsTest, StoreUpdateClientID) {
  auto vid1 = make_vid(1234, 1234);
  auto c1 = make_cap(123,"cid1","authid1");
  mCaps.Store(c1,&vid1);
  EXPECT_EQ(mCaps.ncaps(), 1);
  std::string authid {"authid1"};

  auto k = mCaps.Get(authid);
  EXPECT_EQ(k->id(),123);
  EXPECT_EQ(k->clientid(), "cid1");
  // Test the 3 different views
  auto& client_caps = mCaps.ClientCaps();
  auto& ino_caps = mCaps.ClientInoCaps();
  const auto& mcaps = mCaps.GetCaps();

  EXPECT_EQ(client_caps["cid1"].count("authid1"), 1);
  EXPECT_EQ(ino_caps["cid1"][123].count("authid1"),1);
  const auto it = mcaps.find("authid1");
  EXPECT_EQ(it->second, k);

  //EXPECT_EQ(mcaps["authid1"], k);
  // now update this cap
  c1.set_clientid("clientid_1");
  // If only the clientid is updated without changing the id the other views do
  //not get deleted
  mCaps.Store(c1,&vid1);
  EXPECT_EQ(mCaps.ncaps(), 1);

  auto k2 = mCaps.Get(authid);
  EXPECT_EQ(k2->id(),123);
  EXPECT_EQ(k2->clientid(), "clientid_1");

  EXPECT_EQ(client_caps["cid1"].count("authid1"), 1);
  EXPECT_EQ(ino_caps["cid1"][123].count("authid1"),1);
  // now check the updated values
  EXPECT_EQ(client_caps["clientid_1"].count("authid1"), 1);
  EXPECT_EQ(ino_caps["clientid_1"][123].count("authid1"),1);
  const auto it2 = mcaps.find("authid1");
  EXPECT_EQ(it2->second, k2);

}

TEST_F(CapsTest, StoreUpdateID) {
  auto vid1 = make_vid(1234, 1234);
  auto c1 = make_cap(123,"cid1","authid1");
  mCaps.Store(c1,&vid1);
  EXPECT_EQ(mCaps.ncaps(), 1);
  std::string authid {"authid1"};

  auto k = mCaps.Get(authid);
  EXPECT_EQ(k->id(),123);
  EXPECT_EQ(k->clientid(), "cid1");
  // Test the 3 different views
  auto& client_caps = mCaps.ClientCaps();
  auto& ino_caps = mCaps.ClientInoCaps();
  const auto& mcaps = mCaps.GetCaps();

  EXPECT_EQ(client_caps["cid1"].count("authid1"), 1);
  EXPECT_EQ(ino_caps["cid1"][123].count("authid1"),1);
  const auto it = mcaps.find("authid1");
  EXPECT_EQ(it->second, k);

  // now update this cap
  c1.set_clientid("clientid_1");
  c1.set_id(1234);
  // client_caps & ino_caps will now drop the old client entries, however TimeOrderedCaps will not drop the cap
  mCaps.Store(c1,&vid1);
  EXPECT_EQ(mCaps.ncaps(), 2);

  auto k2 = mCaps.Get(authid);
  EXPECT_EQ(k2->id(),1234);
  EXPECT_EQ(k2->clientid(), "clientid_1");

  EXPECT_EQ(client_caps["cid1"].size(), 0);
  EXPECT_EQ(ino_caps["cid1"][123].size(),0);
  // now check the updated values
  EXPECT_EQ(client_caps["clientid_1"].count("authid1"), 1);
  EXPECT_EQ(ino_caps["clientid_1"][1234].count("authid1"),1);
  const auto it2 = mcaps.find("authid1");
  EXPECT_EQ(it2->second, k2);

}
