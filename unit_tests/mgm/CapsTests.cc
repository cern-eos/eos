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

eos::fusex::cap make_cap(int id,
                         std::string clientid,
                         std::string authid,
                         std::string uuid,
                         uint64_t vtime=0)
{
  eos::fusex::cap c;
  c.set_id(id);
  c.set_clientid(std::move(clientid));
  c.set_authid(std::move(authid));
  c.set_clientuuid(std::move(uuid));
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

TEST_F(CapsTest, PopCaps) {
  auto vid1 = make_vid(1,1);
  auto vid2 = make_vid(2,2);

  mCaps.Store(make_cap(1,"client1","auth1"), &vid1);
  mCaps.Store(make_cap(2,"client2","auth2"), &vid2);

  EXPECT_EQ(mCaps.ncaps(), 2);
  mCaps.pop();
  EXPECT_EQ(mCaps.ncaps(), 1);

  // pop only pops from time ordered caps, the other cap should still be present
  EXPECT_TRUE(mCaps.HasCap("auth1"));
  EXPECT_TRUE(mCaps.HasCap("auth2"));

  std::string filter;
  std::string option{"t"};

  std::string out = mCaps.Print(option, filter);
  EXPECT_TRUE(out.find("client2") != std::string::npos);
  EXPECT_TRUE(out.find("client1") == std::string::npos);
}

TEST_F(CapsTest, ExpireCaps)
{
  auto vid1 = make_vid(1,1);
  auto vid2 = make_vid(2,2);

  uint64_t time20 = static_cast<uint64_t>(time(nullptr)) - 20;
  mCaps.Store(make_cap(1,"client1","auth1", time20), &vid1);
  mCaps.Store(make_cap(2,"client2","auth2"), &vid2);

  EXPECT_EQ(mCaps.ncaps(),2);
  EXPECT_EQ(mCaps.GetCaps().size(), 2);

  // Now expire the caps
  EXPECT_TRUE(mCaps.expire());
  EXPECT_EQ(mCaps.GetCaps().size(), 1);
  EXPECT_TRUE(mCaps.HasCap("auth2"));
  EXPECT_FALSE(mCaps.HasCap("auth1"));
}

TEST_F(CapsTest, DropCaps)
{
  std::string uuid1{"uuid1"};
  auto c1 = make_cap(1,"client1","auth1");
  c1.set_clientuuid(uuid1);
  auto c2 = make_cap(2, "client2", "auth2");
  c2.set_clientuuid("uuid2");
  auto c3 = make_cap(3,"client1", "auth3");
  c3.set_clientuuid(uuid1);

  auto vid1 = make_vid(1,1);
  auto vid2 = make_vid(2,2);
  auto vid3 = make_vid(3,3);

  mCaps.Store(c1, &vid1);
  mCaps.Store(c2, &vid2);
  mCaps.Store(c3, &vid3);

  EXPECT_EQ(mCaps.GetCaps().size(), 3);

  mCaps.dropCaps(uuid1);

  EXPECT_EQ(mCaps.GetCaps().size(),1);
  EXPECT_TRUE(mCaps.HasCap("auth2"));
}

TEST_F(CapsTest, Remove)
{
  auto vid1 = make_vid(1,1);
  auto vid2 = make_vid(2,2);

  mCaps.Store(make_cap(1,"client1","auth1"), &vid1);
  mCaps.Store(make_cap(2,"client2","auth2"), &vid2);

  const auto& client_caps = mCaps.ClientCaps();
  const auto& ino_caps = mCaps.ClientInoCaps();
  const auto& mcaps = mCaps.GetCaps();

  EXPECT_EQ(client_caps.size(), 2);
  EXPECT_EQ(ino_caps.size(), 2);
  EXPECT_EQ(mcaps.size(), 2);

  EXPECT_TRUE(mCaps.Remove(mCaps.Get("auth1")));
  EXPECT_FALSE(mCaps.Remove(mCaps.Get("foo")));
  EXPECT_EQ(client_caps.size(), 1);
  EXPECT_EQ(ino_caps.size(), 1);
  EXPECT_EQ(mcaps.size(), 1);

}


TEST_F(CapsTest, Delete)
{
  auto vid1 = make_vid(1,1);
  auto vid2 = make_vid(2,2);
  auto vid3 = make_vid(3,3);
  mCaps.Store(make_cap(1,"client1","auth1"), &vid1);
  mCaps.Store(make_cap(2,"client2","auth2"), &vid2);
  mCaps.Store(make_cap(1,"client3","auth3"), &vid3);

  const auto& client_caps = mCaps.ClientCaps();
  const auto& ino_caps = mCaps.ClientInoCaps();
  const auto& mcaps = mCaps.GetCaps();

  EXPECT_EQ(client_caps.size(), 3);
  EXPECT_EQ(ino_caps.size(), 3);
  EXPECT_EQ(mcaps.size(), 3);

  EXPECT_EQ(mCaps.Delete(1), 0);
  EXPECT_EQ(mCaps.Delete(123), ENONET);

  EXPECT_TRUE(mCaps.HasCap("auth2"));
  EXPECT_EQ(client_caps.size(), 1);
  EXPECT_EQ(ino_caps.size(), 1);
  EXPECT_EQ(mcaps.size(), 1);

}

TEST_F(CapsTest, BCCap)
{
  auto vid1 = make_vid(1,1);
  auto vid2 = make_vid(2,2);
  std::string auth1 {"auth1"};
  mCaps.Store(make_cap(1,"client1",auth1), &vid1);
  mCaps.Store(make_cap(2,"client2","auth2"), &vid2);


  auto k = mCaps.Get(auth1);
  EXPECT_EQ(k->id(), 1);
  int ret = mCaps.BroadcastCap(k);
  // This function returns -1 regardless whether cap exists or not
  EXPECT_EQ(ret, -1);
}


TEST_F(CapsTest, GetBroadcastCapsTS)
{
  auto vid1 = make_vid(1,1);
  auto vid2 = make_vid(2,2);
  auto vid3 = make_vid(3,3);
  std::string auth1 {"auth1"};
  std::string auth2 {"auth2"};
  std::string auth3 {"auth3"};

  // Use a unordered_set with the same type as the vector elements returned from
  // GetBroadcastCapsTS; the underlying types may not have ordering as they fill
  // the vector from unordered_maps, so this is done to make sure that tests are
  // deterministic
  using bc_result_t = decltype(mCaps.GetBroadcastCapsTS(0));
  using shared_cap_t = bc_result_t::value_type;
  using result_set_t = std::unordered_set<shared_cap_t>;

  mCaps.Store(make_cap(1,"client1",auth1,"uuid1"), &vid1);
  mCaps.Store(make_cap(2,"client2",auth2,"uuid2"), &vid2);
  mCaps.Store(make_cap(1,"client3",auth3,"uuid3"), &vid3);


  {
    auto empty_result = mCaps.GetBroadcastCapsTS(9999,
                                                 mCaps.Get("foo"));
    EXPECT_EQ(empty_result.size(), 0);
  }

  {

    auto result = mCaps.GetBroadcastCapsTS(1,
                                           mCaps.Get(auth1));
    EXPECT_EQ(result.size(), 2);

    result_set_t actual (std::make_move_iterator(result.begin()),
                         std::make_move_iterator(result.end()));
    result_set_t expected = { mCaps.Get(auth1), mCaps.Get(auth3)};
    EXPECT_EQ(expected, actual);
  }

  {
    // this will skip own caps
    eos::fusex::md m;
    m.set_authid(auth1);
    m.set_clientuuid("uuid1");
    auto actual = mCaps.GetBroadcastCapsTS(1,
                                           mCaps.Get(auth1),
                                           &m);
    bc_result_t expected = { mCaps.Get(auth3) };
    EXPECT_EQ(expected, actual);
  }

}

TEST_F(CapsTest, MonitorCapsSimple)
{
  auto vid1 = make_vid(1,1);
  auto vid2 = make_vid(2,2);

  uint64_t time20 = static_cast<uint64_t>(time(nullptr)) - 20;
  mCaps.Store(make_cap(1,"client1","auth1", time20), &vid1);
  mCaps.Store(make_cap(2,"client2","auth2", time20), &vid2);

  EXPECT_EQ(mCaps.ncaps(),2);
  EXPECT_EQ(mCaps.GetCaps().size(), 2);

  // Now expire the caps
  while (true) {
    if (mCaps.expire()) {
      mCaps.pop();
    } else {
      break;
    }
  }

  EXPECT_EQ(mCaps.GetCaps().size(), 0);
  EXPECT_EQ(mCaps.ncaps(), 0);
}

TEST_F(CapsTest, MonitorCapsUpdate)
{
  auto vid1 = make_vid(1,1);
  auto vid2 = make_vid(2,2);
  auto vid3 = make_vid(123,123);

  uint64_t time20 = static_cast<uint64_t>(time(nullptr)) - 20;
  uint64_t time8 = static_cast<uint64_t>(time(nullptr)) - 8;
  auto ucap = make_cap(123, "client123","auth123", time8);


  mCaps.Store(make_cap(1,"client1","auth1", time20), &vid1);
  mCaps.Store(make_cap(2,"client2","auth2", time20), &vid2);
  mCaps.Store(ucap, &vid3);

  EXPECT_EQ(mCaps.ncaps(),3);
  EXPECT_EQ(mCaps.GetCaps().size(), 3);

  // Now expire the caps
  while (true) {
    if (mCaps.expire()) {
      mCaps.pop();
    } else {
      break;
    }
  }

  EXPECT_EQ(mCaps.GetCaps().size(), 1);
  EXPECT_EQ(mCaps.ncaps(), 1);

  // update the cap vtime
  ucap.set_vtime(static_cast<uint64_t>(time(nullptr)) - 7);
  mCaps.Store(ucap, &vid3);
  EXPECT_EQ(mCaps.GetCaps().size(), 1);
  EXPECT_EQ(mCaps.ncaps(), 1);   // FIXME: should this be 2 as we've a new time?

  using namespace std::chrono_literals;
  std::this_thread::sleep_for(2s);

  // Now expire the caps
  while (true) {
    if (mCaps.expire()) {
      mCaps.pop();
    } else {
      break;
    }
  }

  EXPECT_EQ(mCaps.GetCaps().size(),1);
  EXPECT_EQ(mCaps.ncaps(), 0); // FIXME: This should be 1

  std::this_thread::sleep_for(1s);
  // Now expire the caps
  while (true) {
    if (mCaps.expire()) {
      mCaps.pop();
    } else {
      break;
    }
  }


  EXPECT_EQ(mCaps.GetCaps().size(),1) ; // FIXME: Should be 0
  EXPECT_EQ(mCaps.ncaps(), 0);
}
