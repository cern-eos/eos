//------------------------------------------------------------------------------
// File: XrdMgmFileOfsTests.cc
// Author: Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2020 CERN/Switzerland                                  *
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

#include "common/SymKeys.hh"
#include "mgm/access/Access.hh"
#include "mgm/ofs/XrdMgmOfs.hh"
#include "mgm/zmq/ZMQ.hh"
#include "namespace/interface/IFileMD.hh"
#include "proto/ConsoleRequest.pb.h"
#include <XrdSec/XrdSecEntity.hh>
#include <XrdSec/XrdSecEntityAttr.hh>
#define IN_TEST_HARNESS
#include "mgm/ofs/XrdMgmOfsFile.hh"
#undef IN_TEST_HARNESS
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <functional>
#include <optional>
#include <utility>

using namespace eos::mgm;

TEST(XrdMgmOfsFile, ParsingExcludFsids)
{
  XrdMgmOfsFile f;
  std::string opaque_info =
    "eos.excludefsid=2,4,6,8,10,144&eos.ruid=0&eos.rgid=0";
  f.openOpaque = new XrdOucEnv(opaque_info.c_str());
  std::vector<unsigned int> expect_result {2, 4, 6, 8, 10, 144};
  auto result = f.GetExcludedFsids();
  ASSERT_EQ(result.size(), expect_result.size());

  for (const auto& elem : expect_result) {
    ASSERT_TRUE(std::find(result.begin(), result.end(), elem) != result.end());
  }
}

//------------------------------------------------------------------------------
// Identity of a client, and of one of EOS's own engines
//------------------------------------------------------------------------------
static eos::common::VirtualIdentity
MakeClientVid()
{
  eos::common::VirtualIdentity vid;
  vid.prot = "krb5";
  vid.uid = 12345;
  return vid;
}

static eos::common::VirtualIdentity
MakeEngineVid()
{
  eos::common::VirtualIdentity vid;
  vid.prot = "sss";
  vid.uid = DAEMONUID;
  return vid;
}

TEST(XrdMgmOfsFile, GetClientApplicationName)
{
  const auto vid = MakeClientVid();
  ASSERT_STREQ("",
               XrdMgmOfsFile::GetClientApplicationName(nullptr, nullptr, vid).c_str());
  std::string opaque_str = "&key1=val1&key2=val2&key3=val3";
  XrdOucEnv env(opaque_str.c_str());
  XrdSecEntity client("test");
  ASSERT_STREQ("", XrdMgmOfsFile::GetClientApplicationName(&env, &client, vid).c_str());
  client.eaAPI->Add("xrd.appname", "xrd_tag");
  ASSERT_STREQ("xrd_tag",
               XrdMgmOfsFile::GetClientApplicationName(&env, &client, vid).c_str());
  opaque_str = "&key1=val1&key2=val2&key3=val3&eos.app=eos_tag";
  XrdOucEnv env1(opaque_str.c_str());
  ASSERT_STREQ("eos_tag",
               XrdMgmOfsFile::GetClientApplicationName(&env1, &client, vid).c_str());
  ASSERT_STREQ("eos_tag",
               XrdMgmOfsFile::GetClientApplicationName(&env1, nullptr, vid).c_str());
}

TEST(XrdMgmOfsFile, ClientMayNotClaimTheInternalAppPrefix)
{
  const auto client_vid = MakeClientVid();
  // eos.app, the tag a client sets itself
  XrdOucEnv env("&eos.app=eos/drain");
  ASSERT_STREQ(
      "drain",
      XrdMgmOfsFile::GetClientApplicationName(&env, nullptr, client_vid).c_str());
  // no amount of prefixing gets one through
  XrdOucEnv env_twice("&eos.app=eos/eos/converter");
  ASSERT_STREQ(
      "converter",
      XrdMgmOfsFile::GetClientApplicationName(&env_twice, nullptr, client_vid).c_str());
  // xrd.appname, the other source, is the client's just the same
  XrdOucEnv empty_env("&key1=val1");
  XrdSecEntity client("test");
  client.eaAPI->Add("xrd.appname", "eos/fsck");
  ASSERT_STREQ(
      "fsck",
      XrdMgmOfsFile::GetClientApplicationName(&empty_env, &client, client_vid).c_str());
  // a name that merely starts with the same letters is left alone
  XrdOucEnv env_eoscp("&eos.app=eoscp");
  ASSERT_STREQ(
      "eoscp",
      XrdMgmOfsFile::GetClientApplicationName(&env_eoscp, nullptr, client_vid).c_str());
}

TEST(XrdMgmOfsFile, InternalEngineKeepsTheAppPrefix)
{
  const auto engine_vid = MakeEngineVid();
  XrdOucEnv env("&eos.app=eos/converter");
  ASSERT_STREQ(
      "eos/converter",
      XrdMgmOfsFile::GetClientApplicationName(&env, nullptr, engine_vid).c_str());
  // an engine authenticating as something else is not one
  eos::common::VirtualIdentity unix_daemon;
  unix_daemon.prot = "unix";
  unix_daemon.uid = DAEMONUID;
  ASSERT_STREQ(
      "converter",
      XrdMgmOfsFile::GetClientApplicationName(&env, nullptr, unix_daemon).c_str());
}

namespace {

class FileOpenTestMaster : public IMaster {
public:
  MOCK_METHOD(bool, Init, (), (override));
  MOCK_METHOD(bool, BootNamespace, (), (override));
  MOCK_METHOD(bool, ApplyMasterConfig, (std::string&, std::string&, bool), (override));
  MOCK_METHOD(bool, IsMaster, (), (override));
  MOCK_METHOD(bool, IsRemoteMasterOk, (), (const, override));
  MOCK_METHOD(const std::string, GetMasterId, (), (const, override));
  MOCK_METHOD(bool, SetMasterId, (const std::string&, int, std::string&), (override));
  MOCK_METHOD(size_t, GetServiceDelay, (), (override));
  MOCK_METHOD(void, GetLog, (std::string&), (override));
  MOCK_METHOD(std::string, PrintOut, (), (override));
};

class FileOpenTestOfs : public XrdMgmOfs {
public:
  explicit FileOpenTestOfs(XrdSysError* error)
      : XrdMgmOfs(error)
  {
  }

  ~FileOpenTestOfs() override
  {
    if (mZmqContext) {
      mZmqContext->close();
      delete mZmqContext;
      mZmqContext = nullptr;
    }

    mDoneOrderlyShutdown = true;
  }
};

class ProcOpenRoutingTest : public ::testing::Test {
protected:
  void
  SetUp() override
  {
    for (const auto* key : {"EOS_MGM_HTTP_PORT", "EOS_MGM_GRPC_PORT", "EOS_MGM_WNC_PORT",
                            "EOS_MGM_REST_GRPC_PORT"}) {
      const char* value = getenv(key);
      mEnvironment.emplace(key, value ? std::optional<std::string>(value) : std::nullopt);
      setenv(key, "0", 1);
    }

    mPreviousOfs = gOFS;
    mOfs = std::make_unique<FileOpenTestOfs>(&mError);
    gOFS = mOfs.get();
    auto master = std::make_unique<testing::NiceMock<FileOpenTestMaster>>();
    ON_CALL(*master, IsMaster()).WillByDefault(testing::Return(false));
    mOfs->mMaster = std::move(master);

    for (const auto& [key, value] : mEnvironment) {
      value ? setenv(key.c_str(), value->c_str(), 1) : unsetenv(key.c_str());
    }

    eos::common::RWMutexWriteLock lock(Access::gAccessMutex);
    SaveAndClear(Access::gBannedUsers);
    SaveAndClear(Access::gBannedGroups);
    SaveAndClear(Access::gBannedHosts);
    SaveAndClear(Access::gBannedDomains);
    SaveAndClear(Access::gBannedTokens);
    SaveAndClear(Access::gAllowedUsers);
    SaveAndClear(Access::gAllowedGroups);
    SaveAndClear(Access::gAllowedHosts);
    SaveAndClear(Access::gAllowedDomains);
    SaveAndClear(Access::gAllowedTokens);
    SaveAndClear(Access::gRedirectionRules);
    SaveAndClear(Access::gStallRules);
    SaveAndClear(Access::gStallComment);
    SaveAndClear(Access::gStallGlobal);
    SaveAndClear(Access::gStallRead);
    SaveAndClear(Access::gStallWrite);
    SaveAndClear(Access::gStallUserGroup);
    Access::gRedirectionRules["w:*"] = "leader.example:2094";
    // Stop reads before executing a command against an unbooted namespace.
    Access::gStallRules["r:*"] = "17";
    Access::gStallRead = true;
    mVid.uid = 12345;
    mVid.gid = 12345;
    mVid.prot = "krb5";
    mVid.host = "client.example";
    mVid.app = "proc-routing-test";
  }

  void
  TearDown() override
  {
    mOfs.reset();
    gOFS = mPreviousOfs;
    eos::common::RWMutexWriteLock lock(Access::gAccessMutex);

    for (auto& restore : mRestoreAccess) {
      restore();
    }
  }

  template <typename T>
  void
  SaveAndClear(T& value)
  {
    T previous = std::exchange(value, T{});
    mRestoreAccess.emplace_back([&value, previous = std::move(previous)]() mutable {
      value = std::move(previous);
    });
  }

  void
  SaveAndClear(std::atomic<bool>& value)
  {
    const bool previous = value.exchange(false);
    mRestoreAccess.emplace_back([&value, previous]() { value = previous; });
  }

  void
  CheckWriteRedirect(const std::string& opaque)
  {
    XrdMgmOfsFile file;
    ASSERT_EQ(SFS_REDIRECT,
              file.open(&mVid, "/proc/user/", SFS_O_RDONLY, 0, nullptr, opaque.c_str()));
    EXPECT_STREQ("root://leader.example:2094//proc/user/", file.error.getErrText());
  }

  XrdSysError mError{nullptr, "proc-routing-test"};
  XrdMgmOfs* mPreviousOfs{nullptr};
  std::unique_ptr<FileOpenTestOfs> mOfs;
  eos::common::VirtualIdentity mVid;
  std::map<std::string, std::optional<std::string>> mEnvironment;
  std::vector<std::function<void()>> mRestoreAccess;
};

TEST_F(ProcOpenRoutingTest, LegacyWritesRedirectToLeader)
{
  CheckWriteRedirect("mgm.cmd=mkdir&mgm.path=/eos/routing-test");
}

TEST_F(ProcOpenRoutingTest, ProtobufWritesRedirectToLeader)
{
  eos::console::RequestProto request;
  request.mutable_mkdir()->mutable_md()->set_path("/eos/routing-test");
  std::string encoded;
  ASSERT_TRUE(eos::common::SymKey::ProtobufBase64Encode(&request, encoded));
  CheckWriteRedirect("mgm.cmd.proto=" + encoded);
  request.Clear();
  request.mutable_rm()->set_path("/eos/routing-test");
  ASSERT_TRUE(eos::common::SymKey::ProtobufBase64Encode(&request, encoded));
  CheckWriteRedirect("mgm.cmd.proto=" + encoded);
}

TEST_F(ProcOpenRoutingTest, NamespaceStatusUsesLocalReadRules)
{
  eos::console::RequestProto request;
  request.mutable_ns()->mutable_stat();
  std::string encoded;
  ASSERT_TRUE(eos::common::SymKey::ProtobufBase64Encode(&request, encoded));
  const std::string protobuf = "mgm.cmd.proto=" + encoded;

  for (const auto& opaque : {std::string("mgm.cmd=ns&mgm.subcmd=stat"), protobuf}) {
    XrdMgmOfsFile file;
    const int result =
        file.open(&mVid, "/proc/admin/", SFS_O_RDONLY, 0, nullptr, opaque.c_str());
    EXPECT_NE(SFS_REDIRECT, result);
    EXPECT_GE(result, 17);
    EXPECT_LE(result, 22);
  }
}

} // namespace

TEST(XrdMgmOfsFile, GetClientOpenFlags)
{
  XrdSfsFileOpenMode oflags;
  mode_t omode;
  {
    // No tags means read only
    XrdOucEnv env("key1=val1");
    ASSERT_TRUE(XrdMgmOfsFile::GetClientOpenFlags(env, oflags, omode));
    ASSERT_EQ(SFS_O_RDONLY, oflags);
    ASSERT_EQ(0u, omode);
  }
  {
    XrdOucEnv env("eos.client.openflags=rw,cr&eos.client.openmode=640");
    ASSERT_TRUE(XrdMgmOfsFile::GetClientOpenFlags(env, oflags, omode));
    ASSERT_EQ(SFS_O_RDWR | SFS_O_CREAT, oflags);
    ASSERT_EQ((mode_t)0640, omode);
  }
  {
    XrdOucEnv env("eos.client.openflags=wo,tr&eos.client.mkpath=1");
    ASSERT_TRUE(XrdMgmOfsFile::GetClientOpenFlags(env, oflags, omode));
    ASSERT_EQ(SFS_O_WRONLY | SFS_O_TRUNC, oflags);
    ASSERT_EQ((mode_t)SFS_O_MKPTH, omode);
  }
  {
    // Only permission bits are taken from the open mode
    XrdOucEnv env("eos.client.openmode=7777");
    ASSERT_TRUE(XrdMgmOfsFile::GetClientOpenFlags(env, oflags, omode));
    ASSERT_EQ((mode_t)0777, omode);
  }
  {
    XrdOucEnv env("eos.client.openflags=rw&eos.client.openmode=64x");
    ASSERT_FALSE(XrdMgmOfsFile::GetClientOpenFlags(env, oflags, omode));
  }
}
