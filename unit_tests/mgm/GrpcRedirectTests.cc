//------------------------------------------------------------------------------
// File: GrpcRedirectTests.cc
// Author: Gianmaria Del Monte - CERN
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

#include "gtest/gtest.h"

#ifdef EOS_GRPC

#include "mgm/grpc/GrpcNsInterface.hh"
#include "mgm/grpc/GrpcRedirect.hh"
#include "mgm/imaster/IMaster.hh"
#include "mgm/ofs/XrdMgmOfs.hh"
#include "mgm/proc/ProcInterface.hh"
#include "proto/ConsoleRequest.pb.h"
#include "proto/Rpc.grpc.pb.h"
#include <XrdSys/XrdSysError.hh>
// XrdMgmOfs.hh forward-declares zmq::context_t; bring in the full
// definition so the FakeMgmOfs destructor can close/delete it.
#include <zmq.hpp>

#include <cstdlib>
#include <memory>
#include <string>

namespace {

//------------------------------------------------------------------------------
//! Minimal IMaster stand-in that lets a test set IsMaster() and GetMasterId()
//! to arbitrary values without touching QuarkDB or the supervisor thread.
//------------------------------------------------------------------------------
class FakeIMaster : public eos::mgm::IMaster
{
public:
  FakeIMaster(bool is_master, std::string master_id)
    : mIsMaster(is_master), mMasterId(std::move(master_id)) {}

  bool Init() override { return true; }
  bool BootNamespace() override { return true; }
  bool ApplyMasterConfig(std::string&, std::string&, bool) override
  {
    return true;
  }
  bool IsMaster() override { return mIsMaster; }
  bool IsRemoteMasterOk() const override { return true; }
  const std::string GetMasterId() const override { return mMasterId; }
  bool SetMasterId(const std::string&, int, std::string&) override
  {
    return true;
  }
  size_t GetServiceDelay() override { return 0; }
  void GetLog(std::string&) override {}
  std::string PrintOut() override { return ""; }

private:
  bool mIsMaster;
  std::string mMasterId;
};

//------------------------------------------------------------------------------
//! Trivial XrdMgmOfs subclass used only to host an mMaster for the
//! GetSlaveRedirectTarget tests.
//------------------------------------------------------------------------------
class FakeMgmOfs : public XrdMgmOfs
{
public:
  explicit FakeMgmOfs(XrdSysError* lp) : XrdMgmOfs(lp) {}

  ~FakeMgmOfs()
  {
    if (mZmqContext) {
      mZmqContext->close();
      delete mZmqContext;
    }

    mDoneOrderlyShutdown = true;
  }
};

//------------------------------------------------------------------------------
//! Fixture that swaps gOFS for a FakeMgmOfs whose mMaster is settable per
//! test. The HTTP/gRPC ports are forced to zero via env vars so the parent
//! XrdMgmOfs constructor does not try to spin up service threads.
//------------------------------------------------------------------------------
class GrpcRedirectFixture : public ::testing::Test
{
protected:
  static void SetUpTestSuite()
  {
    setenv("EOS_MGM_HTTP_PORT", "0", 1);
    setenv("EOS_MGM_GRPC_PORT", "0", 1);
    setenv("EOS_MGM_WNC_PORT", "0", 1);
    sFakeSysError = new XrdSysError(nullptr, "fake");
    sFakeOfs = new FakeMgmOfs(sFakeSysError);
    sSavedOfs = gOFS;
    gOFS = sFakeOfs;
  }

  static void TearDownTestSuite()
  {
    gOFS = sSavedOfs;
    delete sFakeOfs;
    delete sFakeSysError;
    sFakeOfs = nullptr;
    sFakeSysError = nullptr;
    unsetenv("EOS_MGM_HTTP_PORT");
    unsetenv("EOS_MGM_GRPC_PORT");
    unsetenv("EOS_MGM_WNC_PORT");
  }

  void TearDown() override
  {
    if (sFakeOfs != nullptr) {
      sFakeOfs->mMaster.reset();
    }
  }

  static XrdSysError* sFakeSysError;
  static FakeMgmOfs* sFakeOfs;
  static XrdMgmOfs* sSavedOfs;
};

XrdSysError* GrpcRedirectFixture::sFakeSysError = nullptr;
FakeMgmOfs* GrpcRedirectFixture::sFakeOfs = nullptr;
XrdMgmOfs* GrpcRedirectFixture::sSavedOfs = nullptr;

} // namespace

//------------------------------------------------------------------------------
// GrpcNsInterface::IsWriteRequest classifier
//------------------------------------------------------------------------------
TEST(GrpcNsInterfaceIsWriteRequest, NullRequestIsNotWrite)
{
  EXPECT_FALSE(eos::mgm::GrpcNsInterface::IsWriteRequest(nullptr));
}

TEST(GrpcNsInterfaceIsWriteRequest, EmptyRequestIsTreatedAsWrite)
{
  // No command set: conservative default keeps unknown / future commands on
  // the master.
  eos::rpc::NSRequest req;
  EXPECT_TRUE(eos::mgm::GrpcNsInterface::IsWriteRequest(&req));
}

TEST(GrpcNsInterfaceIsWriteRequest, UnambiguousWrites)
{
  eos::rpc::NSRequest req;
  req.mutable_mkdir();
  EXPECT_TRUE(eos::mgm::GrpcNsInterface::IsWriteRequest(&req));

  req.Clear();
  req.mutable_rmdir();
  EXPECT_TRUE(eos::mgm::GrpcNsInterface::IsWriteRequest(&req));

  req.Clear();
  req.mutable_touch();
  EXPECT_TRUE(eos::mgm::GrpcNsInterface::IsWriteRequest(&req));

  req.Clear();
  req.mutable_unlink();
  EXPECT_TRUE(eos::mgm::GrpcNsInterface::IsWriteRequest(&req));

  req.Clear();
  req.mutable_rm();
  EXPECT_TRUE(eos::mgm::GrpcNsInterface::IsWriteRequest(&req));

  req.Clear();
  req.mutable_rename();
  EXPECT_TRUE(eos::mgm::GrpcNsInterface::IsWriteRequest(&req));

  req.Clear();
  req.mutable_symlink();
  EXPECT_TRUE(eos::mgm::GrpcNsInterface::IsWriteRequest(&req));

  req.Clear();
  req.mutable_xattr();
  EXPECT_TRUE(eos::mgm::GrpcNsInterface::IsWriteRequest(&req));

  req.Clear();
  req.mutable_chown();
  EXPECT_TRUE(eos::mgm::GrpcNsInterface::IsWriteRequest(&req));

  req.Clear();
  req.mutable_chmod();
  EXPECT_TRUE(eos::mgm::GrpcNsInterface::IsWriteRequest(&req));
}

TEST(GrpcNsInterfaceIsWriteRequest, TokenIsRead)
{
  // Token generation is a signing op and does not mutate the namespace -
  // keeps consistent with ProcInterface::IsProtoWriteAccess.
  eos::rpc::NSRequest req;
  req.mutable_token();
  EXPECT_FALSE(eos::mgm::GrpcNsInterface::IsWriteRequest(&req));
}

TEST(GrpcNsInterfaceIsWriteRequest, AclDependsOnSubcommand)
{
  eos::rpc::NSRequest req;
  req.mutable_acl()->set_cmd(eos::rpc::NSRequest::AclRequest::LIST);
  EXPECT_FALSE(eos::mgm::GrpcNsInterface::IsWriteRequest(&req));

  req.mutable_acl()->set_cmd(eos::rpc::NSRequest::AclRequest::MODIFY);
  EXPECT_TRUE(eos::mgm::GrpcNsInterface::IsWriteRequest(&req));
}

TEST(GrpcNsInterfaceIsWriteRequest, VersionDependsOnSubcommand)
{
  eos::rpc::NSRequest req;
  req.mutable_version()->set_cmd(eos::rpc::NSRequest::VersionRequest::LIST);
  EXPECT_FALSE(eos::mgm::GrpcNsInterface::IsWriteRequest(&req));

  req.mutable_version()->set_cmd(eos::rpc::NSRequest::VersionRequest::CREATE);
  EXPECT_TRUE(eos::mgm::GrpcNsInterface::IsWriteRequest(&req));

  req.mutable_version()->set_cmd(eos::rpc::NSRequest::VersionRequest::PURGE);
  EXPECT_TRUE(eos::mgm::GrpcNsInterface::IsWriteRequest(&req));
}

TEST(GrpcNsInterfaceIsWriteRequest, OldRecycleDependsOnSubcommand)
{
  eos::rpc::NSRequest req;
  req.mutable_old_recycle()->set_cmd(
    eos::rpc::NSRequest::RecycleRequest::LIST);
  EXPECT_FALSE(eos::mgm::GrpcNsInterface::IsWriteRequest(&req));

  req.mutable_old_recycle()->set_cmd(
    eos::rpc::NSRequest::RecycleRequest::RESTORE);
  EXPECT_TRUE(eos::mgm::GrpcNsInterface::IsWriteRequest(&req));

  req.mutable_old_recycle()->set_cmd(
    eos::rpc::NSRequest::RecycleRequest::PURGE);
  EXPECT_TRUE(eos::mgm::GrpcNsInterface::IsWriteRequest(&req));
}

TEST(GrpcNsInterfaceIsWriteRequest, RecycleDependsOnSubcommand)
{
  eos::rpc::NSRequest req;
  req.mutable_recycle()->mutable_ls();
  EXPECT_FALSE(eos::mgm::GrpcNsInterface::IsWriteRequest(&req));

  req.Clear();
  req.mutable_recycle()->mutable_purge();
  EXPECT_TRUE(eos::mgm::GrpcNsInterface::IsWriteRequest(&req));

  req.Clear();
  req.mutable_recycle()->mutable_restore();
  EXPECT_TRUE(eos::mgm::GrpcNsInterface::IsWriteRequest(&req));
}

TEST(GrpcNsInterfaceIsWriteRequest, QuotaDependsOnOperation)
{
  eos::rpc::NSRequest req;
  req.mutable_quota()->set_op(eos::rpc::QUOTAOP::GET);
  EXPECT_FALSE(eos::mgm::GrpcNsInterface::IsWriteRequest(&req));

  req.mutable_quota()->set_op(eos::rpc::QUOTAOP::SET);
  EXPECT_TRUE(eos::mgm::GrpcNsInterface::IsWriteRequest(&req));

  req.mutable_quota()->set_op(eos::rpc::QUOTAOP::RM);
  EXPECT_TRUE(eos::mgm::GrpcNsInterface::IsWriteRequest(&req));
}

//------------------------------------------------------------------------------
// ProcInterface::IsProtoWriteAccess: spot-check the read entries we added on
// top of the existing classifier.
//------------------------------------------------------------------------------
TEST(ProcInterfaceIsProtoWriteAccess, AddedReadCases)
{
  eos::console::RequestProto req;
  req.mutable_stat();
  EXPECT_FALSE(eos::mgm::ProcInterface::IsProtoWriteAccess(req));

  req.Clear();
  req.mutable_status();
  EXPECT_FALSE(eos::mgm::ProcInterface::IsProtoWriteAccess(req));

  req.Clear();
  req.mutable_health();
  EXPECT_FALSE(eos::mgm::ProcInterface::IsProtoWriteAccess(req));

  req.Clear();
  req.mutable_fileinfo();
  EXPECT_FALSE(eos::mgm::ProcInterface::IsProtoWriteAccess(req));

  req.Clear();
  req.mutable_version();
  EXPECT_FALSE(eos::mgm::ProcInterface::IsProtoWriteAccess(req));

  req.Clear();
  req.mutable_who();
  EXPECT_FALSE(eos::mgm::ProcInterface::IsProtoWriteAccess(req));

  req.Clear();
  req.mutable_whoami();
  EXPECT_FALSE(eos::mgm::ProcInterface::IsProtoWriteAccess(req));

  req.Clear();
  req.mutable_ls();
  EXPECT_FALSE(eos::mgm::ProcInterface::IsProtoWriteAccess(req));
}

TEST(ProcInterfaceIsProtoWriteAccess, KnownWritesStillWrite)
{
  eos::console::RequestProto req;
  req.mutable_rm();
  EXPECT_TRUE(eos::mgm::ProcInterface::IsProtoWriteAccess(req));

  req.Clear();
  req.mutable_evict();
  EXPECT_TRUE(eos::mgm::ProcInterface::IsProtoWriteAccess(req));

  req.Clear();
  req.mutable_mkdir();
  EXPECT_TRUE(eos::mgm::ProcInterface::IsProtoWriteAccess(req));
}

TEST(ProcInterfaceIsProtoWriteAccess, KnownReadsAreRead)
{
  eos::console::RequestProto req;
  req.mutable_find();
  EXPECT_FALSE(eos::mgm::ProcInterface::IsProtoWriteAccess(req));

  req.Clear();
  req.mutable_io();
  EXPECT_FALSE(eos::mgm::ProcInterface::IsProtoWriteAccess(req));

  req.Clear();
  req.mutable_debug();
  EXPECT_FALSE(eos::mgm::ProcInterface::IsProtoWriteAccess(req));
}

//------------------------------------------------------------------------------
// GetSlaveRedirectTarget: depends on gOFS / gOFS->mMaster state.
//------------------------------------------------------------------------------
TEST_F(GrpcRedirectFixture, NoMasterPluginReturnsEmpty)
{
  ASSERT_EQ(sFakeOfs->mMaster.get(), nullptr);
  EXPECT_EQ(eos::mgm::GetSlaveRedirectTarget(), "");
}

TEST_F(GrpcRedirectFixture, MasterRoleReturnsEmpty)
{
  sFakeOfs->mMaster = std::make_unique<FakeIMaster>(true, "ignored:1094");
  EXPECT_EQ(eos::mgm::GetSlaveRedirectTarget(), "");
}

TEST_F(GrpcRedirectFixture, SlaveRoleReturnsMasterId)
{
  sFakeOfs->mMaster = std::make_unique<FakeIMaster>(false,
                      "eos-mgm1.cern.ch:1094");
  EXPECT_EQ(eos::mgm::GetSlaveRedirectTarget(),
            "eos-mgm1.cern.ch:1094");
}

#endif // EOS_GRPC
