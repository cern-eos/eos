//------------------------------------------------------------------------------
// File: MemberTests.cc
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
#include "mgm/egroup/Egroup.hh"
#include "mgm/ofs/XrdMgmOfs.hh"
#include "mgm/proc/ProcCommand.hh"
#include "mgm/zmq/ZMQ.hh"
#include "gtest/gtest.h"
#include <optional>

namespace {
using namespace eos::common;
using namespace eos::mgm;

class MemberTestOfs : public XrdMgmOfs {
public:
  explicit MemberTestOfs(XrdSysError* error)
      : XrdMgmOfs(error)
  {
  }

  ~MemberTestOfs() override
  {
    if (mZmqContext) {
      mZmqContext->close();
      delete mZmqContext;
      mZmqContext = nullptr;
    }

    mDoneOrderlyShutdown = true;
  }
};

class MemberTest : public ::testing::Test {
protected:
  void
  SetUp() override
  {
    Mapping::Reset();
    // Match the Caps tests: do not start HTTP or gRPC listeners.
    for (const auto* key : {"EOS_MGM_HTTP_PORT", "EOS_MGM_GRPC_PORT", "EOS_MGM_WNC_PORT",
                            "EOS_MGM_REST_GRPC_PORT"}) {
      const char* value = getenv(key);
      mEnvironment.emplace(key, value ? std::optional<std::string>(value) : std::nullopt);
      setenv(key, "0", 1);
    }

    mPreviousOfs = gOFS;
    mOfs = std::make_unique<MemberTestOfs>(&mError);
    gOFS = mOfs.get();
    mOfs->EgroupRefresh = std::make_unique<Egroup>(&mClock);

    for (const auto& [key, value] : mEnvironment) {
      value ? setenv(key.c_str(), value->c_str(), 1) : unsetenv(key.c_str());
    }
  }

  void
  TearDown() override
  {
    mOfs.reset();
    gOFS = mPreviousOfs;
    Mapping::Reset();
  }

  void
  Check(VirtualIdentity identity, const std::string& group, bool update,
        const std::string& expected, int retc = 0)
  {
    ProcCommand command;
    command.SetLogId("member-test", identity);
    const std::string opaque =
        "mgm.cmd=member&mgm.egroup=" + group + (update ? "&mgm.egroupupdate=true" : "");
    ASSERT_EQ(SFS_OK, command.open("/proc/user/", opaque.c_str(), identity, nullptr));
    std::string out, err;
    command.AddOutput(out, err);
    EXPECT_EQ(retc, command.GetRetc());
    EXPECT_EQ(expected.empty() ? expected : expected + "\n", out);
    EXPECT_EQ(retc != 0, !err.empty());
  }

  SteadyClock mClock{true};
  XrdSysError mError{nullptr, "member-test"};
  XrdMgmOfs* mPreviousOfs{nullptr};
  std::unique_ptr<MemberTestOfs> mOfs;
  std::map<std::string, std::optional<std::string>> mEnvironment;
};

TEST_F(MemberTest, UsesSelectedUidWithColdAndWarmNameCache)
{
  int errc = 0;
  const uid_t uid = getuid();
  const std::string username = Mapping::UidToUserName(uid, errc);
  ASSERT_EQ(0, errc);
  Mapping::Reset();
  VirtualIdentity identity;
  identity.uid = uid;
  auto& groups = *mOfs->EgroupRefresh;
  groups.inject(username, "member-test-yes", Egroup::Status::kMember);
  groups.inject(username, "member-test-no", Egroup::Status::kNotMember);
  groups.inject("member-test-authenticated", "member-test-yes",
                Egroup::Status::kNotMember);
  groups.inject("member-test-authenticated", "member-test-no", Egroup::Status::kMember);

  for (const std::string stale_name : {"member-test-authenticated", ""}) {
    identity.uid_string = stale_name;

    for (const auto* group : {"member-test-yes", "member-test-no", "member-test-yes"}) {
      const std::string membership =
          std::string(group) == "member-test-yes" ? "true" : "false";
      Check(identity, group, false,
            "egroup=" + std::string(group) + " user=" + username +
                " member=" + membership + " lifetime=1800");
    }
  }

  // Switch roles while retaining the previous role's username.
  identity.uid = uid == 123456789 ? 123456790 : 123456789;
  identity.uid_string = username;
  const std::string other_username = "member-test-selected-role";
  {
    std::scoped_lock lock(Mapping::gPhysicalUserNameCacheMutex);
    Mapping::gPhysicalUserNameCache[identity.uid] = other_username;
  }

  Check(identity, "member-test-yes", false,
        "egroup=member-test-yes user=" + other_username + " member=false lifetime=1800");
}

TEST_F(MemberTest, RefreshUsesSelectedUid)
{
  int errc = 0;
  VirtualIdentity identity;
  identity.uid = getuid();
  identity.uid_string = "member-test-authenticated";
  const std::string username = Mapping::UidToUserName(identity.uid, errc);
  ASSERT_EQ(0, errc);
  auto& groups = *mOfs->EgroupRefresh;
  groups.inject(username, "member-test", Egroup::Status::kNotMember);
  groups.inject(identity.uid_string, "member-test", Egroup::Status::kNotMember);
  Check(identity, "member-test", false,
        "egroup=member-test user=" + username + " member=false lifetime=1800");
  groups.inject(username, "member-test", Egroup::Status::kMember);
  Check(identity, "member-test", true,
        "egroup=member-test user=" + username + " member=true lifetime=1800");
  Egroup::CachedEntry entry;
  EXPECT_FALSE(groups.fetchCached(identity.uid_string, "member-test", entry));
}

TEST_F(MemberTest, RejectsCachedUidLookupFailure)
{
  VirtualIdentity identity;
  identity.uid = 123456789;
  identity.uid_string = "member-test-authenticated";
  Mapping::gShardedNegativeUserNameCache.store(
      identity.uid, std::make_unique<std::string>(std::to_string(identity.uid)));
  mOfs->EgroupRefresh->inject(identity.uid_string, "member-test",
                              Egroup::Status::kNotMember);
  mOfs->EgroupRefresh->inject(std::to_string(identity.uid), "member-test",
                              Egroup::Status::kNotMember);

  for (const bool update : {false, true}) {
    Check(identity, "member-test", update, "", EINVAL);
  }

  EXPECT_TRUE(mOfs->EgroupRefresh->DumpMembers().empty());
}
} // namespace
