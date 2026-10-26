//------------------------------------------------------------------------------
// File: FsViewStaleQueuePathTests.cc
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

// A standby MGM keeps the FileSystem object of a replaced disk when the
// master's removal is not applied there, then the master re-adds the same
// queuepath with a new id. The shared hash is keyed by queuepath, so the stale
// object starts reporting the new id. These tests rebuild that state on a real
// QuarkDB backed shared hash and check that the standby can still clean up its
// views, which is what promotion to master does first (INC5191622).

#include "common/FileSystem.hh"
#include "common/InstanceName.hh"
#include "common/SymKeys.hh"
#include "common/config/ConfigParsing.hh"
#include "gmock/gmock.h"
#include "mgm/fsview/FsView.hh"
#include "mgm/geotreeengine/GeoTreeEngine.hh"
#include "mgm/imaster/IMaster.hh"
#include "mgm/ofs/XrdMgmOfs.hh"
#include "mgm/proc/proc_fs.hh"
#include "mgm/zmq/ZMQ.hh"
#include "mq/MessagingRealm.hh"
#include "mq/SharedHashWrapper.hh"
#include "unit_tests/with_qdb/TestUtils.hh"
#include "gtest/gtest.h"
#include <chrono>
#include <functional>
#include <optional>
#include <thread>

using namespace eos::mgm;

namespace {

class StaleFsTestMaster : public IMaster {
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

class StaleFsTestOfs : public XrdMgmOfs {
public:
  explicit StaleFsTestOfs(XrdSysError* error)
      : XrdMgmOfs(error)
  {
  }

  ~StaleFsTestOfs() override
  {
    if (mZmqContext) {
      mZmqContext->close();
      delete mZmqContext;
      mZmqContext = nullptr;
    }

    mDoneOrderlyShutdown = true;
  }
};

class FsViewStaleQueuePathTest : public eos::UnitTestsWithQDBFixture {
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

    if (!eos::common::InstanceName::empty()) {
      mPreviousInstance = eos::common::InstanceName::get();
    }

    eos::common::InstanceName::clear();
    eos::common::InstanceName::set("fsview-stale-test");
    mPreviousDisableDefaults = FsSpace::gDisableDefaults.exchange(true);
    mPreviousOfs = gOFS;
    mOfs = std::make_unique<StaleFsTestOfs>(&mError);
    gOFS = mOfs.get();

    for (const auto& [key, value] : mEnvironment) {
      value ? setenv(key.c_str(), value->c_str(), 1) : unsetenv(key.c_str());
    }

    auto master = std::make_unique<testing::NiceMock<StaleFsTestMaster>>();
    ON_CALL(*master, IsMaster()).WillByDefault([this]() { return mIsMaster; });
    mOfs->mMaster = std::move(master);
    mOfs->mMessagingRealm.reset(new eos::mq::MessagingRealm(getSharedManager()));
    mOfs->mGeoTreeEngine.reset(new GeoTreeEngine(mOfs->mMessagingRealm.get()));
    FsView::gFsView.StopHeartBeat();
    // New node views publish the current key
    ASSERT_NE(nullptr,
              eos::common::gSymKeyStore.SetKey64("MTIzNDU2Nzg5MDEyMzQ1Njc4OTA=", 0));
  }

  void
  TearDown() override
  {
    // Reset is the operation under test, after a failure drop the views
    // directly so the next test starts from an empty FsView.
    if (!FsView::gFsView.Reset()) {
      eos::common::RWMutexWriteLock lock(FsView::gFsView.ViewMutex);
      auto& view = FsView::gFsView;

      for (auto& entry : view.mSpaceView) {
        delete entry.second;
      }

      for (auto& entry : view.mGroupView) {
        delete entry.second;
      }

      for (auto& entry : view.mNodeView) {
        delete entry.second;
      }

      view.mSpaceGroupView.clear();
      view.mSpaceView.clear();
      view.mGroupView.clear();
      view.mNodeView.clear();
      view.mIdView.clear();
    }

    mOfs.reset();
    gOFS = mPreviousOfs;
    FsSpace::gDisableDefaults = mPreviousDisableDefaults;
    eos::common::InstanceName::clear();

    if (!mPreviousInstance.empty()) {
      eos::common::InstanceName::set(mPreviousInstance);
    }
  }

  //----------------------------------------------------------------------------
  //! Filesystem config entry as the master stores it
  //----------------------------------------------------------------------------
  std::string
  FsConfig(eos::common::FileSystem::fsid_t fsid, const std::string& mount) const
  {
    return "bootcheck=0 configstatus=rw host=" + mHost + " hostport=" + mHost +
           ":1095 id=" + std::to_string(fsid) + " lifecycle=active path=" + mount +
           " port=1095 queue=" + mNodeQueue + " queuepath=" + mNodeQueue + mount +
           " schedgroup=spare uuid=uuid-" + std::to_string(fsid);
  }

  //----------------------------------------------------------------------------
  //! Publish values in the shared hash of a queuepath. A fresh realm can lose
  //! a write that races with its first resync, so repeat it until it shows.
  //----------------------------------------------------------------------------
  bool
  Publish(const std::string& queuepath, const std::map<std::string, std::string>& values)
  {
    eos::common::FileSystemLocator locator;

    if (!eos::common::FileSystemLocator::fromQueuePath(queuepath, locator)) {
      return false;
    }

    const eos::common::SharedHashLocator hash_locator(locator, false);
    eos::mq::SharedHashWrapper::Batch batch;

    for (const auto& [key, value] : values) {
      batch.SetDurable(key, value);
    }

    auto visible = [&]() {
      eos::mq::SharedHashWrapper hash(gOFS->mMessagingRealm.get(), hash_locator);

      for (const auto& [key, value] : values) {
        if (hash.get(key) != value) {
          return false;
        }
      }

      return true;
    };

    for (int attempt = 0; attempt < 25; ++attempt) {
      if (!eos::mq::SharedHashWrapper(gOFS->mMessagingRealm.get(), hash_locator)
               .set(batch)) {
        return false;
      }

      if (WaitFor(visible, 20)) {
        return true;
      }
    }

    return false;
  }

  //----------------------------------------------------------------------------
  //! Apply a config entry the way the standby config listener does
  //----------------------------------------------------------------------------
  bool
  ApplyConfig(eos::common::FileSystem::fsid_t fsid, const std::string& mount)
  {
    // ApplyFsConfig reads the id right after writing it, publish the entry
    // first as the master would have done long before
    std::map<std::string, std::string> config;

    if (!eos::common::ConfigParsing::parseFilesystemConfig(FsConfig(fsid, mount),
                                                           config) ||
        !Publish(mNodeQueue + mount, config)) {
      return false;
    }

    eos::common::RWMutexWriteLock lock(FsView::gFsView.ViewMutex);
    return FsView::gFsView.ApplyFsConfig((mNodeQueue + mount).c_str(),
                                         FsConfig(fsid, mount), true);
  }

  //----------------------------------------------------------------------------
  //! Publish a new filesystem on an already known queuepath, as the master
  //! does when it adds a replaced disk, without telling the standby views
  //----------------------------------------------------------------------------
  FileSystem*
  ReuseQueuePath(const std::string& mount, eos::common::FileSystem::fsid_t new_fsid)
  {
    FileSystem* fs = FsView::gFsView.mIdView.lookupByQueuePath(mNodeQueue + mount);

    if ((fs == nullptr) ||
        !Publish(mNodeQueue + mount, {{"id", std::to_string(new_fsid)},
                                      {"uuid", "uuid-" + std::to_string(new_fsid)},
                                      {"schedgroup", "spare"},
                                      {"stat.geotag", "site::rack"}}) ||
        !WaitFor([&]() { return fs->GetId() == new_fsid; })) {
      return nullptr;
    }

    return fs;
  }

  static bool
  WaitFor(const std::function<bool()>& condition, int polls = 500)
  {
    for (int i = 0; i < polls; ++i) {
      if (condition()) {
        return true;
      }

      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    return false;
  }

  static bool
  InGroup(const std::string& group, eos::common::FileSystem::fsid_t fsid)
  {
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
    auto it = FsView::gFsView.mGroupView.find(group);
    std::string geotag;
    return (it != FsView::gFsView.mGroupView.end()) &&
           it->second->getGeoTagInTree(fsid, geotag);
  }

  const std::string mHost = "fsview-stale.example";
  const std::string mNodeQueue = "/eos/" + mHost + ":1095/fst";
  bool mIsMaster = false;
  bool mPreviousDisableDefaults = false;
  std::string mPreviousInstance;
  XrdSysError mError{nullptr, "fsview-stale-queuepath-test"};
  XrdMgmOfs* mPreviousOfs = nullptr;
  std::unique_ptr<StaleFsTestOfs> mOfs;
  std::map<std::string, std::optional<std::string>> mEnvironment;
};

} // namespace

//------------------------------------------------------------------------------
// The geotag listener must not register the new shared id in the views, and
// the reset done on promotion must still tear the stale object down
//------------------------------------------------------------------------------
TEST_F(FsViewStaleQueuePathTest, ResetAfterGeotagUpdateOnReusedQueuePath)
{
  ASSERT_TRUE(ApplyConfig(10, "/data66"));
  ASSERT_TRUE(ApplyConfig(11, "/data67"));
  FileSystem* stale = ReuseQueuePath("/data66", 20);
  ASSERT_NE(nullptr, stale);
  gOFS->ProcessGeotagChange(mNodeQueue + "/data66");
  EXPECT_TRUE(InGroup("spare", 10));
  EXPECT_FALSE(InGroup("spare", 20));
  EXPECT_EQ(stale, FsView::gFsView.mIdView.lookupByID(10));
  EXPECT_TRUE(FsView::gFsView.Reset());
  EXPECT_EQ(0u, FsView::gFsView.mIdView.size());
  EXPECT_TRUE(FsView::gFsView.mGroupView.empty());
  EXPECT_TRUE(FsView::gFsView.mSpaceView.empty());
}

//------------------------------------------------------------------------------
// The config entry of the new filesystem replaces the stale object
//------------------------------------------------------------------------------
TEST_F(FsViewStaleQueuePathTest, ConfigForReusedQueuePathReplacesStaleObject)
{
  ASSERT_TRUE(ApplyConfig(10, "/data66"));
  ASSERT_TRUE(ApplyConfig(11, "/data67"));
  ASSERT_NE(nullptr, ReuseQueuePath("/data66", 20));
  EXPECT_TRUE(ApplyConfig(20, "/data66"));
  FileSystem* fs = FsView::gFsView.mIdView.lookupByQueuePath(mNodeQueue + "/data66");
  ASSERT_NE(nullptr, fs);
  EXPECT_EQ(20u, FsView::gFsView.mIdView.lookupByPtr(fs));
  EXPECT_EQ(nullptr, FsView::gFsView.mIdView.lookupByID(10));
  EXPECT_FALSE(FsView::gFsView.HasMapping(10));
  EXPECT_TRUE(FsView::gFsView.HasMapping(20));
  EXPECT_FALSE(InGroup("spare", 10));
  EXPECT_TRUE(InGroup("spare", 20));
  EXPECT_TRUE(InGroup("spare", 11));
  EXPECT_TRUE(FsView::gFsView.Reset());
  EXPECT_EQ(0u, FsView::gFsView.mIdView.size());
}

//------------------------------------------------------------------------------
// A slave replays the master's removal even though it cannot see the
// filesystem as empty, the master keeps refusing to remove an active one
//------------------------------------------------------------------------------
TEST_F(FsViewStaleQueuePathTest, SlaveReplaysMasterRemoval)
{
  ASSERT_TRUE(ApplyConfig(10, "/data66"));
  ASSERT_TRUE(ApplyConfig(11, "/data67"));
  auto root = eos::common::VirtualIdentity::Root();
  std::string node = mNodeQueue;
  std::string id;
  XrdOucString out;
  XrdOucString err;
  std::string mount = "/data67";
  mIsMaster = true;
  {
    eos::common::RWMutexWriteLock lock(FsView::gFsView.ViewMutex);
    EXPECT_EQ(EINVAL, proc_fs_rm(node, mount, id, out, err, root));
  }
  EXPECT_NE(nullptr, FsView::gFsView.mIdView.lookupByID(11));
  mIsMaster = false;
  mount = "/data66";
  {
    eos::common::RWMutexWriteLock lock(FsView::gFsView.ViewMutex);
    EXPECT_EQ(0, proc_fs_rm(node, mount, id, out, err, root)) << err.c_str();
  }
  EXPECT_EQ(nullptr, FsView::gFsView.mIdView.lookupByID(10));
  EXPECT_FALSE(InGroup("spare", 10));
  EXPECT_TRUE(InGroup("spare", 11));
}
