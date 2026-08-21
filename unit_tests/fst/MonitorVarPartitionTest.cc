//------------------------------------------------------------------------------
// File: MonitorVarPartitionTest.cc
// Author: Jozsef Makai <jmakai@cern.ch>
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

#include "Namespace.hh"
#include "TestEnv.hh"
#include "common/Constants.hh"
#include "common/FileSystem.hh"
#include "common/RWMutex.hh"
#include "fst/storage/MonitorVarPartition.hh"
#include "gtest/gtest.h"

eos::fst::test::GTest_Logger gLogger(false);

EOSFSTTEST_NAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Mock class implementing only relevant methods related to unit testing
//------------------------------------------------------------------------------
struct MockFileSystem {
  eos::common::ConfigStatus status;
  eos::common::FsOpMask sched_ops;

  MockFileSystem()
      : status(eos::common::ConfigStatus::kRW)
      , sched_ops(eos::common::kMaskAll)
  {
  }

  void SetString(const std::string& key, const std::string& val)
  {
    if (key == eos::common::FS_SCHED_OPS_NAME) {
      this->sched_ops = eos::common::ParseSchedSpec(val).value_or(eos::common::kMaskNone);
      return;
    }

    this->status = eos::common::FileSystem::GetConfigStatusFromString(val.c_str())
                       .value_or(eos::common::ConfigStatus::kOff);
    this->sched_ops = eos::common::DeriveMaskFromLegacy(this->status);
  }

  eos::common::ConfigStatus GetConfigStatus(bool cached = false)
  {
    return this->status;
  }

  eos::common::FsOpMask
  GetSchedOps(bool cached = false)
  {
    return this->sched_ops;
  }
};

using VarMonitorT = eos::fst::MonitorVarPartition<std::vector<MockFileSystem*>>;

//------------------------------------------------------------------------------
//! Class MonitorVarPartitionTest
//------------------------------------------------------------------------------
class MonitorVarPartitionTest : public ::testing::Test
{
public:
  static constexpr std::int32_t mMonitorInterval = 1;
  std::ofstream fill;
  VarMonitorT monitor;
  std::thread monitor_thread;
  eos::common::RWMutex mFsMutex;
  std::vector<MockFileSystem*> fsVector;

  //----------------------------------------------------------------------------
  //! Method starting the monitoring thread
  //----------------------------------------------------------------------------
  static void StartFstPartitionMonitor(MonitorVarPartitionTest* storage)
  {
    storage->monitor.Monitor(storage->fsVector, storage->mFsMutex);
  }

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  MonitorVarPartitionTest():
    monitor(10.0, MonitorVarPartitionTest::mMonitorInterval, "/mnt/var_test/")
  {}

  virtual void SetUp() override
  {
    // Initialize partition
    (void) !system("mkdir -p /mnt/var_test");
    (void) ! system("mount -t tmpfs -o size=100m tmpfs /mnt/var_test/");
    // Add few fileSystems in the vector
    this->fsVector.push_back(new MockFileSystem());
    this->fsVector.push_back(new MockFileSystem());
    this->fsVector.push_back(new MockFileSystem());
    this->fsVector.push_back(new MockFileSystem());
    fill.open("/mnt/var_test/fill.temp");
    // Start monitoring
    this->monitor_thread =
      std::thread(MonitorVarPartitionTest::StartFstPartitionMonitor, this);
  }

  virtual void TearDown() override
  {
    // Clean resources
    delete this->fsVector[0];
    delete this->fsVector[1];
    delete this->fsVector[2];
    delete this->fsVector[3];
    // Stop monitoring
    this->monitor.StopMonitoring();
    this->monitor_thread.join();
    (void) !system("umount /mnt/var_test/");
    (void) !system("rmdir /mnt/var_test/");
  }
};

//------------------------------------------------------------------------------
// MonitorVarPartition Test
//------------------------------------------------------------------------------
TEST_F(MonitorVarPartitionTest, MonitorVarPartition)
{
  // Fill partition to more than 90%
  GLOG << "Filling partition to 90%" << std::endl;
  std::string megabyte_line(1024 * 1024, 'a');

  for (int i = 1; i <= 90; i++) {
    fill << megabyte_line << std::endl;
  }

  // Wait and check
  usleep(mMonitorInterval * 1000 * 1000);
  mFsMutex.LockRead();

  for (auto fs = fsVector.begin(); fs != fsVector.end(); ++fs) {
    ASSERT_EQ((*fs)->GetConfigStatus(), eos::common::ConfigStatus::kRO);
  }

  mFsMutex.UnLockRead();
  // Setting status of filesystems to RW
  GLOG << "Setting status to RW -- should revert to RO" << std::endl;
  mFsMutex.LockWrite();

  for (auto fs = fsVector.begin(); fs != fsVector.end(); ++fs) {
    (*fs)->SetString("configstatus", "rw");
  }

  mFsMutex.UnLockWrite();
  // Check if status has returned to read-only
  usleep(mMonitorInterval * 1000 * 1000);
  mFsMutex.LockRead();

  for (auto fs = fsVector.begin(); fs != fsVector.end(); ++fs) {
    ASSERT_EQ((*fs)->GetConfigStatus(), eos::common::ConfigStatus::kRO);
  }

  mFsMutex.UnLockRead();
  // Close and delete file
  GLOG << "Deleting file: /mnt/var_test/fill.temp" << std::endl;
  fill.close();
  (void) !system("rm /mnt/var_test/fill.temp");
  // Setting status of filesystems to RW
  GLOG << "Setting status to RW -- should stay at RW" << std::endl;
  mFsMutex.LockWrite();

  for (auto fs = fsVector.begin(); fs != fsVector.end(); ++fs) {
    (*fs)->SetString("configstatus", "rw");
  }

  mFsMutex.UnLockWrite();
  // Check if status remains as read/write
  usleep(mMonitorInterval * 1000 * 1000);
  mFsMutex.LockRead();

  for (auto fs = fsVector.begin(); fs != fsVector.end(); ++fs) {
    ASSERT_EQ((*fs)->GetConfigStatus(), eos::common::ConfigStatus::kRW);
  }

  mFsMutex.UnLockRead();
}

EOSFSTTEST_NAMESPACE_END
