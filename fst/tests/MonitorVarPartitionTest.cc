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

#include "MonitorVarPartitionTest.hh"

#include <fst/storage/MonitorVarPartition.hh>
#include <thread>

EOSFSTTEST_NAMESPACE_BEGIN

using fsstatus_t = eos::common::FileSystem::eConfigStatus;

struct MockFileSystem {
  fsstatus_t status;

  MockFileSystem() : status(eos::common::FileSystem::kRW) {}

  void SetConfigStatus(fsstatus_t status) {
    this->status = status;
  }

  fsstatus_t GetConfigStatus(bool cached = false) {
    return this->status;
  }
};

using VarMonitorT = eos::fst::MonitorVarPartition<std::vector<MockFileSystem *>>;

class TestContext {
public:
  eos::common::RWMutex fsMutex;
  std::vector<MockFileSystem *> fsVector;
  VarMonitorT monitor;
  std::thread monitor_thread;
  std::ofstream fill;
  static constexpr std::int32_t mMonitorInterval = 1;

  static void StartFstPartitionMonitor(TestContext* storage) {
    storage->monitor.Monitor(storage->fsVector, storage->fsMutex);
  }

  TestContext() : monitor(10., TestContext::mMonitorInterval, "/mnt/var_test/") {
    // Init partition
    system("mkdir -p /mnt/var_test");
    system("mount -t tmpfs -o size=100m tmpfs /mnt/var_test/");
    // Add few fileSystems in vector
    this->fsVector.push_back(new MockFileSystem());
    this->fsVector.push_back(new MockFileSystem());
    this->fsVector.push_back(new MockFileSystem());
    this->fsVector.push_back(new MockFileSystem());
    fill.open("/mnt/var_test/fill.temp");
    // Start monitoring.
    this->monitor_thread = std::thread(TestContext::StartFstPartitionMonitor, this);
  }

  virtual ~TestContext() {
    // Cleaning
    delete this->fsVector[0];
    delete this->fsVector[1];
    delete this->fsVector[2];
    delete this->fsVector[3];
    // Terminate monitoring
    this->monitor.StopMonitoring();
    this->monitor_thread.join();
    system("umount /mnt/var_test/");
    system("rmdir /mnt/var_test/");
  }
};

void
VarPartitionMonitoringTest() {
  TestContext context;

  // Fill partition to more than 90%
  std::string megabyte_line(1024 * 1024, 'a');

  for (int i = 0; i < 91; ++i) {
    context.fill << megabyte_line << std::endl;
  }

  // Wait and check
  usleep(TestContext::mMonitorInterval * 1000 * 1000);
  context.fsMutex.LockRead();

  for (auto fs = context.fsVector.begin(); fs != context.fsVector.end(); ++fs) {
    assert(eos::common::FileSystem::kRO == (*fs)->GetConfigStatus());
  }

  context.fsMutex.UnLockRead();
  // Setting status of filesystems to RW.
  context.fsMutex.LockWrite();

  for (auto fs = context.fsVector.begin(); fs != context.fsVector.end(); ++fs) {
    (*fs)->SetConfigStatus(eos::common::FileSystem::kRW);
  }

  context.fsMutex.UnLockWrite();
  // Check if status is returned to readonly
  usleep(context.mMonitorInterval * 1000 * 1000);
  context.fsMutex.LockRead();

  for (auto fs = context.fsVector.begin(); fs != context.fsVector.end(); ++fs) {
    assert(eos::common::FileSystem::kRO == (*fs)->GetConfigStatus());
  }

  context.fsMutex.UnLockRead();
  // Close and delete file,
  context.fill.close();
  system("rm -f /mnt/var_test/fill.temp");
  // Setting status of filesystems to RW
  context.fsMutex.LockWrite();

  for (auto fs = context.fsVector.begin(); fs != context.fsVector.end(); ++fs) {
    (*fs)->SetConfigStatus(eos::common::FileSystem::kRW);
  }

  context.fsMutex.UnLockWrite();
  // Check if status is returned to readonly
  usleep(TestContext::mMonitorInterval * 1000 * 1000);
  context.fsMutex.LockRead();

  for (auto fs = context.fsVector.begin(); fs != context.fsVector.end(); ++fs) {
    assert(eos::common::FileSystem::kRW == (*fs)->GetConfigStatus());
  }

  context.fsMutex.UnLockRead();
}

EOSFSTTEST_NAMESPACE_END