//------------------------------------------------------------------------------
//! @file VarPartitionMonitorTest.cc
//! @author Stefan Isidorovic <stefan.isidorovic@comtrade.com>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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

#include "VarPartitionMonitorTest.hh"
#include <vector>
#include <unistd.h>
#include <sys/statvfs.h>
#include <errno.h>

CPPUNIT_TEST_SUITE_REGISTRATION(VarPartitionMonitorTest);

std::int32_t VarPartitionMonitorTest::mMonitorInterval = 1; // 1 second

//------------------------------------------------------------------------------
// Function starting the monitoring thread
//------------------------------------------------------------------------------
void* VarPartitionMonitorTest::StartFstPartitionMonitor(void* pp)
{
  VarPartitionMonitorTest* storage = reinterpret_cast<VarPartitionMonitorTest*>
                                     (pp);
  storage->monitor.Monitor(storage->fsVector, storage->mFsMutex);
  return 0;
}

//----------------------------------------------------------------------------
// Constructor
//----------------------------------------------------------------------------
VarPartitionMonitorTest::VarPartitionMonitorTest() :
  monitor(10., VarPartitionMonitorTest::mMonitorInterval, "/mnt/var_test/")
{}

//------------------------------------------------------------------------------
// CPPUNIT setUp method
//------------------------------------------------------------------------------
void VarPartitionMonitorTest::setUp(void)
{
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
  this->monitor_thread =
    std::thread(VarPartitionMonitorTest::StartFstPartitionMonitor, this);
}

//------------------------------------------------------------------------------
// Var monitoring test implementation
//------------------------------------------------------------------------------
void VarPartitionMonitorTest::VarMonitorTest()
{
  // Fill partition to more than 90%
  std::string megabyte_line(1024 * 1024, 'a');

  for (int i = 0; i < 91; ++i) {
    fill << megabyte_line << std::endl;
  }

  // Wait and check
  usleep(mMonitorInterval * 1000 * 1000);
  mFsMutex.LockRead();

  for (auto fs = fsVector.begin(); fs != fsVector.end(); ++fs) {
    CPPUNIT_ASSERT((*fs)->GetConfigStatus() == eos::common::FileSystem::kRO);
  }

  mFsMutex.UnLockRead();
  // Setting status of filesystems to RW.
  mFsMutex.LockWrite();

  for (auto fs = fsVector.begin(); fs != fsVector.end(); ++fs) {
    (*fs)->SetConfigStatus(eos::common::FileSystem::kRW);
  }

  mFsMutex.UnLockWrite();
  // Check if status is returned to readonly
  usleep(mMonitorInterval * 1000 * 1000);
  mFsMutex.LockRead();

  for (auto fs = fsVector.begin(); fs != fsVector.end(); ++fs) {
    CPPUNIT_ASSERT((*fs)->GetConfigStatus() == eos::common::FileSystem::kRO);
  }

  mFsMutex.UnLockRead();
  // Close and delete file,
  fill.close();
  system("rm /mnt/var_test/fill.temp");
  // Setting status of filesystems to RW
  mFsMutex.LockWrite();

  for (auto fs = fsVector.begin(); fs != fsVector.end(); ++fs) {
    (*fs)->SetConfigStatus(eos::common::FileSystem::kRW);
  }

  mFsMutex.UnLockWrite();
  // Check if status is returned to readonly
  usleep(mMonitorInterval * 1000 * 1000);
  mFsMutex.LockRead();

  for (auto fs = fsVector.begin(); fs != fsVector.end(); ++fs) {
    CPPUNIT_ASSERT((*fs)->GetConfigStatus() == eos::common::FileSystem::kRW);
  }

  mFsMutex.UnLockRead();
}

//------------------------------------------------------------------------------
// CPPUNIT tearDown method
//------------------------------------------------------------------------------
void VarPartitionMonitorTest::tearDown(void)
{
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
