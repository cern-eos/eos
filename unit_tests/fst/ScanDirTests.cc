//------------------------------------------------------------------------------
// File: ScanDirTests.cc
// Author: Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#define IN_TEST_HARNESS
#include "fst/ScanDir.hh"
#undef IN_TEST_HARNESS
#include "fst/Load.hh"
#include "common/Constants.hh"
#include "TmpDirTree.hh"
//------------------------------------------------------------------------------
// Helper method to convert current timestamp to string microseconds
// representation
//------------------------------------------------------------------------------
std::string GetTimestampSec(eos::common::SteadyClock& clock)
{
  using namespace std::chrono;
  uint64_t now_us = duration_cast<seconds>
                    (clock.getTime().time_since_epoch()).count();
  return std::to_string(now_us);
}

//------------------------------------------------------------------------------
// MockLoad class
//------------------------------------------------------------------------------
class MockLoad: public eos::fst::Load
{
public:
  MOCK_METHOD2(GetDiskRate, double(const char*, const char*));
};

TEST(ScanDir, RescanTiming)
{
  using namespace std::chrono;
  std::string path {"/"};
  eos::common::FileSystem::fsid_t fsid = 1;
  // Scanner completely disabled
  eos::fst::ScanDir sd(path.c_str(), fsid, nullptr, false, 0, 50, true);
  auto& clock = sd.GetClock();
  std::string sinit_ts = GetTimestampSec(clock);
  ASSERT_FALSE(sd.DoRescan(""));
  clock.advance(seconds(65));
  ASSERT_FALSE(sd.DoRescan(sinit_ts));
  // Configure the scan interval to 60 seconds
  sd.SetConfig("scaninterval", 60);
  // First time the file should be scanned
  ASSERT_TRUE(sd.DoRescan(""));
  // Update initial timestamp
  sinit_ts = GetTimestampSec(clock);
  ASSERT_FALSE(sd.DoRescan(sinit_ts));
  clock.advance(seconds(59));
  ASSERT_FALSE(sd.DoRescan(sinit_ts));
  clock.advance(seconds(2));
  ASSERT_TRUE(sd.DoRescan(sinit_ts));
}

TEST(ScanDir, TimestampSmeared)
{
  using namespace std::chrono;
  std::string path {"/"};
  eos::common::FileSystem::fsid_t fsid = 1;
  eos::fst::ScanDir sd(path.c_str(), fsid, nullptr, false, 0, 50, true);
  int interval = 300;
  sd.SetConfig(eos::common::SCAN_ENTRY_INTERVAL_NAME, interval);
  auto& clock = sd.GetClock();
  clock.advance(seconds(5000));

  for (int count = 0; count < 100; ++count) {
    uint64_t ts_sec = duration_cast<seconds>
                      (clock.getTime().time_since_epoch()).count();
    auto sts = sd.GetTimestampSmearedSec();
    ASSERT_TRUE(std::stoull(sts) >= ts_sec - interval);
    ASSERT_TRUE(std::stoull(sts) <= ts_sec + interval);
    clock.advance(seconds(1000));
  }
}

TEST(ScanDir, AdjustScanRate)
{
  using namespace std::chrono;
  using ::testing::_;
  using ::testing::Return;
  // Mock load class to return first a value for the disk rate below the
  // threshold and then only values above the threshold to trigger the
  // adjustment of the scan_rate but not lower then 5 MB/s
  MockLoad load;
  EXPECT_CALL(load, GetDiskRate(_, _)
             ).WillOnce(Return(500.0)).WillRepeatedly(Return(800.0));
  std::string path {"/"};
  eos::common::FileSystem::fsid_t fsid = 1;
  off_t offset = 0;
  int rate = 75;  // MB/s
  eos::fst::ScanDir sd(path.c_str(), fsid, &load, false, 0, rate, true);
  uint64_t open_ts_sec = duration_cast<seconds>
                         (sd.GetClock().getTime().time_since_epoch()).count();
  int old_rate = rate;
  sd.EnforceAndAdjustScanRate(offset, open_ts_sec, rate);
  ASSERT_EQ(rate, old_rate);

  while (rate > 5) {
    old_rate = rate;
    sd.EnforceAndAdjustScanRate(offset, open_ts_sec, rate);
    ASSERT_EQ(rate, (int)(old_rate * 0.9));
  }

  ASSERT_LE(rate, 5);
}

TEST_F(TmpDirTree, ScanDirSetConfig)
{
  MockLoad load;
  eos::common::FileSystem::fsid_t fsid = 1;
  eos::fst::ScanDir sd(TMP_DIR_ROOT.c_str(), fsid, &load, false, 0, 100, true);
  ASSERT_EQ(TMP_DIR_ROOT, "/tmp/fstest");
  ASSERT_EQ(sd.mDirPath, TMP_DIR_ROOT);
  ASSERT_EQ(sd.mDiskIntervalSec, eos::fst::DEFAULT_DISK_INTERVAL);
  ASSERT_EQ(sd.mFsckRefreshIntervalSec, eos::fst::DEFAULT_FSCK_INTERVAL);

  sd.SetConfig(eos::common::SCAN_DISK_INTERVAL_NAME, 3000);
  ASSERT_EQ(sd.mDiskIntervalSec, 3000);
  // This toggle logic is to ensure that CAS functions correctly
  sd.SetConfig(eos::common::SCAN_DISK_INTERVAL_NAME, eos::fst::DEFAULT_DISK_INTERVAL);
  ASSERT_EQ(sd.mDiskIntervalSec, eos::fst::DEFAULT_DISK_INTERVAL);
  sd.SetConfig(eos::common::SCAN_DISK_INTERVAL_NAME, 2500);
  ASSERT_EQ(sd.mDiskIntervalSec, 2500);

  sd.SetConfig(eos::common::FSCK_REFRESH_INTERVAL_NAME, 2000);
  ASSERT_EQ(sd.mFsckRefreshIntervalSec, 2000);
  sd.SetConfig(eos::common::FSCK_REFRESH_INTERVAL_NAME, 2500);
  ASSERT_EQ(sd.mFsckRefreshIntervalSec, 2500);
  sd.SetConfig(eos::common::FSCK_REFRESH_INTERVAL_NAME, eos::fst::DEFAULT_FSCK_INTERVAL);
  ASSERT_EQ(sd.mFsckRefreshIntervalSec, eos::fst::DEFAULT_FSCK_INTERVAL);
}
