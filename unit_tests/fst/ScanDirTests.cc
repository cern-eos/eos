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
#include "fst/utils/ScanRate.hh"
#include "common/Constants.hh"
#include "unit_tests/fst/TmpDirTree.hh"
//------------------------------------------------------------------------------
// Helper method to convert current timestamp to string microseconds
// representation
//------------------------------------------------------------------------------
int64_t GetTimestampSec(eos::common::SteadyClock& clock)
{
  using namespace std::chrono;
  return duration_cast<seconds>
         (clock.GetTime().time_since_epoch()).count();
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
  auto sinit_ts = GetTimestampSec(clock);
  ASSERT_FALSE(sd.DoRescan(seconds(-1)));
  clock.advance(seconds(65));
  ASSERT_FALSE(sd.DoRescan(seconds(sinit_ts)));
  // Configure the scan interval to 60 seconds
  sd.SetConfig("scaninterval", 60);
  // First time the file should be scanned
  ASSERT_TRUE(sd.DoRescan(seconds(-1)));
  // Update initial timestamp
  sinit_ts = GetTimestampSec(clock);
  ASSERT_FALSE(sd.DoRescan(seconds(sinit_ts)));
  clock.advance(seconds(59));
  ASSERT_FALSE(sd.DoRescan(seconds(sinit_ts)));
  clock.advance(seconds(2));
  ASSERT_TRUE(sd.DoRescan(seconds(sinit_ts)));
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
                      (clock.GetTime().time_since_epoch()).count();
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
  std::string path {"/var/"};
  eos::common::FileSystem::fsid_t fsid = 1;
  off_t offset = 0;
  int rate = 75;  // MB/s
  eos::fst::ScanDir sd(path.c_str(), fsid, &load, false, 0, rate, true);
  eos::fst::utils::ScanRateLimiter rate_limiter(rate, &load, path);
  int old_rate = rate_limiter.GetRate();
  rate_limiter.Throttle(offset);
  ASSERT_EQ(rate_limiter.GetRate(), old_rate);

  while (rate_limiter.GetRate() > 5) {
    old_rate = rate_limiter.GetRate();
    rate_limiter.Throttle(offset);
    ASSERT_EQ(rate_limiter.GetRate(), (int)(old_rate * 0.9));
  }

  ASSERT_LE(rate_limiter.GetRate(), 5);
}

//------------------------------------------------------------------------------
// A rate drop must only apply to the data still to be scanned, otherwise the
// already scanned data gets re-priced at the new rate and the scanner blocks
// for the accumulated difference - up to tens of minutes for a big file.
//------------------------------------------------------------------------------
TEST(ScanDir, EnforceScanRateNoRetroactiveSleep)
{
  using namespace std::chrono;
  using ::testing::_;
  using ::testing::Return;
  // Always report a disk load above the threshold so that the rate decays
  // from 100 MB/s down to the 5 MB/s floor
  MockLoad load;
  EXPECT_CALL(load, GetDiskRate(_, _)).WillRepeatedly(Return(800.0));
  const std::string path{"/var/"};
  eos::fst::utils::ScanRateLimiter rate_limiter(100, &load, path, 100);
  const auto start_ts = steady_clock::now();
  // Simulate a scan which is already 10 GB into the file and hands over 64 kB
  // chunks while the rate is being decayed
  off_t offset = 10ll * 1024 * 1024 * 1024;

  for (int i = 0; i < 50; ++i) {
    offset += 64 * 1024;
    rate_limiter.Throttle(offset);
  }

  ASSERT_EQ(rate_limiter.GetRate(), 5);
  // Throttling 3 MB, even at the 5 MB/s floor, must not take more than the
  // fraction of a second needed for the data handed over during this test
  ASSERT_LT(duration_cast<seconds>(steady_clock::now() - start_ts).count(), 10);
}

//------------------------------------------------------------------------------
// A negative or zero rate must disable the throttling instead of blocking
//------------------------------------------------------------------------------
TEST(ScanDir, EnforceScanRateInvalidRate)
{
  using namespace std::chrono;
  const auto start_ts = steady_clock::now();
  eos::fst::utils::ScanRateLimiter no_rate(0);
  eos::fst::utils::ScanRateLimiter bad_rate(-1276116992);
  no_rate.Throttle(10ll * 1024 * 1024 * 1024);
  bad_rate.Throttle(10ll * 1024 * 1024 * 1024);
  ASSERT_LT(duration_cast<seconds>(steady_clock::now() - start_ts).count(), 5);
}

TEST_F(TmpDirTree, ScanDirSetConfig)
{
  MockLoad load;
  eos::common::FileSystem::fsid_t fsid = 1;
  eos::fst::ScanDir sd(TMP_DIR_ROOT.c_str(), fsid, &load, false, 0, 100, true);
  ASSERT_EQ(TMP_DIR_ROOT, "/tmp/fstest");
  ASSERT_EQ(sd.mDirPath, TMP_DIR_ROOT);
  ASSERT_EQ(sd.mDiskInterval.get(), eos::fst::DEFAULT_DISK_INTERVAL);
}
