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

#include "fst/ScanDir.hh"
#include "gtest/gtest.h"

// Helper method to convert current timestamp to string microseconds
// representation
std::string GetTimestampMicroSec(eos::common::SteadyClock& clock)
{
  using namespace std::chrono;
  uint64_t now_us = duration_cast<microseconds>
                    (clock.getTime().time_since_epoch()).count();
  return std::to_string(now_us);
}

TEST(ScanDir, RescanTiming)
{
  using namespace std::chrono;
  std::string dir_path {"/"};
  eos::common::FileSystem::fsid_t fsid = 1;
  // Scanner completely disabled
  eos::fst::ScanDir sd(dir_path.c_str(), fsid, nullptr, false, 0, 50, false,
                       true);
  auto& clock = sd.GetClock();
  std::string sinit_ts = GetTimestampMicroSec(clock);
  ASSERT_FALSE(sd.DoRescan(""));
  clock.advance(seconds(65));
  ASSERT_FALSE(sd.DoRescan(sinit_ts));
  // Configure the scan interval to 60 seconds
  sd.SetConfig("scaninterval", 60);
  // First time the file should be scanned
  ASSERT_TRUE(sd.DoRescan(""));
  // Update initial timestamp
  sinit_ts = GetTimestampMicroSec(clock);
  ASSERT_FALSE(sd.DoRescan(sinit_ts));
  clock.advance(seconds(59));
  ASSERT_FALSE(sd.DoRescan(sinit_ts));
  clock.advance(seconds(2));
  ASSERT_TRUE(sd.DoRescan(sinit_ts));
}

