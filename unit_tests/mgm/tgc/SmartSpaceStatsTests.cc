//------------------------------------------------------------------------------
// File: SmartSpaceStatsTests.cc
// Author: Steven Murray <smurray at cern dot ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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

#include "mgm/tgc/DummyTapeGcMgm.hh"
#include "mgm/tgc/SmartSpaceStats.hh"

#include <gtest/gtest.h>

class SmartSpaceStatsTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(SmartSpaceStatsTest, Constructor) {
  using namespace eos::mgm::tgc;

  const std::string spaceName = "test";
  DummyTapeGcMgm mgm;
  SpaceConfig spaceConfig;
  spaceConfig.availBytes = 10;
  spaceConfig.totalBytes = 20;
  spaceConfig.freeBytesScript = "";
  spaceConfig.queryPeriodSecs = 1;
  std::function<SpaceConfig()> spaceConfigGetter = [&] { return spaceConfig; };
  const std::time_t spaceConfigMaxAgeSecs = 0;
  CachedValue<SpaceConfig> cachedSpaceConfig(spaceConfigGetter, spaceConfigMaxAgeSecs);
  SmartSpaceStats stats(spaceName, mgm, cachedSpaceConfig);
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(SmartSpaceStatsTest, get_without_freebytesscript_set)
{
  using namespace eos::mgm::tgc;

  const std::string spaceName = "test";
  DummyTapeGcMgm mgm;
  SpaceConfig spaceConfig;
  spaceConfig.availBytes = 10;
  spaceConfig.totalBytes = 20;
  spaceConfig.freeBytesScript = "";
  spaceConfig.queryPeriodSecs = 1;
  std::function<SpaceConfig()> spaceConfigGetter = [&]{return spaceConfig;};
  const std::time_t spaceConfigMaxAgeSecs = 0;
  CachedValue<SpaceConfig> cachedSpaceConfig(spaceConfigGetter, spaceConfigMaxAgeSecs);
  SmartSpaceStats stats(spaceName, mgm, cachedSpaceConfig);

  SpaceStats dummyMgmStats;
  dummyMgmStats.totalBytes = 100;
  dummyMgmStats.availBytes = 90;
  mgm.setSpaceStats(spaceName, dummyMgmStats);

  ASSERT_EQ(0, mgm.getNbCallsToGetSpaceStats());
  const auto result = stats.get();
  ASSERT_EQ(1, mgm.getNbCallsToGetSpaceStats());

  ASSERT_EQ(dummyMgmStats, result.stats);
  ASSERT_EQ(SmartSpaceStats::Src::INTERNAL_BECAUSE_SCRIPT_PATH_EMPTY, result.availBytesSrc);
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(SmartSpaceStatsTest, get_with_freebytesscript_set)
{
  using namespace eos::mgm::tgc;

  const std::string spaceName = "test";

  DummyTapeGcMgm mgm;
  SpaceStats internalStats;
  internalStats.totalBytes = 100;
  internalStats.availBytes = 90;
  mgm.setSpaceStats(spaceName, internalStats);
  const std::string scriptAvailBytesString = "80";
  const std::uint64_t scriptAvailBytesUint64 = 80;
  mgm.setStdoutFromShellCmd(scriptAvailBytesString);

  SpaceConfig spaceConfig;
  spaceConfig.availBytes = 10;
  spaceConfig.totalBytes = 20;
  spaceConfig.freeBytesScript = "test";
  spaceConfig.queryPeriodSecs = 1;
  std::function<SpaceConfig()> spaceConfigGetter = [&]{return spaceConfig;};
  const std::time_t spaceConfigMaxAgeSecs = 0;
  CachedValue<SpaceConfig> cachedSpaceConfig(spaceConfigGetter, spaceConfigMaxAgeSecs);
  SmartSpaceStats stats(spaceName, mgm, cachedSpaceConfig);

  {
    const auto result = stats.get();
    switch(result.availBytesSrc) {
    case SmartSpaceStats::Src::INTERNAL_BECAUSE_SCRIPT_PENDING_AND_NO_PREVIOUS_VALUE:
      ASSERT_EQ(internalStats.availBytes, result.stats.availBytes);
      break;
    case SmartSpaceStats::Src::SCRIPT_VALUE_BECAUSE_SCRIPT_JUST_FINISHED:
      ASSERT_EQ(scriptAvailBytesUint64, result.stats.availBytes);
      break;
    default:
      FAIL() << "Unexpected value for result.availBytesSrc: value=" << SmartSpaceStats::srcToStr(result.availBytesSrc);
    }
  }

  {
    const auto result = stats.get();
    switch(result.availBytesSrc) {
    case SmartSpaceStats::Src::INTERNAL_BECAUSE_SCRIPT_PENDING_AND_NO_PREVIOUS_VALUE:
      ASSERT_EQ(internalStats.availBytes, result.stats.availBytes);
      break;
    case SmartSpaceStats::Src::SCRIPT_VALUE_BECAUSE_SCRIPT_JUST_FINISHED:
      ASSERT_EQ(scriptAvailBytesUint64, result.stats.availBytes);
      break;
    case SmartSpaceStats::Src::SCRIPT_PREVIOUS_VALUE_BECAUSE_SCRIPT_PENDING:
      ASSERT_EQ(scriptAvailBytesUint64, result.stats.availBytes);
      break;
    default:
      FAIL() << "Unexpected value for result.availBytesSrc: value=" << SmartSpaceStats::srcToStr(result.availBytesSrc);
    }
  }
}