//------------------------------------------------------------------------------
// File: TrafficShapingTests.cc
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

#include "common/Constants.hh"
#include "mgm/fsview/FsView.hh"
#include "gtest/gtest.h"
#define IN_TEST_HARNESS
#include "mgm/shaping/TrafficShaping.hh"
#undef IN_TEST_HARNESS

TEST(TrafficShapingEngine, DetailConfigReplayDoesNotSyncWhileViewLocked)
{
  eos::mgm::traffic_shaping::TrafficShapingEngine engine;
  {
    eos::common::RWMutexWriteLock wr_lock(eos::mgm::FsView::gFsView.ViewMutex);
    engine.ApplyDetailLevelConfig(eos::common::TRAFFIC_SHAPING_DETAIL_LEVEL_FILESYSTEM);
  }

  ASSERT_EQ(eos::common::TRAFFIC_SHAPING_DETAIL_LEVEL_FILESYSTEM,
            engine.GetDetailLevel());
}

TEST(TrafficShapingManager, IdleDelaySeedIsKeptBeforeEntityTrafficIsSeen)
{
  constexpr double limit_bps = 1024.0 * 1024.0;
  uint64_t delay_us = 0;

  for (int tick = 0; tick < 30; ++tick) {
    delay_us = eos::mgm::traffic_shaping::TrafficShapingManager::CalculateDelayUs(
        limit_bps, 0.0, delay_us, 0.0, false, true);
    ASSERT_EQ(1000000u, delay_us);
  }
}

TEST(TrafficShapingManager, IdleDelayReleasesAfterEntityTrafficIsSeenWithoutPressure)
{
  constexpr double limit_bps = 1024.0 * 1024.0;
  uint64_t delay_us = 1000000;

  for (int tick = 0; tick < 5; ++tick) {
    delay_us = eos::mgm::traffic_shaping::TrafficShapingManager::CalculateDelayUs(
        limit_bps, 0.0, delay_us, 0.0, true, true);
  }

  ASSERT_EQ(0u, delay_us);

  delay_us = eos::mgm::traffic_shaping::TrafficShapingManager::CalculateDelayUs(
      limit_bps, 0.0, delay_us, 0.0, true, true);
  ASSERT_EQ(0u, delay_us);
}

TEST(TrafficShapingManager, IdleDelaySeedIsKeptForExplicitLimitAfterTrafficIsSeen)
{
  constexpr double limit_bps = 1024.0 * 1024.0;
  uint64_t delay_us = 1000000;

  for (int tick = 0; tick < 30; ++tick) {
    delay_us = eos::mgm::traffic_shaping::TrafficShapingManager::CalculateDelayUs(
        limit_bps, 0.0, delay_us, 0.0, true, false);
    ASSERT_EQ(1000000u, delay_us);
  }
}
