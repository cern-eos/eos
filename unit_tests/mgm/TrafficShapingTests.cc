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

TEST(TrafficShapingManager, GlobalRateSeedsDelayWhenNodeShardIsBelowLimit)
{
  constexpr double limit_bps = 200.0 * 1024.0 * 1024.0;
  constexpr double node_shard_bps = 50.0 * 1024.0 * 1024.0;
  constexpr double global_bps = 650.0 * 1024.0 * 1024.0;

  const uint64_t per_node_delay =
      eos::mgm::traffic_shaping::TrafficShapingManager::CalculateDelayUs(
          limit_bps, node_shard_bps, 0, 1.0, true, false);
  ASSERT_EQ(0u, per_node_delay);

  const uint64_t global_delay =
      eos::mgm::traffic_shaping::TrafficShapingManager::CalculateDelayUs(
          limit_bps, global_bps, 0, 1.0, true, false);
  ASSERT_GT(global_delay, 0u);
}

TEST(TrafficShapingManager, DelaySeedScalesWithActiveNodeShare)
{
  constexpr double global_limit_bps = 200.0 * 1024.0 * 1024.0;
  constexpr double global_bps = 650.0 * 1024.0 * 1024.0;
  constexpr double per_node_limit_bps = global_limit_bps / 16.0;
  constexpr double fst_reservation_factor = 4.0;

  const uint64_t global_seed =
      eos::mgm::traffic_shaping::TrafficShapingManager::CalculateDelayUs(
          global_limit_bps, global_bps, 0, 1.0, true, false);
  const uint64_t per_node_seed =
      eos::mgm::traffic_shaping::TrafficShapingManager::CalculateDelayUs(
          global_limit_bps, global_bps, 0, 1.0, true, false,
          per_node_limit_bps * fst_reservation_factor);

  ASSERT_GT(per_node_seed, global_seed);
  ASSERT_NEAR(20000.0, static_cast<double>(per_node_seed), 1000.0);
}

TEST(TrafficShapingManager, AboveLimitKeepsActiveNodeDelaySeedFloor)
{
  constexpr double global_limit_bps = 300.0 * 1000.0 * 1000.0;
  constexpr double global_bps = 1.2 * 1000.0 * 1000.0 * 1000.0;
  constexpr double per_node_limit_bps = global_limit_bps / 15.0;
  constexpr double fst_reservation_factor = 4.0;

  const uint64_t delay_us =
      eos::mgm::traffic_shaping::TrafficShapingManager::CalculateDelayUs(
          global_limit_bps, global_bps, 5000, 1.0, true, false,
          per_node_limit_bps * fst_reservation_factor);

  ASSERT_GT(delay_us, 12000u);
}

TEST(TrafficShapingManager, NearTargetKeepsDelayStable)
{
  constexpr double limit_bps = 300.0 * 1000.0 * 1000.0;
  constexpr uint64_t current_delay_us = 120000;

  const uint64_t slightly_low_delay =
      eos::mgm::traffic_shaping::TrafficShapingManager::CalculateDelayUs(
          limit_bps, limit_bps * 0.97, current_delay_us, 1.0, true, false,
          limit_bps / 15.0);
  const uint64_t slightly_high_delay =
      eos::mgm::traffic_shaping::TrafficShapingManager::CalculateDelayUs(
          limit_bps, limit_bps * 1.03, current_delay_us, 1.0, true, false,
          limit_bps / 15.0);

  ASSERT_EQ(current_delay_us, slightly_low_delay);
  ASSERT_EQ(current_delay_us, slightly_high_delay);
}
