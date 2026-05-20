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

TEST(TrafficShapingEngine, LimitAndReservationTogglesPropagateToManager)
{
  eos::mgm::traffic_shaping::TrafficShapingEngine engine;
  auto manager = engine.GetManager();

  ASSERT_TRUE(engine.ApplyLimitsEnabledConfig(false));
  ASSERT_FALSE(engine.GetLimitsEnabled());
  ASSERT_FALSE(manager->GetLimitsEnabled());

  ASSERT_FALSE(engine.ApplyLimitsEnabledConfig(false));

  ASSERT_TRUE(engine.ApplyReservationsEnabledConfig(false));
  ASSERT_FALSE(engine.GetReservationsEnabled());
  ASSERT_FALSE(manager->GetReservationsEnabled());

  ASSERT_FALSE(engine.ApplyReservationsEnabledConfig(false));

  ASSERT_TRUE(engine.ApplyLimitsEnabledConfig(true));
  ASSERT_TRUE(engine.GetLimitsEnabled());
  ASSERT_TRUE(manager->GetLimitsEnabled());

  ASSERT_TRUE(engine.ApplyReservationsEnabledConfig(true));
  ASSERT_TRUE(engine.GetReservationsEnabled());
  ASSERT_TRUE(manager->GetReservationsEnabled());
}

TEST(TrafficShapingEngine, GarbageCollectionIdleConfigIsClamped)
{
  eos::mgm::traffic_shaping::TrafficShapingEngine engine;

  ASSERT_FALSE(engine.ApplyGarbageCollectionIdleSecondsConfig(
      eos::mgm::traffic_shaping::kDefaultGarbageCollectionIdleSec));
  ASSERT_EQ(eos::mgm::traffic_shaping::kDefaultGarbageCollectionIdleSec,
            engine.GetGarbageCollectionIdleSeconds());

  ASSERT_TRUE(engine.ApplyGarbageCollectionIdleSecondsConfig(0));
  ASSERT_EQ(eos::mgm::traffic_shaping::kMinGarbageCollectionIdleSec,
            engine.GetGarbageCollectionIdleSeconds());

  ASSERT_TRUE(engine.ApplyGarbageCollectionIdleSecondsConfig(
      eos::mgm::traffic_shaping::kMaxGarbageCollectionIdleSec + 1));
  ASSERT_EQ(eos::mgm::traffic_shaping::kMaxGarbageCollectionIdleSec,
            engine.GetGarbageCollectionIdleSeconds());
}

TEST(TrafficShapingManager, FilesystemDetailStatsFollowDetailToggle)
{
  eos::mgm::traffic_shaping::TrafficShapingManager manager;

  auto make_report = [](const uint64_t total_bytes_written) {
    eos::traffic_shaping::FstIoReport report;
    report.set_node_id("/eos/fst.example:1095/fst");
    auto* entry = report.add_entries();
    entry->set_app_name("detail-test-app");
    entry->set_uid(1);
    entry->set_gid(2);
    entry->set_fsid(3);
    entry->set_generation_id(1);
    entry->set_total_bytes_written(total_bytes_written);
    entry->set_total_write_ops(total_bytes_written / 4096);
    return report;
  };

  manager.ProcessReport(make_report(1024 * 1024));
  manager.ProcessReport(make_report(2 * 1024 * 1024));
  manager.UpdateEstimators(1.0);

  ASSERT_FALSE(manager.GetGlobalStats().empty());
  ASSERT_TRUE(manager.GetDiskStats().empty());
  ASSERT_TRUE(manager.GetDetailedStats().empty());

  manager.SetFilesystemDetailEnabled(true);
  manager.ProcessReport(make_report(3 * 1024 * 1024));
  manager.UpdateEstimators(1.0);

  ASSERT_FALSE(manager.GetDiskStats().empty());
  ASSERT_FALSE(manager.GetDetailedStats().empty());
}

TEST(TrafficShapingManager, MapCardinalityStatsTrackInternalMaps)
{
  eos::mgm::traffic_shaping::TrafficShapingManager manager;

  auto make_report = [](const std::string& node_id, const std::string& app,
                        const uint32_t uid, const uint64_t total_bytes_written) {
    eos::traffic_shaping::FstIoReport report;
    report.set_node_id(node_id);
    auto* entry = report.add_entries();
    entry->set_app_name(app);
    entry->set_uid(uid);
    entry->set_gid(2);
    entry->set_fsid(3);
    entry->set_generation_id(1);
    entry->set_total_bytes_written(total_bytes_written);
    entry->set_total_write_ops(total_bytes_written / 4096);
    return report;
  };

  manager.ProcessReport(
      make_report("/eos/fst-a.example:1095/fst", "cardinality-app-a", 1, 1024 * 1024));
  manager.ProcessReport(make_report("/eos/fst-a.example:1095/fst", "cardinality-app-a", 1,
                                    2 * 1024 * 1024));
  manager.ProcessReport(
      make_report("/eos/fst-b.example:1095/fst", "cardinality-app-b", 2, 1024 * 1024));

  ASSERT_TRUE(manager.LoadPoliciesFromString(
      "{\"appPolicies\":{\"cardinality-app-a\":{\"limitWriteBytesPerSec\":1000,"
      "\"isEnabled\":true}},"
      "\"uidPolicies\":{\"1\":{\"limitWriteBytesPerSec\":1000,\"isEnabled\":true}},"
      "\"gidPolicies\":{\"2\":{\"limitWriteBytesPerSec\":1000,\"isEnabled\":true}}}"));

  const auto stats = manager.GetMapCardinalityStats();
  ASSERT_EQ(2u, stats.node_states);
  ASSERT_EQ(2u, stats.node_state_streams);
  ASSERT_EQ(1u, stats.global_stats);
  ASSERT_EQ(1u, stats.node_stats);
  ASSERT_EQ(1u, stats.node_entity_stats);
  ASSERT_EQ(0u, stats.disk_stats);
  ASSERT_EQ(0u, stats.detailed_stats);
  ASSERT_EQ(1u, stats.app_policies);
  ASSERT_EQ(1u, stats.uid_policies);
  ASSERT_EQ(1u, stats.gid_policies);
}

TEST(TrafficShapingManager, DisablingReservationsClearsEphemeralLimits)
{
  eos::mgm::traffic_shaping::TrafficShapingManager manager;

  ASSERT_TRUE(
      manager.LoadPoliciesFromString("{\"appPolicies\":{\"reserved-app\":{"
                                     "\"reservationWriteBytesPerSec\":300000000}}}"));

  auto policy = manager.GetAppPolicy("reserved-app");
  ASSERT_TRUE(policy.has_value());
  policy->controller_limit_write_bytes_per_sec = 300000000;
  manager.SetAppPolicy("reserved-app", *policy);

  policy = manager.GetAppPolicy("reserved-app");
  ASSERT_TRUE(policy.has_value());
  ASSERT_EQ(300000000u, policy->controller_limit_write_bytes_per_sec);

  manager.SetReservationsEnabled(false);

  policy = manager.GetAppPolicy("reserved-app");
  ASSERT_TRUE(policy.has_value());
  ASSERT_EQ(300000000u, policy->reservation_write_bytes_per_sec);
  ASSERT_EQ(0u, policy->controller_limit_write_bytes_per_sec);
  ASSERT_EQ(0u, policy->controller_limit_read_bytes_per_sec);
}

TEST(TrafficShapingManager, EphemeralLimitsExpireWithoutHeartbeat)
{
  eos::mgm::traffic_shaping::TrafficShapingManager manager;

  ASSERT_TRUE(
      manager.LoadPoliciesFromString("{\"appPolicies\":{\"reserved-app\":{"
                                     "\"reservationWriteBytesPerSec\":300000000}}}"));

  auto policy = manager.GetAppPolicy("reserved-app");
  ASSERT_TRUE(policy.has_value());
  policy->controller_limit_write_bytes_per_sec = 300000000;
  manager.SetAppPolicy("reserved-app", *policy);

  policy = manager.GetAppPolicy("reserved-app");
  ASSERT_TRUE(policy.has_value());
  ASSERT_EQ(300000000u, policy->controller_limit_write_bytes_per_sec);
  ASSERT_NE(std::chrono::steady_clock::time_point{},
            policy->controller_limit_write_update_time);

  const auto heartbeat_time = policy->controller_limit_write_update_time;
  ASSERT_EQ(0u,
            manager.ExpireControllerLimits(heartbeat_time + std::chrono::seconds(299)));

  policy = manager.GetAppPolicy("reserved-app");
  ASSERT_TRUE(policy.has_value());
  ASSERT_EQ(300000000u, policy->controller_limit_write_bytes_per_sec);

  ASSERT_EQ(1u, manager.ExpireControllerLimits(heartbeat_time + std::chrono::minutes(5)));

  policy = manager.GetAppPolicy("reserved-app");
  ASSERT_TRUE(policy.has_value());
  ASSERT_EQ(300000000u, policy->reservation_write_bytes_per_sec);
  ASSERT_EQ(0u, policy->controller_limit_write_bytes_per_sec);
}

TEST(TrafficShapingManager, ReservedAppIoPressureOnlyTracksReservedApps)
{
  eos::mgm::traffic_shaping::TrafficShapingManager manager;

  ASSERT_TRUE(manager.LoadPoliciesFromString(
      "{\"appPolicies\":{\"reserved-disabled-app\":{"
      "\"isEnabled\":false,\"reservationWriteBytesPerSec\":300000000},"
      "\"limited-app\":{\"isEnabled\":true,\"limitWriteBytesPerSec\":100000000}}}"));

  const auto pressure = manager.GetReservedAppIoPressure();

  ASSERT_EQ(1u, pressure.size());
  ASSERT_TRUE(pressure.find("reserved-disabled-app") != pressure.end());
  ASSERT_TRUE(pressure.find("limited-app") == pressure.end());
  ASSERT_FALSE(pressure.at("reserved-disabled-app").has_read);
  ASSERT_FALSE(pressure.at("reserved-disabled-app").has_write);
}

TEST(TrafficShapingManager, IdleDelaySeedIsKeptBeforeEntityTrafficIsSeen)
{
  constexpr double limit_bps = 1024.0 * 1024.0;
  uint64_t delay_us = 0;

  for (int tick = 0; tick < 30; ++tick) {
    delay_us = eos::mgm::traffic_shaping::TrafficShapingManager::CalculateDelayUs(
        limit_bps, 0.0, delay_us, 0.0, false, true);
    ASSERT_NEAR(1000000.0, static_cast<double>(delay_us), 1000.0);
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
    ASSERT_NEAR(1000000.0, static_cast<double>(delay_us), 1000.0);
  }
}

TEST(TrafficShapingManager, ExplicitLimitSlowlyReleasesHighDelayOnSparseSample)
{
  constexpr double limit_bps = 1024.0 * 1024.0;
  constexpr uint64_t current_delay_us = 1500000;

  const uint64_t delay_us =
      eos::mgm::traffic_shaping::TrafficShapingManager::CalculateDelayUs(
          limit_bps, 0.0, current_delay_us, 1.0, true, false);

  ASSERT_LT(delay_us, current_delay_us);
  ASSERT_GT(delay_us, 1000000u);
}

TEST(TrafficShapingManager, DelaySeedAccountsForReferenceRate)
{
  constexpr double limit_bps = 1024.0 * 1024.0;
  constexpr double delay_reference_bps = limit_bps * 4.0;

  const uint64_t delay_us =
      eos::mgm::traffic_shaping::TrafficShapingManager::CalculateDelayUs(
          limit_bps, 0.0, 0, 1.0, false, false, delay_reference_bps);

  ASSERT_NEAR(250000.0, static_cast<double>(delay_us), 1000.0);
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

TEST(TrafficShapingManager, DelaySeedScalesWithLowerReferenceRate)
{
  constexpr double global_limit_bps = 200.0 * 1024.0 * 1024.0;
  constexpr double global_bps = 650.0 * 1024.0 * 1024.0;
  constexpr double lower_reference_bps = global_limit_bps / 4.0;

  const uint64_t global_seed =
      eos::mgm::traffic_shaping::TrafficShapingManager::CalculateDelayUs(
          global_limit_bps, global_bps, 0, 1.0, true, false);
  const uint64_t per_node_seed =
      eos::mgm::traffic_shaping::TrafficShapingManager::CalculateDelayUs(
          global_limit_bps, global_bps, 0, 1.0, true, false, lower_reference_bps);

  ASSERT_GT(per_node_seed, global_seed);
  ASSERT_NEAR(177500.0, static_cast<double>(per_node_seed), 1000.0);
}

TEST(TrafficShapingManager, AboveLimitKeepsReferenceDelaySeedFloor)
{
  constexpr double global_limit_bps = 300.0 * 1000.0 * 1000.0;
  constexpr double global_bps = 1.2 * 1000.0 * 1000.0 * 1000.0;
  constexpr double lower_reference_bps = global_limit_bps / 4.0;

  const uint64_t delay_us =
      eos::mgm::traffic_shaping::TrafficShapingManager::CalculateDelayUs(
          global_limit_bps, global_bps, 5000, 1.0, true, false, lower_reference_bps);

  ASSERT_GT(delay_us, 12000u);
}

TEST(TrafficShapingManager, BelowTargetCanReleaseBelowSeedDelay)
{
  constexpr double limit_bps = 200.0 * 1000.0 * 1000.0;
  constexpr uint64_t seed_delay_us =
      static_cast<uint64_t>((1024.0 * 1024.0 * 1000000.0) / limit_bps);

  const uint64_t delay_us =
      eos::mgm::traffic_shaping::TrafficShapingManager::CalculateDelayUs(
          limit_bps, limit_bps * 0.865, seed_delay_us, 1.0, true, false);

  ASSERT_LT(delay_us, seed_delay_us);
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
          limit_bps, limit_bps * 1.01, current_delay_us, 1.0, true, false,
          limit_bps / 15.0);

  ASSERT_EQ(current_delay_us, slightly_low_delay);
  ASSERT_EQ(current_delay_us, slightly_high_delay);
}

TEST(TrafficShapingManager, DefaultReservationControllerRequiresIoPressure)
{
  constexpr uint64_t reservation_bps = 300ULL * 1000ULL * 1000ULL;

  eos::mgm::traffic_shaping::AppState app{};
  app.reservation_write_bps = reservation_bps;
  app.current_write_bps = 1.2 * 1000.0 * 1000.0 * 1000.0;

  std::vector<eos::mgm::traffic_shaping::AppState> apps{app};
  eos::mgm::traffic_shaping::TrafficShapingManager::ApplyDefaultReservationController(
      apps);
  ASSERT_FALSE(apps[0].update_write);

  apps[0] = app;
  apps[0].has_write_io_pressure = true;
  apps[0].current_write_io_pressure = 0.0;
  eos::mgm::traffic_shaping::TrafficShapingManager::ApplyDefaultReservationController(
      apps);
  ASSERT_FALSE(apps[0].update_write);

  apps[0] = app;
  apps[0].has_write_io_pressure = true;
  apps[0].current_write_io_pressure = 0.02;
  eos::mgm::traffic_shaping::TrafficShapingManager::ApplyDefaultReservationController(
      apps);
  ASSERT_FALSE(apps[0].update_write);
}

TEST(TrafficShapingManager, DefaultReservationControllerLimitsCompetitors)
{
  constexpr uint64_t reservation_bps = 1000ULL * 1000ULL * 1000ULL;
  constexpr double reserved_rate_bps = 700.0 * 1000.0 * 1000.0;
  constexpr double competitor_rate_bps = 700.0 * 1000.0 * 1000.0;

  eos::mgm::traffic_shaping::AppState reserved_app{};
  reserved_app.reservation_write_bps = reservation_bps;
  reserved_app.current_write_bps = reserved_rate_bps;
  reserved_app.has_write_io_pressure = true;
  reserved_app.current_write_io_pressure = 0.5;

  eos::mgm::traffic_shaping::AppState competitor{};
  competitor.current_write_bps = competitor_rate_bps;
  competitor.has_write_io_pressure = true;
  competitor.current_write_io_pressure = 0.5;
  competitor.has_write_reservation_competition = true;

  std::vector<eos::mgm::traffic_shaping::AppState> apps{reserved_app, competitor};
  eos::mgm::traffic_shaping::TrafficShapingManager::ApplyDefaultReservationController(
      apps);

  ASSERT_FALSE(apps[0].update_write);
  ASSERT_TRUE(apps[1].update_write);
  ASSERT_EQ(400ULL * 1000ULL * 1000ULL, apps[1].new_controller_limit_write_bps);
}

TEST(TrafficShapingManager, DefaultReservationControllerHonorsMinimumLimit)
{
  constexpr uint64_t reservation_bps = 1000ULL * 1000ULL * 1000ULL;
  constexpr uint64_t controller_min_limit_bps = 100ULL * 1000ULL * 1000ULL;

  eos::mgm::traffic_shaping::AppState reserved_app{};
  reserved_app.reservation_write_bps = reservation_bps;
  reserved_app.current_write_bps = 100.0 * 1000.0 * 1000.0;
  reserved_app.has_write_io_pressure = true;
  reserved_app.current_write_io_pressure = 0.5;

  eos::mgm::traffic_shaping::AppState competitor{};
  competitor.current_write_bps = 150.0 * 1000.0 * 1000.0;
  competitor.has_write_reservation_competition = true;

  std::vector<eos::mgm::traffic_shaping::AppState> apps{reserved_app, competitor};
  eos::mgm::traffic_shaping::TrafficShapingManager::ApplyDefaultReservationController(
      apps, true, controller_min_limit_bps);

  ASSERT_TRUE(apps[1].update_write);
  ASSERT_EQ(controller_min_limit_bps, apps[1].new_controller_limit_write_bps);
}

TEST(TrafficShapingManager, DefaultReservationControllerHonorsPressureThreshold)
{
  constexpr uint64_t reservation_bps = 1000ULL * 1000ULL * 1000ULL;

  eos::mgm::traffic_shaping::AppState reserved_app{};
  reserved_app.reservation_write_bps = reservation_bps;
  reserved_app.current_write_bps = 700.0 * 1000.0 * 1000.0;
  reserved_app.has_write_io_pressure = true;
  reserved_app.current_write_io_pressure = 0.02;

  eos::mgm::traffic_shaping::AppState competitor{};
  competitor.current_write_bps = 700.0 * 1000.0 * 1000.0;
  competitor.controller_limit_write_bps = 400ULL * 1000ULL * 1000ULL;
  competitor.has_write_reservation_competition = true;

  std::vector<eos::mgm::traffic_shaping::AppState> apps{reserved_app, competitor};
  eos::mgm::traffic_shaping::TrafficShapingManager::ApplyDefaultReservationController(
      apps, true, eos::mgm::traffic_shaping::kDefaultControllerMinLimitBps, 0.05);

  ASSERT_TRUE(apps[1].update_write);
  ASSERT_EQ(0u, apps[1].new_controller_limit_write_bps);

  apps = {reserved_app, competitor};
  eos::mgm::traffic_shaping::TrafficShapingManager::ApplyDefaultReservationController(
      apps, true, eos::mgm::traffic_shaping::kDefaultControllerMinLimitBps, 0.01);

  ASSERT_TRUE(apps[1].update_write);
  ASSERT_EQ(400ULL * 1000ULL * 1000ULL, apps[1].new_controller_limit_write_bps);
}

TEST(TrafficShapingManager,
     DefaultReservationControllerDoesNotLimitCompetitorsWithoutPressure)
{
  constexpr uint64_t reservation_bps = 1000ULL * 1000ULL * 1000ULL;

  eos::mgm::traffic_shaping::AppState reserved_app{};
  reserved_app.reservation_write_bps = reservation_bps;
  reserved_app.current_write_bps = 700.0 * 1000.0 * 1000.0;
  reserved_app.has_write_io_pressure = true;
  reserved_app.current_write_io_pressure = 0.0;

  eos::mgm::traffic_shaping::AppState competitor{};
  competitor.current_write_bps = 700.0 * 1000.0 * 1000.0;
  competitor.controller_limit_write_bps = 400ULL * 1000ULL * 1000ULL;
  competitor.has_write_reservation_competition = true;

  std::vector<eos::mgm::traffic_shaping::AppState> apps{reserved_app, competitor};
  eos::mgm::traffic_shaping::TrafficShapingManager::ApplyDefaultReservationController(
      apps);

  ASSERT_FALSE(apps[0].update_write);
  ASSERT_TRUE(apps[1].update_write);
  ASSERT_EQ(0u, apps[1].new_controller_limit_write_bps);
}

TEST(TrafficShapingManager, DefaultReservationControllerIgnoresSmallReservationDeficits)
{
  constexpr uint64_t reservation_bps = 1000ULL * 1000ULL * 1000ULL;

  eos::mgm::traffic_shaping::AppState reserved_app{};
  reserved_app.reservation_write_bps = reservation_bps;
  reserved_app.current_write_bps = 970.0 * 1000.0 * 1000.0;
  reserved_app.has_write_io_pressure = true;
  reserved_app.current_write_io_pressure = 0.5;

  eos::mgm::traffic_shaping::AppState competitor{};
  competitor.current_write_bps = 700.0 * 1000.0 * 1000.0;
  competitor.controller_limit_write_bps = 600ULL * 1000ULL * 1000ULL;
  competitor.has_write_reservation_competition = true;

  std::vector<eos::mgm::traffic_shaping::AppState> apps{reserved_app, competitor};
  eos::mgm::traffic_shaping::TrafficShapingManager::ApplyDefaultReservationController(
      apps);

  ASSERT_FALSE(apps[0].update_write);
  ASSERT_TRUE(apps[1].update_write);
  ASSERT_EQ(0u, apps[1].new_controller_limit_write_bps);
}

TEST(TrafficShapingManager, DefaultReservationControllerRequiresLocalCompetition)
{
  constexpr uint64_t reservation_bps = 1000ULL * 1000ULL * 1000ULL;

  eos::mgm::traffic_shaping::AppState reserved_app{};
  reserved_app.reservation_write_bps = reservation_bps;
  reserved_app.current_write_bps = 700.0 * 1000.0 * 1000.0;
  reserved_app.has_write_io_pressure = true;
  reserved_app.current_write_io_pressure = 0.5;

  eos::mgm::traffic_shaping::AppState competitor{};
  competitor.current_write_bps = 700.0 * 1000.0 * 1000.0;
  competitor.controller_limit_write_bps = 400ULL * 1000ULL * 1000ULL;

  std::vector<eos::mgm::traffic_shaping::AppState> apps{reserved_app, competitor};
  eos::mgm::traffic_shaping::TrafficShapingManager::ApplyDefaultReservationController(
      apps);

  ASSERT_FALSE(apps[0].update_write);
  ASSERT_TRUE(apps[1].update_write);
  ASSERT_EQ(0u, apps[1].new_controller_limit_write_bps);

  apps = {reserved_app, competitor};
  apps[1].has_write_reservation_competition = true;
  eos::mgm::traffic_shaping::TrafficShapingManager::ApplyDefaultReservationController(
      apps);

  ASSERT_FALSE(apps[0].update_write);
  ASSERT_TRUE(apps[1].update_write);
  ASSERT_EQ(400ULL * 1000ULL * 1000ULL, apps[1].new_controller_limit_write_bps);
}

TEST(TrafficShapingManager, DefaultReservationControllerCanBeDisabled)
{
  constexpr uint64_t reservation_bps = 300ULL * 1000ULL * 1000ULL;

  eos::mgm::traffic_shaping::AppState app{};
  app.reservation_write_bps = reservation_bps;
  app.controller_limit_write_bps = reservation_bps;
  app.has_write_io_pressure = true;
  app.current_write_io_pressure = 0.5;

  std::vector<eos::mgm::traffic_shaping::AppState> apps{app};
  eos::mgm::traffic_shaping::TrafficShapingManager::ApplyDefaultReservationController(
      apps, false);

  ASSERT_TRUE(apps[0].update_write);
  ASSERT_EQ(0u, apps[0].new_controller_limit_write_bps);
}

TEST(TrafficShapingManager, DefaultReservationControllerClearsReservedAppLimit)
{
  constexpr uint64_t reservation_bps = 300ULL * 1000ULL * 1000ULL;

  eos::mgm::traffic_shaping::AppState app{};
  app.reservation_write_bps = reservation_bps;
  app.controller_limit_write_bps = reservation_bps;
  app.has_write_io_pressure = true;
  app.current_write_io_pressure = 0.5;

  std::vector<eos::mgm::traffic_shaping::AppState> apps{app};
  eos::mgm::traffic_shaping::TrafficShapingManager::ApplyDefaultReservationController(
      apps);

  ASSERT_TRUE(apps[0].update_write);
  ASSERT_EQ(0u, apps[0].new_controller_limit_write_bps);
}

TEST(TrafficShapingManager, DefaultReservationControllerClearsStaleLimits)
{
  constexpr uint64_t reservation_bps = 300ULL * 1000ULL * 1000ULL;

  eos::mgm::traffic_shaping::AppState app{};
  app.reservation_write_bps = reservation_bps;
  app.controller_limit_write_bps = reservation_bps;
  app.reservation_read_bps = reservation_bps;
  app.controller_limit_read_bps = reservation_bps;

  std::vector<eos::mgm::traffic_shaping::AppState> apps{app};
  eos::mgm::traffic_shaping::TrafficShapingManager::ApplyDefaultReservationController(
      apps);

  ASSERT_TRUE(apps[0].update_write);
  ASSERT_EQ(0u, apps[0].new_controller_limit_write_bps);
  ASSERT_TRUE(apps[0].update_read);
  ASSERT_EQ(0u, apps[0].new_controller_limit_read_bps);
}

TEST(TrafficShapingManager, DefaultReservationControllerHandlesReadReservations)
{
  constexpr uint64_t reservation_bps = 200ULL * 1000ULL * 1000ULL;

  eos::mgm::traffic_shaping::AppState app{};
  app.reservation_read_bps = reservation_bps;
  app.current_read_bps = 600.0 * 1000.0 * 1000.0;
  app.has_read_io_pressure = true;
  app.current_read_io_pressure = 0.03;

  std::vector<eos::mgm::traffic_shaping::AppState> apps{app};
  eos::mgm::traffic_shaping::TrafficShapingManager::ApplyDefaultReservationController(
      apps);

  ASSERT_FALSE(apps[0].update_read);
  ASSERT_FALSE(apps[0].update_write);
}

TEST(TrafficShapingManager, ExplicitLimitsArePublishedWithoutReservationPressure)
{
  eos::mgm::traffic_shaping::TrafficShapingPolicy policy;
  policy.limit_write_bytes_per_sec = 100ULL * 1000ULL * 1000ULL;

  ASSERT_TRUE(eos::mgm::traffic_shaping::TrafficShapingManager::ShouldEmitDelayForPolicy(
      policy, true, 0.0, 0.0, false, true, true));
}

TEST(TrafficShapingManager, EphemeralCompetitorLimitsNeedPressuredReservationNode)
{
  eos::mgm::traffic_shaping::TrafficShapingPolicy policy;
  policy.controller_limit_write_bytes_per_sec = 100ULL * 1000ULL * 1000ULL;

  ASSERT_FALSE(eos::mgm::traffic_shaping::TrafficShapingManager::ShouldEmitDelayForPolicy(
      policy, true, 500.0 * 1000.0 * 1000.0, 1.0, false, true, true));
  ASSERT_TRUE(eos::mgm::traffic_shaping::TrafficShapingManager::ShouldEmitDelayForPolicy(
      policy, true, 500.0 * 1000.0 * 1000.0, 1.0, true, true, true));
  ASSERT_FALSE(eos::mgm::traffic_shaping::TrafficShapingManager::ShouldEmitDelayForPolicy(
      policy, true, 500.0 * 1000.0 * 1000.0, 1.0, true, true, false));
}

TEST(TrafficShapingManager, ReservedAppEphemeralLimitRequiresLocalPressure)
{
  eos::mgm::traffic_shaping::TrafficShapingPolicy policy;
  policy.reservation_write_bytes_per_sec = 300ULL * 1000ULL * 1000ULL;
  policy.controller_limit_write_bytes_per_sec = 100ULL * 1000ULL * 1000ULL;

  ASSERT_FALSE(eos::mgm::traffic_shaping::TrafficShapingManager::ShouldEmitDelayForPolicy(
      policy, true, 0.0, 1.0, true, true, true));
  ASSERT_FALSE(eos::mgm::traffic_shaping::TrafficShapingManager::ShouldEmitDelayForPolicy(
      policy, true, 500.0 * 1000.0 * 1000.0, 0.0, true, true, true));
  ASSERT_TRUE(eos::mgm::traffic_shaping::TrafficShapingManager::ShouldEmitDelayForPolicy(
      policy, true, 500.0 * 1000.0 * 1000.0, 1.0, false, true, true));
}

TEST(TrafficShapingManager, EphemeralDelayPublicationHonorsPressureThreshold)
{
  eos::mgm::traffic_shaping::TrafficShapingPolicy policy;
  policy.reservation_write_bytes_per_sec = 300ULL * 1000ULL * 1000ULL;
  policy.controller_limit_write_bytes_per_sec = 100ULL * 1000ULL * 1000ULL;

  ASSERT_FALSE(eos::mgm::traffic_shaping::TrafficShapingManager::ShouldEmitDelayForPolicy(
      policy, true, 500.0 * 1000.0 * 1000.0, 0.02, false, true, true, 0.05));
  ASSERT_TRUE(eos::mgm::traffic_shaping::TrafficShapingManager::ShouldEmitDelayForPolicy(
      policy, true, 500.0 * 1000.0 * 1000.0, 0.02, false, true, true, 0.01));
}
