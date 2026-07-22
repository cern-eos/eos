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

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

namespace {

std::string
PrepareReservedAppWorkload(eos::mgm::traffic_shaping::TrafficShapingManager& manager,
                           const std::string& reserved_app = "reserved-app",
                           const std::string& competitor_app = "best-effort-app",
                           const std::string& extra_competitor_app = {})
{
  constexpr uint64_t mb = 1000ULL * 1000ULL;
  manager.ApplyThreadConfig(1000, 1000, 1000, 15);

  eos::mgm::traffic_shaping::TrafficShapingPolicy policy;
  policy.reservation_write_bytes_per_sec = 1000 * mb;
  manager.SetAppPolicy(reserved_app, policy);

  const std::string node = "/eos/reserved-workload.example:1095/fst";
  auto make_report = [&](const int64_t timestamp_ms, const uint64_t reserved_bytes,
                         const uint64_t competitor_bytes) {
    eos::traffic_shaping::FstIoReport report;
    report.set_node_id(node);
    report.set_timestamp_ms(timestamp_ms);
    std::vector<std::tuple<std::string, uint32_t, uint64_t>> entries{
        {reserved_app, 1, reserved_bytes}, {competitor_app, 2, competitor_bytes}};
    if (!extra_competitor_app.empty()) {
      entries.emplace_back(extra_competitor_app, 3, competitor_bytes);
    }
    for (const auto& [app, uid, bytes] : entries) {
      auto* entry = report.add_entries();
      entry->set_app_name(app);
      entry->set_uid(uid);
      entry->set_gid(uid);
      entry->set_fsid(42);
      entry->set_generation_id(1);
      entry->set_total_bytes_written(bytes);
    }
    return report;
  };

  manager.ProcessReport(make_report(1000, 0, 0));
  manager.ProcessReport(make_report(2000, 200 * mb, 500 * mb));
  manager.UpdateEstimators(1.0);
  return node;
}

} // namespace

TEST(CumulativeRateHistory, UsesActualElapsedTimeAcrossAllMetrics)
{
  eos::mgm::traffic_shaping::CumulativeRateHistory history(60.0, 1.0);
  history.Push({1350, 2700, 3, 6}, 1.35);

  const auto rate = history.GetRate(1.0);
  EXPECT_DOUBLE_EQ(1000.0, rate.read_rate_bps);
  EXPECT_DOUBLE_EQ(2000.0, rate.write_rate_bps);
  EXPECT_DOUBLE_EQ(3.0 / 1.35, rate.read_iops);
  EXPECT_DOUBLE_EQ(6.0 / 1.35, rate.write_iops);
}

TEST(CumulativeRateHistory, UnderfilledWindowUsesRequestedDuration)
{
  eos::mgm::traffic_shaping::CumulativeRateHistory history(60.0, 1.0);
  history.Push({1000, 0, 0, 0}, 1.0);

  EXPECT_DOUBLE_EQ(200.0, history.GetRate(5.0).read_rate_bps);
}

TEST(CumulativeRateHistory, InterpolatesIrregularWindowBoundary)
{
  eos::mgm::traffic_shaping::CumulativeRateHistory history(60.0, 1.0);
  history.Push({40, 0, 0, 0}, 0.4);
  history.Push({140, 0, 0, 0}, 0.7);

  EXPECT_DOUBLE_EQ(170.0, history.GetRate(1.0).read_rate_bps);
}

TEST(CumulativeRateHistory, FallsBackWhenActualTicksOutrunFineRing)
{
  eos::mgm::traffic_shaping::CumulativeRateHistory history(300.0, 1.0);
  for (size_t i = 0; i < 100; ++i) {
    history.Push({10, 20, 1, 2}, 0.1);
  }

  EXPECT_NEAR(100.0, history.GetRate(5.0).read_rate_bps, 0.001);
  EXPECT_NEAR(200.0, history.GetRate(5.0).write_rate_bps, 0.001);
  EXPECT_NEAR(10.0, history.GetRate(5.0).read_iops, 0.001);
}

TEST(CumulativeRateHistory, InvalidElapsedFallsBackToConfiguredTick)
{
  eos::mgm::traffic_shaping::CumulativeRateHistory history(60.0, 0.5);
  history.Push({50, 0, 0, 0}, std::numeric_limits<double>::infinity());
  history.Push({50, 0, 0, 0}, std::numeric_limits<double>::quiet_NaN());

  EXPECT_DOUBLE_EQ(100.0, history.GetRate(1.0).read_rate_bps);
}

TEST(CumulativeRateHistory, PreservesRatesAfterRingWrap)
{
  eos::mgm::traffic_shaping::CumulativeRateHistory history(2.0, 1.0);
  history.Push({100, 0, 0, 0}, 1.0);
  history.Push({200, 0, 0, 0}, 1.0);
  history.Push({300, 0, 0, 0}, 1.0);
  history.Push({400, 0, 0, 0}, 1.0);

  EXPECT_DOUBLE_EQ(400.0, history.GetRate(1.0).read_rate_bps);
  EXPECT_DOUBLE_EQ(350.0, history.GetRate(2.0).read_rate_bps);
}

TEST(CumulativeRateHistory, PreservesLongRatesAcrossCoarseRingWrap)
{
  eos::mgm::traffic_shaping::CumulativeRateHistory history(300.0, 0.1);
  for (size_t i = 0; i < 6200; ++i) {
    history.Push({100, 200, 1, 2}, 0.1);
  }

  EXPECT_NEAR(1000.0, history.GetRate(60.0).read_rate_bps, 0.001);
  EXPECT_NEAR(2000.0, history.GetRate(300.0).write_rate_bps, 0.001);
  EXPECT_NEAR(10.0, history.GetRate(300.0).read_iops, 0.001);
}

TEST(TrafficShapingManager, DelayedEstimatorTicksKeepEmaNonNegative)
{
  eos::mgm::traffic_shaping::TrafficShapingManager manager;

  auto make_report = [](const uint64_t bytes_written) {
    eos::traffic_shaping::FstIoReport report;
    report.set_node_id("/eos/fst.example:1095/fst");
    auto* entry = report.add_entries();
    entry->set_app_name("delayed-estimator-app");
    entry->set_uid(1);
    entry->set_gid(2);
    entry->set_generation_id(1);
    entry->set_total_bytes_written(bytes_written);
    return report;
  };

  manager.ProcessReport(make_report(0));
  manager.ProcessReport(make_report(3000));
  manager.UpdateEstimators(3.0);

  auto stats = manager.GetGlobalStats();
  ASSERT_EQ(1u, stats.size());
  const double busy_ema =
      stats.begin()->second.ema[eos::mgm::traffic_shaping::Ema1s].write_rate_bps;
  EXPECT_GT(busy_ema, 0.0);

  manager.UpdateEstimators(3.0);
  stats = manager.GetGlobalStats();
  const double idle_ema =
      stats.begin()->second.ema[eos::mgm::traffic_shaping::Ema1s].write_rate_bps;
  EXPECT_GE(idle_ema, 0.0);
  EXPECT_LT(idle_ema, busy_ema);
}

TEST(TrafficShapingManager, MaxCadenceJitterPreservesTotalsAndNormalizesRate)
{
  eos::mgm::traffic_shaping::TrafficShapingManager manager;
  manager.ApplyThreadConfig(3000, 3000, 3000, 15);

  auto make_report = [](const int64_t timestamp_ms, const uint64_t bytes_written,
                        const uint64_t write_ops) {
    eos::traffic_shaping::FstIoReport report;
    report.set_node_id("/eos/fst.example:1095/fst");
    report.set_timestamp_ms(timestamp_ms);
    auto* entry = report.add_entries();
    entry->set_app_name("cadence-jitter-app");
    entry->set_uid(1);
    entry->set_gid(2);
    entry->set_generation_id(1);
    entry->set_total_bytes_written(bytes_written);
    entry->set_total_write_ops(write_ops);
    return report;
  };

  manager.ProcessReport(make_report(1000, 0, 0));
  manager.ProcessReport(make_report(4050, 3050, 305));
  manager.UpdateEstimators(3.0);

  const auto totals = manager.GetTotalCumulativeStats();
  EXPECT_EQ(3050u, totals.bytes_written_total);
  EXPECT_EQ(305u, totals.write_ops_total);
  const auto stats = manager.GetGlobalStats();
  ASSERT_EQ(1u, stats.size());
  EXPECT_NEAR(1000.0,
              stats.begin()->second.ema[eos::mgm::traffic_shaping::Ema1s].write_rate_bps,
              0.001);
  EXPECT_NEAR(100.0,
              stats.begin()->second.ema[eos::mgm::traffic_shaping::Ema1s].write_iops,
              0.001);
}

TEST(TrafficShapingManager, LongReportGapPreservesCumulativeTraffic)
{
  eos::mgm::traffic_shaping::TrafficShapingManager manager;
  manager.ApplyThreadConfig(3000, 3000, 3000, 15);

  auto make_report = [](const int64_t timestamp_ms, const uint64_t bytes_written) {
    eos::traffic_shaping::FstIoReport report;
    report.set_node_id("/eos/fst.example:1095/fst");
    report.set_timestamp_ms(timestamp_ms);
    auto* entry = report.add_entries();
    entry->set_app_name("long-gap-app");
    entry->set_uid(1);
    entry->set_gid(2);
    entry->set_generation_id(1);
    entry->set_total_bytes_written(bytes_written);
    return report;
  };

  manager.ProcessReport(make_report(1000, 0));
  manager.ProcessReport(make_report(11000, 10000));
  manager.UpdateEstimators(3.0);

  EXPECT_EQ(10000u, manager.GetTotalCumulativeStats().bytes_written_total);
  const auto stats = manager.GetGlobalStats();
  ASSERT_EQ(1u, stats.size());
  EXPECT_NEAR(1000.0,
              stats.begin()->second.ema[eos::mgm::traffic_shaping::Ema1s].write_rate_bps,
              0.001);
}

TEST(TrafficShapingManager, LongReportGapUsesEstimatorCadenceForRateSample)
{
  eos::mgm::traffic_shaping::TrafficShapingManager manager;
  manager.ApplyThreadConfig(200, 200, 200, 15);

  auto make_report = [](const int64_t timestamp_ms, const uint64_t bytes_written) {
    eos::traffic_shaping::FstIoReport report;
    report.set_node_id("/eos/fst.example:1095/fst");
    report.set_timestamp_ms(timestamp_ms);
    auto* entry = report.add_entries();
    entry->set_app_name("long-gap-fast-estimator-app");
    entry->set_uid(1);
    entry->set_gid(2);
    entry->set_generation_id(1);
    entry->set_total_bytes_written(bytes_written);
    return report;
  };

  manager.ProcessReport(make_report(1000, 0));
  manager.ProcessReport(make_report(11000, 10000));
  manager.UpdateEstimators(0.2);

  EXPECT_EQ(10000u, manager.GetTotalCumulativeStats().bytes_written_total);
  const auto stats = manager.GetGlobalStats();
  ASSERT_EQ(1u, stats.size());
  // The 10-second report delta is normalized to the 200 ms estimator cadence:
  // 200 bytes / 0.2 s = 1000 B/s. The first one-second EMA sample then applies
  // alpha=2*0.2/(1+0.2)=1/3.
  EXPECT_NEAR(1000.0 / 3.0,
              stats.begin()->second.ema[eos::mgm::traffic_shaping::Ema1s].write_rate_bps,
              0.001);
}

TEST(TrafficShapingManager, DelayedReportUsesConfiguredCadence)
{
  eos::mgm::traffic_shaping::TrafficShapingManager manager;
  manager.ApplyThreadConfig(200, 200, 200, 15);

  auto make_report = [](const int64_t timestamp_ms, const uint64_t bytes_written) {
    eos::traffic_shaping::FstIoReport report;
    report.set_node_id("/eos/fst.example:1095/fst");
    report.set_timestamp_ms(timestamp_ms);
    auto* entry = report.add_entries();
    entry->set_app_name("slow-report-fast-estimator-app");
    entry->set_uid(1);
    entry->set_gid(2);
    entry->set_generation_id(1);
    entry->set_total_bytes_written(bytes_written);
    return report;
  };

  manager.ProcessReport(make_report(1000, 0));
  manager.ProcessReport(make_report(4000, 3000));
  manager.UpdateEstimators(0.2);

  const auto stats = manager.GetGlobalStats();
  ASSERT_EQ(1u, stats.size());
  EXPECT_NEAR(1000.0 / 3.0,
              stats.begin()->second.ema[eos::mgm::traffic_shaping::Ema1s].write_rate_bps,
              0.001);
}

TEST(TrafficShapingManager, HistoricalNodeReportOnlyUpdatesCumulativeTraffic)
{
  eos::mgm::traffic_shaping::TrafficShapingManager manager;
  manager.ApplyThreadConfig(3000, 3000, 3000, 15);

  auto make_report = [](const int64_t timestamp_ms,
                        const std::optional<uint64_t> bytes_written) {
    eos::traffic_shaping::FstIoReport report;
    report.set_node_id("/eos/fst.example:1095/fst");
    report.set_timestamp_ms(timestamp_ms);
    if (bytes_written) {
      auto* entry = report.add_entries();
      entry->set_app_name("historical-report-app");
      entry->set_uid(1);
      entry->set_gid(2);
      entry->set_generation_id(1);
      entry->set_total_bytes_written(*bytes_written);
    }
    return report;
  };

  manager.ProcessReport(make_report(1000, 0));
  manager.ProcessReport(make_report(5000, std::nullopt));
  manager.ProcessReport(make_report(4000, 3000));
  manager.UpdateEstimators(3.0);

  EXPECT_EQ(3000u, manager.GetTotalCumulativeStats().bytes_written_total);
  auto stats = manager.GetGlobalStats();
  ASSERT_EQ(1u, stats.size());
  EXPECT_DOUBLE_EQ(
      0.0, stats.begin()->second.ema[eos::mgm::traffic_shaping::Ema1s].write_rate_bps);

  manager.ProcessReport(make_report(8000, 6000));
  manager.UpdateEstimators(3.0);
  stats = manager.GetGlobalStats();
  EXPECT_EQ(6000u, manager.GetTotalCumulativeStats().bytes_written_total);
  EXPECT_NEAR(1000.0,
              stats.begin()->second.ema[eos::mgm::traffic_shaping::Ema1s].write_rate_bps,
              0.001);
}

TEST(TrafficShapingManager, NonFiniteEstimatorIntervalsDoNotPoisonRates)
{
  eos::mgm::traffic_shaping::TrafficShapingManager manager;
  manager.ApplyThreadConfig(200, 200, 200, 15);

  auto make_report = [](const int64_t timestamp_ms, const uint64_t bytes_written) {
    eos::traffic_shaping::FstIoReport report;
    report.set_node_id("/eos/fst.example:1095/fst");
    report.set_timestamp_ms(timestamp_ms);
    auto* entry = report.add_entries();
    entry->set_app_name("finite-estimator-app");
    entry->set_uid(1);
    entry->set_gid(2);
    entry->set_generation_id(1);
    entry->set_total_bytes_written(bytes_written);
    return report;
  };

  manager.ProcessReport(make_report(1000, 0));
  manager.ProcessReport(make_report(1200, 200));
  manager.UpdateEstimators(std::numeric_limits<double>::quiet_NaN());
  manager.UpdateEstimators(std::numeric_limits<double>::infinity());
  manager.UpdateEstimators(0.2);

  const auto stats = manager.GetGlobalStats();
  ASSERT_EQ(1u, stats.size());
  const double rate =
      stats.begin()->second.ema[eos::mgm::traffic_shaping::Ema1s].write_rate_bps;
  EXPECT_TRUE(std::isfinite(rate));
  EXPECT_NEAR(1000.0 / 3.0, rate, 0.001);
}

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

TEST(TrafficShapingEngine, StartAcceptsUnchangedDefaultThreadConfig)
{
  eos::mgm::traffic_shaping::TrafficShapingEngine engine;

  EXPECT_TRUE(engine.Start());
  EXPECT_TRUE(engine.IsEnabled());

  engine.Stop();
  EXPECT_FALSE(engine.IsEnabled());
}

TEST(TrafficShapingEngine, FailedStartCleansUpThreadsAndCanRetry)
{
  eos::mgm::traffic_shaping::TrafficShapingEngine engine;
  engine.GetManager()->FailNextThreadConfigPreparationForTest();

  EXPECT_FALSE(engine.Start());
  EXPECT_FALSE(engine.IsEnabled());

  EXPECT_TRUE(engine.Start());
  EXPECT_TRUE(engine.IsEnabled());
  engine.Stop();
  EXPECT_FALSE(engine.IsEnabled());
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

TEST(TrafficShapingEngine, ActiveNodeRateThresholdPropagatesToManager)
{
  eos::mgm::traffic_shaping::TrafficShapingEngine engine;
  auto manager = engine.GetManager();
  constexpr uint64_t threshold_bps = 100ULL * 1000ULL;

  ASSERT_TRUE(engine.ApplyActiveNodeRateThresholdConfig(threshold_bps));
  ASSERT_EQ(threshold_bps, engine.GetActiveNodeRateThreshold());
  ASSERT_EQ(threshold_bps, manager->GetActiveNodeRateThreshold());

  ASSERT_FALSE(engine.ApplyActiveNodeRateThresholdConfig(threshold_bps));
}

TEST(TrafficShapingEngine, ReportQueueOverflowHonorsCountBound)
{
  eos::mgm::traffic_shaping::TrafficShapingEngine engine;
  engine.mRunning.store(true, std::memory_order_release);
  eos::traffic_shaping::FstIoReport report;
  report.set_node_id("queue-count-node");
  for (size_t i = 0; i < 2001; ++i) {
    engine.AddReportToQueue(report);
  }

  const auto manager = engine.GetManager();
  EXPECT_EQ(2000u, manager->GetFstReportQueueDepth());
  EXPECT_EQ(1u, manager->GetFstReportsDroppedTotal());
  EXPECT_GT(manager->GetFstReportQueueEstimatedBytes(), 0u);

  engine.ProcessAllQueuedReports();
  EXPECT_EQ(0u, manager->GetFstReportQueueDepth());
  EXPECT_EQ(0u, manager->GetFstReportQueueEstimatedBytes());
  engine.mRunning.store(false, std::memory_order_release);
}

TEST(TrafficShapingEngine, ReportQueueHighWaterShedsSerializedReports)
{
  eos::mgm::traffic_shaping::TrafficShapingEngine engine;
  engine.mRunning.store(true, std::memory_order_release);
  eos::traffic_shaping::FstIoReport report;
  report.set_node_id("queue-below-bound-node");
  for (size_t i = 0; i < 1600; ++i) {
    engine.AddReportToQueue(report);
  }

  const auto manager = engine.GetManager();
  EXPECT_EQ(1600u, manager->GetFstReportQueueDepth());
  EXPECT_EQ(0u, manager->GetFstReportsDroppedTotal());
  EXPECT_GT(manager->GetFstReportQueueEstimatedBytes(), 0u);

  eos::traffic_shaping::FstIoReport serialized_report;
  serialized_report.set_node_id("queue-shed-node");
  engine.ProcessSerializedFstIoReportNonBlocking(serialized_report.SerializeAsString());

  EXPECT_EQ(1600u, manager->GetFstReportQueueDepth());
  EXPECT_EQ(1u, manager->GetFstReportsDroppedTotal());

  engine.ProcessAllQueuedReports();
  EXPECT_EQ(0u, manager->GetFstReportQueueDepth());
  EXPECT_EQ(0u, manager->GetFstReportQueueEstimatedBytes());
  engine.mRunning.store(false, std::memory_order_release);
}

TEST(TrafficShapingEngine, ReportQueueByteBudgetEvictsOldestReports)
{
  eos::mgm::traffic_shaping::TrafficShapingEngine engine;
  engine.mRunning.store(true, std::memory_order_release);
  eos::traffic_shaping::FstIoReport report;
  report.set_node_id("/eos/byte-heavy.example:1095/fst");
  const std::string app_name(1000, 'a');
  for (uint32_t i = 0; i < 3000; ++i) {
    auto* entry = report.add_entries();
    entry->set_app_name(app_name);
    entry->set_uid(i);
    entry->set_gid(i);
    entry->set_fsid(i + 1);
  }

  for (size_t i = 0; i < 20; ++i) {
    report.set_timestamp_ms(static_cast<int64_t>(i));
    engine.AddReportToQueue(report);
  }

  const auto manager = engine.GetManager();
  EXPECT_GT(manager->GetFstReportsDroppedTotal(), 0u);
  EXPECT_LT(manager->GetFstReportQueueDepth(), 20u);
  EXPECT_GT(manager->GetFstReportQueueEstimatedBytes(), 0u);
  EXPECT_LE(manager->GetFstReportQueueEstimatedBytes(), 64ULL * 1024ULL * 1024ULL);
  ASSERT_FALSE(engine.mReportQueue.empty());
  EXPECT_GT(engine.mReportQueue.front().report.timestamp_ms(), 0);
  EXPECT_EQ(19, engine.mReportQueue.back().report.timestamp_ms());
  engine.ProcessAllQueuedReports();
  EXPECT_EQ(0u, manager->GetFstReportQueueDepth());
  EXPECT_EQ(0u, manager->GetFstReportQueueEstimatedBytes());
  engine.mRunning.store(false, std::memory_order_release);
}

TEST(TrafficShapingEngine, OversizedReportIdentityIsRejected)
{
  eos::mgm::traffic_shaping::TrafficShapingEngine engine;
  engine.mRunning.store(true, std::memory_order_release);
  eos::traffic_shaping::FstIoReport report;
  report.set_node_id(std::string(1025, 'n'));

  engine.AddReportToQueue(std::move(report));

  const auto manager = engine.GetManager();
  EXPECT_EQ(0u, manager->GetFstReportQueueDepth());
  EXPECT_EQ(0u, manager->GetFstReportQueueEstimatedBytes());
  EXPECT_EQ(1u, manager->GetFstReportsDroppedTotal());
  engine.mRunning.store(false, std::memory_order_release);
}

TEST(TrafficShapingEngine, SerializedReportWirePreflightAcceptsValidReport)
{
  eos::mgm::traffic_shaping::TrafficShapingEngine engine;
  engine.mRunning.store(true, std::memory_order_release);
  eos::traffic_shaping::FstIoReport report;
  report.set_node_id("/eos/fst-a.example:1095/fst");
  auto* entry = report.add_entries();
  entry->set_app_name("wire-valid-app");
  entry->set_uid(1);
  entry->set_gid(2);
  entry->set_fsid(3);

  engine.ProcessSerializedFstIoReportNonBlocking(report.SerializeAsString());

  EXPECT_EQ(1u, engine.GetManager()->GetFstReportQueueDepth());
  EXPECT_EQ(0u, engine.GetManager()->GetFstReportsDroppedTotal());
  engine.mRunning.store(false, std::memory_order_release);
}

TEST(TrafficShapingEngine, SerializedReportWirePreflightRejectsExcessEntries)
{
  eos::mgm::traffic_shaping::TrafficShapingEngine engine;
  engine.mRunning.store(true, std::memory_order_release);
  std::string serialized_report;
  serialized_report.reserve((8192 + 1) * 2);
  for (size_t i = 0; i < 8192 + 1; ++i) {
    // Field 4 is the length-delimited FstIoEntry field. Empty entries are valid
    // protobuf, so this exercises the allocation-free cardinality check rather
    // than the parser's malformed-input handling.
    serialized_report.append("\x22\x00", 2);
  }

  engine.ProcessSerializedFstIoReportNonBlocking(serialized_report);

  EXPECT_EQ(0u, engine.GetManager()->GetFstReportQueueDepth());
  EXPECT_EQ(1u, engine.GetManager()->GetFstReportsDroppedTotal());
  engine.mRunning.store(false, std::memory_order_release);
}

TEST(TrafficShapingEngine, SerializedReportWirePreflightRejectsOversizedIdentity)
{
  eos::mgm::traffic_shaping::TrafficShapingEngine engine;
  engine.mRunning.store(true, std::memory_order_release);
  eos::traffic_shaping::FstIoReport report;
  report.set_node_id("/eos/fst-a.example:1095/fst");
  report.add_entries()->set_app_name(std::string(1025, 'a'));

  engine.ProcessSerializedFstIoReportNonBlocking(report.SerializeAsString());

  EXPECT_EQ(0u, engine.GetManager()->GetFstReportQueueDepth());
  EXPECT_EQ(1u, engine.GetManager()->GetFstReportsDroppedTotal());
  engine.mRunning.store(false, std::memory_order_release);
}

TEST(TrafficShapingEngine, SerializedReportRejectsOversizedAndMalformedWire)
{
  eos::mgm::traffic_shaping::TrafficShapingEngine engine;
  engine.mRunning.store(true, std::memory_order_release);

  engine.ProcessSerializedFstIoReportNonBlocking(
      std::string(eos::mgm::traffic_shaping::kMaxSerializedFstIoReportBytes + 1, 'x'));
  engine.ProcessSerializedFstIoReportNonBlocking(
      std::string{static_cast<char>(0x0a), static_cast<char>(0x80)});

  EXPECT_EQ(0u, engine.GetManager()->GetFstReportQueueDepth());
  EXPECT_EQ(2u, engine.GetManager()->GetFstReportsDroppedTotal());
  engine.mRunning.store(false, std::memory_order_release);
}

TEST(TrafficShapingEngine, PredecodeRejectionIsAccounted)
{
  eos::mgm::traffic_shaping::TrafficShapingEngine engine;

  engine.RejectFstIoReportNonBlocking("test predecode rejection");

  EXPECT_EQ(1u, engine.GetManager()->GetFstReportsDroppedTotal());
  EXPECT_EQ(0u, engine.GetManager()->GetFstReportQueueDepth());
}

TEST(TrafficShapingEngine, SerializedReportQueuesDuringConcurrentBatchProcessing)
{
  eos::mgm::traffic_shaping::TrafficShapingEngine engine;
  engine.mRunning.store(true, std::memory_order_release);
  const auto manager = engine.GetManager();
  eos::traffic_shaping::FstIoReport first_report;
  first_report.set_node_id("/eos/fst-a.example:1095/fst");
  first_report.set_timestamp_ms(1);
  engine.AddReportToQueue(std::move(first_report));

  auto state_lock = manager->LockStateForTest();
  std::thread processor([&engine]() { engine.ProcessAllQueuedReports(); });
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
  while (manager->GetFstReportQueueDepth() != 0 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  EXPECT_EQ(0u, manager->GetFstReportQueueDepth());

  eos::traffic_shaping::FstIoReport next_report;
  next_report.set_node_id("/eos/fst-b.example:1095/fst");
  next_report.set_timestamp_ms(2);
  engine.ProcessSerializedFstIoReportNonBlocking(next_report.SerializeAsString());

  EXPECT_EQ(1u, manager->GetFstReportQueueDepth());
  EXPECT_EQ(0u, manager->GetFstReportsDroppedTotal());

  state_lock.unlock();
  processor.join();

  EXPECT_EQ(1u, manager->GetFstReportQueueDepth());
  engine.mRunning.store(false, std::memory_order_release);
}

TEST(TrafficShapingEngine, QueueAdmissionRechecksRuntimeStateAfterParsing)
{
  eos::mgm::traffic_shaping::TrafficShapingEngine engine;
  engine.mRunning.store(false, std::memory_order_release);
  eos::traffic_shaping::FstIoReport report;
  report.set_node_id("/eos/fst-a.example:1095/fst");

  engine.AddReportToQueue(std::move(report));

  EXPECT_EQ(0u, engine.GetManager()->GetFstReportQueueDepth());
  EXPECT_EQ(1u, engine.GetManager()->GetFstReportsDroppedTotal());
}

TEST(TrafficShapingEngine, StopRuntimeClearsQueuedReportsAndByteAccounting)
{
  eos::mgm::traffic_shaping::TrafficShapingEngine engine;
  engine.mRunning.store(true, std::memory_order_release);
  eos::traffic_shaping::FstIoReport report;
  report.set_node_id("/eos/fst-a.example:1095/fst");
  report.add_entries()->set_app_name("queued-app");
  engine.AddReportToQueue(std::move(report));

  ASSERT_EQ(1u, engine.GetManager()->GetFstReportQueueDepth());
  ASSERT_GT(engine.GetManager()->GetFstReportQueueEstimatedBytes(), 0u);

  EXPECT_TRUE(engine.StopRuntime());

  EXPECT_FALSE(engine.IsEnabled());
  EXPECT_EQ(0u, engine.GetManager()->GetFstReportQueueDepth());
  EXPECT_EQ(0u, engine.GetManager()->GetFstReportQueueEstimatedBytes());
}

TEST(TrafficShapingEngine, FstSyncThreadStartFailureIsContainedAndRetryable)
{
  eos::mgm::traffic_shaping::TrafficShapingEngine engine;
  engine.FailNextFstEnabledSyncThreadStartForTest();

  EXPECT_FALSE(engine.SetEnabled(true));
  EXPECT_FALSE(engine.IsEnabled());
  EXPECT_TRUE(engine.EnsureFstEnabledSyncThread());
  engine.StopFstEnabledSyncThread();
  EXPECT_TRUE(engine.EnsureFstEnabledSyncThread());
  engine.Stop();
}

TEST(TrafficShapingEngine, EnabledConfigStoreFailureRollsBackRuntime)
{
  eos::mgm::traffic_shaping::TrafficShapingEngine engine;
  engine.FailNextEnabledConfigStoreForTest();

  EXPECT_FALSE(engine.SetEnabled(true));
  EXPECT_FALSE(engine.IsEnabled());
  engine.Stop();
}

TEST(TrafficShapingManager, IdenticalThreadConfigPreservesRatesAndControllerState)
{
  eos::mgm::traffic_shaping::TrafficShapingManager manager;
  manager.ApplyThreadConfig(200, 200, 200, 15);

  auto make_report = [](const int64_t timestamp_ms, const uint64_t bytes_written) {
    eos::traffic_shaping::FstIoReport report;
    report.set_node_id("/eos/fst.example:1095/fst");
    report.set_timestamp_ms(timestamp_ms);
    auto* entry = report.add_entries();
    entry->set_app_name("config-idempotence-app");
    entry->set_uid(1);
    entry->set_gid(2);
    entry->set_generation_id(1);
    entry->set_total_bytes_written(bytes_written);
    return report;
  };
  manager.ProcessReport(make_report(1000, 0));
  manager.ProcessReport(make_report(1200, 200));
  manager.UpdateEstimators(0.2);
  const auto before_stats = manager.GetGlobalStats();
  ASSERT_EQ(1u, before_stats.size());

  eos::mgm::traffic_shaping::NodeReservationControllerRuntime runtime;
  runtime.app_limits["config-idempotence-app"].write_bps = 1000;
  manager.SetNodeReservationControllerRuntimeForTest("node", runtime);

  manager.ApplyThreadConfig(200, 200, 200, 15);

  const auto after_stats = manager.GetGlobalStats();
  ASSERT_EQ(1u, after_stats.size());
  EXPECT_DOUBLE_EQ(
      before_stats.begin()->second.ema[eos::mgm::traffic_shaping::Ema1s].write_rate_bps,
      after_stats.begin()->second.ema[eos::mgm::traffic_shaping::Ema1s].write_rate_bps);
  const auto runtimes = manager.GetNodeReservationControllerRuntimes();
  ASSERT_NE(runtimes.end(), runtimes.find("node"));
  EXPECT_EQ(1000u, runtimes.at("node").app_limits.at("config-idempotence-app").write_bps);
}

TEST(TrafficShapingManager,
     EstimatorCadenceChangeRebuildsRuntimeRatesAndPreservesCumulativeState)
{
  eos::mgm::traffic_shaping::TrafficShapingManager manager;
  manager.ApplyThreadConfig(1000, 1000, 1000, 15);

  auto make_report = [](const int64_t timestamp_ms, const uint64_t bytes_written) {
    eos::traffic_shaping::FstIoReport report;
    report.set_node_id("/eos/config-change.example:1095/fst");
    report.set_timestamp_ms(timestamp_ms);
    auto* entry = report.add_entries();
    entry->set_app_name("config-change-app");
    entry->set_uid(1);
    entry->set_gid(2);
    entry->set_generation_id(1);
    entry->set_total_bytes_written(bytes_written);
    return report;
  };

  manager.ProcessReport(make_report(1000, 0));
  manager.ProcessReport(make_report(2000, 1000));
  manager.UpdateEstimators(1.0);
  ASSERT_EQ(1u, manager.GetGlobalStats().size());
  EXPECT_EQ(1000u, manager.GetTotalCumulativeStats().bytes_written_total);

  eos::mgm::traffic_shaping::NodeReservationControllerRuntime runtime;
  runtime.app_limits["config-change-app"].write_bps = 1000;
  manager.SetNodeReservationControllerRuntimeForTest("config-change-node", runtime);

  manager.ApplyThreadConfig(200, 200, 200, 15);

  EXPECT_TRUE(manager.GetGlobalStats().empty());
  EXPECT_TRUE(manager.GetNodeStats().empty());
  EXPECT_DOUBLE_EQ(
      0.0, manager.GetTotalStats().ema[eos::mgm::traffic_shaping::Ema1s].write_rate_bps);
  EXPECT_EQ(1000u, manager.GetTotalCumulativeStats().bytes_written_total);
  const auto runtimes = manager.GetNodeReservationControllerRuntimes();
  ASSERT_NE(runtimes.end(), runtimes.find("config-change-node"));
  EXPECT_EQ(
      1000u,
      runtimes.at("config-change-node").app_limits.at("config-change-app").write_bps);

  manager.ProcessReport(make_report(2200, 1200));
  manager.UpdateEstimators(0.2);
  const auto rebuilt_stats = manager.GetGlobalStats();
  ASSERT_EQ(1u, rebuilt_stats.size());
  EXPECT_GT(
      rebuilt_stats.begin()->second.ema[eos::mgm::traffic_shaping::Ema1s].write_rate_bps,
      0.0);
  EXPECT_EQ(1200u, manager.GetTotalCumulativeStats().bytes_written_total);
}

TEST(TrafficShapingManager, ThreadConfigPreparationFailureIsContainedAndTransactional)
{
  eos::mgm::traffic_shaping::TrafficShapingManager manager;
  manager.ApplyThreadConfig(200, 200, 200, 15);

  auto make_report = [](const int64_t timestamp_ms, const uint64_t bytes_written) {
    eos::traffic_shaping::FstIoReport report;
    report.set_node_id("/eos/config-failure.example:1095/fst");
    report.set_timestamp_ms(timestamp_ms);
    auto* entry = report.add_entries();
    entry->set_app_name("config-failure-app");
    entry->set_uid(1);
    entry->set_gid(2);
    entry->set_generation_id(1);
    entry->set_total_bytes_written(bytes_written);
    return report;
  };

  manager.ProcessReport(make_report(1000, 0));
  manager.ProcessReport(make_report(1200, 200));
  manager.UpdateEstimators(0.2);
  const auto before_stats = manager.GetGlobalStats();
  ASSERT_EQ(1u, before_stats.size());
  const double before_rate =
      before_stats.begin()->second.ema[eos::mgm::traffic_shaping::Ema1s].write_rate_bps;

  manager.FailNextThreadConfigPreparationForTest();
  bool applied = true;
  EXPECT_NO_THROW(applied = manager.ApplyThreadConfig(1000, 1000, 1000, 30));
  EXPECT_FALSE(applied);

  const auto after_failure_stats = manager.GetGlobalStats();
  ASSERT_EQ(1u, after_failure_stats.size());
  EXPECT_DOUBLE_EQ(before_rate, after_failure_stats.begin()
                                    ->second.ema[eos::mgm::traffic_shaping::Ema1s]
                                    .write_rate_bps);
  EXPECT_EQ(200u, manager.GetTotalCumulativeStats().bytes_written_total);

  // The failed attempt must leave the previous values installed, so applying
  // them again is idempotent and does not clear the live runtime sample.
  manager.ApplyThreadConfig(200, 200, 200, 15);
  const auto after_idempotent_apply = manager.GetGlobalStats();
  ASSERT_EQ(1u, after_idempotent_apply.size());
  EXPECT_DOUBLE_EQ(before_rate, after_idempotent_apply.begin()
                                    ->second.ema[eos::mgm::traffic_shaping::Ema1s]
                                    .write_rate_bps);
}

TEST(TrafficShapingEngine, FailedThreadConfigIsNotPublished)
{
  eos::mgm::traffic_shaping::TrafficShapingEngine engine;
  ASSERT_FALSE(engine.ApplyThreadConfig(200, 200, 200, 15));
  const auto manager = engine.GetManager();
  ASSERT_NE(nullptr, manager);

  manager->FailNextThreadConfigPreparationForTest();
  EXPECT_FALSE(engine.ApplyThreadConfig(1000, 1000, 1000, 30));
  EXPECT_EQ(200u, engine.GetEstimatorsUpdateThreadPeriodMilliseconds());
  EXPECT_EQ(200u, engine.GetFstIoPolicyUpdateThreadPeriodMilliseconds());
  EXPECT_EQ(200u, engine.GetFstIoStatsReportThreadPeriodMilliseconds());
  EXPECT_EQ(15u, engine.GetSystemStatsWindowSeconds());

  EXPECT_TRUE(engine.ApplyThreadConfig(1000, 1000, 1000, 30));
  EXPECT_EQ(1000u, engine.GetEstimatorsUpdateThreadPeriodMilliseconds());
  EXPECT_EQ(1000u, engine.GetFstIoPolicyUpdateThreadPeriodMilliseconds());
  EXPECT_EQ(1000u, engine.GetFstIoStatsReportThreadPeriodMilliseconds());
  EXPECT_EQ(30u, engine.GetSystemStatsWindowSeconds());
}

TEST(TrafficShapingManager, GarbageCollectionPreservesUnrelatedControllerState)
{
  eos::mgm::traffic_shaping::TrafficShapingManager manager;
  auto make_report = [](const uint64_t bytes_written) {
    eos::traffic_shaping::FstIoReport report;
    report.set_node_id("/eos/stale.example:1095/fst");
    auto* entry = report.add_entries();
    entry->set_app_name("stale-app");
    entry->set_uid(1);
    entry->set_gid(2);
    entry->set_generation_id(1);
    entry->set_total_bytes_written(bytes_written);
    return report;
  };
  manager.ProcessReport(make_report(0));
  manager.ProcessReport(make_report(1000));
  manager.UpdateEstimators(0.5);

  eos::mgm::traffic_shaping::NodeReservationControllerRuntime runtime;
  runtime.app_limits["active-app"].write_bps = 1000;
  manager.SetNodeReservationControllerRuntimeForTest("active-node", runtime);

  manager.GarbageCollect(-1);

  const auto runtimes = manager.GetNodeReservationControllerRuntimes();
  ASSERT_NE(runtimes.end(), runtimes.find("active-node"));
  EXPECT_EQ(1000u, runtimes.at("active-node").app_limits.at("active-app").write_bps);
}

TEST(TrafficShapingManager, StreamAdmissionAccountingIsReleasedByGcAndClear)
{
  eos::mgm::traffic_shaping::TrafficShapingManager manager;
  auto make_report = []() {
    eos::traffic_shaping::FstIoReport report;
    report.set_node_id("/eos/accounting.example:1095/fst");
    auto* entry = report.add_entries();
    entry->set_app_name("accounting-app");
    entry->set_uid(1);
    entry->set_gid(2);
    entry->set_fsid(3);
    entry->set_generation_id(1);
    return report;
  };

  manager.ProcessReport(make_report());
  auto [count, estimated_bytes] = manager.GetFstStreamStateAccountingForTest();
  EXPECT_EQ(1u, count);
  EXPECT_GT(estimated_bytes, 0u);

  manager.GarbageCollect(-1);
  std::tie(count, estimated_bytes) = manager.GetFstStreamStateAccountingForTest();
  EXPECT_EQ(0u, count);
  EXPECT_EQ(0u, estimated_bytes);

  manager.ProcessReport(make_report());
  EXPECT_EQ(1u, manager.GetFstStreamStateAccountingForTest().first);
  manager.ClearRuntimeStats();
  std::tie(count, estimated_bytes) = manager.GetFstStreamStateAccountingForTest();
  EXPECT_EQ(0u, count);
  EXPECT_EQ(0u, estimated_bytes);
}

TEST(TrafficShapingManager, EmptyHeartbeatDoesNotHideFirstFreshTraffic)
{
  eos::mgm::traffic_shaping::TrafficShapingManager manager;
  eos::traffic_shaping::FstIoReport heartbeat;
  heartbeat.set_node_id("/eos/empty.example:1095/fst");

  manager.ProcessReport(heartbeat);

  EXPECT_EQ(0u, manager.GetMapCardinalityStats().node_states);

  const int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::system_clock::now().time_since_epoch())
                             .count();
  eos::traffic_shaping::FstIoReport report;
  report.set_node_id(heartbeat.node_id());
  report.set_node_start_time_ms(now_ms);
  report.set_timestamp_ms(now_ms);
  auto* entry = report.add_entries();
  entry->set_app_name("fresh-app");
  entry->set_uid(1);
  entry->set_gid(2);
  entry->set_fsid(3);
  entry->set_generation_id(static_cast<uint64_t>(now_ms));
  entry->set_total_bytes_written(10 * 1024 * 1024);
  entry->set_total_write_ops(1);

  manager.ProcessReport(report);

  const auto cardinality = manager.GetMapCardinalityStats();
  EXPECT_EQ(1u, cardinality.node_states);
  EXPECT_EQ(1u, cardinality.node_state_streams);
  EXPECT_EQ(10u * 1024u * 1024u, manager.GetTotalCumulativeStats().bytes_written_total);
}

TEST(TrafficShapingManager, StreamStateMemoryBudgetAndAccounting)
{
  eos::mgm::traffic_shaping::TrafficShapingManager manager;
  eos::traffic_shaping::FstIoReport report;
  report.set_node_id("/eos/fst-state-budget.example:1095/fst");
  report.set_timestamp_ms(1000);
  for (uint32_t i = 0; i < 2100; ++i) {
    auto* entry = report.add_entries();
    entry->set_app_name("state-budget-app");
    entry->set_uid(i);
    entry->set_gid(i);
    entry->set_fsid(i + 1);
    entry->set_generation_id(1);
  }

  manager.ProcessReport(report);

  const auto cardinality = manager.GetMapCardinalityStats();
  constexpr uint64_t expected_stream_charge = 131504;
  constexpr uint64_t expected_admitted_streams = 2100;
  EXPECT_EQ(expected_admitted_streams, cardinality.node_state_streams);
  EXPECT_EQ(0u, cardinality.node_state_rejections_total);
  EXPECT_EQ(expected_admitted_streams * expected_stream_charge,
            cardinality.node_state_estimated_bytes);

  const auto memory = manager.GetMemoryStats();
  EXPECT_EQ(cardinality.node_state_estimated_bytes, memory.stream_state_estimated_bytes);
  EXPECT_EQ(memory.stream_state_estimated_bytes, memory.estimated_bytes);
  EXPECT_EQ(8ULL * 1024ULL * 1024ULL * 1024ULL, memory.stream_state_limit_bytes);
  EXPECT_EQ(65536u, memory.stream_state_limit_entries);
  EXPECT_EQ(64ULL * 1024ULL * 1024ULL, memory.report_queue_limit_bytes);
  EXPECT_EQ(memory.stream_state_limit_bytes + memory.report_queue_limit_bytes,
            memory.limit_bytes);

  eos::traffic_shaping::FstIoReport update;
  update.set_node_id(report.node_id());
  update.set_timestamp_ms(2000);
  auto* admitted = update.add_entries();
  admitted->set_app_name("state-budget-app");
  admitted->set_uid(0);
  admitted->set_gid(0);
  admitted->set_fsid(1);
  admitted->set_generation_id(1);
  admitted->set_total_bytes_written(1024);
  manager.ProcessReport(update);

  EXPECT_EQ(1024u, manager.GetTotalCumulativeStats().bytes_written_total);
  EXPECT_EQ(0u, manager.GetMapCardinalityStats().node_state_rejections_total);
}

TEST(TrafficShapingManager, FreshFilesystemIoPressureParsing)
{
  constexpr int64_t now_ms = 1'000'000;
  const auto parse = [&](const std::string& load, const int64_t published_ms) {
    return eos::mgm::traffic_shaping::ParseFreshFilesystemIoPressureForTest(
        load, std::to_string(published_ms), now_ms);
  };

  const auto fresh = parse("0.4", now_ms);
  ASSERT_TRUE(fresh.has_value());
  EXPECT_DOUBLE_EQ(0.4, *fresh);

  const auto thirty_seconds_old = parse("0.5", now_ms - 30'000);
  ASSERT_TRUE(thirty_seconds_old.has_value());
  EXPECT_DOUBLE_EQ(0.5, *thirty_seconds_old);

  const auto oldest_accepted = parse("0.6", now_ms - 45'000);
  ASSERT_TRUE(oldest_accepted.has_value());
  EXPECT_DOUBLE_EQ(0.6, *oldest_accepted);
  EXPECT_FALSE(parse("0.6", now_ms - 45'001).has_value());

  const auto maximum_future_skew = parse("0.7", now_ms + 5'000);
  ASSERT_TRUE(maximum_future_skew.has_value());
  EXPECT_DOUBLE_EQ(0.7, *maximum_future_skew);
  EXPECT_FALSE(parse("0.7", now_ms + 5'001).has_value());

  EXPECT_FALSE(
      eos::mgm::traffic_shaping::ParseFreshFilesystemIoPressureForTest("0.5", "", now_ms)
          .has_value());
  EXPECT_FALSE(eos::mgm::traffic_shaping::ParseFreshFilesystemIoPressureForTest(
                   "0.5", "not-a-timestamp", now_ms)
                   .has_value());
  EXPECT_FALSE(parse("not-a-load", now_ms).has_value());
  EXPECT_FALSE(parse("nan", now_ms).has_value());

  const auto idle = parse("0", now_ms);
  ASSERT_TRUE(idle.has_value());
  EXPECT_DOUBLE_EQ(0.0, *idle);
  const auto clamped_high = parse("1.5", now_ms);
  ASSERT_TRUE(clamped_high.has_value());
  EXPECT_DOUBLE_EQ(1.0, *clamped_high);
  const auto clamped_low = parse("-0.5", now_ms);
  ASSERT_TRUE(clamped_low.has_value());
  EXPECT_DOUBLE_EQ(0.0, *clamped_low);
}

TEST(TrafficShapingManager, ReservationControllerAndFstLimitLoopStatsAreSeparate)
{
  eos::mgm::traffic_shaping::TrafficShapingManager manager;
  manager.ApplyThreadConfig(200, 200, 200, 15);

  manager.UpdateReservationControllerLoopMicroSec(123456);
  manager.UpdateFstLimitsLoopMicroSec(789);

  const auto [controller_median, controller_min, controller_max] =
      manager.GetReservationControllerUpdateLoopMicroSecStats();
  const auto [limits_median, limits_min, limits_max] =
      manager.GetFstLimitsUpdateLoopMicroSecStats();

  ASSERT_EQ(123456u, controller_median);
  ASSERT_EQ(123456u, controller_min);
  ASSERT_EQ(123456u, controller_max);
  ASSERT_EQ(789u, limits_median);
  ASSERT_EQ(789u, limits_min);
  ASSERT_EQ(789u, limits_max);
}

TEST(TrafficShapingManager, DurationHistogramsAndLoopProgressAreCumulative)
{
  eos::mgm::traffic_shaping::TrafficShapingManager manager;
  manager.ApplyThreadConfig(200, 200, 200, 15);

  manager.UpdateEstimatorsLoopMicroSec(2);
  manager.UpdateFstPolicyLoopMicroSec(200000, 195000, true);
  manager.UpdateReservationControllerLoopMicroSec(0, false);
  manager.UpdateReservationControllerLoopMicroSec(123456, true);
  manager.UpdateFstLimitsLoopMicroSec(789);
  manager.UpdateFsViewLockMicroSec(25, 456);

  const auto timing = manager.GetSystemTimingSnapshot();
  EXPECT_EQ(1u, timing.estimators.duration.sample_count);
  EXPECT_DOUBLE_EQ(0.000002, timing.estimators.duration.sample_sum_seconds);
  ASSERT_EQ(timing.estimators.duration.upper_bounds_seconds.size(),
            timing.estimators.duration.cumulative_bucket_counts.size());
  ASSERT_GE(timing.estimators.duration.cumulative_bucket_counts.size(), 2u);
  EXPECT_EQ(0u, timing.estimators.duration.cumulative_bucket_counts[0]);
  EXPECT_EQ(1u, timing.estimators.duration.cumulative_bucket_counts[1]);
  EXPECT_EQ(1u, timing.estimators.iterations_total);
  EXPECT_GT(timing.estimators.last_completed_timestamp_seconds, 0u);

  EXPECT_EQ(1u, timing.fst_policy.duration.sample_count);
  EXPECT_DOUBLE_EQ(0.2, timing.fst_policy.duration.sample_sum_seconds);
  EXPECT_EQ(1u, timing.fst_policy.iterations_total);
  EXPECT_GT(timing.fst_policy.last_completed_timestamp_seconds, 0u);
  EXPECT_EQ(1u, timing.io_pressure.duration.sample_count);
  EXPECT_DOUBLE_EQ(0.195, timing.io_pressure.duration.sample_sum_seconds);
  EXPECT_EQ(1u, timing.io_pressure.iterations_total);
  EXPECT_GT(timing.io_pressure.last_completed_timestamp_seconds, 0u);
  EXPECT_EQ(1u, timing.fst_policy_slow_iterations_total);

  EXPECT_EQ(1u, timing.reservation_controller.duration.sample_count);
  EXPECT_EQ(1u, timing.reservation_controller.iterations_total);
  EXPECT_NEAR(0.123456, timing.reservation_controller.duration.sample_sum_seconds, 1e-12);
  EXPECT_GT(timing.reservation_controller.last_completed_timestamp_seconds, 0u);

  EXPECT_EQ(1u, timing.fst_limits.duration.sample_count);
  EXPECT_EQ(1u, timing.fst_limits.iterations_total);
  EXPECT_NEAR(0.000789, timing.fst_limits.duration.sample_sum_seconds, 1e-12);
  EXPECT_GT(timing.fst_limits.last_completed_timestamp_seconds, 0u);

  EXPECT_EQ(1u, timing.fsview_lock_wait.sample_count);
  EXPECT_DOUBLE_EQ(0.000025, timing.fsview_lock_wait.sample_sum_seconds);
  EXPECT_EQ(1u, timing.fsview_lock_hold.sample_count);
  EXPECT_DOUBLE_EQ(0.000456, timing.fsview_lock_hold.sample_sum_seconds);

  manager.ApplyThreadConfig(1000, 1000, 1000, 30);
  const auto after_reconfigure = manager.GetSystemTimingSnapshot();
  EXPECT_EQ(1u, after_reconfigure.estimators.duration.sample_count);
  EXPECT_EQ(1u, after_reconfigure.fst_policy.duration.sample_count);
  EXPECT_EQ(1u, after_reconfigure.io_pressure.duration.sample_count);
  EXPECT_EQ(1u, after_reconfigure.reservation_controller.duration.sample_count);
  EXPECT_EQ(1u, after_reconfigure.fst_limits.duration.sample_count);
  EXPECT_EQ(1u, after_reconfigure.fsview_lock_wait.sample_count);
  EXPECT_EQ(1u, after_reconfigure.fsview_lock_hold.sample_count);
  EXPECT_EQ(1u, after_reconfigure.fst_policy_slow_iterations_total);
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

TEST(TrafficShapingManager, OutOfOrderReportsDoNotRegressStreamBaselines)
{
  eos::mgm::traffic_shaping::TrafficShapingManager manager;

  auto make_report = [](const int64_t timestamp_ms, const uint64_t bytes_written,
                        const uint64_t write_ops) {
    eos::traffic_shaping::FstIoReport report;
    report.set_node_id("/eos/fst.example:1095/fst");
    report.set_timestamp_ms(timestamp_ms);
    auto* entry = report.add_entries();
    entry->set_app_name("ordered-report-app");
    entry->set_uid(1);
    entry->set_gid(2);
    entry->set_fsid(3);
    entry->set_generation_id(1);
    entry->set_total_bytes_written(bytes_written);
    entry->set_total_write_ops(write_ops);
    return report;
  };

  manager.ProcessReport(make_report(1000, 100, 1)); // Baseline.
  manager.ProcessReport(make_report(3000, 300, 3)); // +200 bytes, +2 ops.
  manager.ProcessReport(make_report(2000, 200, 2)); // Stale: must be ignored.
  manager.ProcessReport(make_report(4000, 400, 4)); // +100 bytes, +1 op.

  const auto totals = manager.GetTotalCumulativeStats();
  EXPECT_EQ(300u, totals.bytes_written_total);
  EXPECT_EQ(3u, totals.write_ops_total);
}

TEST(TrafficShapingManager, ZeroTimestampReportsKeepMonotonicBaselines)
{
  eos::mgm::traffic_shaping::TrafficShapingManager manager;

  auto make_report = [](const uint64_t bytes_written) {
    eos::traffic_shaping::FstIoReport report;
    report.set_node_id("/eos/fst.example:1095/fst");
    auto* entry = report.add_entries();
    entry->set_app_name("legacy-report-app");
    entry->set_uid(1);
    entry->set_gid(2);
    entry->set_generation_id(1);
    entry->set_total_bytes_written(bytes_written);
    return report;
  };

  manager.ProcessReport(make_report(100));
  manager.ProcessReport(make_report(300));
  manager.ProcessReport(make_report(200));
  manager.ProcessReport(make_report(400));

  const auto totals = manager.GetTotalCumulativeStats();
  EXPECT_EQ(300u, totals.bytes_written_total);
}

TEST(TrafficShapingManager, GenerationIdsAreOpaqueAndTimestampsOrderChanges)
{
  eos::mgm::traffic_shaping::TrafficShapingManager manager;

  auto make_report = [](const uint64_t generation, const int64_t timestamp_ms,
                        const uint64_t bytes_written) {
    eos::traffic_shaping::FstIoReport report;
    report.set_node_id("/eos/fst.example:1095/fst");
    report.set_timestamp_ms(timestamp_ms);
    auto* entry = report.add_entries();
    entry->set_app_name("restarted-report-app");
    entry->set_uid(1);
    entry->set_gid(2);
    entry->set_generation_id(generation);
    entry->set_total_bytes_written(bytes_written);
    return report;
  };

  manager.ProcessReport(make_report(200, 3000, 100)); // Initial baseline.
  manager.ProcessReport(make_report(100, 4000, 50));  // Lower opaque ID: +50.
  manager.ProcessReport(make_report(100, 4100, 100)); // +50.
  manager.ProcessReport(make_report(200, 3500, 500)); // Older report: ignored.

  const auto totals = manager.GetTotalCumulativeStats();
  EXPECT_EQ(100u, totals.bytes_written_total);
}

TEST(TrafficShapingManager, FutureSourceTimestampDoesNotPoisonRateAnchor)
{
  eos::mgm::traffic_shaping::TrafficShapingManager manager;
  manager.ApplyThreadConfig(200, 200, 200, 15);

  auto make_report = [](const int64_t timestamp_ms, const uint64_t bytes_written) {
    eos::traffic_shaping::FstIoReport report;
    report.set_node_id("/eos/future-clock.example:1095/fst");
    report.set_timestamp_ms(timestamp_ms);
    auto* entry = report.add_entries();
    entry->set_app_name("future-clock-app");
    entry->set_uid(1);
    entry->set_gid(2);
    entry->set_generation_id(1);
    entry->set_total_bytes_written(bytes_written);
    return report;
  };

  const int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::system_clock::now().time_since_epoch())
                             .count();
  manager.ProcessReport(make_report(now_ms, 0));
  manager.ProcessReport(make_report(now_ms + 60'000, 200));
  manager.ProcessReport(make_report(now_ms + 200, 400));
  manager.UpdateEstimators(0.2);

  EXPECT_EQ(400u, manager.GetTotalCumulativeStats().bytes_written_total);
  const auto stats = manager.GetGlobalStats();
  ASSERT_EQ(1u, stats.size());
  EXPECT_GT(stats.begin()->second.ema[eos::mgm::traffic_shaping::Ema1s].write_rate_bps,
            500.0);
}

TEST(TrafficShapingManager, NodeIncarnationHandlesClockRollbackAndDelayedReports)
{
  eos::mgm::traffic_shaping::TrafficShapingManager manager;

  auto make_report = [](const int64_t node_start_time_ms, const int64_t timestamp_ms,
                        const uint64_t bytes_written) {
    eos::traffic_shaping::FstIoReport report;
    report.set_node_id("/eos/restarted.example:1095/fst");
    report.set_node_start_time_ms(node_start_time_ms);
    report.set_timestamp_ms(timestamp_ms);
    auto* entry = report.add_entries();
    entry->set_app_name("restart-app");
    entry->set_uid(1);
    entry->set_gid(2);
    entry->set_generation_id(static_cast<uint64_t>(node_start_time_ms));
    entry->set_total_bytes_written(bytes_written);
    return report;
  };

  manager.ProcessReport(make_report(2000, 2000, 100));
  manager.ProcessReport(make_report(2000, 2200, 200));
  EXPECT_EQ(100u, manager.GetTotalCumulativeStats().bytes_written_total);

  // A restarted node may have a lower wall-clock value. Its first report is a
  // new baseline, and a delayed report from the retired incarnation is ignored.
  manager.ProcessReport(make_report(1000, 1000, 50));
  manager.ProcessReport(make_report(1000, 1200, 100));
  manager.ProcessReport(make_report(2000, 2400, 500));

  EXPECT_EQ(150u, manager.GetTotalCumulativeStats().bytes_written_total);
}

TEST(TrafficShapingManager, GlobalStatsAggregateAcrossFilesystems)
{
  eos::mgm::traffic_shaping::TrafficShapingManager manager;

  auto make_report = [](const uint32_t fsid, const uint64_t total_bytes_written) {
    eos::traffic_shaping::FstIoReport report;
    report.set_node_id("/eos/fst.example:1095/fst");
    auto* entry = report.add_entries();
    entry->set_app_name("aggregate-app");
    entry->set_uid(1);
    entry->set_gid(2);
    entry->set_fsid(fsid);
    entry->set_generation_id(1);
    entry->set_total_bytes_written(total_bytes_written);
    entry->set_total_write_ops(total_bytes_written / 4096);
    return report;
  };

  manager.ProcessReport(make_report(3, 1024 * 1024));
  manager.ProcessReport(make_report(3, 2 * 1024 * 1024));
  manager.ProcessReport(make_report(4, 1024 * 1024));
  manager.ProcessReport(make_report(4, 2 * 1024 * 1024));
  manager.UpdateEstimators(1.0);

  const auto global_stats = manager.GetGlobalStats();
  ASSERT_EQ(1u, global_stats.size());
  ASSERT_EQ(0u, global_stats.begin()->first.fsid);
  EXPECT_DOUBLE_EQ(
      2.0 * 1024.0 * 1024.0,
      global_stats.begin()->second.ema[eos::mgm::traffic_shaping::Ema1s].write_rate_bps);

  const auto cardinality = manager.GetMapCardinalityStats();
  EXPECT_EQ(2u, cardinality.node_state_streams);
  EXPECT_EQ(1u, cardinality.global_stats);
  EXPECT_EQ(1u, cardinality.global_cumulative_stats);
}

TEST(TrafficShapingManager, GlobalStatsKeepFilesystemWhenDetailEnabled)
{
  eos::mgm::traffic_shaping::TrafficShapingManager manager;
  manager.SetFilesystemDetailEnabled(true);

  auto make_report = [](const uint32_t fsid, const uint64_t total_bytes_written) {
    eos::traffic_shaping::FstIoReport report;
    report.set_node_id("/eos/fst.example:1095/fst");
    auto* entry = report.add_entries();
    entry->set_app_name("fs-detail-app");
    entry->set_uid(1);
    entry->set_gid(2);
    entry->set_fsid(fsid);
    entry->set_generation_id(1);
    entry->set_total_bytes_written(total_bytes_written);
    entry->set_total_write_ops(total_bytes_written / 4096);
    return report;
  };

  manager.ProcessReport(make_report(3, 1024 * 1024));
  manager.ProcessReport(make_report(3, 2 * 1024 * 1024));
  manager.ProcessReport(make_report(4, 1024 * 1024));
  manager.ProcessReport(make_report(4, 2 * 1024 * 1024));
  manager.UpdateEstimators(1.0);

  const auto cardinality = manager.GetMapCardinalityStats();
  EXPECT_EQ(2u, cardinality.global_stats);
  EXPECT_EQ(2u, cardinality.global_cumulative_stats);
  EXPECT_EQ(2u, cardinality.disk_stats);
  EXPECT_EQ(2u, cardinality.detailed_stats);
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
  manager.ProcessReport(make_report("/eos/fst-b.example:1095/fst", "cardinality-app-b", 2,
                                    2 * 1024 * 1024));

  ASSERT_TRUE(manager.LoadPoliciesFromString(
      "{\"appPolicies\":{\"cardinality-app-a\":{\"limitWriteBytesPerSec\":1000,"
      "\"isEnabled\":true}},"
      "\"uidPolicies\":{\"1\":{\"limitWriteBytesPerSec\":1000,\"isEnabled\":true}},"
      "\"gidPolicies\":{\"2\":{\"limitWriteBytesPerSec\":1000,\"isEnabled\":true}}}"));

  const auto stats = manager.GetMapCardinalityStats();
  ASSERT_EQ(2u, stats.node_states);
  ASSERT_EQ(2u, stats.node_state_streams);
  ASSERT_EQ(2u, stats.global_stats);
  ASSERT_EQ(2u, stats.node_stats);
  ASSERT_EQ(2u, stats.node_entity_stats);
  ASSERT_EQ(0u, stats.disk_stats);
  ASSERT_EQ(0u, stats.detailed_stats);
  ASSERT_EQ(2u, stats.projection_app_cumulative_stats);
  ASSERT_EQ(2u, stats.projection_uid_cumulative_stats);
  ASSERT_EQ(1u, stats.projection_gid_cumulative_stats);
  ASSERT_EQ(2u, stats.projection_node_cumulative_stats);
  ASSERT_EQ(1u, stats.app_policies);
  ASSERT_EQ(1u, stats.uid_policies);
  ASSERT_EQ(1u, stats.gid_policies);
}

TEST(TrafficShapingManager, RejectsOversizedPolicyInputs)
{
  eos::mgm::traffic_shaping::TrafficShapingManager manager;
  eos::mgm::traffic_shaping::TrafficShapingPolicy policy;
  policy.limit_write_bytes_per_sec = 1000;
  policy.is_enabled = true;

  const std::string oversized_app(eos::common::TRAFFIC_SHAPING_FST_IDENTITY_MAX_BYTES + 1,
                                  'x');
  EXPECT_THROW(manager.SetAppPolicy(oversized_app, policy), std::length_error);
  EXPECT_TRUE(manager.GetAppPolicies().empty());

  const std::string oversized_config(
      eos::common::TRAFFIC_SHAPING_POLICY_CONFIG_MAX_BYTES + 1, 'x');
  EXPECT_FALSE(manager.LoadPoliciesFromString(oversized_config));
  EXPECT_TRUE(manager.GetAppPolicies().empty());
}

TEST(TrafficShapingManager, GarbageCollectionPrunesCumulativeStats)
{
  eos::mgm::traffic_shaping::TrafficShapingManager manager;
  manager.SetFilesystemDetailEnabled(true);

  auto make_report = [](const std::string& node_id, const uint64_t total_bytes_written) {
    eos::traffic_shaping::FstIoReport report;
    report.set_node_id(node_id);
    auto* entry = report.add_entries();
    entry->set_app_name("gc-app");
    entry->set_uid(1);
    entry->set_gid(2);
    entry->set_fsid(3);
    entry->set_generation_id(1);
    entry->set_total_bytes_written(total_bytes_written);
    entry->set_total_write_ops(total_bytes_written / 4096);
    return report;
  };

  manager.ProcessReport(make_report("/eos/fst-a.example:1095/fst", 1024 * 1024));
  manager.ProcessReport(make_report("/eos/fst-a.example:1095/fst", 2 * 1024 * 1024));
  manager.UpdateEstimators(1.0);

  auto cardinality = manager.GetMapCardinalityStats();
  ASSERT_EQ(1u, cardinality.global_cumulative_stats);
  ASSERT_EQ(1u, cardinality.node_cumulative_stats);
  ASSERT_EQ(1u, cardinality.disk_cumulative_stats);
  ASSERT_EQ(1u, cardinality.detailed_cumulative_stats);
  auto projection_stats = manager.GetProjectionCumulativeStats();
  ASSERT_EQ(1u, projection_stats.app.size());
  ASSERT_EQ(1024u * 1024u, projection_stats.app["gc-app"].bytes_written_total);
  ASSERT_EQ(1u, projection_stats.uid.size());
  ASSERT_EQ(1u, projection_stats.gid.size());
  ASSERT_EQ(1u, projection_stats.node.size());

  manager.GarbageCollect(3600);

  cardinality = manager.GetMapCardinalityStats();
  EXPECT_EQ(1u, cardinality.global_cumulative_stats);
  EXPECT_EQ(1u, cardinality.node_cumulative_stats);
  EXPECT_EQ(1u, cardinality.disk_cumulative_stats);
  EXPECT_EQ(1u, cardinality.detailed_cumulative_stats);

  manager.GarbageCollect(-1);

  cardinality = manager.GetMapCardinalityStats();
  EXPECT_EQ(0u, cardinality.global_cumulative_stats);
  EXPECT_EQ(0u, cardinality.node_cumulative_stats);
  EXPECT_EQ(0u, cardinality.disk_cumulative_stats);
  EXPECT_EQ(0u, cardinality.detailed_cumulative_stats);

  projection_stats = manager.GetProjectionCumulativeStats();
  EXPECT_TRUE(projection_stats.app.empty());
  EXPECT_TRUE(projection_stats.uid.empty());
  EXPECT_TRUE(projection_stats.gid.empty());
  EXPECT_TRUE(projection_stats.node.empty());

  EXPECT_EQ(1u, cardinality.garbage_collection_removed_nodes_total);
  EXPECT_EQ(1u, cardinality.garbage_collection_removed_node_streams_total);
  EXPECT_EQ(1u, cardinality.garbage_collection_removed_global_streams_total);
  EXPECT_EQ(1u, cardinality.garbage_collection_removed_disk_stats_total);
  EXPECT_EQ(1u, cardinality.garbage_collection_removed_detailed_stats_total);
  EXPECT_EQ(2u, manager.GetSystemTimingSnapshot().garbage_collection.iterations_total);
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

TEST(TrafficShapingManager, NodeLimitExpiryOnlyResetsAffectedDirection)
{
  eos::mgm::traffic_shaping::TrafficShapingManager manager;
  const auto now = std::chrono::steady_clock::now();
  eos::mgm::traffic_shaping::NodeReservationControllerRuntime runtime;
  runtime.feedback.read.consecutive_deficit_samples = 7;
  runtime.feedback.write.consecutive_deficit_samples = 9;
  auto& limit = runtime.app_limits["competitor"];
  limit.read_bps = 100;
  limit.read_update_time = now;
  limit.write_bps = 200;
  limit.write_update_time = now - std::chrono::minutes(6);
  manager.SetNodeReservationControllerRuntimeForTest("node", runtime);

  ASSERT_EQ(1u, manager.ExpireControllerLimits(now));
  const auto runtimes = manager.GetNodeReservationControllerRuntimes();
  ASSERT_NE(runtimes.end(), runtimes.find("node"));
  ASSERT_NE(runtimes.at("node").app_limits.end(),
            runtimes.at("node").app_limits.find("competitor"));
  EXPECT_EQ(100u, runtimes.at("node").app_limits.at("competitor").read_bps);
  EXPECT_EQ(0u, runtimes.at("node").app_limits.at("competitor").write_bps);
  EXPECT_EQ(7u, runtimes.at("node").feedback.read.consecutive_deficit_samples);
  EXPECT_EQ(0u, runtimes.at("node").feedback.write.consecutive_deficit_samples);
}

TEST(TrafficShapingManager, NodeControllerSnapshotExposesLimitsAndFeedback)
{
  eos::mgm::traffic_shaping::TrafficShapingManager manager;
  const auto now = std::chrono::steady_clock::time_point{std::chrono::seconds(100)};
  eos::mgm::traffic_shaping::NodeReservationControllerRuntime runtime;
  runtime.app_limits["tsb-batch"].write_bps = 400000000;
  runtime.app_limits["tsb-user"].read_bps = 200000000;

  auto& write = runtime.feedback.write;
  write.consecutive_deficit_samples = 2;
  write.protected_apps.emplace(
      "tsb-daq",
      eos::mgm::traffic_shaping::ReservationControllerState::ProtectedAppAction{
          1000000000.0, 700000000.0, 300000000.0});
  write.applied_reduction_bps = 300000000.0;
  write.last_observed_protected_gain_bps = 150000000.0;
  write.last_response_ratio = 0.5;
  write.last_adjustment_time = now - std::chrono::seconds(1);

  auto& read = runtime.feedback.read;
  read.ineffective_probe_count = 2;
  read.failed_protected_apps.emplace(
      "tsb-online",
      eos::mgm::traffic_shaping::ReservationControllerState::FailedProtectedApp{
          300000000.0, 100000000.0, 200000000.0, 100000000.0});
  read.suppressed_until = now + std::chrono::seconds(7);
  manager.SetNodeReservationControllerRuntimeForTest("node", runtime);

  const auto snapshot = manager.GetNodeReservationControllerSnapshot(now);
  ASSERT_EQ(2u, snapshot.limits.size());
  EXPECT_EQ("tsb-batch", snapshot.limits[0].app);
  EXPECT_EQ(400000000u, snapshot.limits[0].write_bytes_per_sec);
  EXPECT_EQ("tsb-user", snapshot.limits[1].app);
  EXPECT_EQ(200000000u, snapshot.limits[1].read_bytes_per_sec);

  ASSERT_EQ(2u, snapshot.feedback.size());
  const auto& read_snapshot = snapshot.feedback[0];
  EXPECT_FALSE(read_snapshot.is_write);
  EXPECT_TRUE(read_snapshot.suppressed);
  EXPECT_DOUBLE_EQ(7.0, read_snapshot.suppression_remaining_seconds);
  EXPECT_EQ(1u, read_snapshot.failed_protected_app_count);
  EXPECT_EQ(2u, read_snapshot.ineffective_probe_count);

  const auto& write_snapshot = snapshot.feedback[1];
  EXPECT_TRUE(write_snapshot.is_write);
  EXPECT_TRUE(write_snapshot.awaiting_response);
  EXPECT_EQ(1u, write_snapshot.protected_app_count);
  EXPECT_DOUBLE_EQ(300000000.0, write_snapshot.applied_reduction_bps);
  EXPECT_DOUBLE_EQ(150000000.0, write_snapshot.observed_protected_gain_bps);
  EXPECT_DOUBLE_EQ(0.5, write_snapshot.response_ratio);

  ASSERT_EQ(2u, snapshot.cohort_apps.size());
  const auto& failed_cohort = snapshot.cohort_apps[0];
  EXPECT_EQ("tsb-online", failed_cohort.app);
  EXPECT_TRUE(failed_cohort.failed);
  EXPECT_DOUBLE_EQ(200000000.0, failed_cohort.assigned_reduction_bps);
  EXPECT_DOUBLE_EQ(100000000.0, failed_cohort.rate_at_failure_bps);
  const auto& active_cohort = snapshot.cohort_apps[1];
  EXPECT_EQ("tsb-daq", active_cohort.app);
  EXPECT_FALSE(active_cohort.failed);
  EXPECT_DOUBLE_EQ(700000000.0, active_cohort.baseline_rate_bps);
}

TEST(TrafficShapingManager, ReservedAppIoPressureOmitsDisabledPolicies)
{
  eos::mgm::traffic_shaping::TrafficShapingManager manager;

  ASSERT_TRUE(manager.LoadPoliciesFromString(
      "{\"appPolicies\":{\"reserved-disabled-app\":{"
      "\"isEnabled\":false,\"reservationWriteBytesPerSec\":300000000},"
      "\"limited-app\":{\"isEnabled\":true,\"limitWriteBytesPerSec\":100000000}}}"));

  const auto pressure = manager.GetReservedAppIoPressure();

  ASSERT_TRUE(pressure.empty());
}

TEST(TrafficShapingManager, ReservedAppIoPressureKeepsMissingNodePressureUnknown)
{
  eos::mgm::traffic_shaping::TrafficShapingManager manager;
  PrepareReservedAppWorkload(manager);

  const auto pressure = manager.GetReservedAppIoPressure();

  ASSERT_NE(pressure.end(), pressure.find("reserved-app"));
  EXPECT_FALSE(pressure.at("reserved-app").has_read);
  EXPECT_FALSE(pressure.at("reserved-app").has_write);
  EXPECT_DOUBLE_EQ(0.0, pressure.at("reserved-app").read);
  EXPECT_DOUBLE_EQ(0.0, pressure.at("reserved-app").write);
}

TEST(TrafficShapingManager, ReservedAppNodeIoPressureIsSparseAcrossOnlineNodes)
{
  constexpr uint64_t mb = 1000ULL * 1000ULL;
  eos::mgm::traffic_shaping::TrafficShapingManager manager;
  eos::mgm::traffic_shaping::TrafficShapingPolicy reserved;
  reserved.reservation_write_bytes_per_sec = 1000 * mb;
  for (uint32_t i = 0; i < 32; ++i) {
    manager.SetAppPolicy("reserved-" + std::to_string(i), reserved);
  }
  auto disabled = reserved;
  disabled.is_enabled = false;
  manager.SetAppPolicy("reserved-disabled", disabled);

  std::vector<std::string> online_nodes;
  online_nodes.reserve(64);
  for (uint32_t i = 0; i < 64; ++i) {
    online_nodes.push_back("node-" + std::to_string(i));
  }

  eos::mgm::traffic_shaping::RateMetrics fast{};
  eos::mgm::traffic_shaping::RateMetrics stable{};
  fast.write_rate_bps = 200 * mb;
  stable.write_rate_bps = 300 * mb;
  manager.SetGlobalEmaRatesForTest({"reserved-0", 1, 1, 0}, fast, stable);
  fast.write_rate_bps = 1200 * mb;
  stable.write_rate_bps = 1100 * mb;
  manager.SetGlobalEmaRatesForTest({"reserved-1", 2, 2, 0}, fast, stable);
  fast.write_rate_bps = 50 * mb;
  stable.write_rate_bps = 50 * mb;
  manager.SetGlobalEmaRatesForTest({"reserved-2", 3, 3, 0}, fast, stable);

  eos::mgm::traffic_shaping::RateMetrics local{};
  local.write_rate_bps = 200 * mb;
  manager.SetNodeEntityRateForTest("node-0", {"reserved-0", 1, 1, 0}, local);
  manager.SetNodeEntityRateForTest("node-0", {"reserved-1", 2, 2, 0}, local);
  local.write_rate_bps = 50 * mb;
  manager.SetNodeEntityRateForTest("node-1", {"reserved-2", 3, 3, 0}, local);
  manager.SetNodeEntityRateForTest("node-2", {"reserved-3", 4, 4, 0}, local);
  manager.SetNodeEntityLastActivityForTest("node-2", {"reserved-3", 4, 4, 0},
                                           time(nullptr) - 10);
  manager.SetNodeEntityRateForTest("offline-node", {"reserved-3", 4, 4, 0}, local);
  manager.SetNodeEntityRateForTest("node-0", {"reserved-disabled", 5, 5, 0}, local);
  manager.SetNodeEntityRateForTest("node-0", {"unreserved", 6, 6, 0}, local);

  eos::mgm::traffic_shaping::NodeReservationControllerRuntime active_runtime;
  active_runtime.app_limits["reserved-0"].write_bps = 123 * mb;
  manager.SetNodeReservationControllerRuntimeForTest("node-0", active_runtime);
  eos::mgm::traffic_shaping::NodeReservationControllerRuntime limit_only_runtime;
  limit_only_runtime.app_limits["reserved-4"].write_bps = 456 * mb;
  manager.SetNodeReservationControllerRuntimeForTest("node-2", limit_only_runtime);

  const auto snapshots =
      manager.GetReservedAppNodeIoPressureForTest({{"node-0", 0.9}}, online_nodes);
  ASSERT_EQ(3u, snapshots.size());

  auto find_pair = [&](const std::string& app, const std::string& node) {
    return std::find_if(snapshots.begin(), snapshots.end(), [&](const auto& snapshot) {
      return snapshot.app == app && snapshot.node_id == node;
    });
  };
  const auto deficient = find_pair("reserved-0", "node-0");
  const auto healthy = find_pair("reserved-1", "node-0");
  const auto unknown_pressure = find_pair("reserved-2", "node-1");
  ASSERT_NE(snapshots.end(), deficient);
  ASSERT_NE(snapshots.end(), healthy);
  ASSERT_NE(snapshots.end(), unknown_pressure);
  EXPECT_EQ(300 * mb, deficient->global_write_rate_bps);
  EXPECT_EQ(123 * mb, deficient->node_controller_write_limit_bytes_per_sec);
  EXPECT_TRUE(deficient->write_triggers_competitor_throttling);
  EXPECT_TRUE(deficient->node_has_pressured_write_reservation);
  EXPECT_EQ(1200 * mb, healthy->global_write_rate_bps);
  EXPECT_FALSE(healthy->write_reservation_deficit_active);
  EXPECT_FALSE(healthy->write_triggers_competitor_throttling);
  EXPECT_TRUE(healthy->node_has_pressured_write_reservation);
  EXPECT_FALSE(unknown_pressure->has_node_io_pressure);
  EXPECT_FALSE(unknown_pressure->has_read_io_pressure);
  EXPECT_FALSE(unknown_pressure->has_write_io_pressure);
  EXPECT_FALSE(unknown_pressure->write_pressure_active);
  EXPECT_EQ(snapshots.end(), find_pair("reserved-3", "node-2"));
  EXPECT_EQ(snapshots.end(), find_pair("reserved-3", "offline-node"));
  EXPECT_EQ(snapshots.end(), find_pair("reserved-disabled", "node-0"));
  EXPECT_EQ(snapshots.end(), find_pair("reserved-4", "node-2"));
}

TEST(TrafficShapingManager, ReservedAppNodeIoPressureAggregatesBeforeWindowSelection)
{
  constexpr uint64_t mb = 1000ULL * 1000ULL;
  eos::mgm::traffic_shaping::TrafficShapingManager manager;
  eos::mgm::traffic_shaping::TrafficShapingPolicy policy;
  policy.reservation_read_bytes_per_sec = 1000 * mb;
  policy.reservation_write_bytes_per_sec = 1000 * mb;
  manager.SetAppPolicy("reserved", policy);

  eos::mgm::traffic_shaping::RateMetrics fast_a{};
  eos::mgm::traffic_shaping::RateMetrics stable_a{};
  fast_a.read_rate_bps = 100 * mb;
  fast_a.write_rate_bps = 200 * mb;
  stable_a.read_rate_bps = 300 * mb;
  stable_a.write_rate_bps = 500 * mb;
  manager.SetGlobalEmaRatesForTest({"reserved", 1, 1, 0}, fast_a, stable_a);

  eos::mgm::traffic_shaping::RateMetrics fast_b{};
  eos::mgm::traffic_shaping::RateMetrics stable_b{};
  fast_b.read_rate_bps = 400 * mb;
  fast_b.write_rate_bps = 600 * mb;
  stable_b.read_rate_bps = 50 * mb;
  stable_b.write_rate_bps = 100 * mb;
  manager.SetGlobalEmaRatesForTest({"reserved", 2, 2, 0}, fast_b, stable_b);

  eos::mgm::traffic_shaping::RateMetrics local_a{};
  local_a.read_rate_bps = 120 * mb;
  local_a.write_rate_bps = 200 * mb;
  manager.SetNodeEntityRateForTest("node", {"reserved", 1, 1, 0}, local_a);
  eos::mgm::traffic_shaping::RateMetrics local_b{};
  local_b.read_rate_bps = 180 * mb;
  local_b.write_rate_bps = 300 * mb;
  manager.SetNodeEntityRateForTest("node", {"reserved", 2, 2, 0}, local_b);

  const auto snapshots =
      manager.GetReservedAppNodeIoPressureForTest({{"node", 0.9}}, {"node"});
  ASSERT_EQ(1u, snapshots.size());
  const auto& snapshot = snapshots.front();
  EXPECT_DOUBLE_EQ(500 * mb, snapshot.global_read_rate_bps);
  EXPECT_DOUBLE_EQ(800 * mb, snapshot.global_write_rate_bps);
  EXPECT_DOUBLE_EQ(300 * mb, snapshot.read_rate_bps);
  EXPECT_DOUBLE_EQ(500 * mb, snapshot.write_rate_bps);
  EXPECT_TRUE(snapshot.read_triggers_competitor_throttling);
  EXPECT_TRUE(snapshot.write_triggers_competitor_throttling);
  EXPECT_TRUE(snapshot.node_has_pressured_read_reservation);
  EXPECT_TRUE(snapshot.node_has_pressured_write_reservation);
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

TEST(TrafficShapingManager, DelayFeedbackIsAlwaysBounded)
{
  constexpr double limit_bps = 1024.0 * 1024.0;
  const uint64_t bounded =
      eos::mgm::traffic_shaping::TrafficShapingManager::CalculateDelayUs(
          limit_bps, 0.0, std::numeric_limits<uint64_t>::max(), 1.0, true, false);
  EXPECT_LE(bounded, 2000000u);

  EXPECT_EQ(0u, eos::mgm::traffic_shaping::TrafficShapingManager::CalculateDelayUs(
                    std::numeric_limits<double>::quiet_NaN(), limit_bps, 1000, 1.0, true,
                    false));
  EXPECT_LE(
      eos::mgm::traffic_shaping::TrafficShapingManager::CalculateDelayUs(
          limit_bps, std::numeric_limits<double>::infinity(), 1000, 1.0, true, false),
      2000000u);
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

TEST(TrafficShapingManager, ReservationControllerUsesPressuredNodeRateDomain)
{
  constexpr uint64_t mb = 1000ULL * 1000ULL;
  eos::mgm::traffic_shaping::TrafficShapingManager manager;
  manager.ApplyThreadConfig(1000, 1000, 1000, 15);

  eos::mgm::traffic_shaping::TrafficShapingPolicy reserved_policy;
  reserved_policy.reservation_write_bytes_per_sec = 1000 * mb;
  manager.SetAppPolicy("reserved-app", reserved_policy);

  auto make_report = [](const std::string& node, const int64_t timestamp_ms,
                        const uint64_t reserved_bytes, const uint64_t competitor_bytes) {
    eos::traffic_shaping::FstIoReport report;
    report.set_node_id(node);
    report.set_timestamp_ms(timestamp_ms);
    if (reserved_bytes > 0 || node.find("pressured") != std::string::npos) {
      auto* entry = report.add_entries();
      entry->set_app_name("reserved-app");
      entry->set_uid(1);
      entry->set_gid(1);
      entry->set_generation_id(1);
      entry->set_total_bytes_written(reserved_bytes);
    }
    auto* competitor = report.add_entries();
    competitor->set_app_name("best-effort-app");
    competitor->set_uid(2);
    competitor->set_gid(2);
    competitor->set_generation_id(1);
    competitor->set_total_bytes_written(competitor_bytes);
    return report;
  };

  const std::string pressured_node = "/eos/pressured.example:1095/fst";
  const std::string healthy_node = "/eos/healthy.example:1095/fst";
  manager.ProcessReport(make_report(pressured_node, 1000, 0, 0));
  manager.ProcessReport(make_report(healthy_node, 1000, 0, 0));
  manager.ProcessReport(make_report(pressured_node, 2000, 200 * mb, 500 * mb));
  manager.ProcessReport(make_report(healthy_node, 2000, 0, 10'000 * mb));
  manager.UpdateEstimators(1.0);

  const std::unordered_map<std::string, double> pressure{{pressured_node, 0.5},
                                                         {healthy_node, 0.0}};
  manager.UpdateTrafficShapingController(pressure);
  manager.UpdateTrafficShapingController(pressure);

  const auto runtimes = manager.GetNodeReservationControllerRuntimes();
  const auto node_it = runtimes.find(pressured_node);
  ASSERT_NE(runtimes.end(), node_it);
  const auto app_it = node_it->second.app_limits.find("best-effort-app");
  ASSERT_NE(node_it->second.app_limits.end(), app_it);
  EXPECT_EQ(300 * mb, app_it->second.write_bps);
  EXPECT_EQ(runtimes.end(), runtimes.find(healthy_node));
}

TEST(TrafficShapingManager, ControllerPublicationFailurePreservesPriorState)
{
  eos::mgm::traffic_shaping::TrafficShapingManager manager;
  eos::mgm::traffic_shaping::NodeReservationControllerRuntime runtime;
  auto& limit = runtime.app_limits["existing-app"];
  limit.write_bps = 123456;
  limit.write_update_time = std::chrono::steady_clock::now();
  manager.SetNodeReservationControllerRuntimeForTest("existing-node", runtime);

  manager.FailNextControllerPublicationForTest();
  EXPECT_NO_THROW(manager.UpdateTrafficShapingController({}));

  const auto after_failure = manager.GetNodeReservationControllerRuntimes();
  ASSERT_NE(after_failure.end(), after_failure.find("existing-node"));
  EXPECT_EQ(123456u,
            after_failure.at("existing-node").app_limits.at("existing-app").write_bps);

  EXPECT_NO_THROW(manager.UpdateTrafficShapingController({}));
  EXPECT_TRUE(manager.GetNodeReservationControllerRuntimes().empty());
}

TEST(TrafficShapingManager, ReservationResetInvalidatesStaleControllerPublication)
{
  eos::mgm::traffic_shaping::TrafficShapingManager manager;
  const std::string node = PrepareReservedAppWorkload(manager);
  const std::unordered_map<std::string, double> pressure{{node, 0.5}};

  // Prime the two-sample engagement state. The paused second update would
  // publish a limit if its snapshot were allowed to overwrite the reset.
  manager.UpdateTrafficShapingController(pressure);
  manager.PauseControllerBeforePublicationForTest();
  std::thread controller([&] { manager.UpdateTrafficShapingController(pressure); });

  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!manager.IsControllerPublicationPausedForTest() &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  const bool publication_paused = manager.IsControllerPublicationPausedForTest();
  if (!publication_paused) {
    manager.ResumeControllerPublicationForTest();
    controller.join();
  }
  ASSERT_TRUE(publication_paused);

  manager.SetReservationsEnabled(false);
  manager.ResumeControllerPublicationForTest();
  controller.join();

  EXPECT_FALSE(manager.GetReservationsEnabled());
  EXPECT_TRUE(manager.GetNodeReservationControllerRuntimes().empty());
}

TEST(TrafficShapingManager, GloballySatisfiedReservationDoesNotThrottlePressuredNodes)
{
  constexpr uint64_t mb = 1000ULL * 1000ULL;
  eos::mgm::traffic_shaping::TrafficShapingManager manager;
  manager.ApplyThreadConfig(1000, 1000, 1000, 15);

  eos::mgm::traffic_shaping::TrafficShapingPolicy reserved_policy;
  reserved_policy.reservation_write_bytes_per_sec = 1000 * mb;
  manager.SetAppPolicy("reserved-app", reserved_policy);

  auto make_report = [](const std::string& node, const int64_t timestamp_ms,
                        const uint64_t reserved_bytes, const uint64_t competitor_bytes) {
    eos::traffic_shaping::FstIoReport report;
    report.set_node_id(node);
    report.set_timestamp_ms(timestamp_ms);
    for (const auto& [app, uid, bytes] :
         std::vector<std::tuple<std::string, uint32_t, uint64_t>>{
             {"reserved-app", 1, reserved_bytes},
             {"best-effort-app", 2, competitor_bytes}}) {
      auto* entry = report.add_entries();
      entry->set_app_name(app);
      entry->set_uid(uid);
      entry->set_gid(uid);
      entry->set_generation_id(1);
      entry->set_total_bytes_written(bytes);
    }
    return report;
  };

  const std::string node_a = "/eos/node-a.example:1095/fst";
  const std::string node_b = "/eos/node-b.example:1095/fst";
  manager.ProcessReport(make_report(node_a, 1000, 0, 0));
  manager.ProcessReport(make_report(node_b, 1000, 0, 0));
  manager.ProcessReport(make_report(node_a, 2000, 600 * mb, 500 * mb));
  manager.ProcessReport(make_report(node_b, 2000, 600 * mb, 500 * mb));
  manager.UpdateEstimators(1.0);

  const std::unordered_map<std::string, double> pressure{{node_a, 0.5}, {node_b, 0.5}};
  manager.UpdateTrafficShapingController(pressure);
  manager.UpdateTrafficShapingController(pressure);

  double global_reserved_write_rate = 0.0;
  for (const auto& [stream, stats] : manager.GetGlobalStats()) {
    if (stream.app == "reserved-app") {
      global_reserved_write_rate +=
          stats.ema[eos::mgm::traffic_shaping::Ema1s].write_rate_bps;
    }
  }
  EXPECT_GT(global_reserved_write_rate, 1000.0 * mb);
  EXPECT_TRUE(manager.GetNodeReservationControllerRuntimes().empty());
  const auto controller_snapshot = manager.GetNodeReservationControllerSnapshot();
  EXPECT_TRUE(controller_snapshot.limits.empty());
  EXPECT_TRUE(controller_snapshot.feedback.empty());
}

TEST(TrafficShapingManager, RecoveredReservationClearsNodeLimitsImmediately)
{
  constexpr uint64_t mb = 1000ULL * 1000ULL;
  eos::mgm::traffic_shaping::TrafficShapingManager manager;
  manager.ApplyThreadConfig(1000, 1000, 1000, 15);

  eos::mgm::traffic_shaping::TrafficShapingPolicy reserved_policy;
  reserved_policy.reservation_write_bytes_per_sec = 1000 * mb;
  manager.SetAppPolicy("reserved-app", reserved_policy);

  const std::string node = "/eos/node-a.example:1095/fst";
  eos::mgm::traffic_shaping::NodeReservationControllerRuntime runtime;
  runtime.feedback.write.consecutive_deficit_samples = 2;
  runtime.app_limits["best-effort-app"].write_bps = 300 * mb;
  manager.SetNodeReservationControllerRuntimeForTest(node, runtime);

  auto make_report = [&](const int64_t timestamp_ms, const uint64_t reserved_bytes,
                         const uint64_t competitor_bytes) {
    eos::traffic_shaping::FstIoReport report;
    report.set_node_id(node);
    report.set_timestamp_ms(timestamp_ms);
    for (const auto& [app, uid, bytes] :
         std::vector<std::tuple<std::string, uint32_t, uint64_t>>{
             {"reserved-app", 1, reserved_bytes},
             {"best-effort-app", 2, competitor_bytes}}) {
      auto* entry = report.add_entries();
      entry->set_app_name(app);
      entry->set_uid(uid);
      entry->set_gid(uid);
      entry->set_generation_id(1);
      entry->set_total_bytes_written(bytes);
    }
    return report;
  };

  manager.ProcessReport(make_report(1000, 0, 0));
  manager.ProcessReport(make_report(2000, 1200 * mb, 500 * mb));
  manager.UpdateEstimators(1.0);
  manager.UpdateTrafficShapingController({{node, 0.5}});

  EXPECT_TRUE(manager.GetNodeReservationControllerRuntimes().empty());
  const auto controller_snapshot = manager.GetNodeReservationControllerSnapshot();
  EXPECT_TRUE(controller_snapshot.limits.empty());
  EXPECT_TRUE(controller_snapshot.feedback.empty());
}

TEST(TrafficShapingManager, BriefBurstGapDoesNotCreateReservationDeficit)
{
  constexpr uint64_t mb = 1000ULL * 1000ULL;
  eos::mgm::traffic_shaping::TrafficShapingManager manager;
  manager.ApplyThreadConfig(200, 200, 200, 15);

  eos::mgm::traffic_shaping::TrafficShapingPolicy reserved_policy;
  reserved_policy.reservation_write_bytes_per_sec = 1000 * mb;
  manager.SetAppPolicy("reserved-app", reserved_policy);

  auto make_report = [](const std::string& node, const int64_t timestamp_ms,
                        const uint64_t reserved_bytes, const uint64_t competitor_bytes) {
    eos::traffic_shaping::FstIoReport report;
    report.set_node_id(node);
    report.set_timestamp_ms(timestamp_ms);
    for (const auto& [app, uid, bytes] :
         std::vector<std::tuple<std::string, uint32_t, uint64_t>>{
             {"reserved-app", 1, reserved_bytes},
             {"best-effort-app", 2, competitor_bytes}}) {
      auto* entry = report.add_entries();
      entry->set_app_name(app);
      entry->set_uid(uid);
      entry->set_gid(uid);
      entry->set_generation_id(1);
      entry->set_total_bytes_written(bytes);
    }
    return report;
  };

  const std::string node_a = "/eos/node-a.example:1095/fst";
  const std::string node_b = "/eos/node-b.example:1095/fst";
  uint64_t reserved_bytes = 0;
  uint64_t competitor_bytes = 0;
  manager.ProcessReport(make_report(node_a, 0, 0, 0));
  manager.ProcessReport(make_report(node_b, 0, 0, 0));
  for (int tick = 1; tick <= 30; ++tick) {
    reserved_bytes += 200 * mb;
    competitor_bytes += 100 * mb;
    manager.ProcessReport(
        make_report(node_a, tick * 200, reserved_bytes, competitor_bytes));
    manager.ProcessReport(
        make_report(node_b, tick * 200, reserved_bytes, competitor_bytes));
    manager.UpdateEstimators(0.2);
  }

  // FTS commonly pauses briefly between files. The one-second signal falls
  // below the reservation, while the five-second demand signal remains healthy.
  manager.UpdateEstimators(0.2);
  manager.UpdateEstimators(0.2);
  manager.UpdateEstimators(0.2);

  double one_second_rate = 0.0;
  double five_second_rate = 0.0;
  for (const auto& [stream, stats] : manager.GetGlobalStats()) {
    if (stream.app == "reserved-app") {
      one_second_rate += stats.ema[eos::mgm::traffic_shaping::Ema1s].write_rate_bps;
      five_second_rate += stats.ema[eos::mgm::traffic_shaping::Ema5s].write_rate_bps;
    }
  }
  EXPECT_LT(one_second_rate, 1000.0 * mb);
  EXPECT_GT(five_second_rate, 1000.0 * mb);

  const std::unordered_map<std::string, double> pressure{{node_a, 0.5}, {node_b, 0.5}};
  manager.UpdateTrafficShapingController(pressure);
  manager.UpdateTrafficShapingController(pressure);
  EXPECT_TRUE(manager.GetNodeReservationControllerRuntimes().empty());
}

TEST(TrafficShapingManager, BriefGapPreservesIneffectiveProbeSuppression)
{
  constexpr uint64_t mb = 1000ULL * 1000ULL;
  eos::mgm::traffic_shaping::TrafficShapingManager manager;

  eos::mgm::traffic_shaping::TrafficShapingPolicy policy;
  policy.reservation_write_bytes_per_sec = 1000 * mb;
  manager.SetAppPolicy("reserved-app", policy);

  eos::mgm::traffic_shaping::RateMetrics healthy_rate{};
  healthy_rate.write_rate_bps = 1200 * mb;
  manager.SetGlobalEmaRatesForTest({"reserved-app", 1, 1, 0}, healthy_rate, healthy_rate);

  const std::string node = "/eos/pressured.example:1095/fst";
  eos::mgm::traffic_shaping::RateMetrics reserved_rate{};
  reserved_rate.write_rate_bps = 300 * mb;
  manager.SetNodeEntityRateForTest(node, {"reserved-app", 1, 1, 0}, reserved_rate);
  eos::mgm::traffic_shaping::RateMetrics competitor_rate{};
  competitor_rate.write_rate_bps = 700 * mb;
  manager.SetNodeEntityRateForTest(node, {"best-effort-app", 2, 2, 0}, competitor_rate);

  const auto now = std::chrono::steady_clock::now();
  const auto suppressed_until = now + std::chrono::seconds(30);
  eos::mgm::traffic_shaping::NodeReservationControllerRuntime runtime;
  auto& write = runtime.feedback.write;
  write.consecutive_deficit_samples = 2;
  write.protected_apps.emplace(
      "reserved-app",
      eos::mgm::traffic_shaping::ReservationControllerState::ProtectedAppAction{
          1000.0 * mb, 300.0 * mb, 300.0 * mb});
  write.applied_reduction_bps = 300.0 * mb;
  write.ineffective_probe_count = 1;
  write.failed_protected_apps.emplace(
      "reserved-app",
      eos::mgm::traffic_shaping::ReservationControllerState::FailedProtectedApp{
          1000.0 * mb, 300.0 * mb, 300.0 * mb, 300.0 * mb});
  write.last_observed_protected_gain_bps = 0.0;
  write.last_response_ratio = 0.0;
  write.last_adjustment_time = now;
  write.healthy_since = now;
  write.suppressed_until = suppressed_until;
  auto& limit = runtime.app_limits["best-effort-app"];
  limit.write_bps = 400 * mb;
  limit.write_update_time = now;
  manager.SetNodeReservationControllerRuntimeForTest(node, runtime);

  manager.UpdateTrafficShapingController({{node, 0.5}});

  auto runtimes = manager.GetNodeReservationControllerRuntimes();
  ASSERT_NE(runtimes.end(), runtimes.find(node));
  EXPECT_TRUE(runtimes.at(node).app_limits.empty());
  const auto& gap_state = runtimes.at(node).feedback.write;
  EXPECT_EQ(0u, gap_state.consecutive_deficit_samples);
  EXPECT_TRUE(gap_state.protected_apps.empty());
  EXPECT_DOUBLE_EQ(0.0, gap_state.applied_reduction_bps);
  EXPECT_EQ(std::chrono::steady_clock::time_point{}, gap_state.last_adjustment_time);
  EXPECT_EQ(std::chrono::steady_clock::time_point{}, gap_state.healthy_since);
  EXPECT_EQ(1u, gap_state.ineffective_probe_count);
  EXPECT_EQ(1u, gap_state.failed_protected_apps.size());
  EXPECT_EQ(suppressed_until, gap_state.suppressed_until);

  eos::mgm::traffic_shaping::RateMetrics deficient_rate{};
  deficient_rate.write_rate_bps = 300 * mb;
  manager.SetGlobalEmaRatesForTest({"reserved-app", 1, 1, 0}, deficient_rate,
                                   deficient_rate);
  manager.UpdateTrafficShapingController({{node, 0.5}});
  manager.UpdateTrafficShapingController({{node, 0.5}});

  runtimes = manager.GetNodeReservationControllerRuntimes();
  ASSERT_NE(runtimes.end(), runtimes.find(node));
  EXPECT_TRUE(runtimes.at(node).app_limits.empty());
  const auto& resumed_state = runtimes.at(node).feedback.write;
  EXPECT_EQ(0u, resumed_state.consecutive_deficit_samples);
  EXPECT_EQ(1u, resumed_state.ineffective_probe_count);
  EXPECT_EQ(suppressed_until, resumed_state.suppressed_until);
}

TEST(TrafficShapingManager, ExpiredProbeSuppressionIsDiscardedDuringGap)
{
  eos::mgm::traffic_shaping::TrafficShapingManager manager;
  const auto now = std::chrono::steady_clock::now();
  eos::mgm::traffic_shaping::NodeReservationControllerRuntime runtime;
  runtime.feedback.write.ineffective_probe_count = 1;
  runtime.feedback.write.failed_protected_apps.emplace(
      "reserved-app",
      eos::mgm::traffic_shaping::ReservationControllerState::FailedProtectedApp{});
  runtime.feedback.write.suppressed_until = now - std::chrono::seconds(1);
  manager.SetNodeReservationControllerRuntimeForTest("node", runtime);

  manager.UpdateTrafficShapingController({});

  EXPECT_TRUE(manager.GetNodeReservationControllerRuntimes().empty());
}

TEST(TrafficShapingManager, ReservationHealthTakesMaxAfterFsidAggregation)
{
  constexpr uint64_t mb = 1000ULL * 1000ULL;
  eos::mgm::traffic_shaping::TrafficShapingManager manager;

  eos::mgm::traffic_shaping::TrafficShapingPolicy policy;
  policy.reservation_write_bytes_per_sec = 1000 * mb;
  manager.SetAppPolicy("reserved-app", policy);

  eos::mgm::traffic_shaping::RateMetrics fast{};
  eos::mgm::traffic_shaping::RateMetrics stable{};
  fast.write_rate_bps = 800 * mb;
  manager.SetGlobalEmaRatesForTest({"reserved-app", 1, 1, 1}, fast, {});
  stable.write_rate_bps = 800 * mb;
  manager.SetGlobalEmaRatesForTest({"reserved-app", 1, 1, 2}, {}, stable);

  const std::string node = "/eos/pressured.example:1095/fst";
  eos::mgm::traffic_shaping::RateMetrics reserved_rate{};
  reserved_rate.write_rate_bps = 500 * mb;
  manager.SetNodeEntityRateForTest(node, {"reserved-app", 1, 1, 0}, reserved_rate);
  eos::mgm::traffic_shaping::RateMetrics competitor_rate{};
  competitor_rate.write_rate_bps = 500 * mb;
  manager.SetNodeEntityRateForTest(node, {"best-effort-app", 2, 2, 0}, competitor_rate);

  manager.UpdateTrafficShapingController({{node, 0.5}});
  manager.UpdateTrafficShapingController({{node, 0.5}});

  const auto runtimes = manager.GetNodeReservationControllerRuntimes();
  const auto node_it = runtimes.find(node);
  ASSERT_NE(runtimes.end(), node_it);
  const auto app_it = node_it->second.app_limits.find("best-effort-app");
  ASSERT_NE(node_it->second.app_limits.end(), app_it);
  EXPECT_EQ(300 * mb, app_it->second.write_bps);
}

TEST(TrafficShapingManager, BuiltInControllerSupportsLongAppNames)
{
  constexpr uint64_t mb = 1000ULL * 1000ULL;
  const std::string competitor_app(200, 'x');
  const std::string node = "/eos/pressured.example:1095/fst";
  eos::mgm::traffic_shaping::TrafficShapingManager manager;
  manager.ApplyThreadConfig(1000, 1000, 1000, 15);

  eos::mgm::traffic_shaping::TrafficShapingPolicy policy;
  policy.reservation_write_bytes_per_sec = 1000 * mb;
  manager.SetAppPolicy("reserved-app", policy);

  auto make_report = [&](const int64_t timestamp_ms, const uint64_t reserved_bytes,
                         const uint64_t competitor_bytes) {
    eos::traffic_shaping::FstIoReport report;
    report.set_node_id(node);
    report.set_timestamp_ms(timestamp_ms);
    auto* reserved = report.add_entries();
    reserved->set_app_name("reserved-app");
    reserved->set_uid(1);
    reserved->set_gid(1);
    reserved->set_generation_id(1);
    reserved->set_total_bytes_written(reserved_bytes);
    auto* competitor = report.add_entries();
    competitor->set_app_name(competitor_app);
    competitor->set_uid(2);
    competitor->set_gid(2);
    competitor->set_generation_id(1);
    competitor->set_total_bytes_written(competitor_bytes);
    return report;
  };

  manager.ProcessReport(make_report(1000, 0, 0));
  manager.ProcessReport(make_report(2000, 200 * mb, 500 * mb));
  manager.UpdateEstimators(1.0);
  const std::unordered_map<std::string, double> pressure{{node, 0.5}};
  manager.UpdateTrafficShapingController(pressure);
  manager.UpdateTrafficShapingController(pressure);

  const auto runtimes = manager.GetNodeReservationControllerRuntimes();
  ASSERT_NE(runtimes.end(), runtimes.find(node));
  ASSERT_NE(runtimes.at(node).app_limits.end(),
            runtimes.at(node).app_limits.find(competitor_app));
  EXPECT_EQ(300 * mb, runtimes.at(node).app_limits.at(competitor_app).write_bps);
}

TEST(TrafficShapingManager, InfeasibleReservationDoesNotThrottleCompetitors)
{
  constexpr uint64_t mb = 1000ULL * 1000ULL;
  const std::string node = "/eos/pressured.example:1095/fst";
  eos::mgm::traffic_shaping::TrafficShapingManager manager;
  manager.ApplyThreadConfig(1000, 1000, 1000, 15);

  eos::mgm::traffic_shaping::TrafficShapingPolicy policy;
  policy.limit_write_bytes_per_sec = 200 * mb;
  policy.reservation_write_bytes_per_sec = 1000 * mb;
  manager.SetAppPolicy("reserved-app", policy);

  auto make_report = [&](const int64_t timestamp_ms, const uint64_t reserved_bytes,
                         const uint64_t competitor_bytes) {
    eos::traffic_shaping::FstIoReport report;
    report.set_node_id(node);
    report.set_timestamp_ms(timestamp_ms);
    for (const auto& [app, uid, bytes] :
         std::vector<std::tuple<std::string, uint32_t, uint64_t>>{
             {"reserved-app", 1, reserved_bytes},
             {"best-effort-app", 2, competitor_bytes}}) {
      auto* entry = report.add_entries();
      entry->set_app_name(app);
      entry->set_uid(uid);
      entry->set_gid(uid);
      entry->set_generation_id(1);
      entry->set_total_bytes_written(bytes);
    }
    return report;
  };

  manager.ProcessReport(make_report(1000, 0, 0));
  manager.ProcessReport(make_report(2000, 200 * mb, 500 * mb));
  manager.UpdateEstimators(1.0);
  const std::unordered_map<std::string, double> pressure{{node, 0.5}};
  manager.UpdateTrafficShapingController(pressure);
  manager.UpdateTrafficShapingController(pressure);

  EXPECT_TRUE(manager.GetNodeReservationControllerRuntimes().empty());
}

TEST(TrafficShapingManager, ReservationDeficitIsConservedAcrossPressuredNodes)
{
  constexpr uint64_t mb = 1000ULL * 1000ULL;
  eos::mgm::traffic_shaping::TrafficShapingManager manager;
  manager.ApplyThreadConfig(1000, 1000, 1000, 15);

  eos::mgm::traffic_shaping::TrafficShapingPolicy policy;
  policy.reservation_write_bytes_per_sec = 1000 * mb;
  manager.SetAppPolicy("reserved-app", policy);

  auto make_report = [](const std::string& node, const int64_t timestamp_ms,
                        const uint64_t reserved_bytes, const uint64_t competitor_bytes) {
    eos::traffic_shaping::FstIoReport report;
    report.set_node_id(node);
    report.set_timestamp_ms(timestamp_ms);
    auto* reserved = report.add_entries();
    reserved->set_app_name("reserved-app");
    reserved->set_uid(1);
    reserved->set_gid(1);
    reserved->set_generation_id(1);
    reserved->set_total_bytes_written(reserved_bytes);
    auto* competitor = report.add_entries();
    competitor->set_app_name("best-effort-app");
    competitor->set_uid(2);
    competitor->set_gid(2);
    competitor->set_generation_id(1);
    competitor->set_total_bytes_written(competitor_bytes);
    return report;
  };

  const std::string node_a = "/eos/node-a.example:1095/fst";
  const std::string node_b = "/eos/node-b.example:1095/fst";
  manager.ProcessReport(make_report(node_a, 1000, 0, 0));
  manager.ProcessReport(make_report(node_b, 1000, 0, 0));
  manager.ProcessReport(make_report(node_a, 2000, 200 * mb, 1000 * mb));
  manager.ProcessReport(make_report(node_b, 2000, 300 * mb, 1000 * mb));
  manager.UpdateEstimators(1.0);

  const std::unordered_map<std::string, double> pressure{{node_a, 0.5}, {node_b, 0.5}};
  manager.UpdateTrafficShapingController(pressure);
  manager.UpdateTrafficShapingController(pressure);

  const auto runtimes = manager.GetNodeReservationControllerRuntimes();
  ASSERT_EQ(2u, runtimes.size());
  EXPECT_EQ(800 * mb, runtimes.at(node_a).app_limits.at("best-effort-app").write_bps);
  EXPECT_EQ(700 * mb, runtimes.at(node_b).app_limits.at("best-effort-app").write_bps);
}

TEST(TrafficShapingManager, ReservationDeficitsAreComputedPerApp)
{
  constexpr double gb = 1000.0 * 1000.0 * 1000.0;

  eos::mgm::traffic_shaping::AppState starved{};
  starved.reservation_write_bps = static_cast<uint64_t>(gb);
  starved.current_write_bps = 0.2 * gb;
  starved.has_write_io_pressure = true;
  starved.current_write_io_pressure = 0.5;
  starved.has_write_reservation_competition = true;

  eos::mgm::traffic_shaping::AppState above_reservation{};
  above_reservation.reservation_write_bps = static_cast<uint64_t>(gb);
  above_reservation.current_write_bps = 1.8 * gb;
  above_reservation.has_write_io_pressure = true;
  above_reservation.current_write_io_pressure = 0.5;
  above_reservation.has_write_reservation_competition = true;

  eos::mgm::traffic_shaping::AppState best_effort{};
  best_effort.current_write_bps = gb;
  best_effort.has_write_reservation_competition = true;

  std::vector<eos::mgm::traffic_shaping::AppState> apps{starved, above_reservation,
                                                        best_effort};
  eos::mgm::traffic_shaping::TrafficShapingManager::ApplyDefaultReservationController(
      apps, true, 0);

  ASSERT_FALSE(apps[0].update_write);
  ASSERT_TRUE(apps[1].update_write);
  ASSERT_TRUE(apps[2].update_write);
  EXPECT_GE(apps[1].new_controller_limit_write_bps,
            above_reservation.reservation_write_bps);

  const double emitted_budget =
      static_cast<double>(apps[1].new_controller_limit_write_bps) +
      static_cast<double>(apps[2].new_controller_limit_write_bps);
  // The app above its reservation cannot hide the starved app's 800 MB/s
  // deficit; that exact amount is reclaimed fairly from available headroom.
  EXPECT_NEAR(2.0 * gb, emitted_budget, 2.0);
}

TEST(TrafficShapingManager, ReservationControllerDoesNotRepeatedlyCollapseLimit)
{
  constexpr uint64_t reservation_bps = 1000ULL * 1000ULL * 1000ULL;

  eos::mgm::traffic_shaping::AppState reserved{};
  reserved.reservation_write_bps = reservation_bps;
  reserved.current_write_bps = 700.0 * 1000.0 * 1000.0;
  reserved.has_write_io_pressure = true;
  reserved.current_write_io_pressure = 0.5;

  eos::mgm::traffic_shaping::AppState competitor{};
  competitor.current_write_bps = 700.0 * 1000.0 * 1000.0;
  competitor.has_write_reservation_competition = true;

  std::vector<eos::mgm::traffic_shaping::AppState> apps{reserved, competitor};
  eos::mgm::traffic_shaping::ReservationControllerState state;
  const auto start = std::chrono::steady_clock::time_point{std::chrono::seconds(1)};

  eos::mgm::traffic_shaping::TrafficShapingManager::ApplyDefaultReservationController(
      apps, true, eos::mgm::traffic_shaping::kDefaultControllerMinLimitBps,
      eos::mgm::traffic_shaping::kDefaultIoPressureThreshold,
      eos::mgm::traffic_shaping::kDefaultActiveNodeRateThresholdBps, &state, start);
  ASSERT_FALSE(apps[1].update_write);

  eos::mgm::traffic_shaping::TrafficShapingManager::ApplyDefaultReservationController(
      apps, true, eos::mgm::traffic_shaping::kDefaultControllerMinLimitBps,
      eos::mgm::traffic_shaping::kDefaultIoPressureThreshold,
      eos::mgm::traffic_shaping::kDefaultActiveNodeRateThresholdBps, &state,
      start + std::chrono::milliseconds(500));
  ASSERT_TRUE(apps[1].update_write);
  const uint64_t initial_limit = apps[1].new_controller_limit_write_bps;
  ASSERT_EQ(400ULL * 1000ULL * 1000ULL, initial_limit);

  apps[1].controller_limit_write_bps = initial_limit;
  apps[1].update_write = false;
  eos::mgm::traffic_shaping::TrafficShapingManager::ApplyDefaultReservationController(
      apps, true, eos::mgm::traffic_shaping::kDefaultControllerMinLimitBps,
      eos::mgm::traffic_shaping::kDefaultIoPressureThreshold,
      eos::mgm::traffic_shaping::kDefaultActiveNodeRateThresholdBps, &state,
      start + std::chrono::seconds(2));

  ASSERT_TRUE(apps[1].update_write);
  ASSERT_EQ(initial_limit, apps[1].new_controller_limit_write_bps);
}

TEST(TrafficShapingManager, IneffectiveReservationProbesBackOff)
{
  constexpr uint64_t reservation_bps = 1000ULL * 1000ULL * 1000ULL;

  eos::mgm::traffic_shaping::AppState reserved{};
  reserved.reservation_write_bps = reservation_bps;
  reserved.current_write_bps = 700.0 * 1000.0 * 1000.0;
  reserved.has_write_io_pressure = true;
  reserved.current_write_io_pressure = 0.5;

  eos::mgm::traffic_shaping::AppState competitor{};
  competitor.current_write_bps = 700.0 * 1000.0 * 1000.0;
  competitor.has_write_reservation_competition = true;

  std::vector<eos::mgm::traffic_shaping::AppState> apps{reserved, competitor};
  eos::mgm::traffic_shaping::ReservationControllerState state;
  const auto start = std::chrono::steady_clock::time_point{std::chrono::seconds(1)};

  eos::mgm::traffic_shaping::TrafficShapingManager::ApplyDefaultReservationController(
      apps, true, eos::mgm::traffic_shaping::kDefaultControllerMinLimitBps,
      eos::mgm::traffic_shaping::kDefaultIoPressureThreshold,
      eos::mgm::traffic_shaping::kDefaultActiveNodeRateThresholdBps, &state, start);
  eos::mgm::traffic_shaping::TrafficShapingManager::ApplyDefaultReservationController(
      apps, true, eos::mgm::traffic_shaping::kDefaultControllerMinLimitBps,
      eos::mgm::traffic_shaping::kDefaultIoPressureThreshold,
      eos::mgm::traffic_shaping::kDefaultActiveNodeRateThresholdBps, &state,
      start + std::chrono::milliseconds(500));
  ASSERT_TRUE(apps[1].update_write);

  apps[1].controller_limit_write_bps = apps[1].new_controller_limit_write_bps;
  apps[1].update_write = false;
  eos::mgm::traffic_shaping::TrafficShapingManager::ApplyDefaultReservationController(
      apps, true, eos::mgm::traffic_shaping::kDefaultControllerMinLimitBps,
      eos::mgm::traffic_shaping::kDefaultIoPressureThreshold,
      eos::mgm::traffic_shaping::kDefaultActiveNodeRateThresholdBps, &state,
      start + std::chrono::seconds(11));

  ASSERT_TRUE(apps[1].update_write);
  ASSERT_EQ(0u, apps[1].new_controller_limit_write_bps);
  ASSERT_EQ(1u, state.write.ineffective_probe_count);

  apps[1].controller_limit_write_bps = 0;
  apps[1].update_write = false;
  eos::mgm::traffic_shaping::TrafficShapingManager::ApplyDefaultReservationController(
      apps, true, eos::mgm::traffic_shaping::kDefaultControllerMinLimitBps,
      eos::mgm::traffic_shaping::kDefaultIoPressureThreshold,
      eos::mgm::traffic_shaping::kDefaultActiveNodeRateThresholdBps, &state,
      start + std::chrono::seconds(20));
  ASSERT_FALSE(apps[1].update_write);

  // After the first backoff, probe at half amplitude instead of repeating the
  // original 300 MB/s cut.
  eos::mgm::traffic_shaping::TrafficShapingManager::ApplyDefaultReservationController(
      apps, true, eos::mgm::traffic_shaping::kDefaultControllerMinLimitBps,
      eos::mgm::traffic_shaping::kDefaultIoPressureThreshold,
      eos::mgm::traffic_shaping::kDefaultActiveNodeRateThresholdBps, &state,
      start + std::chrono::seconds(27));
  ASSERT_FALSE(apps[1].update_write);
  eos::mgm::traffic_shaping::TrafficShapingManager::ApplyDefaultReservationController(
      apps, true, eos::mgm::traffic_shaping::kDefaultControllerMinLimitBps,
      eos::mgm::traffic_shaping::kDefaultIoPressureThreshold,
      eos::mgm::traffic_shaping::kDefaultActiveNodeRateThresholdBps, &state,
      start + std::chrono::milliseconds(27500));
  ASSERT_TRUE(apps[1].update_write);
  ASSERT_EQ(550ULL * 1000ULL * 1000ULL, apps[1].new_controller_limit_write_bps);

  apps[1].controller_limit_write_bps = apps[1].new_controller_limit_write_bps;
  apps[1].update_write = false;
  eos::mgm::traffic_shaping::TrafficShapingManager::ApplyDefaultReservationController(
      apps, true, eos::mgm::traffic_shaping::kDefaultControllerMinLimitBps,
      eos::mgm::traffic_shaping::kDefaultIoPressureThreshold,
      eos::mgm::traffic_shaping::kDefaultActiveNodeRateThresholdBps, &state,
      start + std::chrono::seconds(38));
  ASSERT_TRUE(apps[1].update_write);
  ASSERT_EQ(0u, apps[1].new_controller_limit_write_bps);
  ASSERT_EQ(2u, state.write.ineffective_probe_count);

  apps[1].controller_limit_write_bps = 0;
  apps[1].update_write = false;
  eos::mgm::traffic_shaping::TrafficShapingManager::ApplyDefaultReservationController(
      apps, true, eos::mgm::traffic_shaping::kDefaultControllerMinLimitBps,
      eos::mgm::traffic_shaping::kDefaultIoPressureThreshold,
      eos::mgm::traffic_shaping::kDefaultActiveNodeRateThresholdBps, &state,
      start + std::chrono::seconds(60));
  ASSERT_FALSE(apps[1].update_write);
}

TEST(TrafficShapingManager, SmallReservationRetryCanDemonstrateRecovery)
{
  constexpr double mb = 1000.0 * 1000.0;

  eos::mgm::traffic_shaping::AppState reserved{};
  reserved.reservation_write_bps = 1000ULL * 1000ULL * 1000ULL;
  reserved.current_write_bps = 994.0 * mb;
  reserved.has_write_io_pressure = true;
  reserved.current_write_io_pressure = 0.5;

  eos::mgm::traffic_shaping::AppState competitor{};
  competitor.current_write_bps = 100.0 * mb;
  competitor.has_write_reservation_competition = true;

  std::vector<eos::mgm::traffic_shaping::AppState> apps{reserved, competitor};
  eos::mgm::traffic_shaping::ReservationControllerState state;
  state.write.ineffective_probe_count = 1;
  const auto start = std::chrono::steady_clock::time_point{std::chrono::seconds(1)};

  eos::mgm::traffic_shaping::TrafficShapingManager::ApplyDefaultReservationController(
      apps, true, 0, eos::mgm::traffic_shaping::kDefaultIoPressureThreshold,
      eos::mgm::traffic_shaping::kDefaultActiveNodeRateThresholdBps, &state, start,
      nullptr, nullptr, true);
  eos::mgm::traffic_shaping::TrafficShapingManager::ApplyDefaultReservationController(
      apps, true, 0, eos::mgm::traffic_shaping::kDefaultIoPressureThreshold,
      eos::mgm::traffic_shaping::kDefaultActiveNodeRateThresholdBps, &state,
      start + std::chrono::milliseconds(500), nullptr, nullptr, true);
  ASSERT_TRUE(apps[1].update_write);
  ASSERT_EQ(97ULL * 1000ULL * 1000ULL, apps[1].new_controller_limit_write_bps);

  apps[0].current_write_bps = 994.5 * mb;
  apps[1].controller_limit_write_bps = apps[1].new_controller_limit_write_bps;
  apps[1].update_write = false;
  eos::mgm::traffic_shaping::TrafficShapingManager::ApplyDefaultReservationController(
      apps, true, 0, eos::mgm::traffic_shaping::kDefaultIoPressureThreshold,
      eos::mgm::traffic_shaping::kDefaultActiveNodeRateThresholdBps, &state,
      start + std::chrono::seconds(11), nullptr, nullptr, true);

  // A 0.5 MB/s response is more than 10% of the 3 MB/s retry. It must be
  // accepted even though it is below the former fixed 1 MiB/s response floor.
  ASSERT_TRUE(apps[1].update_write);
  EXPECT_GT(apps[1].new_controller_limit_write_bps, 0u);
  EXPECT_EQ(0u, state.write.ineffective_probe_count);
  EXPECT_EQ(std::chrono::steady_clock::time_point{}, state.write.suppressed_until);
}

TEST(TrafficShapingManager, ReservationResponseMustBenefitEveryProtectedApp)
{
  constexpr uint64_t mb = 1000ULL * 1000ULL;

  eos::mgm::traffic_shaping::AppState daq{};
  daq.app_name = "tsb-daq";
  daq.reservation_write_bps = 1000 * mb;
  daq.current_write_bps = 700.0 * static_cast<double>(mb);
  daq.has_write_io_pressure = true;
  daq.current_write_io_pressure = 0.5;

  eos::mgm::traffic_shaping::AppState online{};
  online.app_name = "tsb-online";
  online.reservation_write_bps = 1000 * mb;
  online.current_write_bps = 700.0 * static_cast<double>(mb);
  online.has_write_io_pressure = true;
  online.current_write_io_pressure = 0.5;

  eos::mgm::traffic_shaping::AppState competitor{};
  competitor.app_name = "tsb-batch";
  competitor.current_write_bps = 1200.0 * static_cast<double>(mb);
  competitor.has_write_reservation_competition = true;

  std::vector<eos::mgm::traffic_shaping::AppState> apps{daq, online, competitor};
  eos::mgm::traffic_shaping::ReservationControllerState state;
  const auto start = std::chrono::steady_clock::time_point{std::chrono::seconds(1)};
  auto update = [&](const auto now) {
    eos::mgm::traffic_shaping::TrafficShapingManager::ApplyDefaultReservationController(
        apps, true, eos::mgm::traffic_shaping::kDefaultControllerMinLimitBps,
        eos::mgm::traffic_shaping::kDefaultIoPressureThreshold,
        eos::mgm::traffic_shaping::kDefaultActiveNodeRateThresholdBps, &state, now,
        nullptr, nullptr, false);
  };

  update(start);
  update(start + std::chrono::milliseconds(500));
  ASSERT_TRUE(apps[2].update_write);
  ASSERT_EQ(650 * mb, apps[2].new_controller_limit_write_bps);
  ASSERT_EQ(2u, state.write.protected_apps.size());

  apps[0].current_write_bps = 760.0 * static_cast<double>(mb);
  apps[2].controller_limit_write_bps = apps[2].new_controller_limit_write_bps;
  apps[2].update_write = false;
  update(start + std::chrono::seconds(11));

  // The aggregate gain exceeds 10% of the cut, but only DAQ benefited.
  // Keeping the limit would needlessly throttle the competitor while the
  // second reservation remains flat.
  ASSERT_TRUE(apps[2].update_write);
  EXPECT_EQ(0u, apps[2].new_controller_limit_write_bps);
  EXPECT_EQ(1u, state.write.ineffective_probe_count);
  EXPECT_DOUBLE_EQ(60.0 * static_cast<double>(mb),
                   state.write.last_observed_protected_gain_bps);
  EXPECT_DOUBLE_EQ(0.0, state.write.last_response_ratio);
  ASSERT_EQ(1u, state.write.failed_protected_apps.size());
  EXPECT_TRUE(state.write.failed_protected_apps.count("tsb-online"));

  const auto suppressed_until = state.write.suppressed_until;
  apps[0].current_write_bps = 840.0 * static_cast<double>(mb);
  apps[2].controller_limit_write_bps = 0;
  apps[2].update_write = false;
  update(start + std::chrono::seconds(12));

  // Further DAQ improvement cannot cancel the backoff caused by the flat
  // online transfer. Only evidence from the actual non-responder can retry.
  EXPECT_FALSE(apps[2].update_write);
  EXPECT_EQ(suppressed_until, state.write.suppressed_until);
  EXPECT_TRUE(state.write.failed_protected_apps.count("tsb-online"));

  apps[1].current_write_bps = 780.0 * static_cast<double>(mb);
  update(start + std::chrono::seconds(13));
  EXPECT_EQ(std::chrono::steady_clock::time_point{}, state.write.suppressed_until);
  EXPECT_TRUE(state.write.failed_protected_apps.empty());
}

TEST(TrafficShapingManager, NewReservationDoesNotCancelProbeSuppression)
{
  constexpr uint64_t mb = 1000ULL * 1000ULL;

  eos::mgm::traffic_shaping::AppState daq{};
  daq.app_name = "tsb-daq";
  daq.reservation_write_bps = 1000 * mb;
  daq.current_write_bps = 700.0 * static_cast<double>(mb);
  daq.has_write_io_pressure = true;
  daq.current_write_io_pressure = 0.5;

  eos::mgm::traffic_shaping::AppState competitor{};
  competitor.app_name = "tsb-batch";
  competitor.current_write_bps = 700.0 * static_cast<double>(mb);
  competitor.has_write_reservation_competition = true;

  std::vector<eos::mgm::traffic_shaping::AppState> apps{daq, competitor};
  eos::mgm::traffic_shaping::ReservationControllerState state;
  const auto start = std::chrono::steady_clock::time_point{std::chrono::seconds(1)};
  auto update = [&](const auto now) {
    eos::mgm::traffic_shaping::TrafficShapingManager::ApplyDefaultReservationController(
        apps, true, eos::mgm::traffic_shaping::kDefaultControllerMinLimitBps,
        eos::mgm::traffic_shaping::kDefaultIoPressureThreshold,
        eos::mgm::traffic_shaping::kDefaultActiveNodeRateThresholdBps, &state, now,
        nullptr, nullptr, false);
  };

  update(start);
  update(start + std::chrono::milliseconds(500));
  ASSERT_EQ(400 * mb, apps[1].new_controller_limit_write_bps);

  apps[1].controller_limit_write_bps = apps[1].new_controller_limit_write_bps;
  apps[1].update_write = false;
  update(start + std::chrono::seconds(11));
  ASSERT_EQ(1u, state.write.ineffective_probe_count);
  ASSERT_TRUE(state.write.failed_protected_apps.count("tsb-daq"));
  const auto suppressed_until = state.write.suppressed_until;

  eos::mgm::traffic_shaping::AppState newcomer{};
  newcomer.app_name = "tsb-new-reservation";
  newcomer.reservation_write_bps = 1000 * mb;
  newcomer.current_write_bps = 1000.0 * static_cast<double>(mb);
  newcomer.has_write_io_pressure = true;
  newcomer.current_write_io_pressure = 0.5;
  competitor.controller_limit_write_bps = 0;
  competitor.update_write = false;
  apps = {daq, newcomer, competitor};
  update(start + std::chrono::seconds(12));

  // The new healthy reservation increases aggregate protected throughput, but
  // it is not evidence that the failed DAQ cohort can consume reclaimed IO.
  EXPECT_FALSE(apps[2].update_write);
  EXPECT_EQ(1u, state.write.ineffective_probe_count);
  EXPECT_EQ(suppressed_until, state.write.suppressed_until);
  EXPECT_TRUE(state.write.failed_protected_apps.count("tsb-daq"));
}

TEST(TrafficShapingManager, ReservationRetuneGainTracksMeasuredBenefit)
{
  constexpr uint64_t mb = 1000ULL * 1000ULL;

  auto retuned_limit = [&](const double protected_rate_mb, double* response_ratio) {
    eos::mgm::traffic_shaping::AppState reserved{};
    reserved.app_name = "tsb-daq";
    reserved.reservation_write_bps = 1000 * mb;
    reserved.current_write_bps = 700.0 * static_cast<double>(mb);
    reserved.has_write_io_pressure = true;
    reserved.current_write_io_pressure = 0.5;

    eos::mgm::traffic_shaping::AppState competitor{};
    competitor.app_name = "tsb-batch";
    competitor.current_write_bps = 700.0 * static_cast<double>(mb);
    competitor.has_write_reservation_competition = true;

    std::vector<eos::mgm::traffic_shaping::AppState> apps{reserved, competitor};
    eos::mgm::traffic_shaping::ReservationControllerState state;
    const auto start = std::chrono::steady_clock::time_point{std::chrono::seconds(1)};
    auto update = [&](const auto now) {
      eos::mgm::traffic_shaping::TrafficShapingManager::ApplyDefaultReservationController(
          apps, true, eos::mgm::traffic_shaping::kDefaultControllerMinLimitBps,
          eos::mgm::traffic_shaping::kDefaultIoPressureThreshold,
          eos::mgm::traffic_shaping::kDefaultActiveNodeRateThresholdBps, &state, now,
          nullptr, nullptr, false);
    };

    update(start);
    update(start + std::chrono::milliseconds(500));
    EXPECT_EQ(400 * mb, apps[1].new_controller_limit_write_bps);
    apps[0].current_write_bps = protected_rate_mb * static_cast<double>(mb);
    apps[1].controller_limit_write_bps = apps[1].new_controller_limit_write_bps;
    apps[1].update_write = false;
    update(start + std::chrono::seconds(11));
    *response_ratio = state.write.last_response_ratio;
    return apps[1].new_controller_limit_write_bps;
  };

  double marginal_ratio = 0.0;
  const uint64_t marginal_limit = retuned_limit(730.0, &marginal_ratio);
  EXPECT_NEAR(0.10, marginal_ratio, 1e-12);
  EXPECT_EQ(332500000u, marginal_limit);

  double strong_ratio = 0.0;
  const uint64_t strong_limit = retuned_limit(850.0, &strong_ratio);
  EXPECT_NEAR(0.50, strong_ratio, 1e-12);
  EXPECT_EQ(250 * mb, strong_limit);
}

TEST(TrafficShapingManager, ReservationFeedbackKeepsFullAppNames)
{
  constexpr uint64_t mb = 1000ULL * 1000ULL;
  const std::string common_prefix(160, 'x');
  const std::string first_name = common_prefix + "-daq";
  const std::string second_name = common_prefix + "-online";

  eos::mgm::traffic_shaping::AppState first{};
  first.app_name = first_name;
  first.reservation_write_bps = 1000 * mb;
  first.current_write_bps = 700.0 * static_cast<double>(mb);
  first.has_write_io_pressure = true;
  first.current_write_io_pressure = 0.5;
  eos::mgm::traffic_shaping::AppState second = first;
  second.app_name = second_name;

  eos::mgm::traffic_shaping::AppState competitor{};
  competitor.app_name = "tsb-batch";
  competitor.current_write_bps = 1200.0 * static_cast<double>(mb);
  competitor.has_write_reservation_competition = true;

  std::vector<eos::mgm::traffic_shaping::AppState> apps{first, second, competitor};
  eos::mgm::traffic_shaping::ReservationControllerState state;
  const auto start = std::chrono::steady_clock::time_point{std::chrono::seconds(1)};
  auto update = [&](const auto now) {
    eos::mgm::traffic_shaping::TrafficShapingManager::ApplyDefaultReservationController(
        apps, true, eos::mgm::traffic_shaping::kDefaultControllerMinLimitBps,
        eos::mgm::traffic_shaping::kDefaultIoPressureThreshold,
        eos::mgm::traffic_shaping::kDefaultActiveNodeRateThresholdBps, &state, now,
        nullptr, nullptr, false);
  };

  update(start);
  update(start + std::chrono::milliseconds(500));
  ASSERT_TRUE(state.write.protected_apps.count(first_name));
  ASSERT_TRUE(state.write.protected_apps.count(second_name));

  apps[2].controller_limit_write_bps = apps[2].new_controller_limit_write_bps;
  apps[2].update_write = false;
  update(start + std::chrono::seconds(11));

  ASSERT_TRUE(state.write.failed_protected_apps.count(first_name));
  ASSERT_TRUE(state.write.failed_protected_apps.count(second_name));
  const auto suppressed_until = state.write.suppressed_until;

  apps[0].current_write_bps = 760.0 * static_cast<double>(mb);
  apps[2].controller_limit_write_bps = 0;
  apps[2].update_write = false;
  update(start + std::chrono::seconds(12));
  EXPECT_EQ(suppressed_until, state.write.suppressed_until);

  apps[1].current_write_bps = 760.0 * static_cast<double>(mb);
  update(start + std::chrono::seconds(13));
  EXPECT_EQ(std::chrono::steady_clock::time_point{}, state.write.suppressed_until);
  EXPECT_TRUE(state.write.failed_protected_apps.empty());
}

TEST(TrafficShapingManager, SustainedRecoveryResetsReservationProbeBackoff)
{
  constexpr uint64_t mb = 1000ULL * 1000ULL;

  eos::mgm::traffic_shaping::AppState reserved{};
  reserved.reservation_write_bps = 1000 * mb;
  reserved.current_write_bps = 1000.0 * static_cast<double>(mb);
  reserved.has_write_io_pressure = true;
  reserved.current_write_io_pressure = 0.5;

  eos::mgm::traffic_shaping::AppState competitor{};
  competitor.current_write_bps = 400.0 * static_cast<double>(mb);
  competitor.controller_limit_write_bps = 400 * mb;
  competitor.has_write_reservation_competition = true;

  std::vector<eos::mgm::traffic_shaping::AppState> apps{reserved, competitor};
  eos::mgm::traffic_shaping::ReservationControllerState state;
  const auto start = std::chrono::steady_clock::time_point{std::chrono::seconds(1)};
  state.write.ineffective_probe_count = 2;
  state.write.failed_protected_apps.emplace(
      "tsb-daq",
      eos::mgm::traffic_shaping::ReservationControllerState::FailedProtectedApp{
          1000.0 * static_cast<double>(mb), 700.0 * static_cast<double>(mb),
          300.0 * static_cast<double>(mb), 700.0 * static_cast<double>(mb)});
  state.write.suppressed_until = start + std::chrono::minutes(5);

  eos::mgm::traffic_shaping::TrafficShapingManager::ApplyDefaultReservationController(
      apps, true, eos::mgm::traffic_shaping::kDefaultControllerMinLimitBps,
      eos::mgm::traffic_shaping::kDefaultIoPressureThreshold,
      eos::mgm::traffic_shaping::kDefaultActiveNodeRateThresholdBps, &state, start);
  ASSERT_EQ(400 * mb, apps[1].new_controller_limit_write_bps);

  apps[1].update_write = false;
  eos::mgm::traffic_shaping::TrafficShapingManager::ApplyDefaultReservationController(
      apps, true, eos::mgm::traffic_shaping::kDefaultControllerMinLimitBps,
      eos::mgm::traffic_shaping::kDefaultIoPressureThreshold,
      eos::mgm::traffic_shaping::kDefaultActiveNodeRateThresholdBps, &state,
      start + std::chrono::seconds(10));
  ASSERT_EQ(480 * mb, apps[1].new_controller_limit_write_bps);
  ASSERT_EQ(0u, state.write.ineffective_probe_count);
  ASSERT_TRUE(state.write.failed_protected_apps.empty());
  ASSERT_EQ(std::chrono::steady_clock::time_point{}, state.write.suppressed_until);

  apps[0].current_write_bps = 700.0 * static_cast<double>(mb);
  apps[1].current_write_bps = 700.0 * static_cast<double>(mb);
  apps[1].controller_limit_write_bps = apps[1].new_controller_limit_write_bps;
  apps[1].update_write = false;
  eos::mgm::traffic_shaping::TrafficShapingManager::ApplyDefaultReservationController(
      apps, true, eos::mgm::traffic_shaping::kDefaultControllerMinLimitBps,
      eos::mgm::traffic_shaping::kDefaultIoPressureThreshold,
      eos::mgm::traffic_shaping::kDefaultActiveNodeRateThresholdBps, &state,
      start + std::chrono::milliseconds(10500));
  ASSERT_FALSE(apps[1].update_write);
  eos::mgm::traffic_shaping::TrafficShapingManager::ApplyDefaultReservationController(
      apps, true, eos::mgm::traffic_shaping::kDefaultControllerMinLimitBps,
      eos::mgm::traffic_shaping::kDefaultIoPressureThreshold,
      eos::mgm::traffic_shaping::kDefaultActiveNodeRateThresholdBps, &state,
      start + std::chrono::seconds(11));

  // The recovered controller must apply the normal 50% retune (150 MB/s),
  // not the quarter-strength retry inherited from the old failed probes.
  ASSERT_TRUE(apps[1].update_write);
  EXPECT_EQ(330 * mb, apps[1].new_controller_limit_write_bps);
}

TEST(TrafficShapingManager, HealthyReservationReleasesLimitAtResponseIntervals)
{
  constexpr uint64_t reservation_bps = 1000ULL * 1000ULL * 1000ULL;

  eos::mgm::traffic_shaping::AppState reserved{};
  reserved.reservation_write_bps = reservation_bps;
  reserved.current_write_bps = 700.0 * 1000.0 * 1000.0;
  reserved.has_write_io_pressure = true;
  reserved.current_write_io_pressure = 0.5;

  eos::mgm::traffic_shaping::AppState competitor{};
  competitor.current_write_bps = 700.0 * 1000.0 * 1000.0;
  competitor.has_write_reservation_competition = true;

  std::vector<eos::mgm::traffic_shaping::AppState> apps{reserved, competitor};
  eos::mgm::traffic_shaping::ReservationControllerState state;
  const auto start = std::chrono::steady_clock::time_point{std::chrono::seconds(1)};

  eos::mgm::traffic_shaping::TrafficShapingManager::ApplyDefaultReservationController(
      apps, true, eos::mgm::traffic_shaping::kDefaultControllerMinLimitBps,
      eos::mgm::traffic_shaping::kDefaultIoPressureThreshold,
      eos::mgm::traffic_shaping::kDefaultActiveNodeRateThresholdBps, &state, start);
  eos::mgm::traffic_shaping::TrafficShapingManager::ApplyDefaultReservationController(
      apps, true, eos::mgm::traffic_shaping::kDefaultControllerMinLimitBps,
      eos::mgm::traffic_shaping::kDefaultIoPressureThreshold,
      eos::mgm::traffic_shaping::kDefaultActiveNodeRateThresholdBps, &state,
      start + std::chrono::milliseconds(500));
  ASSERT_EQ(400ULL * 1000ULL * 1000ULL, apps[1].new_controller_limit_write_bps);

  reserved.current_write_bps = reservation_bps;
  competitor.current_write_bps = 400.0 * 1000.0 * 1000.0;
  competitor.controller_limit_write_bps = 400ULL * 1000ULL * 1000ULL;
  apps = {reserved, competitor};
  eos::mgm::traffic_shaping::TrafficShapingManager::ApplyDefaultReservationController(
      apps, true, eos::mgm::traffic_shaping::kDefaultControllerMinLimitBps,
      eos::mgm::traffic_shaping::kDefaultIoPressureThreshold,
      eos::mgm::traffic_shaping::kDefaultActiveNodeRateThresholdBps, &state,
      start + std::chrono::seconds(1));
  ASSERT_EQ(400ULL * 1000ULL * 1000ULL, apps[1].new_controller_limit_write_bps);

  apps[1].update_write = false;
  eos::mgm::traffic_shaping::TrafficShapingManager::ApplyDefaultReservationController(
      apps, true, eos::mgm::traffic_shaping::kDefaultControllerMinLimitBps,
      eos::mgm::traffic_shaping::kDefaultIoPressureThreshold,
      eos::mgm::traffic_shaping::kDefaultActiveNodeRateThresholdBps, &state,
      start + std::chrono::seconds(10));
  ASSERT_EQ(400ULL * 1000ULL * 1000ULL, apps[1].new_controller_limit_write_bps);

  apps[1].update_write = false;
  eos::mgm::traffic_shaping::TrafficShapingManager::ApplyDefaultReservationController(
      apps, true, eos::mgm::traffic_shaping::kDefaultControllerMinLimitBps,
      eos::mgm::traffic_shaping::kDefaultIoPressureThreshold,
      eos::mgm::traffic_shaping::kDefaultActiveNodeRateThresholdBps, &state,
      start + std::chrono::seconds(11));
  ASSERT_EQ(480ULL * 1000ULL * 1000ULL, apps[1].new_controller_limit_write_bps);
}

TEST(TrafficShapingPolicy, DisableSuspendsAllEffectiveLimits)
{
  eos::mgm::traffic_shaping::TrafficShapingPolicy policy;
  policy.limit_write_bytes_per_sec = 200ULL * 1000ULL * 1000ULL;
  policy.controller_limit_write_bytes_per_sec = 100ULL * 1000ULL * 1000ULL;
  policy.is_enabled = false;

  EXPECT_EQ(0u, policy.GetEffectiveWriteLimit());
  EXPECT_FALSE(policy.IsActive());
  EXPECT_FALSE(eos::mgm::traffic_shaping::TrafficShapingManager::ShouldEmitDelayForPolicy(
      policy, true, 500.0 * 1000.0 * 1000.0, 1.0, true, true, true));
}

TEST(TrafficShapingPolicy, DisableAllowsInfeasibleReservationToBeStopped)
{
  eos::mgm::traffic_shaping::TrafficShapingPolicy policy;
  policy.reservation_write_bytes_per_sec = 200ULL * 1000ULL * 1000ULL;
  policy.controller_limit_write_bytes_per_sec = 100ULL * 1000ULL * 1000ULL;

  ASSERT_FALSE(policy.IsReservationConfigurationFeasible());
  policy.is_enabled = false;
  EXPECT_TRUE(policy.IsReservationConfigurationFeasible());
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
  // A single update tightens at most half of available headroom, then later
  // response samples can converge toward (but never below) the configured floor.
  ASSERT_EQ(125ULL * 1000ULL * 1000ULL, apps[1].new_controller_limit_write_bps);
  ASSERT_GE(apps[1].new_controller_limit_write_bps, controller_min_limit_bps);
}

TEST(TrafficShapingManager, ReservationControllerRaisesExistingLimitToProtectedFloor)
{
  constexpr uint64_t reservation_bps = 1000ULL * 1000ULL * 1000ULL;
  constexpr uint64_t controller_min_limit_bps = 100ULL * 1000ULL * 1000ULL;

  eos::mgm::traffic_shaping::AppState reserved{};
  reserved.reservation_write_bps = reservation_bps;
  reserved.current_write_bps = 500.0 * 1000.0 * 1000.0;
  reserved.has_write_io_pressure = true;
  reserved.current_write_io_pressure = 0.5;

  eos::mgm::traffic_shaping::AppState competitor{};
  competitor.current_write_bps = 50.0 * 1000.0 * 1000.0;
  competitor.controller_limit_write_bps = 50ULL * 1000ULL * 1000ULL;
  competitor.has_write_reservation_competition = true;

  std::vector<eos::mgm::traffic_shaping::AppState> apps{reserved, competitor};
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
  ASSERT_FALSE(eos::mgm::traffic_shaping::TrafficShapingManager::ShouldEmitDelayForPolicy(
      policy, true, 500.0 * 1000.0 * 1000.0, 1.0, false, true, true));
  ASSERT_TRUE(eos::mgm::traffic_shaping::TrafficShapingManager::ShouldEmitDelayForPolicy(
      policy, true, 500.0 * 1000.0 * 1000.0, 1.0, true, true, true));
}

TEST(TrafficShapingManager, EphemeralDelayPublicationHonorsPressureThreshold)
{
  eos::mgm::traffic_shaping::TrafficShapingPolicy policy;
  policy.reservation_write_bytes_per_sec = 300ULL * 1000ULL * 1000ULL;
  policy.controller_limit_write_bytes_per_sec = 100ULL * 1000ULL * 1000ULL;

  ASSERT_FALSE(eos::mgm::traffic_shaping::TrafficShapingManager::ShouldEmitDelayForPolicy(
      policy, true, 500.0 * 1000.0 * 1000.0, 0.02, true, true, true, 0.05));
  ASSERT_TRUE(eos::mgm::traffic_shaping::TrafficShapingManager::ShouldEmitDelayForPolicy(
      policy, true, 500.0 * 1000.0 * 1000.0, 0.02, true, true, true, 0.01));
}

TEST(TrafficShapingManager, ReservedAppEphemeralLimitHonorsActiveNodeRateThreshold)
{
  eos::mgm::traffic_shaping::TrafficShapingPolicy policy;
  policy.reservation_write_bytes_per_sec = 300ULL * 1000ULL * 1000ULL;
  policy.controller_limit_write_bytes_per_sec = 100ULL * 1000ULL * 1000ULL;

  ASSERT_FALSE(eos::mgm::traffic_shaping::TrafficShapingManager::ShouldEmitDelayForPolicy(
      policy, true, 500.0 * 1000.0, 1.0, true, true, true));
  ASSERT_TRUE(eos::mgm::traffic_shaping::TrafficShapingManager::ShouldEmitDelayForPolicy(
      policy, true, 500.0 * 1000.0, 1.0, true, true, true,
      eos::mgm::traffic_shaping::kDefaultIoPressureThreshold, 100ULL * 1000ULL));
}
