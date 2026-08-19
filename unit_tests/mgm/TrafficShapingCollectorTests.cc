#include "mgm/monitoring/TrafficShapingCollector.hh"

#include "mgm/shaping/TrafficShaping.hh"

#include <gtest/gtest.h>

#include <string>

namespace eos::mgm::monitoring {
namespace {

TEST(TrafficShapingCollector, EmitsComprehensiveTrafficShapingMetrics)
{
  traffic_shaping::TrafficShapingEngine engine;
  engine.Start();
  auto manager = engine.GetManager();

  // Set policies
  traffic_shaping::TrafficShapingPolicy app_policy;
  app_policy.limit_read_bytes_per_sec = 500'000'000ULL;
  app_policy.reservation_read_bytes_per_sec = 100'000'000ULL;
  app_policy.limit_write_bytes_per_sec = 300'000'000ULL;
  app_policy.reservation_write_bytes_per_sec = 50'000'000ULL;
  manager->SetAppPolicy("root", app_policy);

  traffic_shaping::TrafficShapingPolicy uid_policy;
  uid_policy.limit_read_bytes_per_sec = 200'000'000ULL;
  manager->SetUidPolicy(1001, uid_policy);

  traffic_shaping::TrafficShapingPolicy gid_policy;
  gid_policy.limit_write_bytes_per_sec = 150'000'000ULL;
  manager->SetGidPolicy(2001, gid_policy);

  // Send FST report
  eos::traffic_shaping::FstIoReport report;
  report.set_node_id("fst1.cern.ch:1095");
  report.set_timestamp_ms(1000);
  auto* entry = report.add_entries();
  entry->set_app_name("root");
  entry->set_uid(1001);
  entry->set_gid(2001);
  entry->set_fsid(12);
  entry->set_generation_id(1);
  entry->set_total_bytes_read(10'000'000ULL);
  entry->set_total_bytes_written(5'000'000ULL);
  entry->set_total_read_ops(100);
  entry->set_total_write_ops(50);
  manager->ProcessReport(report);

  // Advance time and second report
  report.set_timestamp_ms(2000);
  entry->set_total_bytes_read(20'000'000ULL);
  entry->set_total_bytes_written(10'000'000ULL);
  entry->set_total_read_ops(200);
  entry->set_total_write_ops(100);
  manager->ProcessReport(report);

  manager->UpdateEstimators(1.0);

  TrafficShapingCollector collector(engine, "test-cluster");
  std::string out;
  collector.Collect(out);

  // 1. Config & Enablement
  EXPECT_NE(out.find("eos_io_shaping_config_enabled{cluster=\"test-cluster\"}"),
            std::string::npos);
  EXPECT_NE(out.find("eos_ns_traffic_shaping_enabled{cluster=\"test-cluster\"}"),
            std::string::npos);
  EXPECT_NE(out.find("eos_io_shaping_config_limits_enabled"), std::string::npos);
  EXPECT_NE(out.find("eos_io_shaping_config_reservations_enabled"), std::string::npos);
  EXPECT_NE(out.find("eos_io_shaping_config_estimators_update_period_milliseconds"),
            std::string::npos);
  EXPECT_NE(out.find("eos_io_shaping_config_fst_io_policy_update_period_milliseconds"),
            std::string::npos);
  EXPECT_NE(out.find("eos_io_shaping_config_fst_io_stats_reporting_period_milliseconds"),
            std::string::npos);
  EXPECT_NE(out.find("eos_io_shaping_config_garbage_collection_idle_seconds"),
            std::string::npos);
  EXPECT_NE(out.find("eos_io_shaping_config_system_stats_time_window_seconds"),
            std::string::npos);
  EXPECT_NE(out.find("eos_io_shaping_config_io_pressure_threshold"), std::string::npos);

  // 2. Aggregate & Runtime stats
  EXPECT_NE(out.find("eos_io_shaping_all_entries"), std::string::npos);
  EXPECT_NE(out.find("eos_io_shaping_all_entries_exported"), std::string::npos);
  EXPECT_NE(out.find("eos_io_shaping_all_entries_limited"), std::string::npos);

  // 3. Rate & Cumulative metrics
  EXPECT_NE(out.find("eos_io_shaping_all_bytes_total"), std::string::npos);
  EXPECT_NE(out.find("eos_io_shaping_all_operations_total"), std::string::npos);
  EXPECT_NE(out.find("eos_io_shaping_bytes_total"), std::string::npos);
  EXPECT_NE(out.find("eos_io_shaping_operations_total"), std::string::npos);
  EXPECT_NE(out.find("app=\"root\""), std::string::npos);
  EXPECT_NE(out.find("uid=\"1001\""), std::string::npos);
  EXPECT_NE(out.find("gid=\"2001\""), std::string::npos);

  // 4. Policy limits & reservations
  EXPECT_NE(out.find("eos_io_shaping_policy_bytes"), std::string::npos);
  EXPECT_NE(out.find("type=\"app\""), std::string::npos);
  EXPECT_NE(out.find("type=\"uid\""), std::string::npos);
  EXPECT_NE(out.find("type=\"gid\""), std::string::npos);
  EXPECT_NE(out.find("rule=\"limit\""), std::string::npos);
  EXPECT_NE(out.find("rule=\"reservation\""), std::string::npos);

  // 5. System, Queue, Memory & Map Cardinality
  EXPECT_NE(out.find("eos_io_shaping_loop_duration_seconds"), std::string::npos);
  EXPECT_NE(out.find("eos_io_shaping_loop_iterations_total"), std::string::npos);
  EXPECT_NE(out.find("eos_io_shaping_slow_iterations_total"), std::string::npos);
  EXPECT_NE(out.find("eos_io_shaping_fsview_lock_duration_seconds"), std::string::npos);
  EXPECT_NE(out.find("eos_io_shaping_reports_processed_per_sec"), std::string::npos);
  EXPECT_NE(out.find("eos_io_shaping_report_queue_depth"), std::string::npos);
  EXPECT_NE(out.find("eos_io_shaping_stream_state_estimated_bytes"), std::string::npos);
  EXPECT_NE(out.find("eos_io_shaping_memory_limit_bytes"), std::string::npos);
  EXPECT_NE(out.find("eos_io_shaping_map_cardinality"), std::string::npos);

  // 6. Actuator & Pressure Headers
  EXPECT_NE(out.find("eos_io_shaping_fst_actuator_active_waiters"), std::string::npos);
  EXPECT_NE(out.find("eos_io_shaping_app_io_pressure"), std::string::npos);
  EXPECT_NE(out.find("eos_io_shaping_app_node_io_pressure"), std::string::npos);

  engine.Stop();
}

} // namespace
} // namespace eos::mgm::monitoring
