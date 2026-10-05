#define IN_TEST_HARNESS
#include "mgm/shaping/TrafficShaping.hh"
#undef IN_TEST_HARNESS

#include "mgm/monitoring/TrafficShapingCollector.hh"

#include "common/shaping/Identity.hh"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <ctime>
#include <map>
#include <regex>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace eos::mgm::monitoring {
namespace {

using MetricLabels = std::map<std::string, std::string>;

MetricLabels
AllLabels(const std::string& node, const std::string& fsid)
{
  const auto uid = eos::common::traffic_shaping::UidLabel(1);
  const auto gid = eos::common::traffic_shaping::GidLabel(2);
  return {{"cluster", "test-cluster"},
          {"app", "aggregate-app"},
          {"uid_id", "1"},
          {"uid", uid},
          {"uid_name", uid},
          {"gid_id", "2"},
          {"gid", gid},
          {"gid_name", gid},
          {"groups", gid},
          {"fsid", fsid},
          {"node_id", node}};
}

struct MetricSample {
  MetricLabels labels;
  double value;
};

std::vector<MetricSample>
Samples(const std::string& text, const std::string& name)
{
  static const std::regex label_pattern(
      R"label(([a-zA-Z_][a-zA-Z0-9_]*)="((?:\\.|[^"\\])*)")label");
  std::vector<MetricSample> samples;
  std::istringstream input(text);
  std::string line;
  while (std::getline(input, line)) {
    if (line.compare(0, name.size(), name) != 0 || line.size() <= name.size() ||
        (line[name.size()] != '{' && line[name.size()] != ' ')) {
      continue;
    }
    MetricLabels labels;
    auto value_position = name.size();
    if (line[value_position] == '{') {
      const auto end = line.rfind('}');
      const auto label_text = line.substr(value_position + 1, end - value_position - 1);
      for (std::sregex_iterator it(label_text.begin(), label_text.end(), label_pattern),
           last;
           it != last; ++it) {
        const auto encoded = (*it)[2].str();
        std::string decoded;
        for (size_t i = 0; i < encoded.size(); ++i) {
          if (encoded[i] == '\\' && i + 1 < encoded.size()) {
            ++i;
            decoded += encoded[i] == 'n' ? '\n' : encoded[i];
          } else {
            decoded += encoded[i];
          }
        }
        labels.emplace((*it)[1].str(), std::move(decoded));
      }
      value_position = end + 1;
    }
    samples.push_back({std::move(labels), std::stod(line.substr(value_position))});
  }
  return samples;
}

void
ExpectSample(const std::string& text, const std::string& name, const MetricLabels& labels,
             const double value)
{
  const auto samples = Samples(text, name);
  const auto sample =
      std::find_if(samples.begin(), samples.end(),
                   [&labels](const auto& metric) { return metric.labels == labels; });
  ASSERT_NE(samples.end(), sample) << name;
  EXPECT_DOUBLE_EQ(value, sample->value) << name;
  EXPECT_EQ(
      1, std::count_if(samples.begin(), samples.end(),
                       [&labels](const auto& metric) { return metric.labels == labels; }))
      << name;
}

eos::traffic_shaping::FstIoReport
MakeReport(const std::string& node, const int64_t timestamp_ms, const uint64_t bytes,
           const bool two_filesystems)
{
  eos::traffic_shaping::FstIoReport report;
  report.set_node_id(node);
  report.set_timestamp_ms(timestamp_ms);
  for (const uint32_t fsid : {3u, 4u}) {
    if (fsid == 4 && !two_filesystems) {
      break;
    }
    auto* entry = report.add_entries();
    entry->set_app_name("aggregate-app");
    entry->set_uid(1);
    entry->set_gid(2);
    entry->set_fsid(fsid);
    entry->set_generation_id(1);
    entry->set_total_bytes_read(bytes / 2);
    entry->set_total_bytes_written(bytes);
    entry->set_total_read_ops(bytes / 2048);
    entry->set_total_write_ops(bytes / 4096);
  }
  return report;
}

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
  EXPECT_NE(out.find("uid_id=\"1001\""), std::string::npos);
  EXPECT_NE(out.find("gid_id=\"2001\""), std::string::npos);

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

TEST(TrafficShapingCollector, AggregateTrafficPreservesNodeAndClientAttribution)
{
  traffic_shaping::TrafficShapingEngine engine;
  auto manager = engine.GetManager();
  for (const auto& node :
       {"/eos/fst-a.example:1095/fst", "/eos/fst-b.example:1095/fst"}) {
    const bool two_filesystems = std::string(node).find("fst-a") != std::string::npos;
    manager->ProcessReport(MakeReport(node, 1000, 0, two_filesystems));
    manager->ProcessReport(MakeReport(node, 2000, 4096, two_filesystems));
    manager->ProcessReport(MakeReport(node, 3000, 8192, two_filesystems));
  }

  TrafficShapingCollector collector(engine, "test-cluster");
  std::string out;
  collector.Collect(out);
  ASSERT_EQ(4u, Samples(out, "eos_io_shaping_all_bytes_total").size());
  ASSERT_EQ(4u, Samples(out, "eos_io_shaping_all_operations_total").size());
  ASSERT_EQ(2u, Samples(out, "eos_io_shaping_all_counter_generation").size());
  for (const auto& [node, written] : std::map<std::string, uint64_t>{
           {"fst-a.example:1095", 16384}, {"fst-b.example:1095", 8192}}) {
    auto labels = AllLabels(node, "0");
    labels["operation"] = "write";
    ExpectSample(out, "eos_io_shaping_all_bytes_total", labels, written);
    ExpectSample(out, "eos_io_shaping_all_operations_total", labels, written / 4096);
    labels["operation"] = "read";
    ExpectSample(out, "eos_io_shaping_all_bytes_total", labels, written / 2);
    ExpectSample(out, "eos_io_shaping_all_operations_total", labels, written / 2048);
  }
  for (const auto& name :
       {"eos_io_shaping_all_entries", "eos_io_shaping_all_entries_exported"}) {
    ExpectSample(out, name, {{"cluster", "test-cluster"}}, 2);
  }
  ExpectSample(out, "eos_io_shaping_all_entries_limited", {{"cluster", "test-cluster"}},
               0);
  const auto cardinality = Samples(out, "eos_io_shaping_map_cardinality");
  ASSERT_FALSE(cardinality.empty());
  for (const auto& metric : cardinality) {
    ASSERT_EQ(1u, metric.labels.count("map"));
    EXPECT_EQ(0u, metric.labels.count("map_name"));
    const auto& map = metric.labels.at("map");
    EXPECT_NE("global_cumulative_stats", map);
    EXPECT_NE("node_cumulative_stats", map);
    EXPECT_NE("node_entity_cumulative_stats", map);
  }
  for (const auto& map : {"node_entity_stats", "projection_node_cumulative_stats"}) {
    ExpectSample(out, "eos_io_shaping_map_cardinality",
                 {{"cluster", "test-cluster"}, {"map", map}}, 2);
  }
  EXPECT_TRUE(Samples(out, "eos_io_shaping_fs_bytes_total").empty());
  EXPECT_TRUE(Samples(out, "eos_io_shaping_fs_operations_total").empty());
  EXPECT_EQ(0u, manager->GetMapCardinalityStats().detailed_cumulative_stats);
  EXPECT_EQ(2u, manager->GetMapCardinalityStats().node_entity_stats);
}

TEST(TrafficShapingCollector, CounterLifetimeDistinguishesIdleExpiryFromReset)
{
  traffic_shaping::TrafficShapingEngine engine;
  auto manager = engine.GetManager();
  const std::string node = "/eos/fst-a.example:1095/fst";
  const MetricLabels cluster = {{"cluster", "test-cluster"}};
  TrafficShapingCollector collector(engine, "test-cluster");
  auto collect = [&]() {
    std::string out;
    collector.Collect(out);
    ExpectSample(out, "eos_io_shaping_counter_snapshot_consistent", cluster, 1);
    ExpectSample(out, "eos_io_shaping_counter_epoch", cluster,
                 static_cast<double>(manager->GetCounterEpoch()));
    return out;
  };
  auto generation = [&](const std::string& out) {
    const auto samples = Samples(out, "eos_io_shaping_all_counter_generation");
    EXPECT_EQ(1u, samples.size());
    if (samples.empty()) {
      return 0.0;
    }
    EXPECT_EQ(AllLabels("fst-a.example:1095", "0"), samples[0].labels);
    EXPECT_GT(samples[0].value, 0);
    return samples[0].value;
  };
  manager->ProcessReport(MakeReport(node, 1000, 0, false));
  manager->ProcessReport(MakeReport(node, 2000, 4096, false));
  const auto first_epoch = manager->GetCounterEpoch();
  const auto first_generation = generation(collect());
  manager->ProcessReport(MakeReport(node, 3000, 8192, false));
  EXPECT_EQ(first_generation, generation(collect()));

  // Resetting an FST stream adds its next deltas to the same MGM lifetime.
  auto report = MakeReport(node, 4000, 0, false);
  report.mutable_entries(0)->set_generation_id(2);
  manager->ProcessReport(report);
  report.set_timestamp_ms(5000);
  report.mutable_entries(0)->set_total_bytes_written(512);
  manager->ProcessReport(report);
  EXPECT_EQ(first_generation, generation(collect()));
  EXPECT_EQ(first_epoch, manager->GetCounterEpoch());

  manager->SetNodeEntityLastActivityForTest(node, {"aggregate-app", 1, 2, 0},
                                            time(nullptr) - 301);
  manager->GarbageCollect(300);
  EXPECT_TRUE(Samples(collect(), "eos_io_shaping_all_counter_generation").empty());
  EXPECT_EQ(first_epoch, manager->GetCounterEpoch());
  report.set_timestamp_ms(6000);
  report.mutable_entries(0)->set_total_bytes_written(1024);
  manager->ProcessReport(report);
  const auto recreated = collect();
  EXPECT_NE(first_generation, generation(recreated));
  auto labels = AllLabels("fst-a.example:1095", "0");
  labels["operation"] = "write";
  ExpectSample(recreated, "eos_io_shaping_all_bytes_total", labels, 512);
  EXPECT_EQ(first_epoch, manager->GetCounterEpoch());

  manager->GarbageCollect(600);
  EXPECT_NE(first_epoch, manager->GetCounterEpoch());
  auto epoch = manager->GetCounterEpoch();
  manager->ClearDetailedRuntimeStats();
  EXPECT_NE(epoch, manager->GetCounterEpoch());
  epoch = manager->GetCounterEpoch();
  manager->ClearRuntimeStats();
  EXPECT_NE(epoch, manager->GetCounterEpoch());
  epoch = manager->GetCounterEpoch();
  manager->Clear();
  EXPECT_NE(epoch, manager->GetCounterEpoch());
  traffic_shaping::TrafficShapingEngine other;
  EXPECT_NE(manager->GetCounterEpoch(), other.GetManager()->GetCounterEpoch());
}

TEST(TrafficShapingCollector, FilesystemDetailDoesNotDoubleCountNodeTotals)
{
  traffic_shaping::TrafficShapingEngine engine;
  engine.SetDetailLevel(eos::common::TRAFFIC_SHAPING_DETAIL_LEVEL_FILESYSTEM);
  auto manager = engine.GetManager();
  const std::string node = "/eos/fst-a.example:1095/fst";
  manager->ProcessReport(MakeReport(node, 1000, 0, true));
  manager->ProcessReport(MakeReport(node, 2000, 4096, true));

  TrafficShapingCollector collector(engine, "test-cluster");
  std::string out;
  collector.Collect(out);
  ASSERT_EQ(4u, Samples(out, "eos_io_shaping_all_bytes_total").size());
  ASSERT_EQ(4u, Samples(out, "eos_io_shaping_all_operations_total").size());
  ASSERT_EQ(2u, Samples(out, "eos_io_shaping_all_counter_generation").size());
  for (const auto& fsid : {"3", "4"}) {
    auto labels = AllLabels("fst-a.example:1095", fsid);
    labels["operation"] = "write";
    ExpectSample(out, "eos_io_shaping_all_bytes_total", labels, 4096);
    ExpectSample(out, "eos_io_shaping_all_operations_total", labels, 1);
    labels["operation"] = "read";
    ExpectSample(out, "eos_io_shaping_all_bytes_total", labels, 2048);
    ExpectSample(out, "eos_io_shaping_all_operations_total", labels, 2);
  }
  ExpectSample(out, "eos_io_shaping_all_entries", {{"cluster", "test-cluster"}}, 2);
  ExpectSample(out, "eos_io_shaping_bytes_total",
               {{"cluster", "test-cluster"},
                {"type", "node"},
                {"id", "fst-a.example:1095"},
                {"operation", "write"}},
               8192);
  ExpectSample(out, "eos_io_shaping_operations_total",
               {{"cluster", "test-cluster"},
                {"type", "node"},
                {"id", "fst-a.example:1095"},
                {"operation", "write"}},
               2);
}

TEST(TrafficShapingCollector, PreservesRuntimeMetricLabelsAndAccounting)
{
  traffic_shaping::TrafficShapingEngine engine;
  engine.SetDetailLevel(eos::common::TRAFFIC_SHAPING_DETAIL_LEVEL_FILESYSTEM);
  auto manager = engine.GetManager();
  traffic_shaping::TrafficShapingPolicy policy;
  policy.reservation_read_bytes_per_sec = 4096;
  manager->SetAppPolicy("aggregate-app", policy);
  manager->SetUidPolicy(1, policy);
  manager->SetGidPolicy(2, policy);
  ASSERT_TRUE(manager->ApplyThreadConfig(1000, 1000, 1000, 5));
  manager->ProcessReport(MakeReport("/eos/fst-a.example:1095/fst", 1000, 0, true));
  manager->ProcessReport(MakeReport("/eos/fst-a.example:1095/fst", 2000, 4096, true));
  manager->UpdateFsViewLockMicroSec(1000, 2000);
  manager->UpdateFstReportQueueStats(3, 1234, 2);
  manager->UpdateFstReportsProcessed(4);
  manager->UpdateEstimatorsLoopMicroSec(1000);

  TrafficShapingCollector collector(engine, "test-cluster");
  std::string out;
  collector.Collect(out);
  ExpectSample(out, "eos_io_shaping_fsview_lock_duration_seconds_count",
               {{"cluster", "test-cluster"}, {"phase", "wait"}}, 1);
  ExpectSample(out, "eos_io_shaping_fsview_lock_duration_seconds_sum",
               {{"cluster", "test-cluster"}, {"phase", "wait"}}, 0.001);
  ExpectSample(out, "eos_io_shaping_fsview_lock_duration_seconds_sum",
               {{"cluster", "test-cluster"}, {"phase", "hold"}}, 0.002);
  ExpectSample(out, "eos_io_shaping_reports_processed_per_sec",
               {{"cluster", "test-cluster"}, {"stat", "mean"}}, 1);
  ExpectSample(out, "eos_io_shaping_report_queue_depth", {{"cluster", "test-cluster"}},
               3);
  ExpectSample(out, "eos_io_shaping_report_queue_estimated_bytes",
               {{"cluster", "test-cluster"}}, 1234);
  ExpectSample(out, "eos_io_shaping_reports_dropped_total", {{"cluster", "test-cluster"}},
               2);
  const auto memory = manager->GetMemoryStats();
  ExpectSample(out, "eos_io_shaping_memory_limit_bytes",
               {{"cluster", "test-cluster"}, {"component", "report_queue"}},
               memory.report_queue_limit_bytes);
  ExpectSample(out, "eos_io_shaping_estimated_memory_bytes",
               {{"cluster", "test-cluster"}}, memory.stream_state_estimated_bytes + 1234);
  ExpectSample(out, "eos_io_shaping_stream_states_rejected_total",
               {{"cluster", "test-cluster"}}, 0);
  for (const auto& map : {"app_policies", "uid_policies", "gid_policies"}) {
    ExpectSample(out, "eos_io_shaping_map_cardinality",
                 {{"cluster", "test-cluster"}, {"map", map}}, 1);
  }
  for (const auto& operation : {"read", "write"}) {
    ExpectSample(
        out, "eos_io_shaping_app_io_pressure_sample",
        {{"cluster", "test-cluster"}, {"app", "aggregate-app"}, {"operation", operation}},
        0);
  }
  EXPECT_TRUE(Samples(out, "eos_io_shaping_app_io_pressure").empty());

  manager->GarbageCollect(-1);
  out.clear();
  collector.Collect(out);
  for (const auto& [map, count] :
       std::map<std::string, uint64_t>{{"nodes", 1},
                                       {"node_streams", 2},
                                       {"global_streams", 2},
                                       {"disk_stats", 2},
                                       {"detailed_stats", 2}}) {
    ExpectSample(out, "eos_io_shaping_garbage_collection_removed_entries_total",
                 {{"cluster", "test-cluster"}, {"map", map}}, count);
  }
  ExpectSample(out, "eos_io_shaping_map_cardinality",
               {{"cluster", "test-cluster"}, {"map", "node_entity_stats"}}, 0);
  EXPECT_TRUE(Samples(out, "eos_io_shaping_all_bytes_total").empty());
}

TEST(TrafficShapingCollector, ExposesStreamAdmissionRejections)
{
  traffic_shaping::TrafficShapingEngine engine;
  auto manager = engine.GetManager();
  eos::traffic_shaping::FstIoReport report;
  report.set_node_id("/eos/fst-a.example:1095/fst");
  report.set_timestamp_ms(1000);
  for (uint32_t uid = 0; uid < 8192; ++uid) {
    auto* entry = report.add_entries();
    entry->set_app_name("admission-app");
    entry->set_uid(uid);
    entry->set_generation_id(1);
  }
  manager->ProcessReport(report);
  report.clear_entries();
  report.set_timestamp_ms(2000);
  auto* entry = report.add_entries();
  entry->set_app_name("admission-app");
  entry->set_uid(8192);
  entry->set_generation_id(1);
  manager->ProcessReport(report);

  TrafficShapingCollector collector(engine, "test-cluster");
  std::string out;
  collector.Collect(out);
  ExpectSample(out, "eos_io_shaping_stream_states_rejected_total",
               {{"cluster", "test-cluster"}}, 1);
  ExpectSample(out, "eos_io_shaping_map_cardinality",
               {{"cluster", "test-cluster"}, {"map", "node_state_streams"}}, 8192);
  ExpectSample(out, "eos_io_shaping_all_entries", {{"cluster", "test-cluster"}}, 0);
}

TEST(TrafficShapingCollector, LimitsAllTagsWithoutSuppressingProjections)
{
  traffic_shaping::TrafficShapingEngine engine;
  auto manager = engine.GetManager();
  const std::string node = "/eos/fst-a.example:1095/fst";
  manager->ProcessReport(MakeReport(node, 1000, 0, false));
  manager->ProcessReport(MakeReport(node, 2000, 4096, false));
  // Exercise the export bound without allocating histories for 50,000 FST streams.
  for (uint32_t app = 1; app <= 50000; ++app) {
    manager->SetNodeEntityRateForTest(node, {"cap-app-" + std::to_string(app), 1, 2, 0},
                                      {});
  }

  TrafficShapingCollector collector(engine, "test-cluster");
  std::string out;
  collector.Collect(out);
  ExpectSample(out, "eos_io_shaping_all_entries", {{"cluster", "test-cluster"}}, 50001);
  ExpectSample(out, "eos_io_shaping_all_entries_exported", {{"cluster", "test-cluster"}},
               0);
  ExpectSample(out, "eos_io_shaping_all_entries_limited", {{"cluster", "test-cluster"}},
               1);
  EXPECT_TRUE(Samples(out, "eos_io_shaping_all_bytes_total").empty());
  EXPECT_TRUE(Samples(out, "eos_io_shaping_all_operations_total").empty());
  EXPECT_TRUE(Samples(out, "eos_io_shaping_all_counter_generation").empty());
  ExpectSample(out, "eos_io_shaping_bytes_total",
               {{"cluster", "test-cluster"},
                {"id", "aggregate-app"},
                {"operation", "write"},
                {"type", "app"}},
               4096);
  ExpectSample(out, "eos_io_shaping_bytes_total",
               {{"cluster", "test-cluster"},
                {"type", "node"},
                {"id", "fst-a.example:1095"},
                {"operation", "write"}},
               4096);

  manager->SetNodeEntityLastActivityForTest(node, {"cap-app-50000", 1, 2, 0},
                                            time(nullptr) - 3600);
  manager->GarbageCollect(3000);
  out.clear();
  collector.Collect(out);
  ExpectSample(out, "eos_io_shaping_all_entries", {{"cluster", "test-cluster"}}, 50000);
  ExpectSample(out, "eos_io_shaping_all_entries_exported", {{"cluster", "test-cluster"}},
               50000);
  ExpectSample(out, "eos_io_shaping_all_entries_limited", {{"cluster", "test-cluster"}},
               0);
  for (const auto& name :
       {"eos_io_shaping_all_bytes_total", "eos_io_shaping_all_operations_total"}) {
    const std::string prefix = std::string("\n") + name + "{";
    size_t count = 0;
    size_t position = 0;
    while ((position = out.find(prefix, position)) != std::string::npos) {
      ++count;
      position += prefix.size();
    }
    EXPECT_EQ(100000u, count);
  }
}

} // namespace
} // namespace eos::mgm::monitoring
