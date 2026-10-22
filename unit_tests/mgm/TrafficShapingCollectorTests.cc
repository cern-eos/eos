#include "common/Constants.hh"
#include "common/shaping/Identity.hh"
#include "mgm/monitoring/PrometheusExporter.hh"
#include "prometheus/collectable.h"
#include "prometheus/metric_family.h"
#include "gtest/gtest.h"

#include <algorithm>
#include <map>
#include <string>

namespace {

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

const prometheus::MetricFamily*
FindFamily(const std::vector<prometheus::MetricFamily>& families, const std::string& name)
{
  const auto it = std::find_if(families.begin(), families.end(),
                               [&](const auto& family) { return family.name == name; });
  return it == families.end() ? nullptr : &*it;
}

std::map<std::string, std::string>
Labels(const prometheus::ClientMetric& metric)
{
  std::map<std::string, std::string> labels;
  for (const auto& label : metric.label) {
    labels[label.name] = label.value;
  }
  return labels;
}

} // namespace

TEST(TrafficShapingCollector, AggregateTrafficPreservesNodeAndClientAttribution)
{
  eos::mgm::traffic_shaping::TrafficShapingEngine engine;
  auto manager = engine.GetManager();
  for (const auto& node :
       {"/eos/fst-a.example:1095/fst", "/eos/fst-b.example:1095/fst"}) {
    const bool two_filesystems = std::string(node).find("fst-a") != std::string::npos;
    manager->ProcessReport(MakeReport(node, 1000, 0, two_filesystems));
    manager->ProcessReport(MakeReport(node, 2000, 4096, two_filesystems));
    manager->ProcessReport(MakeReport(node, 3000, 8192, two_filesystems));
  }

  const auto collector =
      eos::mgm::monitoring::CreateTrafficShapingCollector(engine, "ams");
  const auto families = collector->Collect();
  const auto* bytes = FindFamily(families, "eos_io_shaping_all_bytes_total");
  ASSERT_NE(nullptr, bytes);
  ASSERT_EQ(4u, bytes->metric.size());
  for (const auto& metric : bytes->metric) {
    const auto labels = Labels(metric);
    EXPECT_EQ("ams", labels.at("cluster"));
    EXPECT_EQ("aggregate-app", labels.at("app"));
    EXPECT_EQ("1", labels.at("uid_id"));
    EXPECT_EQ("2", labels.at("gid_id"));
    EXPECT_EQ("0", labels.at("fsid"));
    ASSERT_TRUE(labels.at("node_id") == "fst-a.example:1095" ||
                labels.at("node_id") == "fst-b.example:1095");
    const double written = labels.at("node_id") == "fst-a.example:1095" ? 16384 : 8192;
    EXPECT_DOUBLE_EQ(labels.at("operation") == "write" ? written : written / 2,
                     metric.counter.value);
  }
  const auto* operations = FindFamily(families, "eos_io_shaping_all_operations_total");
  ASSERT_NE(nullptr, operations);
  ASSERT_EQ(4u, operations->metric.size());
  for (const auto& metric : operations->metric) {
    const auto labels = Labels(metric);
    const double written = labels.at("node_id") == "fst-a.example:1095" ? 4 : 2;
    EXPECT_DOUBLE_EQ(labels.at("operation") == "write" ? written : written * 2,
                     metric.counter.value);
  }
  for (const auto& name :
       {"eos_io_shaping_all_entries", "eos_io_shaping_all_entries_exported"}) {
    const auto* family = FindFamily(families, name);
    ASSERT_NE(nullptr, family);
    ASSERT_EQ(1u, family->metric.size());
    EXPECT_DOUBLE_EQ(2, family->metric[0].gauge.value);
  }
  const auto* limited = FindFamily(families, "eos_io_shaping_all_entries_limited");
  ASSERT_NE(nullptr, limited);
  EXPECT_DOUBLE_EQ(0, limited->metric[0].gauge.value);
  const auto* cardinality = FindFamily(families, "eos_io_shaping_map_cardinality");
  ASSERT_NE(nullptr, cardinality);
  for (const auto& metric : cardinality->metric) {
    const auto map = Labels(metric).at("map");
    EXPECT_NE("global_cumulative_stats", map);
    EXPECT_NE("node_cumulative_stats", map);
    EXPECT_NE("node_entity_cumulative_stats", map);
    if (map == "node_entity_stats" || map == "projection_node_cumulative_stats") {
      EXPECT_DOUBLE_EQ(2, metric.gauge.value);
    }
  }
  const auto* fs_bytes = FindFamily(families, "eos_io_shaping_fs_bytes_total");
  ASSERT_NE(nullptr, fs_bytes);
  EXPECT_TRUE(fs_bytes->metric.empty());
  EXPECT_EQ(0u, manager->GetMapCardinalityStats().detailed_cumulative_stats);
  EXPECT_EQ(2u, manager->GetMapCardinalityStats().node_entity_stats);
}

TEST(TrafficShapingCollector, FilesystemDetailDoesNotDoubleCountNodeTotals)
{
  eos::mgm::traffic_shaping::TrafficShapingEngine engine;
  engine.SetDetailLevel(eos::common::TRAFFIC_SHAPING_DETAIL_LEVEL_FILESYSTEM);
  auto manager = engine.GetManager();
  const std::string node = "/eos/fst-a.example:1095/fst";
  manager->ProcessReport(MakeReport(node, 1000, 0, true));
  manager->ProcessReport(MakeReport(node, 2000, 4096, true));
  const auto collector =
      eos::mgm::monitoring::CreateTrafficShapingCollector(engine, "ams");
  const auto families = collector->Collect();
  const auto* bytes = FindFamily(families, "eos_io_shaping_all_bytes_total");
  ASSERT_NE(nullptr, bytes);
  ASSERT_EQ(4u, bytes->metric.size());
  for (const auto& metric : bytes->metric) {
    const auto labels = Labels(metric);
    EXPECT_EQ("fst-a.example:1095", labels.at("node_id"));
    EXPECT_TRUE(labels.at("fsid") == "3" || labels.at("fsid") == "4");
    EXPECT_DOUBLE_EQ(labels.at("operation") == "write" ? 4096 : 2048,
                     metric.counter.value);
  }
  const auto* entries = FindFamily(families, "eos_io_shaping_all_entries");
  ASSERT_NE(nullptr, entries);
  EXPECT_DOUBLE_EQ(2, entries->metric[0].gauge.value);
  EXPECT_EQ(8192u,
            manager->GetNodeEntityCumulativeStats().begin()->second.bytes_written_total);
}

TEST(TrafficShapingCollector, ResolvesLabelsOncePerScrapeAcrossMetricFamilies)
{
  eos::mgm::traffic_shaping::TrafficShapingEngine engine;
  engine.SetDetailLevel(eos::common::TRAFFIC_SHAPING_DETAIL_LEVEL_FILESYSTEM);
  auto manager = engine.GetManager();
  const std::vector<std::string> nodes{"/eos/fst-a.example:1095/fst",
                                       "/eos/fst-b.example:1095/fst"};
  for (const auto& node : nodes) {
    manager->ProcessReport(MakeReport(node, 1000, 0, true));
    manager->ProcessReport(MakeReport(node, 2000, 4096, true));
  }
  eos::mgm::traffic_shaping::TrafficShapingPolicy policy;
  policy.limit_write_bytes_per_sec = 1024;
  manager->SetUidPolicy(1, policy);
  manager->SetGidPolicy(2, policy);

  std::map<uint32_t, size_t> uid_calls;
  std::map<uint32_t, size_t> gid_calls;
  std::map<std::string, size_t> node_calls;
  size_t revision = 0;
  eos::mgm::monitoring::TrafficShapingLabelResolvers resolvers;
  resolvers.uid = [&](const uint32_t uid) {
    ++uid_calls[uid];
    return std::to_string(uid) + "(user-" + std::to_string(revision) + ")";
  };
  resolvers.gid = [&](const uint32_t gid) {
    ++gid_calls[gid];
    return std::to_string(gid) + "(group-" + std::to_string(revision) + ")";
  };
  resolvers.node = [&](const std::string& node) {
    ++node_calls[node];
    return eos::common::traffic_shaping::NodeLabel(node) + "-" + std::to_string(revision);
  };
  const auto collector =
      eos::mgm::monitoring::CreateTrafficShapingCollector(engine, "ams", resolvers);

  for (revision = 1; revision <= 2; ++revision) {
    const auto families = collector->Collect();
    const std::string uid_label = "1(user-" + std::to_string(revision) + ")";
    const std::string gid_label = "2(group-" + std::to_string(revision) + ")";
    const auto* all_bytes = FindFamily(families, "eos_io_shaping_all_bytes_total");
    ASSERT_NE(nullptr, all_bytes);
    ASSERT_EQ(8u, all_bytes->metric.size());
    for (const auto& metric : all_bytes->metric) {
      const auto labels = Labels(metric);
      EXPECT_EQ(uid_label, labels.at("uid"));
      EXPECT_EQ(uid_label, labels.at("uid_name"));
      EXPECT_EQ("1", labels.at("uid_id"));
      EXPECT_EQ(gid_label, labels.at("gid"));
      EXPECT_EQ(gid_label, labels.at("gid_name"));
      EXPECT_EQ(gid_label, labels.at("groups"));
      EXPECT_EQ("2", labels.at("gid_id"));
      EXPECT_TRUE(
          labels.at("node_id") == "fst-a.example:1095-" + std::to_string(revision) ||
          labels.at("node_id") == "fst-b.example:1095-" + std::to_string(revision));
      EXPECT_DOUBLE_EQ(labels.at("operation") == "write" ? 4096 : 2048,
                       metric.counter.value);
    }

    for (const auto& name :
         {"eos_io_shaping_bytes_total", "eos_io_shaping_policy_bytes"}) {
      const auto* family = FindFamily(families, name);
      ASSERT_NE(nullptr, family);
      bool found_uid = false;
      bool found_gid = false;
      for (const auto& metric : family->metric) {
        const auto labels = Labels(metric);
        if (labels.at("type") == "uid") {
          EXPECT_EQ(uid_label, labels.at("id"));
          found_uid = true;
        } else if (labels.at("type") == "gid") {
          EXPECT_EQ(gid_label, labels.at("id"));
          found_gid = true;
        }
      }
      EXPECT_TRUE(found_uid);
      EXPECT_TRUE(found_gid);
    }
    ASSERT_EQ(1u, uid_calls.size());
    ASSERT_EQ(1u, gid_calls.size());
    EXPECT_EQ(revision, uid_calls.at(1));
    EXPECT_EQ(revision, gid_calls.at(2));
    ASSERT_EQ(nodes.size(), node_calls.size());
    for (const auto& node : nodes) {
      EXPECT_EQ(revision, node_calls.at(node));
    }
  }
}

TEST(TrafficShapingCollector, CachedNodeLabelsPreserveNormalizedAggregation)
{
  eos::mgm::traffic_shaping::TrafficShapingEngine engine;
  auto manager = engine.GetManager();
  const std::vector<std::string> nodes{"/eos/fst-a.example:1095",
                                       "/eos//eos/fst-a.example:1095/fst"};
  for (const auto& node : nodes) {
    manager->ProcessReport(MakeReport(node, 1000, 0, false));
    manager->ProcessReport(MakeReport(node, 2000, 4096, false));
  }
  std::map<std::string, size_t> node_calls;
  eos::mgm::monitoring::TrafficShapingLabelResolvers resolvers;
  resolvers.node = [&](const std::string& node) {
    ++node_calls[node];
    return eos::common::traffic_shaping::NodeLabel(node);
  };
  const auto collector =
      eos::mgm::monitoring::CreateTrafficShapingCollector(engine, "ams", resolvers);
  const auto families = collector->Collect();
  const auto* all_bytes = FindFamily(families, "eos_io_shaping_all_bytes_total");
  ASSERT_NE(nullptr, all_bytes);
  ASSERT_EQ(2u, all_bytes->metric.size());
  for (const auto& metric : all_bytes->metric) {
    const auto labels = Labels(metric);
    EXPECT_EQ("/eos/fst-a.example:1095", labels.at("node_id"));
    EXPECT_DOUBLE_EQ(labels.at("operation") == "write" ? 8192 : 4096,
                     metric.counter.value);
  }
  for (const auto& name :
       {"eos_io_shaping_all_entries", "eos_io_shaping_all_entries_exported",
        "eos_io_shaping_all_entries_limited"}) {
    const auto* family = FindFamily(families, name);
    ASSERT_NE(nullptr, family);
    ASSERT_EQ(1u, family->metric.size());
    const double expected = std::string(name) == "eos_io_shaping_all_entries" ? 2
                            : std::string(name) == "eos_io_shaping_all_entries_exported"
                                ? 1
                                : 0;
    EXPECT_DOUBLE_EQ(expected, family->metric[0].gauge.value);
  }
  const auto* bytes = FindFamily(families, "eos_io_shaping_bytes_total");
  ASSERT_NE(nullptr, bytes);
  size_t node_series = 0;
  for (const auto& metric : bytes->metric) {
    const auto labels = Labels(metric);
    if (labels.at("type") == "node") {
      ++node_series;
      EXPECT_EQ("/eos/fst-a.example:1095", labels.at("id"));
      EXPECT_DOUBLE_EQ(labels.at("operation") == "write" ? 8192 : 4096,
                       metric.counter.value);
    }
  }
  EXPECT_EQ(2u, node_series);
  ASSERT_EQ(nodes.size(), node_calls.size());
  for (const auto& node : nodes) {
    EXPECT_EQ(1u, node_calls.at(node));
  }
}
