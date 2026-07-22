#include "mgm/monitoring/FstStatusCollector.hh"

#include <gtest/gtest.h>

#include <map>
#include <string>

namespace eos::mgm::monitoring {
namespace {

const prometheus::MetricFamily*
FindFamily(const std::vector<prometheus::MetricFamily>& families, const std::string& name)
{
  for (const auto& family : families) {
    if (family.name == name) {
      return &family;
    }
  }

  return nullptr;
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

TEST(FstStatusCollector, PreservesIndependentStatusDimensions)
{
  FstStatusSnapshot snapshot;
  snapshot.nodes = {{"/eos/fst-01.example:1095/fst", "online", "on", "site::rack", 2},
                    {"/eos/fst-02.example:1095/fst", "offline", "off", "", 1}};
  snapshot.filesystems = {
      {"/eos/fst-01.example:1095/fst", 1, "online", "rw", "nodrain", "booted", 1000, 250},
      {"/eos/fst-01.example:1095/fst", 2, "online", "draindead", "failed", "bootfailure",
       std::nullopt, std::nullopt},
      {"/eos/fst-02.example:1095/fst", 3, "offline", "wo", "waiting", "down", 2000,
       1500}};
  snapshot.collected_timestamp_seconds = 1234;
  snapshot.collection_duration_seconds = 0.012;

  const auto families = BuildFstStatusMetricFamilies(snapshot, "test-cluster");
  const auto* nodes = FindFamily(families, "eos_fst_node_status_info");
  const auto* node_filesystems = FindFamily(families, "eos_fst_node_filesystems");
  const auto* filesystems = FindFamily(families, "eos_fst_filesystem_status_info");

  ASSERT_NE(nodes, nullptr);
  ASSERT_NE(node_filesystems, nullptr);
  ASSERT_NE(filesystems, nullptr);
  ASSERT_EQ(nodes->metric.size(), 2);
  ASSERT_EQ(node_filesystems->metric.size(), 2);
  ASSERT_EQ(filesystems->metric.size(), 3);

  const auto node_labels = Labels(nodes->metric[1]);
  EXPECT_EQ(node_labels.at("active_status"), "offline");
  EXPECT_EQ(node_labels.at("config_status"), "off");
  EXPECT_EQ(node_labels.at("node_id"), "/eos/fst-02.example:1095/fst");
  EXPECT_DOUBLE_EQ(nodes->metric[1].gauge.value, 1.0);

  const auto failed_drain_labels = Labels(filesystems->metric[1]);
  EXPECT_EQ(failed_drain_labels.at("config_status"), "draindead");
  EXPECT_EQ(failed_drain_labels.at("drain_status"), "failed");
  EXPECT_EQ(failed_drain_labels.at("boot_status"), "bootfailure");
  EXPECT_EQ(failed_drain_labels.at("fsid"), "2");

  const auto waiting_labels = Labels(filesystems->metric[2]);
  EXPECT_EQ(waiting_labels.at("active_status"), "offline");
  EXPECT_EQ(waiting_labels.at("config_status"), "wo");
  EXPECT_EQ(waiting_labels.at("drain_status"), "waiting");

  const auto* capacity = FindFamily(families, "eos_fst_filesystem_capacity_bytes");
  const auto* used = FindFamily(families, "eos_fst_filesystem_used_bytes");
  ASSERT_NE(capacity, nullptr);
  ASSERT_NE(used, nullptr);
  ASSERT_EQ(capacity->metric.size(), 2);
  ASSERT_EQ(used->metric.size(), 2);
  EXPECT_DOUBLE_EQ(capacity->metric[0].gauge.value, 1000.0);
  EXPECT_DOUBLE_EQ(used->metric[0].gauge.value, 250.0);
  EXPECT_EQ(Labels(capacity->metric[0]).at("fsid"), "1");
}

TEST(FstStatusCollector, ExposesSnapshotFreshnessAndCollectionCost)
{
  FstStatusSnapshot snapshot;
  snapshot.collected_timestamp_seconds = 9876;
  snapshot.collection_duration_seconds = 0.25;

  const auto families = BuildFstStatusMetricFamilies(snapshot, "test-cluster");
  const auto* timestamp =
      FindFamily(families, "eos_fst_status_snapshot_timestamp_seconds");
  const auto* duration =
      FindFamily(families, "eos_fst_status_collection_duration_seconds");

  ASSERT_NE(timestamp, nullptr);
  ASSERT_NE(duration, nullptr);
  ASSERT_EQ(timestamp->metric.size(), 1);
  ASSERT_EQ(duration->metric.size(), 1);
  EXPECT_DOUBLE_EQ(timestamp->metric[0].gauge.value, 9876.0);
  EXPECT_DOUBLE_EQ(duration->metric[0].gauge.value, 0.25);
  EXPECT_EQ(Labels(timestamp->metric[0]).at("cluster"), "test-cluster");
}

} // namespace
} // namespace eos::mgm::monitoring
