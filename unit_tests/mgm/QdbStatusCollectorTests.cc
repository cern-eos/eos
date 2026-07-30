#include "mgm/monitoring/QdbStatusCollector.hh"

#include <gtest/gtest.h>

#include <algorithm>
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

TEST(QdbStatusCollector, PreservesMembershipVersionsAndLeadership)
{
  const auto snapshots = ParseQdbRaftInfo({
      "LEADER qdb-02.example:7777",
      "MYSELF qdb-02.example:7777",
      "VERSION 0.5.0",
      "STATUS LEADER",
      "NODES qdb-01.example:7777,qdb-02.example:7777,qdb-03.example:7777",
      "REPLICA qdb-01.example:7777 | ONLINE | VERSION 0.4.2",
      "REPLICA qdb-03.example:7777 | OFFLINE | VERSION 0.4.1",
  });

  ASSERT_EQ(snapshots.size(), 3);
  EXPECT_EQ(snapshots[0].qdb_id, "qdb-02.example:7777");
  EXPECT_TRUE(snapshots[0].is_leader);
  EXPECT_EQ(snapshots[0].version, "0.5.0");
  EXPECT_EQ(snapshots[0].status, "online");

  const auto families = BuildQdbStatusMetricFamilies(snapshots, "test-cluster");
  const auto* info = FindFamily(families, "eos_qdb_info");
  ASSERT_NE(info, nullptr);
  ASSERT_EQ(info->metric.size(), 3);
  EXPECT_EQ(Labels(info->metric[0]).at("role"), "leader");

  const auto offline =
      std::find_if(info->metric.begin(), info->metric.end(), [](const auto& metric) {
        return Labels(metric).at("qdb_id") == "qdb-03.example:7777";
      });
  ASSERT_NE(offline, info->metric.end());
  EXPECT_EQ(Labels(*offline).at("status"), "offline");
  EXPECT_EQ(Labels(*offline).at("qdb_version"), "0.4.1");
}

} // namespace
} // namespace eos::mgm::monitoring
