#include "mgm/monitoring/MgmStatusCollector.hh"

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

TEST(MgmStatusCollector, ExposesObservedLeaseHolderWithoutMasterFiltering)
{
  const MgmStatusCollector leader_collector("test-cluster", []() {
    return std::vector<MgmStatusSnapshot>{
        {"mgm-01.example:1094", "mgm-01.example:1094", true}};
  });
  const MgmStatusCollector follower_collector("test-cluster", []() {
    return std::vector<MgmStatusSnapshot>{
        {"mgm-02.example:1094", "mgm-01.example:1094", false}};
  });
  const auto leader = leader_collector.Collect();
  const auto follower = follower_collector.Collect();

  const auto* leader_role = FindFamily(leader, "eos_mgm_master");
  const auto* follower_role = FindFamily(follower, "eos_mgm_master");
  ASSERT_NE(leader_role, nullptr);
  ASSERT_NE(follower_role, nullptr);
  ASSERT_EQ(leader_role->metric.size(), 1);
  ASSERT_EQ(follower_role->metric.size(), 1);
  EXPECT_DOUBLE_EQ(leader_role->metric[0].gauge.value, 1.0);
  EXPECT_DOUBLE_EQ(follower_role->metric[0].gauge.value, 0.0);
  EXPECT_EQ(Labels(leader_role->metric[0]).at("master_id"), "mgm-01.example:1094");
  EXPECT_EQ(Labels(follower_role->metric[0]).at("mgm_id"), "mgm-02.example:1094");
  EXPECT_EQ(Labels(follower_role->metric[0]).at("master_id"), "mgm-01.example:1094");
}

TEST(MgmStatusCollector, PreservesRoleAndLeaseDisagreement)
{
  const MgmStatusCollector collector("test-cluster", []() {
    return std::vector<MgmStatusSnapshot>{
        {"mgm-01.example:1094", "mgm-02.example:1094", true}};
  });
  const auto families = collector.Collect();
  const auto* role = FindFamily(families, "eos_mgm_master");

  ASSERT_NE(role, nullptr);
  ASSERT_EQ(role->metric.size(), 1);
  EXPECT_DOUBLE_EQ(role->metric[0].gauge.value, 1.0);
  EXPECT_EQ(Labels(role->metric[0]).at("mgm_id"), "mgm-01.example:1094");
  EXPECT_EQ(Labels(role->metric[0]).at("master_id"), "mgm-02.example:1094");
}

TEST(MgmStatusCollector, ExposesConfiguredCandidatesAndLocalMgm)
{
  const auto snapshots =
      BuildMgmStatusSnapshots("mgm-02.example:1094", true, "mgm-02.example:1094", 1094,
                              {"mgm-01.example", "mgm-03.example"});
  const auto families = BuildMgmStatusMetricFamilies(snapshots, "test-cluster");
  const auto* role = FindFamily(families, "eos_mgm_master");

  ASSERT_NE(role, nullptr);
  ASSERT_EQ(role->metric.size(), 3);
  EXPECT_DOUBLE_EQ(role->metric[0].gauge.value, 0.0);
  EXPECT_DOUBLE_EQ(role->metric[1].gauge.value, 0.0);
  EXPECT_DOUBLE_EQ(role->metric[2].gauge.value, 1.0);
  EXPECT_EQ(Labels(role->metric[0]).at("mgm_id"), "mgm-01.example:1094");
  EXPECT_EQ(Labels(role->metric[1]).at("mgm_id"), "mgm-03.example:1094");
  EXPECT_EQ(Labels(role->metric[2]).at("mgm_id"), "mgm-02.example:1094");
}

} // namespace
} // namespace eos::mgm::monitoring
