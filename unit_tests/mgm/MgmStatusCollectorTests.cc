#include "mgm/monitoring/MgmStatusCollector.hh"

#include <gtest/gtest.h>

#include <string>

namespace eos::mgm::monitoring {
namespace {

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

  std::string leader_out;
  leader_collector.Collect(leader_out);
  std::string follower_out;
  follower_collector.Collect(follower_out);

  EXPECT_NE(leader_out.find("# TYPE eos_mgm_master gauge"), std::string::npos);
  EXPECT_NE(leader_out.find("eos_mgm_master{cluster=\"test-cluster\",master_id=\"mgm-01."
                            "example:1094\",mgm_id=\"mgm-01.example:1094\"} 1"),
            std::string::npos);

  EXPECT_NE(follower_out.find("# TYPE eos_mgm_master gauge"), std::string::npos);
  EXPECT_NE(follower_out.find("eos_mgm_master{cluster=\"test-cluster\",master_id=\"mgm-"
                              "01.example:1094\",mgm_id=\"mgm-02.example:1094\"} 0"),
            std::string::npos);
}

TEST(MgmStatusCollector, PreservesRoleAndLeaseDisagreement)
{
  const MgmStatusCollector collector("test-cluster", []() {
    return std::vector<MgmStatusSnapshot>{
        {"mgm-01.example:1094", "mgm-02.example:1094", true}};
  });
  std::string out;
  collector.Collect(out);

  EXPECT_NE(out.find("eos_mgm_master{cluster=\"test-cluster\",master_id=\"mgm-02.example:"
                     "1094\",mgm_id=\"mgm-01.example:1094\"} 1"),
            std::string::npos);
}

TEST(MgmStatusCollector, ExposesConfiguredCandidatesAndLocalMgm)
{
  const auto snapshots =
      BuildMgmStatusSnapshots("mgm-02.example:1094", true, "mgm-02.example:1094", 1094,
                              {"mgm-01.example", "mgm-03.example"});
  std::string out;
  EmitMgmStatusMetrics(snapshots, "test-cluster", out);

  EXPECT_NE(out.find("eos_mgm_master{cluster=\"test-cluster\",master_id=\"mgm-02.example:"
                     "1094\",mgm_id=\"mgm-01.example:1094\"} 0"),
            std::string::npos);
  EXPECT_NE(out.find("eos_mgm_master{cluster=\"test-cluster\",master_id=\"mgm-02.example:"
                     "1094\",mgm_id=\"mgm-03.example:1094\"} 0"),
            std::string::npos);
  EXPECT_NE(out.find("eos_mgm_master{cluster=\"test-cluster\",master_id=\"mgm-02.example:"
                     "1094\",mgm_id=\"mgm-02.example:1094\"} 1"),
            std::string::npos);
}

} // namespace
} // namespace eos::mgm::monitoring
