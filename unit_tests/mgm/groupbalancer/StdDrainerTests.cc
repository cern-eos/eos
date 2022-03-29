#include "gtest/gtest.h"
#include "mgm/groupbalancer/BalancerEngine.hh"
#include "mgm/groupbalancer/StdDrainerEngine.hh"

using namespace eos::mgm::group_balancer;

TEST(StdDrainerEngine, simple)
{
  auto engine = std::make_unique<StdDrainerEngine>();
  engine->populateGroupsInfo({{"group0", {GroupStatus::DRAIN, 90, 100}},
                              {"group1", {GroupStatus::ON, 80,100}},
                              {"group2", {GroupStatus::ON, 85,100}},
                              {"group4", {GroupStatus::ON, 75,100}}
  });
  std::unordered_set<std::string> expected_targets = {"group1", "group4"};
  auto d = engine->get_data();
  EXPECT_EQ(d.mGroupSizes.size(), 4);
  EXPECT_EQ(d.mGroupsOverThreshold.size(),1);
  EXPECT_EQ(d.mGroupsUnderThreshold.size(), 2);
  EXPECT_EQ(d.mGroupsUnderThreshold, expected_targets);
}