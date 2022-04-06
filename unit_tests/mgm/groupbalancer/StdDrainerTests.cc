#include "gtest/gtest.h"
#include "mgm/groupbalancer/BalancerEngine.hh"
#include "mgm/groupbalancer/StdDrainerEngine.hh"
#include "mgm/groupbalancer/BalancerEngineUtils.hh"

using namespace eos::mgm::group_balancer;

TEST(StdDrainerEngine, defaultconf)
{
  auto engine = std::make_unique<StdDrainerEngine>();
  engine->configure({});
  EXPECT_DOUBLE_EQ(static_cast<StdDrainerEngine*>(engine.get())->get_threshold(),
                   0.0001);
}


TEST(StdDrainerEngine, simple)
{
  auto engine = std::make_unique<StdDrainerEngine>();
  engine_conf_t conf {{"threshold","2"}};
  engine->configure(conf);
  engine->populateGroupsInfo({{"group0", {GroupStatus::DRAIN, 95, 100}},
                              {"group1", {GroupStatus::ON, 80,100}},
                              {"group2", {GroupStatus::ON, 86,100}},
                              {"group4", {GroupStatus::ON, 80,100}}
  });

  ASSERT_EQ(static_cast<StdDrainerEngine*>(engine.get())->get_threshold(), 0.02);
  std::unordered_set<std::string> expected_targets = {"group1", "group4"};
  auto d = engine->get_data();
  EXPECT_EQ(d.mGroupSizes.size(), 4);
  EXPECT_EQ(d.mGroupsOverThreshold.size(),1);
  EXPECT_EQ(d.mGroupsUnderThreshold.size(), 2);
  EXPECT_EQ(d.mGroupsUnderThreshold, expected_targets);
}
