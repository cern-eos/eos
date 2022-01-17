#include "gtest/gtest.h"
#include "mgm/groupbalancer/BalancerEngine.hh"
#include "mgm/groupbalancer/MinMaxBalancerEngine.hh"


using namespace eos::mgm::group_balancer;

TEST(MinMaxBalancerEngine, configure)
{
  std::unique_ptr<BalancerEngine> engine = std::make_unique<MinMaxBalancerEngine>();
  engine->configure({{"min_threshold","5"}});
  auto e = static_cast<MinMaxBalancerEngine*>(engine.get());
  EXPECT_DOUBLE_EQ(e->get_min_threshold(), 0.05);
}

TEST(MinMaxBalancerEngine, simple)
{
  std::unique_ptr<BalancerEngine> engine = std::make_unique<MinMaxBalancerEngine>();
  engine->configure({{"min_threshold","80"},{"max_threshold","90"}});
  engine->populateGroupsInfo({{"group1",{75,100}},
                              {"group2",{81,100}},
                              {"group3",{85,100}},
                              {"group4",{89,100}},
                              {"group5",{95,100}}});

  auto d = engine->get_data();
  EXPECT_NEAR(calculateAvg(d.mGroupSizes),0.85,0.0000001);
  EXPECT_EQ(d.mGroupSizes.size(),5);
  EXPECT_EQ(d.mGroupsOverThreshold.size(),1);
  EXPECT_EQ(d.mGroupsUnderThreshold.size(),1);
  auto [over,under] = engine->pickGroupsforTransfer();
  EXPECT_EQ(over,"group5");
  EXPECT_EQ(under,"group1");
}


TEST(MinMaxBalancerEngine, updatethreshold)
{
  std::unique_ptr<BalancerEngine> engine = std::make_unique<MinMaxBalancerEngine>();
  // If we pick at the point of item,
  engine->configure({{"min_threshold","81"},{"max_threshold","89"}});

  engine->populateGroupsInfo({{"group1",{75,100}},
                              {"group2",{81,100}},
                              {"group3",{85,100}},
                              {"group4",{89,100}},
                              {"group5",{95,100}}});

  {
    auto d = engine->get_data();
    EXPECT_NEAR(calculateAvg(d.mGroupSizes),0.85,0.0000001);
    EXPECT_EQ(d.mGroupSizes.size(),5);
    EXPECT_EQ(d.mGroupsOverThreshold.size(),1);
    EXPECT_EQ(d.mGroupsUnderThreshold.size(),1);
  }

  engine->configure({{"min_threshold","82"},{"max_threshold","88"}});
  engine->updateGroups();
  auto e = static_cast<MinMaxBalancerEngine*>(engine.get());
  EXPECT_DOUBLE_EQ(e->get_min_threshold(), 0.82);

  {
    auto d = engine->get_data();
    EXPECT_NEAR(calculateAvg(d.mGroupSizes),0.85,0.0000001);
    EXPECT_EQ(d.mGroupSizes.size(),5);
    EXPECT_EQ(d.mGroupsOverThreshold.size(),2);
    EXPECT_EQ(d.mGroupsUnderThreshold.size(),2);
    // FIXME this actually happens because we do a floating point compare in
    // this case using diffWithAvg will be slightly greater than threshold at
    // boundary values when doing the subtraction
    std::unordered_set<std::string> grps_over = {"group5","group4"};
    std::unordered_set<std::string> grps_under = {"group1","group2"};
    EXPECT_EQ(d.mGroupsOverThreshold, grps_over);
    EXPECT_EQ(d.mGroupsUnderThreshold, grps_under);
  }

}
