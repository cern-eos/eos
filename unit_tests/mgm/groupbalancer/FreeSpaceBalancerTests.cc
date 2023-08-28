#include "gtest/gtest.h"
#include "mgm/groupbalancer/FreeSpaceBalancerEngine.hh"
#include <memory>

using namespace eos::mgm::group_balancer;

TEST(FreeSpaceBalancerEngine, simple)
{
  auto engine = std::make_unique<FreeSpaceBalancerEngine>();
  // This is not a realistic scenario, since we have so small bytes as the total
  // space, but easy to do the mental math to understand
  engine->populateGroupsInfo({{"group1",{GroupStatus::ON, 800, 1000}},
                              {"group2",{GroupStatus::ON, 1800, 2000}},
                              {"group3",{GroupStatus::ON, 500, 1000}},
                              {"group4",{GroupStatus::ON, 700, 1500}},
                              {"group5",{GroupStatus::ON, 1200,1500}}});


  EXPECT_EQ(400, engine->getGroupFreeSpace());
  EXPECT_EQ(408, engine->getFreeSpaceULimit());
  EXPECT_EQ(392, engine->getFreeSpaceLLimit());

  threshold_group_set expected_sources = {"group3","group4"};   // Freebytes > 400
  threshold_group_set expected_targets = {"group1","group2","group5"};
  auto d = engine->get_data();

  EXPECT_EQ(d.mGroupSizes.size(), 5);
  EXPECT_EQ(d.mGroupsOverThreshold.size(), 2);
  EXPECT_EQ(d.mGroupsUnderThreshold.size(), 3);
  EXPECT_EQ(d.mGroupsUnderThreshold, expected_targets);
  EXPECT_EQ(d.mGroupsOverThreshold, expected_sources);

}

TEST(FreeSpaceBalancerEngine, blocklisting)
{
  auto engine = std::make_unique<FreeSpaceBalancerEngine>();
  engine->populateGroupsInfo({{"group1",{GroupStatus::ON, 800, 1000}},
                              {"group2",{GroupStatus::ON, 1800, 2000}},
                              {"group3",{GroupStatus::ON, 500, 1000}},
                              {"group4",{GroupStatus::ON, 700, 1500}},
                              {"group5",{GroupStatus::ON, 1200,1500}}});


  EXPECT_EQ(400, engine->getGroupFreeSpace());
  EXPECT_EQ(408, engine->getFreeSpaceULimit());
  EXPECT_EQ(392, engine->getFreeSpaceLLimit());

  threshold_group_set expected_sources = {"group3","group4"};   // Freebytes > 400
  threshold_group_set expected_targets = {"group1","group2","group5"};
  auto d = engine->get_data();

  EXPECT_EQ(d.mGroupSizes.size(), 5);
  EXPECT_EQ(d.mGroupsOverThreshold.size(), 2);
  EXPECT_EQ(d.mGroupsUnderThreshold.size(), 3);
  EXPECT_EQ(d.mGroupsUnderThreshold, expected_targets);
  EXPECT_EQ(d.mGroupsOverThreshold, expected_sources);

  engine_conf_t conf {{"blocklisted_groups","group3, group2"}};
  threshold_group_set expected_sources2 = {"group4"};
  threshold_group_set expected_targets2 = {"group1","group5"};

  engine->configure(conf);
  engine->recalculate();
  engine->updateGroups();
  EXPECT_EQ(433, engine->getGroupFreeSpace());
  EXPECT_EQ(441, engine->getFreeSpaceULimit());
  EXPECT_EQ(424, engine->getFreeSpaceLLimit());

  auto d2 = engine->get_data();
  EXPECT_EQ(d2.mGroupSizes.size(), 5);
  EXPECT_EQ(d2.mGroupsOverThreshold.size(), 1);
  EXPECT_EQ(d2.mGroupsUnderThreshold.size(), 2);
  EXPECT_EQ(d2.mGroupsUnderThreshold, expected_targets2);
  EXPECT_EQ(d2.mGroupsOverThreshold, expected_sources2);
}
