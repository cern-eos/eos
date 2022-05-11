#include "common/utils/ContainerUtils.hh"
#include "mgm/groupbalancer/BalancerEngine.hh"
#include "mgm/groupbalancer/BalancerEngineUtils.hh"
#include "mgm/groupbalancer/StdDrainerEngine.hh"
#include "gtest/gtest.h"

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
  threshold_group_set expected_targets = {"group1", "group4"};
  auto d = engine->get_data();
  EXPECT_EQ(d.mGroupSizes.size(), 4);
  EXPECT_EQ(d.mGroupsOverThreshold.size(),1);
  EXPECT_EQ(d.mGroupsUnderThreshold.size(), 2);
  EXPECT_EQ(d.mGroupsUnderThreshold, expected_targets);
}

TEST(StdDrainerEngine, RRTestsimple)
{
  auto engine = std::make_unique<StdDrainerEngine>();
  engine_conf_t conf {{"threshold","0"}};
  engine->configure(conf);
  engine->populateGroupsInfo({{"group0", {GroupStatus::DRAIN, 95, 100}},
                              {"group1", {GroupStatus::ON, 80,100}},
                              {"group2", {GroupStatus::ON, 86,100}},
                              {"group3", {GroupStatus::ON, 80,100}},
                              {"group4", {GroupStatus::DRAIN, 99, 100}},
                              {"group5", {GroupStatus::DRAIN, 20, 100}}
  });

  // These are some basic assumptions to ensure that we're consistent
  ASSERT_EQ(static_cast<StdDrainerEngine*>(engine.get())->get_threshold(), 0);
  threshold_group_set expected_sources = {"group0","group4", "group5"};
  threshold_group_set expected_targets = {"group1", "group2", "group3"};
  auto d = engine->get_data();
  EXPECT_EQ(d.mGroupSizes.size(), 6);
  EXPECT_EQ(d.mGroupsOverThreshold.size(),3);
  EXPECT_EQ(d.mGroupsUnderThreshold.size(),3);
  EXPECT_EQ(d.mGroupsUnderThreshold, expected_targets);
  EXPECT_EQ(d.mGroupsOverThreshold, expected_sources);

  auto [src, tgt] = engine->pickGroupsforTransfer(0);
  EXPECT_EQ("group0", src);
  EXPECT_EQ("group1", tgt);

  std::tie(src, tgt) = engine->pickGroupsforTransfer(2);
  EXPECT_EQ("group5", src);
  EXPECT_EQ("group3", tgt);
}


TEST(StdDrainerEngine, RRTestsLoop)
{
  auto engine = std::make_unique<StdDrainerEngine>();
  engine_conf_t conf {{"threshold","0"}};
  engine->configure(conf);
  engine->populateGroupsInfo({{"group0", {GroupStatus::DRAIN, 95, 100}},
                              {"group1", {GroupStatus::ON, 80,100}},
                              {"group2", {GroupStatus::ON, 86,100}},
                              {"group3", {GroupStatus::ON, 80,100}},
                              {"group4", {GroupStatus::DRAIN, 99, 100}},
                              {"group5", {GroupStatus::DRAIN, 20, 100}}
  });
  std::string src, tgt;
  std::vector<std::string> sources = {"group0","group4","group5"};
  std::vector<std::string> targets = {"group1","group2","group3"};
  uint8_t seed {0};

  // Simulate a logic similar to the inf. loop of prepareTransfers
  // We'll loop continously as soon as we find some free slots to push transfers to
  // So we need to ensure that we constantly wrap around and not fixate on 0-indices
  // between the various runs of the inner loop!
  for (int i=0; i < 10; i++) {
    for (int j = 0; j < 500; j++) {
      uint8_t curr_seed = seed;
      std::tie(src, tgt) = engine->pickGroupsforTransfer(seed++);
      EXPECT_EQ(eos::common::pickIndexRR(sources, curr_seed), src);
      EXPECT_EQ(eos::common::pickIndexRR(targets, curr_seed), tgt);
    }
  }
  uint64_t max_uint8_t = std::numeric_limits<uint8_t>::max() + 1;
  EXPECT_EQ(seed, 5000 % max_uint8_t);
}