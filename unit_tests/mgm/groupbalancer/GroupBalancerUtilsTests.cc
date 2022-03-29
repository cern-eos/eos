#include "gtest/gtest.h"
#include "mgm/groupbalancer/BalancerEngineUtils.hh"

using namespace eos::mgm::group_balancer;




TEST(GroupBalancerUtils, Avg)
{
  EXPECT_DOUBLE_EQ(0, calculateAvg({}));

  group_size_map m {{"group1",{75,100}},
                    {"group2",{81,100}},
                    {"group3",{85,100}},
                    {"group4",{89,100}},
                    {"group5",{95,100}}};

  EXPECT_DOUBLE_EQ(0.85, calculateAvg(m));

  // Div By 0 for capacity isn't a concern as GroupBalancerInfo::fetch()
  // validates capacity before filling
  m.insert_or_assign("group2", GroupSizeInfo{80,100});
  EXPECT_DOUBLE_EQ(.848, calculateAvg(m));

  m.insert_or_assign("group4", GroupSizeInfo{90,100});
  EXPECT_DOUBLE_EQ(0.85, calculateAvg(m));

  m.insert_or_assign("group6", GroupSizeInfo{85,100});
  EXPECT_DOUBLE_EQ(0.85, calculateAvg(m));

  m.insert_or_assign("group7", GroupSizeInfo{92,100});
  EXPECT_DOUBLE_EQ(0.86, calculateAvg(m));

}



TEST(GroupBalancerUtils, threshold)
{
  EXPECT_TRUE(is_valid_threshold("1"));
  EXPECT_TRUE(is_valid_threshold("0.01"));
  EXPECT_TRUE(is_valid_threshold("10.0"));
  EXPECT_FALSE(is_valid_threshold("-1"));
  EXPECT_FALSE(is_valid_threshold("0"));
  EXPECT_FALSE(is_valid_threshold("0.0f"));
  EXPECT_FALSE(is_valid_threshold("kitchensink"));
}

TEST(GroupBalancerUtils, threshold_multi)
{
  EXPECT_TRUE(is_valid_threshold("1","2"));
  EXPECT_TRUE(is_valid_threshold("0.01","1"));
  EXPECT_TRUE(is_valid_threshold("10.0","90.0","1"));
  EXPECT_TRUE(is_valid_threshold("1","2","3"));
  EXPECT_FALSE(is_valid_threshold("1","-1"));
  EXPECT_FALSE(is_valid_threshold("0","1"));
  EXPECT_FALSE(is_valid_threshold("2", "0.0f"));
  EXPECT_FALSE(is_valid_threshold("10","2","kitchensink"));
}
