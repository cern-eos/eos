#include "gtest/gtest.h"
#include "mgm/groupbalancer/BalancerEngineUtils.hh"
#include "mgm/groupbalancer/ConverterUtils.hh"

using namespace eos::mgm::group_balancer;

TEST(GroupBalancerUtils, Avg)
{
  EXPECT_DOUBLE_EQ(0, calculateAvg({}));
  group_size_map m {{"group1", {75, 100}},
    {"group2", {81, 100}},
    {"group3", {85, 100}},
    {"group4", {89, 100}},
    {"group5", {95, 100}}};
  EXPECT_DOUBLE_EQ(0.85, calculateAvg(m));
  // Div By 0 for capacity isn't a concern as GroupBalancerInfo::fetch()
  // validates capacity before filling
  m.insert_or_assign("group2", GroupSizeInfo{80, 100});
  EXPECT_DOUBLE_EQ(.848, calculateAvg(m));
  m.insert_or_assign("group4", GroupSizeInfo{90, 100});
  EXPECT_DOUBLE_EQ(0.85, calculateAvg(m));
  m.insert_or_assign("group6", GroupSizeInfo{85, 100});
  EXPECT_DOUBLE_EQ(0.85, calculateAvg(m));
  m.insert_or_assign("group7", GroupSizeInfo{92, 100});
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
  EXPECT_TRUE(is_valid_threshold("1", "2"));
  EXPECT_TRUE(is_valid_threshold("0.01", "1"));
  EXPECT_TRUE(is_valid_threshold("10.0", "90.0", "1"));
  EXPECT_TRUE(is_valid_threshold("1", "2", "3"));
  EXPECT_FALSE(is_valid_threshold("1", "-1"));
  EXPECT_FALSE(is_valid_threshold("0", "1"));
  EXPECT_FALSE(is_valid_threshold("2", "0.0f"));
  EXPECT_FALSE(is_valid_threshold("10", "2", "kitchensink"));
}


TEST(GroupBalancerUtils, extract_percent_value_simple)
{
  engine_conf_t conf;
  conf["min_threshold"]="5";
  EXPECT_DOUBLE_EQ(extract_percent_value(conf,"min_threshold"),0.05);
}

TEST(GroupBalancerUtils, extract_percent_value_null)
{
  engine_conf_t conf;
  EXPECT_DOUBLE_EQ(extract_percent_value(conf,"min_threshold"),0.0);
}

TEST(GroupBalancerUtils, extract_percent_value_default)
{
  engine_conf_t conf;
  EXPECT_DOUBLE_EQ(extract_percent_value(conf,"min_threshold",5.0),0.05);
}

TEST(GroupBalancerUtils, extract_commalist_value)
{
  engine_conf_t conf;
  conf["blocklist_groups"] = "group1,group2, group3, group4";
  std::unordered_set<std::string> expected {"group2","group1","group3","group4"};
  EXPECT_EQ(expected,
            extract_commalist_value(conf, "blocklist_groups"));

  std::unordered_set<std::string> empty;
  EXPECT_EQ(empty,
            extract_commalist_value(conf, "some key"));
}

// a function that behaves like the skipFiles call in
// getProcTransferNameAndSize
std::string fakeSkipFile(const SkipFileFn& skip_file_fn,
                         std::string_view path)
{
  if (skip_file_fn && skip_file_fn(path)) {
    return std::string("");
  }
  return std::string(path);
}

TEST(GroupBalancerUtils, SkipFilesNullFilter)
{
  EXPECT_FALSE(NullFilter);
  EXPECT_EQ("/proc/foo",fakeSkipFile(NullFilter, "/proc/foo"));
  EXPECT_EQ("/00001/bar",fakeSkipFile(NullFilter, "/00001/bar"));
}

TEST(GroupBalancerUtils, SkipFiles)
{
  PrefixFilter procFilter{"/proc/"};
  EXPECT_EQ("", fakeSkipFile(procFilter, "/proc/foo"));
  EXPECT_EQ("/000001/bar",fakeSkipFile(procFilter, "/000001/bar"));
}
