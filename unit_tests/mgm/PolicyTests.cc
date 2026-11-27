#include "mgm/policy/Policy.hh"
#include "mgm/misc/Constants.hh"
#include "namespace/interface/IContainerMD.hh"
#include "gtest/gtest.h"

using namespace eos::mgm;

TEST(Policy, GetConfigKeys)
{
  ASSERT_EQ(Policy::gBasePolicyKeys, Policy::GetConfigKeys());
}

TEST(Policy, RWParams)
{
  Policy::RWParams params("user1", "group1", "app1", true);
  ASSERT_EQ(params.user_key, ".user:user1");
  ASSERT_EQ(params.group_key, ".group:group1");
  ASSERT_EQ(params.app_key, ".app:app1");
  ASSERT_EQ(params.rw_marker, ":w");
}

TEST(Policy, RWParams_rw_marker)
{
  Policy::RWParams params("", "", "", true);
  ASSERT_EQ(params.rw_marker, ":w");
  Policy::RWParams params2("", "", "", false);
  ASSERT_EQ(params2.rw_marker, ":r");
}

TEST(Policy, RWParams_get_key)
{
  Policy::RWParams params("", "", "", true);
  ASSERT_EQ(params.getKey("test"), "test:w");
  Policy::RWParams params4("", "", "", false);
  ASSERT_EQ(params4.getKey("test"), "test:r");
}

TEST(Policy, CompressionRequested)
{
  eos::IContainerMD::XAttrMap map;
  EXPECT_FALSE(Policy::CompressionRequested(map));

  map[SYS_FORCED_COMPRESSION] = "1";
  EXPECT_TRUE(Policy::CompressionRequested(map));

  map[SYS_FORCED_COMPRESSION] = "0";
  EXPECT_FALSE(Policy::CompressionRequested(map));

  map.clear();
  map[USER_FORCED_COMPRESSION] = "1";
  EXPECT_TRUE(Policy::CompressionRequested(map));

  map[SYS_FORCED_COMPRESSION] = "1";
  map[USER_FORCED_COMPRESSION] = "0";
  EXPECT_FALSE(Policy::CompressionRequested(map));

  map["sys.forced.nouserlayout"] = "1";
  EXPECT_TRUE(Policy::CompressionRequested(map));

  map.clear();
  map[USER_FORCED_COMPRESSION] = "1";
  map["sys.forced.nouserlayout"] = "1";
  EXPECT_FALSE(Policy::CompressionRequested(map));
}

TEST(Policy, GetRWConfigKey)
{
  Policy::RWParams params("user1", "group1", "eoscp", false);
  std::vector<std::string> expected {
    "policy:bandwidth:r.app:eoscp",
    "policy:bandwidth:r.user:user1",
    "policy:bandwidth:r.group:group1",
    "policy:bandwidth:r"};
  ASSERT_EQ(params.getKeys("policy:bandwidth"),
            expected);
}
