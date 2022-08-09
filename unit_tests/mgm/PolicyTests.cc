#include "mgm/Policy.hh"
#include "mgm/Constants.hh"
#include "gtest/gtest.h"

using namespace eos::mgm;

TEST(Policy, GetConfigKeys)
{
  ASSERT_EQ(Policy::gBasePolicyKeys, Policy::GetConfigKeys());
  ASSERT_EQ(Policy::gBaseLocalPolicyKeys, Policy::GetConfigKeys(true));
}

TEST(Policy, RWParams)
{
  Policy::RWParams params("user1", "group1", "app1", true, false);
  ASSERT_EQ(params.user_key, ".user:user1");
  ASSERT_EQ(params.group_key, ".group:group1");
  ASSERT_EQ(params.app_key, ".app:app1");
  ASSERT_EQ(params.rw_marker, ":w");
  ASSERT_EQ(params.local_prefix, "");
}

TEST(Policy, RWParams_rw_marker)
{
  Policy::RWParams params("","","",true,false);
  ASSERT_EQ(params.rw_marker, ":w");

  Policy::RWParams params2("","","",false,false);
  ASSERT_EQ(params2.rw_marker, ":r");

}

TEST(Policy, RWParams_get_key)
{
  Policy::RWParams params("", "", "", true, false);
  ASSERT_EQ(params.getKey("test"), "test:w");

  Policy::RWParams params2("", "", "", true, true);
  ASSERT_EQ(params2.getKey("test"), "local.test:w");

  Policy::RWParams params3("", "", "", false, true);
  ASSERT_EQ(params3.getKey("test"), "local.test:r");

  Policy::RWParams params4("", "", "", false, false);
  ASSERT_EQ(params4.getKey("test"), "test:r");
}

TEST(Policy, GetRWConfigKey)
{

  Policy::RWParams params("user1","group1","eoscp",false,false);
  std::vector<std::string> expected {
      "policy:bandwidth:r.app:eoscp",
      "policy:bandwidth:r.user:user1",
      "policy:bandwidth:r.group:group1",
      "policy:bandwidth:r"};
  ASSERT_EQ(params.getKeys("policy:bandwidth"),
            expected);
}
