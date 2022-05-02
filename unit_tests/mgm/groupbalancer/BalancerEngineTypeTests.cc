#include "gtest/gtest.h"
#include "mgm/groupbalancer/BalancerEngineTypes.hh"

// Check at compile time
using namespace eos::mgm::group_balancer;
using namespace std::string_view_literals;
static_assert(getGroupStatus("on"sv) == GroupStatus::ON);
static_assert(getGroupStatus("drain"sv) == GroupStatus::DRAIN);
static_assert(getGroupStatus("foo"sv) == GroupStatus::OFF);

TEST(GroupBalancerTypes, Status)
{
  ASSERT_EQ(getGroupStatus("on"), GroupStatus::ON);
  std::string off = "off";
  // Test the various char/str -> sv conversions
  ASSERT_EQ(getGroupStatus(off.c_str()), GroupStatus::OFF);
  ASSERT_EQ(getGroupStatus(off), GroupStatus::OFF);
  const char* drain = "drain";
  ASSERT_EQ(getGroupStatus(drain), GroupStatus::DRAIN);
}
