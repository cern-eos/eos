#include "gtest/gtest.h"
#include "mgm/groupbalancer/GroupsInfoFetcher.hh"

using eos::mgm::group_balancer::eosGroupsInfoFetcher;
using eos::mgm::group_balancer::GroupStatus;

TEST(GroupsInfoFetcher, default_is_valid_status)
{
  eosGroupsInfoFetcher fetcher("default");
  ASSERT_TRUE(fetcher.is_valid_status(GroupStatus::ON));
  ASSERT_FALSE(fetcher.is_valid_status(GroupStatus::DRAIN));
  ASSERT_FALSE(fetcher.is_valid_status(GroupStatus::OFF));
}

TEST(GroupsInfoFetcher, drain_status)
{
  using eos::mgm::group_balancer::GroupStatusFilter;
  struct DrainStatusFilter: public GroupStatusFilter {
    bool operator()(GroupStatus status) override {
      return status == GroupStatus::DRAIN || status == GroupStatus::ON;
    }
  };

  eosGroupsInfoFetcher fetcher("default", DrainStatusFilter{});
  ASSERT_TRUE(fetcher.is_valid_status(GroupStatus::DRAIN));
  ASSERT_TRUE(fetcher.is_valid_status(GroupStatus::ON));
  ASSERT_FALSE(fetcher.is_valid_status(GroupStatus::OFF));
}