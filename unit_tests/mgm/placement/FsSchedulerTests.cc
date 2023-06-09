/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2023 CERN/Switzerland                           *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include "mgm/placement/FsScheduler.hh"
#include "gtest/gtest.h"

using eos::mgm::placement::ClusterMgr;

struct TestClusterMgrHandler : public eos::mgm::placement::ClusterMgrHandler
{

  std::unique_ptr<ClusterMgr> make_cluster_mgr(const std::string& spacename) override {
    auto mgr = std::make_unique<ClusterMgr>();
    int n_disks_per_group = 16;
    int n_groups = 32;
    {
      using namespace eos::mgm::placement;
      auto sh = mgr->getStorageHandler(2048);
      sh.addBucket(get_bucket_type(StdBucketType::ROOT), 0);
      for (int i=0; i< n_groups; ++i) {
        sh.addBucket(get_bucket_type(StdBucketType::GROUP), -100-i, 0);
      }

      for (int i=0; i < n_groups*n_disks_per_group; i++) {
        sh.addDisk(Disk(i+1, ConfigStatus::kRW, ActiveStatus::kOnline, 1),
                   -100 - i/n_disks_per_group);
      }
    }
    return mgr;
  }


  eos::mgm::placement::ClusterMapT make_cluster_mgr() override {
    eos::mgm::placement::ClusterMapT cluster_map;
    cluster_map.insert_or_assign("default", make_cluster_mgr("default"));
    return cluster_map;
  }
};

using eos::mgm::placement::FSScheduler;

TEST(FSScheduler, construction)
{
  FSScheduler fs_scheduler(2048, std::make_unique<TestClusterMgrHandler>());
  fs_scheduler.updateClusterData();
}

TEST(FSScheduler, null_handler)
{
  FSScheduler null_scheduler(2048, nullptr);
  null_scheduler.updateClusterData();
}

TEST(FSScheduler, default_scheduler)
{
  FSScheduler fs_scheduler(2048, std::make_unique<TestClusterMgrHandler>());
  auto strategy = fs_scheduler.getPlacementStrategy();
  ASSERT_EQ(strategy, eos::mgm::placement::PlacementStrategyT::kGeoScheduler);
}

TEST(FSScheduler, geo_sched_err)
{
  FSScheduler fs_scheduler(2048, std::make_unique<TestClusterMgrHandler>());
  fs_scheduler.updateClusterData();
  auto result = fs_scheduler.schedule("default",2);
  ASSERT_FALSE(result);
  EXPECT_EQ(result.ret_code, EINVAL);
  EXPECT_EQ(result.error_string(),
           "Not a valid PlacementStrategy");
}

TEST(FSScheduler, round_robin)
{
  FSScheduler fs_scheduler(2048, std::make_unique<TestClusterMgrHandler>());
  fs_scheduler.updateClusterData();
  fs_scheduler.setPlacementStrategy("roundrobin");
  auto strategy = fs_scheduler.getPlacementStrategy();
  ASSERT_EQ(strategy, eos::mgm::placement::PlacementStrategyT::kRoundRobin);
  auto result = fs_scheduler.schedule("default",2);
  ASSERT_TRUE(result);
}

TEST(FSScheduler, setPlacementStrategy)
{
  FSScheduler fs_scheduler(2048, std::make_unique<TestClusterMgrHandler>());
  fs_scheduler.updateClusterData();
  fs_scheduler.setPlacementStrategy("roundrobin");
  EXPECT_EQ(fs_scheduler.getPlacementStrategy(),
                eos::mgm::placement::PlacementStrategyT::kRoundRobin);
  EXPECT_EQ(fs_scheduler.getPlacementStrategy("default"),
                eos::mgm::placement::PlacementStrategyT::kRoundRobin);
  EXPECT_EQ(fs_scheduler.getPlacementStrategy("foobar"),
                eos::mgm::placement::PlacementStrategyT::kRoundRobin);
}

TEST(FSScheduler, setPlacementStrategySpace)
{
  FSScheduler fs_scheduler(2048, std::make_unique<TestClusterMgrHandler>());
  fs_scheduler.updateClusterData();
  fs_scheduler.setPlacementStrategy("default", "weightedrandom");
  EXPECT_EQ(fs_scheduler.getPlacementStrategy(),
            eos::mgm::placement::PlacementStrategyT::kGeoScheduler);
  EXPECT_EQ(fs_scheduler.getPlacementStrategy("default"),
            eos::mgm::placement::PlacementStrategyT::kWeightedRandom);
  EXPECT_EQ(fs_scheduler.getPlacementStrategy("tape"),
            eos::mgm::placement::PlacementStrategyT::kGeoScheduler);
}