// ----------------------------------------------------------------------
// File: SchedulerTests.cc
// Author: Abhishek Lekshmanan - CERN
// ----------------------------------------------------------------------

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

#include "common/utils/ContainerUtils.hh"
#include "mgm/placement/RoundRobinPlacementStrategy.hh"
#include "mgm/placement/WeightedPlacementStrategy.hh"
#include "mgm/placement/FlatScheduler.hh"
#include "mgm/placement/PlacementStrategy.hh"
#include "unit_tests/mgm/placement/ClusterMapFixture.hh"
#include "gtest/gtest.h"
using eos::mgm::placement::item_id_t;

TEST_F(SimpleClusterF, RoundRobinBasic)
{
  eos::mgm::placement::RoundRobinPlacement rr_placement(eos::mgm::placement::PlacementStrategyT::kRoundRobin,
                                                        256);

  auto cluster_data_ptr = mgr.getClusterData();

  // TODO: write a higher level function to do recursive descent
  // Choose 1 site - from ROOT
  auto res = rr_placement.placeFiles(cluster_data_ptr(), {0, 1});
  ASSERT_TRUE(res);
  EXPECT_EQ(res.n_replicas, 1);
  EXPECT_EQ(res.ids[0], -1);

  // Choose 1 group from SITE
  auto site_id = res.ids[0];
  auto group_res = rr_placement.placeFiles(cluster_data_ptr(), {site_id, 1});
  ASSERT_TRUE(group_res);
  EXPECT_EQ(group_res.n_replicas, 1);


  // choose 2 disks from group!
  auto disks_res =
      rr_placement.placeFiles(cluster_data_ptr(), {group_res.ids[0], 2});
  ASSERT_TRUE(disks_res);
  EXPECT_EQ(disks_res.n_replicas, 2);

}

TEST_F(SimpleClusterF, RandomBasic)
{
  eos::mgm::placement::RoundRobinPlacement rand_placement(eos::mgm::placement::PlacementStrategyT::kRandom,
                                                          256);

  auto cluster_data_ptr = mgr.getClusterData();

  auto res = rand_placement.placeFiles(cluster_data_ptr(), {0, 1});
  ASSERT_TRUE(res);
  EXPECT_EQ(res.n_replicas, 1);

  auto site_id = res.ids[0];
  auto group_res = rand_placement.placeFiles(cluster_data_ptr(), {site_id, 1});
  ASSERT_TRUE(group_res);
  EXPECT_EQ(group_res.n_replicas, 1);

  auto disks_res =
      rand_placement.placeFiles(cluster_data_ptr(), {group_res.ids[0], 2});
  ASSERT_TRUE(disks_res);
  std::cout << disks_res << "\n";
  EXPECT_EQ(disks_res.n_replicas, 2);
}

TEST_F(SimpleClusterF, TLRoundRobinBasic)
{
  eos::mgm::placement::RoundRobinPlacement rr_placement(eos::mgm::placement::PlacementStrategyT::kThreadLocalRoundRobin,
                                                        256);

  auto cluster_data_ptr = mgr.getClusterData();

  // TODO: write a higher level function to do recursive descent
  // Choose 1 site - from ROOT
  auto res = rr_placement.placeFiles(cluster_data_ptr(), {0, 1});
  ASSERT_TRUE(res);
  EXPECT_EQ(res.n_replicas, 1);
  // We cannot assert on the id here because the thread local round robin would
  // have a random starting point, only the looping behaviour is easier to reason


  // Choose 1 group from SITE
  auto site_id = res.ids[0];
  auto group_res = rr_placement.placeFiles(cluster_data_ptr(), {site_id, 1});
  ASSERT_TRUE(group_res);
  EXPECT_EQ(group_res.n_replicas, 1);


  // choose 2 disks from group!
  auto disks_res =
      rr_placement.placeFiles(cluster_data_ptr(), {group_res.ids[0], 2});
  ASSERT_TRUE(disks_res);
  EXPECT_EQ(disks_res.n_replicas, 2);

}

TEST_F(SimpleClusterF, RoundRobinBasicLoop)
{
  eos::mgm::placement::RoundRobinPlacement rr_placement(eos::mgm::placement::PlacementStrategyT::kRoundRobin,
                                                        256);

  auto cluster_data_ptr = mgr.getClusterData();

  std::map<int32_t,uint32_t> site_id_ctr;
  std::map<int32_t,uint32_t> group_id_ctr;
  std::map<int32_t,uint32_t> disk_id_ctr;
  std::vector<int32_t> disk_ids_vec;
  // TODO: write a higher level function to do recursive descent
  // Choose 1 site - from ROOT
  // Loop over 30 times, which is the total size of the disks to ensure that all
  // elements are chosen
  for (int i = 0; i < 30; i++)
  {
    auto res = rr_placement.placeFiles(cluster_data_ptr(), {0, 1});

    ASSERT_TRUE(res);
    ASSERT_EQ(res.n_replicas, 1);

    site_id_ctr[res.ids[0]]++;

    // Choose 1 group from SITE
    auto site_id = res.ids[0];
    auto group_res = rr_placement.placeFiles(cluster_data_ptr(), {site_id, 1});

    ASSERT_TRUE(group_res);
    ASSERT_EQ(group_res.n_replicas, 1);
    group_id_ctr[group_res.ids[0]]++;


    // choose 2 disks from group!
    auto disks_res =
        rr_placement.placeFiles(cluster_data_ptr(), {group_res.ids[0], 2});

    ASSERT_TRUE(disks_res);
    ASSERT_EQ(disks_res.n_replicas, 2);
    disk_id_ctr[disks_res.ids[0]]++;
    disk_id_ctr[disks_res.ids[1]]++;


    disk_ids_vec.push_back(disks_res.ids[0]);
    disk_ids_vec.push_back(disks_res.ids[1]);

  }

  // SITE1 gets 15 requests, SITE2 gets 15 requests;
  ASSERT_EQ(site_id_ctr[-1], 15);
  ASSERT_EQ(site_id_ctr[-2], 15);


  // 30 items chosen in site1 among 20 disks
  // 30 items chosen in site2 among 10 disks
  ASSERT_EQ(group_id_ctr[-102], 15);

  // This is a bit more involved to reason, actually just a consequence of an
  // empty starting cluster, where we'd expect roundrobin to start from the initial
  // elements, hence, group1 is chosen first, and thus gets a request extra
  // if you do the LCM you'd be able to reach a point where you'd schedule equally
  // group1 & group2; group3 would still have 2X requests if you RR over the sites first

  EXPECT_EQ(group_id_ctr[-100], 8);
  EXPECT_EQ(group_id_ctr[-101], 7);
  // All the disks are chosen at least once, due to the non uniform nature here,
  // site 2 would have its disks chosen twice as often as site 1
  ASSERT_EQ(disk_ids_vec.size(), 60);
  ASSERT_EQ(disk_id_ctr.size(), 30);



  // Check SITE1 ctr, atleast 1; initial disks would be twice as filled as latter
  for (int i=1; i <=20; i++) {
    ASSERT_GE(disk_id_ctr[i], 1);
  }

  // Check SITE2 ctr, all disks would've been scheduled twice, initial disks twice often as the others

  for (int i=21; i <=30; i++) {
    ASSERT_GE(disk_id_ctr[i],2);
  }
}

TEST_F(SimpleClusterF, TLRoundRobinBasicLoop)
{
  eos::mgm::placement::RoundRobinPlacement rr_placement(eos::mgm::placement::PlacementStrategyT::kThreadLocalRoundRobin,
                                                        256);

  auto cluster_data_ptr = mgr.getClusterData();

  std::map<int32_t,uint32_t> site_id_ctr;
  std::map<int32_t,uint32_t> group_id_ctr;
  std::map<int32_t,uint32_t> disk_id_ctr;
  std::vector<int32_t> disk_ids_vec;
  // TODO: write a higher level function to do recursive descent
  // Choose 1 site - from ROOT
  // Loop over 30 times, which is the total size of the disks to ensure that all
  // elements are chosen
  for (int i = 0; i < 30; i++)
  {
    auto res = rr_placement.placeFiles(cluster_data_ptr(), {0, 1});

    ASSERT_TRUE(res);
    ASSERT_EQ(res.n_replicas, 1);

    site_id_ctr[res.ids[0]]++;

    // Choose 1 group from SITE
    auto site_id = res.ids[0];
    auto group_res = rr_placement.placeFiles(cluster_data_ptr(), {site_id, 1});

    ASSERT_TRUE(group_res);
    ASSERT_EQ(group_res.n_replicas, 1);
    group_id_ctr[group_res.ids[0]]++;


    // choose 2 disks from group!
    auto disks_res =
        rr_placement.placeFiles(cluster_data_ptr(), {group_res.ids[0], 2});

    ASSERT_TRUE(disks_res);
    ASSERT_EQ(disks_res.n_replicas, 2);
    disk_id_ctr[disks_res.ids[0]]++;
    disk_id_ctr[disks_res.ids[1]]++;


    disk_ids_vec.push_back(disks_res.ids[0]);
    disk_ids_vec.push_back(disks_res.ids[1]);

  }

  // SITE1 gets 15 requests, SITE2 gets 15 requests;
  ASSERT_EQ(site_id_ctr[-1], 15);
  ASSERT_EQ(site_id_ctr[-2], 15);


  // 30 items chosen in site1 among 20 disks
  // 30 items chosen in site2 among 10 disks
  ASSERT_EQ(group_id_ctr[-102], 15);
  // All the disks are chosen at least once, due to the non uniform nature here,
  // site 2 would have its disks chosen twice as often as site 1
  ASSERT_EQ(disk_ids_vec.size(), 60);
  ASSERT_EQ(disk_id_ctr.size(), 30);

  // Check SITE1 ctr, atleast 1; initial disks would be twice as filled as latter
  for (int i=1; i <=20; i++) {
    ASSERT_GE(disk_id_ctr[i], 1);
  }

  // Check SITE2 ctr, all disks would've been scheduled twice, initial disks twice often as the others
  for (int i=21; i <=30; i++) {
    ASSERT_GE(disk_id_ctr[i],2);
  }
}


TEST_F(SimpleClusterF, FlatSchedulerBasic)
{
  using eos::mgm::placement::PlacementStrategyT;

  eos::mgm::placement::FlatScheduler flat_scheduler(
      eos::mgm::placement::PlacementStrategyT::kRoundRobin,
                                                    256);

  auto cluster_data_ptr = mgr.getClusterData();

  auto result = flat_scheduler.schedule(cluster_data_ptr(),
                                        {2});
  eos::mgm::placement::PlacementResult expected_result;
  expected_result.ids = {1,2};
  expected_result.ret_code = 0;
  ASSERT_TRUE(result);

  ASSERT_TRUE(result.is_valid_placement(2));
  EXPECT_EQ(result, expected_result);

  auto result2 = flat_scheduler.schedule(cluster_data_ptr(),
                                         {2});
  ASSERT_TRUE(result.is_valid_placement(2));
}



TEST_F(SimpleClusterF, FlatSchedulerBasicLoop)
{
  using eos::mgm::placement::PlacementStrategyT;

  eos::mgm::placement::FlatScheduler flat_scheduler(
      eos::mgm::placement::PlacementStrategyT::kRoundRobin,
                                                    256);

  auto cluster_data_ptr = mgr.getClusterData();

  std::map<int32_t,uint32_t> disk_id_ctr;
  std::vector<int32_t> disk_ids_vec;

  for (int i=0; i <30; ++i) {
    auto result = flat_scheduler.schedule(cluster_data_ptr(),
                                          {2});
    ASSERT_TRUE(result);
    ASSERT_TRUE(result.is_valid_placement(2));
    disk_id_ctr[result.ids[0]]++;
    disk_id_ctr[result.ids[1]]++;
    disk_ids_vec.push_back(result.ids[0]);
    disk_ids_vec.push_back(result.ids[1]);
  }
  // All the disks are chosen at least once, due to the non uniform nature here,
  // site 2 would have its disks chosen twice as often as site 1
  ASSERT_EQ(disk_ids_vec.size(), 60);
  ASSERT_EQ(disk_id_ctr.size(), 30);

  // Check SITE1 ctr, atleast 1; initial disks would be twice as filled as latter
  for (int i=1; i <=20; i++) {
    ASSERT_GE(disk_id_ctr[i], 1);
  }

  // Check SITE2 ctr, all disks would've been scheduled twice,
  // initial disks twice often as the others

  for (int i=21; i <=30; i++) {
    ASSERT_GE(disk_id_ctr[i],2);
  }

}

TEST_F(SimpleClusterF, TLFlatSchedulerBasicLoop)
{
  using eos::mgm::placement::PlacementStrategyT;

  eos::mgm::placement::FlatScheduler flat_scheduler(
      eos::mgm::placement::PlacementStrategyT::kThreadLocalRoundRobin,
                                                    256);

  auto cluster_data_ptr = mgr.getClusterData();

  std::map<int32_t,uint32_t> disk_id_ctr;
  std::vector<int32_t> disk_ids_vec;

  for (int i=0; i <30; ++i) {
    auto result = flat_scheduler.schedule(cluster_data_ptr(),
                                          {2});
    ASSERT_TRUE(result);
    ASSERT_TRUE(result.is_valid_placement(2));
    disk_id_ctr[result.ids[0]]++;
    disk_id_ctr[result.ids[1]]++;
    disk_ids_vec.push_back(result.ids[0]);
    disk_ids_vec.push_back(result.ids[1]);
  }
  // All the disks are chosen at least once, due to the non uniform nature here,
  // site 2 would have its disks chosen twice as often as site 1
  ASSERT_EQ(disk_ids_vec.size(), 60);
  ASSERT_EQ(disk_id_ctr.size(), 30);

  // Check SITE1 ctr, atleast 1; initial disks would be twice as filled as latter
  for (int i=1; i <=20; i++) {
    ASSERT_GE(disk_id_ctr[i], 1);
  }

  // Check SITE2 ctr, all disks would've been scheduled twice,
  // initial disks twice often as the others

  for (int i=21; i <=30; i++) {
    ASSERT_GE(disk_id_ctr[i],2);
  }

}


TEST(FlatScheduler, SingleSite)
{
  using namespace eos::mgm::placement;
  ClusterMgr mgr;
  using eos::mgm::placement::PlacementStrategyT;

  eos::mgm::placement::FlatScheduler flat_scheduler(PlacementStrategyT::kRoundRobin,
                                                    2048);

  {
    auto sh = mgr.getStorageHandler(1024);
    ASSERT_TRUE(sh.addBucket(get_bucket_type(StdBucketType::ROOT), 0));
    ASSERT_TRUE(sh.addBucket(get_bucket_type(StdBucketType::SITE), -1, 0));
    ASSERT_TRUE(sh.addBucket(get_bucket_type(StdBucketType::GROUP), -100, -1));

    ASSERT_TRUE(sh.addDisk(Disk(1, ConfigStatus::kRW, ActiveStatus::kOnline, 1), -100));
    ASSERT_TRUE(sh.addDisk(Disk(2, ConfigStatus::kRW, ActiveStatus::kOnline, 1), -100));
    ASSERT_TRUE(sh.addDisk(Disk(3, ConfigStatus::kRW, ActiveStatus::kOnline, 1), -100));
    ASSERT_TRUE(sh.addDisk(Disk(4, ConfigStatus::kRW, ActiveStatus::kOnline, 1), -100));
    ASSERT_TRUE(sh.addDisk(Disk(5, ConfigStatus::kRW, ActiveStatus::kOnline, 1), -100));
  }

  auto data = mgr.getClusterData();
  std::vector<int32_t> disk_ids_vec {-1};
  std::vector<int32_t> site_ids_vec {-100};
  std::vector<int32_t> group_ids_vec {1,2,3,4,5};
  ASSERT_EQ(data->buckets[0].items, disk_ids_vec);
  ASSERT_EQ(data->buckets[1].items, site_ids_vec);
  ASSERT_EQ(data->buckets[100].items, group_ids_vec);

  auto cluster_data_ptr = mgr.getClusterData();
  auto result = flat_scheduler.schedule(cluster_data_ptr(),
                                        {2});
  std::cout << result.err_msg.value_or("") << ", " << result << std::endl;
  ASSERT_TRUE(result);
  ASSERT_TRUE(result.is_valid_placement(2));
}

TEST(FlatScheduler, TLSingleSite)
{
  using namespace eos::mgm::placement;
  ClusterMgr mgr;
  using eos::mgm::placement::PlacementStrategyT;

  eos::mgm::placement::FlatScheduler flat_scheduler(PlacementStrategyT::kThreadLocalRoundRobin,
                                                    2048);

  {
    auto sh = mgr.getStorageHandler(1024);
    ASSERT_TRUE(sh.addBucket(get_bucket_type(StdBucketType::ROOT), 0));
    ASSERT_TRUE(sh.addBucket(get_bucket_type(StdBucketType::SITE), -1, 0));
    ASSERT_TRUE(sh.addBucket(get_bucket_type(StdBucketType::GROUP), -100, -1));

    ASSERT_TRUE(sh.addDisk(Disk(1, ConfigStatus::kRW, ActiveStatus::kOnline, 1), -100));
    ASSERT_TRUE(sh.addDisk(Disk(2, ConfigStatus::kRW, ActiveStatus::kOnline, 1), -100));
    ASSERT_TRUE(sh.addDisk(Disk(3, ConfigStatus::kRW, ActiveStatus::kOnline, 1), -100));
    ASSERT_TRUE(sh.addDisk(Disk(4, ConfigStatus::kRW, ActiveStatus::kOnline, 1), -100));
    ASSERT_TRUE(sh.addDisk(Disk(5, ConfigStatus::kRW, ActiveStatus::kOnline, 1), -100));
  }

  auto data = mgr.getClusterData();
  std::vector<int32_t> disk_ids_vec {-1};
  std::vector<int32_t> site_ids_vec {-100};
  std::vector<int32_t> group_ids_vec {1,2,3,4,5};
  ASSERT_EQ(data->buckets[0].items, disk_ids_vec);
  ASSERT_EQ(data->buckets[1].items, site_ids_vec);
  ASSERT_EQ(data->buckets[100].items, group_ids_vec);

  auto cluster_data_ptr = mgr.getClusterData();
  auto result = flat_scheduler.schedule(cluster_data_ptr(),
                                        {2});
  std::cout << result.err_msg.value_or("") << std::endl;
  ASSERT_TRUE(result);
  ASSERT_TRUE(result.is_valid_placement(2));
}

TEST(FlatScheduler, TLSingleSiteWeighted)
{
  using namespace eos::mgm::placement;
  ClusterMgr mgr;
  using eos::mgm::placement::PlacementStrategyT;

  eos::mgm::placement::FlatScheduler flat_scheduler(PlacementStrategyT::kWeightedRandom,
                                                    2048);

  {
    auto sh = mgr.getStorageHandler(1024);
    ASSERT_TRUE(sh.addBucket(get_bucket_type(StdBucketType::ROOT), 0));
    ASSERT_TRUE(sh.addBucket(get_bucket_type(StdBucketType::SITE), -1, 0));
    ASSERT_TRUE(sh.addBucket(get_bucket_type(StdBucketType::GROUP), -100, -1));

    ASSERT_TRUE(sh.addDisk(Disk(1, ConfigStatus::kRW, ActiveStatus::kOnline, 1), -100));
    ASSERT_TRUE(sh.addDisk(Disk(2, ConfigStatus::kRW, ActiveStatus::kOnline, 1), -100));
    ASSERT_TRUE(sh.addDisk(Disk(3, ConfigStatus::kRW, ActiveStatus::kOnline, 1), -100));
    ASSERT_TRUE(sh.addDisk(Disk(4, ConfigStatus::kRW, ActiveStatus::kOnline, 1), -100));
    ASSERT_TRUE(sh.addDisk(Disk(5, ConfigStatus::kRW, ActiveStatus::kOnline, 1), -100));
  }

  auto data = mgr.getClusterData();
  std::vector<int32_t> disk_ids_vec {-1};
  std::vector<int32_t> site_ids_vec {-100};
  std::vector<int32_t> group_ids_vec {1,2,3,4,5};
  ASSERT_EQ(data->buckets[0].items, disk_ids_vec);
  ASSERT_EQ(data->buckets[1].items, site_ids_vec);
  ASSERT_EQ(data->buckets[100].items, group_ids_vec);

  auto cluster_data_ptr = mgr.getClusterData();
  auto result = flat_scheduler.schedule(cluster_data_ptr(),
                                        {2});
  std::cout << result.err_msg.value_or("") << std::endl;
  ASSERT_TRUE(result);
  ASSERT_TRUE(result.is_valid_placement(2));
}

TEST(FlatScheduler, TLNoSite)
{
  using namespace eos::mgm::placement;
  eos::mgm::placement::ClusterMgr mgr;
  int n_elements = 1024;
  int n_disks_per_group = 16;
  int n_groups = 32;
  eos::mgm::placement::FlatScheduler flat_scheduler(PlacementStrategyT::kThreadLocalRoundRobin,
                                                    2048);

  {

    auto sh = mgr.getStorageHandler(n_elements);
    ASSERT_TRUE(sh.addBucket(get_bucket_type(StdBucketType::ROOT), 0));
    for (int i=0; i< n_groups; ++i) {
      ASSERT_TRUE(sh.addBucket(get_bucket_type(StdBucketType::GROUP), -100-i, 0));
    }

    for (int i=0; i < n_groups*n_disks_per_group; i++) {
      ASSERT_TRUE(sh.addDisk(Disk(i+1, ConfigStatus::kRW, ActiveStatus::kOnline, 1),
                             -100 - i/n_disks_per_group));
    }

  }
  auto cluster_data = mgr.getClusterData();
  EXPECT_EQ(cluster_data->disks.size(), 32*16);
  EXPECT_EQ(cluster_data->buckets.size(), n_elements);
  auto root_bucket = cluster_data->buckets[0];
  EXPECT_EQ(root_bucket.items.size(), n_groups);
  for (auto it: root_bucket.items) {
    EXPECT_EQ(cluster_data->buckets.at(-it).items.size(), n_disks_per_group);
  }

  for (int i = 0; i < 1000; i++) {
    auto result = flat_scheduler.schedule(cluster_data(), {2});
    ASSERT_TRUE(result);
    ASSERT_TRUE(result.is_valid_placement(2));
  }
}

TEST(FlatScheduler, TLNoSiteUniformWeighted)
{
  using namespace eos::mgm::placement;
  eos::mgm::placement::ClusterMgr mgr;
  int n_elements = 1024;
  int n_disks_per_group = 16;
  int n_groups = 32;
  eos::mgm::placement::FlatScheduler flat_scheduler(PlacementStrategyT::kWeightedRandom,
                                                    2048);

  {

    auto sh = mgr.getStorageHandler(n_elements);
    ASSERT_TRUE(sh.addBucket(get_bucket_type(StdBucketType::ROOT), 0));
    for (int i=0; i< n_groups; ++i) {
      ASSERT_TRUE(sh.addBucket(get_bucket_type(StdBucketType::GROUP), -100-i, 0));
    }

    for (int i=0; i < n_groups*n_disks_per_group; i++) {
      ASSERT_TRUE(sh.addDisk(Disk(i+1, ConfigStatus::kRW, ActiveStatus::kOnline, 1),
                             -100 - i/n_disks_per_group));
    }

  }
  auto cluster_data = mgr.getClusterData();
  EXPECT_EQ(cluster_data->disks.size(), 32*16);
  EXPECT_EQ(cluster_data->buckets.size(), n_elements);
  auto root_bucket = cluster_data->buckets[0];
  EXPECT_EQ(root_bucket.items.size(), n_groups);
  for (auto it: root_bucket.items) {
    EXPECT_EQ(cluster_data->buckets.at(-it).items.size(), n_disks_per_group);
  }

  for (int i = 0; i < 1000; i++) {
    auto result = flat_scheduler.schedule(cluster_data(), {2});
    ASSERT_TRUE(result);
    ASSERT_TRUE(result.is_valid_placement(2));
  }
}


TEST(FlatScheduler, TLNoSiteWeighted)
{
  using namespace eos::mgm::placement;
  eos::mgm::placement::ClusterMgr mgr;
  int n_elements = 1024;
  int n_disks_per_group = 32;
  int n_groups = 32;
  eos::mgm::placement::FlatScheduler flat_scheduler(PlacementStrategyT::kWeightedRandom,
                                                    2048);
  std::map<int, int> disk_wt_map;
  {
    std::vector<int> weights = {4, 8, 12, 22};
    auto sh = mgr.getStorageHandler(n_elements);
    ASSERT_TRUE(sh.addBucket(get_bucket_type(StdBucketType::ROOT), 0));
    for (int i=0; i< n_groups; ++i) {
      ASSERT_TRUE(sh.addBucket(get_bucket_type(StdBucketType::GROUP), -100-i, 0));
    }

    for (int i=0; i < n_groups*n_disks_per_group; i++) {
      auto weight = eos::common::pickIndexRR(weights, i);
      disk_wt_map[i+1] = weight;
      ASSERT_TRUE(sh.addDisk(Disk(i+1, ConfigStatus::kRW, ActiveStatus::kOnline,
                                  weight),
                             -100 - i/n_disks_per_group));
    }

  }
  auto cluster_data = mgr.getClusterData();
  EXPECT_EQ(cluster_data->disks.size(), 32*32);
  EXPECT_EQ(cluster_data->buckets.size(), n_elements);
  auto root_bucket = cluster_data->buckets[0];
  EXPECT_EQ(root_bucket.items.size(), n_groups);
  for (auto it: root_bucket.items) {
    EXPECT_EQ(cluster_data->buckets.at(-it).items.size(), n_disks_per_group);
  }
  std::map<int, int> weight_counter;

  for (int i = 0; i < 10000; i++) {
    auto result = flat_scheduler.schedule(cluster_data(), {2});
    ASSERT_TRUE(result);
    ASSERT_TRUE(result.is_valid_placement(2));
    weight_counter[disk_wt_map[result.ids[0]]]++;
    weight_counter[disk_wt_map[result.ids[1]]]++;
  }
  ASSERT_TRUE(weight_counter[4] < weight_counter[8]);
  ASSERT_TRUE(weight_counter[8] < weight_counter[12]);
  ASSERT_TRUE(weight_counter[12] < weight_counter[22]);

  std::cout << "Disk Weight distribution: " << std::endl;
  for (const auto &kv: weight_counter) {
    std::cout << kv.first << " : " << kv.second << std::endl;
  }
}



TEST(ClusterMap, Concurrency)
{
  using namespace eos::mgm::placement;
  ClusterMgr mgr;
  using eos::mgm::placement::PlacementStrategyT;

  eos::mgm::placement::FlatScheduler flat_scheduler(PlacementStrategyT::kRoundRobin,
                                                    2048);

  {
    auto sh = mgr.getStorageHandler(1024);
    ASSERT_TRUE(sh.addBucket(get_bucket_type(StdBucketType::ROOT), 0));
    ASSERT_TRUE(sh.addBucket(get_bucket_type(StdBucketType::SITE), -1, 0));
    ASSERT_TRUE(sh.addBucket(get_bucket_type(StdBucketType::GROUP), -100, -1));

    ASSERT_TRUE(sh.addDisk(Disk(1, ConfigStatus::kRW, ActiveStatus::kOnline, 1), -100));
    ASSERT_TRUE(sh.addDisk(Disk(2, ConfigStatus::kRW, ActiveStatus::kOnline, 1), -100));
    ASSERT_TRUE(sh.addDisk(Disk(3, ConfigStatus::kRW, ActiveStatus::kOnline, 1), -100));
    ASSERT_TRUE(sh.addDisk(Disk(4, ConfigStatus::kRW, ActiveStatus::kOnline, 1), -100));
    ASSERT_TRUE(sh.addDisk(Disk(5, ConfigStatus::kRW, ActiveStatus::kOnline, 1), -100));
  }

  auto mgr_ptr = &mgr;

  auto add_fn = [mgr_ptr]() {
    for (int i=0; i < 10; i++) {
      std::cout << "Writer thread: " << std::this_thread::get_id() << " ctr"
                << i << std::endl;
      {
        auto sh = mgr_ptr->getStorageHandlerWithData();
        auto group_id = -101 - i;
        std::cout << "Adding group with id=" << group_id << std::endl;
        ASSERT_TRUE(sh.addBucket(get_bucket_type(StdBucketType::GROUP),
                                 group_id, -1));
        for (int k = 0; k < 10; k++) {
          ASSERT_TRUE(sh.addDisk(Disk((i+1)*10 + k + 1, ConfigStatus::kRW, ActiveStatus::kOnline, 1), group_id));
        }
      }
    }
    std::cout << "Done with writer at " << std::this_thread::get_id() << std::endl;

  };



  auto read_fn = [&flat_scheduler, mgr_ptr]() {
    for (int i=0; i < 1000; i++) {
      auto data = mgr_ptr->getClusterData();

      ASSERT_TRUE(data->buckets.size());
      ASSERT_TRUE(data->disks.size());
      auto result = flat_scheduler.schedule(data(), {2});

      ASSERT_TRUE(result);
      ASSERT_TRUE(result.is_valid_placement(2));
    }
    std::cout << "Done with reader at " << std::this_thread::get_id() << std::endl;
  };

  std::vector<std::thread> reader_threads;
  for (int i=0; i<100;i++) {
    reader_threads.emplace_back(read_fn);
  }

  std::vector<std::thread> writer_threads;
  for (int i=0; i < 5; i++) {
    writer_threads.emplace_back(add_fn);
  }

  for (auto& t: writer_threads) {
    t.join();
  }

  for (auto& t: reader_threads) {
    t.join();
  }

}
