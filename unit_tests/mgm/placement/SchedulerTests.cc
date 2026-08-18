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
#include "mgm/placement/FlatScheduler.hh"
#include "mgm/placement/RoundRobinStrategy.hh"
#include "mgm/placement/SelectionStrategy.hh"
#include "mgm/placement/WeightedRandomStrategy.hh"
#include "unit_tests/mgm/placement/ClusterMgrFixture.hh"
#include "gtest/gtest.h"
#include <algorithm>
#include <set>
#include <utility>
using eos::mgm::placement::ItemIdT;

TEST_F(SimpleClusterF, RoundRobinBasic)
{
  eos::mgm::placement::RoundRobinStrategy rr_placement(
      eos::mgm::placement::PlacementStrategyT::kRoundRobin, 256);

  auto cluster_data_ptr = mgr.GetClusterData();

  // TODO: write a higher level function to do recursive descent
  // Choose 1 site - from ROOT
  auto res = rr_placement.Placement(cluster_data_ptr(), {0, 1});
  ASSERT_TRUE(res);
  EXPECT_EQ(res.n_replicas, 1);
  EXPECT_EQ(res.ids[0], -1);

  // Choose 1 group from SITE
  auto site_id = res.ids[0];
  auto group_res = rr_placement.Placement(cluster_data_ptr(), {site_id, 1});
  ASSERT_TRUE(group_res);
  EXPECT_EQ(group_res.n_replicas, 1);


  // choose 2 disks from group!
  auto disks_res = rr_placement.Placement(cluster_data_ptr(), {group_res.ids[0], 2});
  ASSERT_TRUE(disks_res);
  EXPECT_EQ(disks_res.n_replicas, 2);

}

TEST_F(SimpleClusterF, RandomBasic)
{
  eos::mgm::placement::RoundRobinStrategy rand_placement(
      eos::mgm::placement::PlacementStrategyT::kRandom, 256);

  auto cluster_data_ptr = mgr.GetClusterData();

  auto res = rand_placement.Placement(cluster_data_ptr(), {0, 1});
  ASSERT_TRUE(res);
  EXPECT_EQ(res.n_replicas, 1);

  auto site_id = res.ids[0];
  auto group_res = rand_placement.Placement(cluster_data_ptr(), {site_id, 1});
  ASSERT_TRUE(group_res);
  EXPECT_EQ(group_res.n_replicas, 1);

  auto disks_res = rand_placement.Placement(cluster_data_ptr(), {group_res.ids[0], 2});
  ASSERT_TRUE(disks_res);
  std::cout << disks_res << "\n";
  EXPECT_EQ(disks_res.n_replicas, 2);
}

TEST_F(SimpleClusterF, TLRoundRobinBasic)
{
  eos::mgm::placement::RoundRobinStrategy rr_placement(
      eos::mgm::placement::PlacementStrategyT::kThreadLocalRoundRobin, 256);

  auto cluster_data_ptr = mgr.GetClusterData();

  // TODO: write a higher level function to do recursive descent
  // Choose 1 site - from ROOT
  auto res = rr_placement.Placement(cluster_data_ptr(), {0, 1});
  ASSERT_TRUE(res);
  EXPECT_EQ(res.n_replicas, 1);
  // We cannot assert on the id here because the thread local round robin would
  // have a random starting point, only the looping behaviour is easier to reason


  // Choose 1 group from SITE
  auto site_id = res.ids[0];
  auto group_res = rr_placement.Placement(cluster_data_ptr(), {site_id, 1});
  ASSERT_TRUE(group_res);
  EXPECT_EQ(group_res.n_replicas, 1);


  // choose 2 disks from group!
  auto disks_res = rr_placement.Placement(cluster_data_ptr(), {group_res.ids[0], 2});
  ASSERT_TRUE(disks_res);
  EXPECT_EQ(disks_res.n_replicas, 2);

}

TEST_F(SimpleClusterF, RoundRobinBasicLoop)
{
  eos::mgm::placement::RoundRobinStrategy rr_placement(
      eos::mgm::placement::PlacementStrategyT::kRoundRobin, 256);

  auto cluster_data_ptr = mgr.GetClusterData();

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
    auto res = rr_placement.Placement(cluster_data_ptr(), {0, 1});

    ASSERT_TRUE(res);
    ASSERT_EQ(res.n_replicas, 1);

    site_id_ctr[res.ids[0]]++;

    // Choose 1 group from SITE
    auto site_id = res.ids[0];
    auto group_res = rr_placement.Placement(cluster_data_ptr(), {site_id, 1});

    ASSERT_TRUE(group_res);
    ASSERT_EQ(group_res.n_replicas, 1);
    group_id_ctr[group_res.ids[0]]++;


    // choose 2 disks from group!
    auto disks_res = rr_placement.Placement(cluster_data_ptr(), {group_res.ids[0], 2});

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



  // Check SITE1 ctr, at least 1; initial disks would be twice as filled as latter
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
  eos::mgm::placement::RoundRobinStrategy rr_placement(
      eos::mgm::placement::PlacementStrategyT::kThreadLocalRoundRobin, 256);

  auto cluster_data_ptr = mgr.GetClusterData();

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
    auto res = rr_placement.Placement(cluster_data_ptr(), {0, 1});

    ASSERT_TRUE(res);
    ASSERT_EQ(res.n_replicas, 1);

    site_id_ctr[res.ids[0]]++;

    // Choose 1 group from SITE
    auto site_id = res.ids[0];
    auto group_res = rr_placement.Placement(cluster_data_ptr(), {site_id, 1});

    ASSERT_TRUE(group_res);
    ASSERT_EQ(group_res.n_replicas, 1);
    group_id_ctr[group_res.ids[0]]++;


    // choose 2 disks from group!
    auto disks_res = rr_placement.Placement(cluster_data_ptr(), {group_res.ids[0], 2});

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

  // Check SITE1 ctr, at least 1; initial disks would be twice as filled as latter
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

  auto cluster_data_ptr = mgr.GetClusterData();

  auto result = flat_scheduler.Schedule(cluster_data_ptr(), {2});
  eos::mgm::placement::PlacementResult expected_result;
  expected_result.ids = {1,2};
  expected_result.ret_code = 0;
  ASSERT_TRUE(result);

  ASSERT_TRUE(result.IsValidPlacement(2));
  EXPECT_EQ(result, expected_result);

  auto result2 = flat_scheduler.Schedule(cluster_data_ptr(), {2});
  ASSERT_TRUE(result.IsValidPlacement(2));
}



TEST_F(SimpleClusterF, FlatSchedulerBasicLoop)
{
  using eos::mgm::placement::PlacementStrategyT;

  eos::mgm::placement::FlatScheduler flat_scheduler(
      eos::mgm::placement::PlacementStrategyT::kRoundRobin,
                                                    256);

  auto cluster_data_ptr = mgr.GetClusterData();

  std::map<int32_t,uint32_t> disk_id_ctr;
  std::vector<int32_t> disk_ids_vec;

  for (int i=0; i <30; ++i) {
    auto result = flat_scheduler.Schedule(cluster_data_ptr(), {2});
    ASSERT_TRUE(result);
    ASSERT_TRUE(result.IsValidPlacement(2));
    disk_id_ctr[result.ids[0]]++;
    disk_id_ctr[result.ids[1]]++;
    disk_ids_vec.push_back(result.ids[0]);
    disk_ids_vec.push_back(result.ids[1]);
  }
  // All the disks are chosen at least once, due to the non uniform nature here,
  // site 2 would have its disks chosen twice as often as site 1
  ASSERT_EQ(disk_ids_vec.size(), 60);
  ASSERT_EQ(disk_id_ctr.size(), 30);

  // Check SITE1 ctr, at least 1; initial disks would be twice as filled as latter
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

  auto cluster_data_ptr = mgr.GetClusterData();

  std::map<int32_t,uint32_t> disk_id_ctr;
  std::vector<int32_t> disk_ids_vec;

  for (int i=0; i <30; ++i) {
    auto result = flat_scheduler.Schedule(cluster_data_ptr(), {2});
    ASSERT_TRUE(result);
    ASSERT_TRUE(result.IsValidPlacement(2));
    disk_id_ctr[result.ids[0]]++;
    disk_id_ctr[result.ids[1]]++;
    disk_ids_vec.push_back(result.ids[0]);
    disk_ids_vec.push_back(result.ids[1]);
  }
  // All the disks are chosen at least once, due to the non uniform nature here,
  // site 2 would have its disks chosen twice as often as site 1
  ASSERT_EQ(disk_ids_vec.size(), 60);
  ASSERT_EQ(disk_id_ctr.size(), 30);

  // Check SITE1 ctr, at least 1; initial disks would be twice as filled as latter
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
    auto sh = mgr.GetSnapshotBuilder(1024);
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::ROOT), 0));
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::SITE), -1, 0));
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::GROUP), -100, -1));

    ASSERT_TRUE(sh.AddDisk(Disk(1, ConfigStatus::kRW, ActiveStatus::kOnline, 1), -100));
    ASSERT_TRUE(sh.AddDisk(Disk(2, ConfigStatus::kRW, ActiveStatus::kOnline, 1), -100));
    ASSERT_TRUE(sh.AddDisk(Disk(3, ConfigStatus::kRW, ActiveStatus::kOnline, 1), -100));
    ASSERT_TRUE(sh.AddDisk(Disk(4, ConfigStatus::kRW, ActiveStatus::kOnline, 1), -100));
    ASSERT_TRUE(sh.AddDisk(Disk(5, ConfigStatus::kRW, ActiveStatus::kOnline, 1), -100));
  }

  auto data = mgr.GetClusterData();
  std::vector<int32_t> disk_ids_vec {-1};
  std::vector<int32_t> site_ids_vec {-100};
  std::vector<int32_t> group_ids_vec {1,2,3,4,5};
  ASSERT_EQ(data->buckets[0].items, disk_ids_vec);
  ASSERT_EQ(data->buckets[1].items, site_ids_vec);
  ASSERT_EQ(data->buckets[100].items, group_ids_vec);

  auto cluster_data_ptr = mgr.GetClusterData();
  auto result = flat_scheduler.Schedule(cluster_data_ptr(), {2});
  std::cout << result.err_msg.value_or("") << ", " << result << std::endl;
  ASSERT_TRUE(result);
  ASSERT_TRUE(result.IsValidPlacement(2));
}

TEST(FlatScheduler, TLSingleSite)
{
  using namespace eos::mgm::placement;
  ClusterMgr mgr;
  using eos::mgm::placement::PlacementStrategyT;

  eos::mgm::placement::FlatScheduler flat_scheduler(PlacementStrategyT::kThreadLocalRoundRobin,
                                                    2048);

  {
    auto sh = mgr.GetSnapshotBuilder(1024);
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::ROOT), 0));
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::SITE), -1, 0));
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::GROUP), -100, -1));

    ASSERT_TRUE(sh.AddDisk(Disk(1, ConfigStatus::kRW, ActiveStatus::kOnline, 1), -100));
    ASSERT_TRUE(sh.AddDisk(Disk(2, ConfigStatus::kRW, ActiveStatus::kOnline, 1), -100));
    ASSERT_TRUE(sh.AddDisk(Disk(3, ConfigStatus::kRW, ActiveStatus::kOnline, 1), -100));
    ASSERT_TRUE(sh.AddDisk(Disk(4, ConfigStatus::kRW, ActiveStatus::kOnline, 1), -100));
    ASSERT_TRUE(sh.AddDisk(Disk(5, ConfigStatus::kRW, ActiveStatus::kOnline, 1), -100));
  }

  auto data = mgr.GetClusterData();
  std::vector<int32_t> disk_ids_vec {-1};
  std::vector<int32_t> site_ids_vec {-100};
  std::vector<int32_t> group_ids_vec {1,2,3,4,5};
  ASSERT_EQ(data->buckets[0].items, disk_ids_vec);
  ASSERT_EQ(data->buckets[1].items, site_ids_vec);
  ASSERT_EQ(data->buckets[100].items, group_ids_vec);

  auto cluster_data_ptr = mgr.GetClusterData();
  auto result = flat_scheduler.Schedule(cluster_data_ptr(), {2});
  std::cout << result.err_msg.value_or("") << std::endl;
  ASSERT_TRUE(result);
  ASSERT_TRUE(result.IsValidPlacement(2));
}

TEST(FlatScheduler, TLSingleSiteWeighted)
{
  using namespace eos::mgm::placement;
  ClusterMgr mgr;
  using eos::mgm::placement::PlacementStrategyT;
  PlacementStrategyT strategy = PlacementStrategyT::kWeightedRandom;
  eos::mgm::placement::FlatScheduler flat_scheduler(strategy,
                                                    2048);

  {
    auto sh = mgr.GetSnapshotBuilder(1024);
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::ROOT), 0));
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::SITE), -1, 0));
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::GROUP), -100, -1));

    ASSERT_TRUE(sh.AddDisk(Disk(1, ConfigStatus::kRW, ActiveStatus::kOnline, 1), -100));
    ASSERT_TRUE(sh.AddDisk(Disk(2, ConfigStatus::kRW, ActiveStatus::kOnline, 1), -100));
    ASSERT_TRUE(sh.AddDisk(Disk(3, ConfigStatus::kRW, ActiveStatus::kOnline, 1), -100));
    ASSERT_TRUE(sh.AddDisk(Disk(4, ConfigStatus::kRW, ActiveStatus::kOnline, 1), -100));
    ASSERT_TRUE(sh.AddDisk(Disk(5, ConfigStatus::kRW, ActiveStatus::kOnline, 1), -100));
  }

  auto data = mgr.GetClusterData();
  std::vector<int32_t> disk_ids_vec {-1};
  std::vector<int32_t> site_ids_vec {-100};
  std::vector<int32_t> group_ids_vec {1,2,3,4,5};
  ASSERT_EQ(data->buckets[0].items, disk_ids_vec);
  ASSERT_EQ(data->buckets[1].items, site_ids_vec);
  ASSERT_EQ(data->buckets[100].items, group_ids_vec);

  auto cluster_data_ptr = mgr.GetClusterData();
  auto result = flat_scheduler.Schedule(cluster_data_ptr(), {2});
  std::cout << result.err_msg.value_or("") << std::endl;
  ASSERT_TRUE(result);
  ASSERT_TRUE(result.IsValidPlacement(2));
  size_t index {std::numeric_limits<size_t>::max()};
  std::vector<uint32_t> result_vector (result.ids.begin(), result.ids.end());
  AccessArgs access_args{index, strategy, result_vector};
  auto status = flat_scheduler.Access(cluster_data_ptr(), access_args);
  ASSERT_EQ(status, 0);
  // The returned value must be a valid *index* into the input vector, not an
  // fsid: strictly less than size (regression guard for the fsid/index bug).
  ASSERT_LT(index, result_vector.size());
  // The selected entry must be one of the actually-placed replicas (i.e. not a
  // zero-padding slot) and resolve to an online, readable disk.
  auto selected_fsid = result_vector[index];
  ASSERT_GT(selected_fsid, 0u);
  ASSERT_LE(selected_fsid, data->disks.size());
  bool among_replicas = false;
  for (int i = 0; i < result.n_replicas; ++i) {
    if (result.ids[i] == static_cast<ItemIdT>(selected_fsid)) {
      among_replicas = true;
      break;
    }
  }
  ASSERT_TRUE(among_replicas);
  const auto& sel_disk = data->disks[selected_fsid - 1];
  ASSERT_EQ(sel_disk.active_status.load(), ActiveStatus::kOnline);
  ASSERT_GE(sel_disk.config_status.load(), ConfigStatus::kRO);
}

// Regression for 1.6: a flat bucket with more items than the fixed
// MAX_PLACEMENT_ATTEMPTS window must still place when the only usable disks
// lie outside any single window. Two RW disks are put at items[0] and
// items[128] of a 256-item group, a cyclic distance of 128 that no
// 100-consecutive-item window can span; every other disk is kRO and thus not
// a placement candidate. Before the fix the scan gave up after 100
// consecutive items and returned ENOSPC while space existed.
TEST(RoundRobinStrategy, PlacesBeyondAttemptWindowInLargeBucket)
{
  using namespace eos::mgm::placement;
  ClusterMgr mgr;
  constexpr int n_disks = 256;

  {
    auto sh = mgr.GetSnapshotBuilder(n_disks);
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::ROOT), 0));
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::GROUP), -100, 0));

    for (int i = 0; i < n_disks; ++i) {
      // Only the disks at items[0] (fsid 1) and items[128] (fsid 129) are
      // writable, everything else is read-only and cannot take a replica.
      ConfigStatus cs = (i == 0 || i == 128) ? ConfigStatus::kRW : ConfigStatus::kRO;
      ASSERT_TRUE(sh.AddDisk(Disk(i + 1, cs, ActiveStatus::kOnline, 1), -100));
    }
  }

  auto data = mgr.GetClusterData();
  ASSERT_EQ(data->buckets.at(100).items.size(), (size_t)n_disks);

  RoundRobinStrategy rr(PlacementStrategyT::kRoundRobin, n_disks);
  PlacementArgs args(static_cast<ItemIdT>(-100), static_cast<uint8_t>(2));
  auto result = rr.Placement(data(), args);
  ASSERT_TRUE(result) << result.ErrorString();
  ASSERT_TRUE(result.IsValidPlacement(2));
  // The two writable disks are the only valid picks
  EXPECT_TRUE(result.Contains(1));
  EXPECT_TRUE(result.Contains(129));
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

    auto sh = mgr.GetSnapshotBuilder(n_elements);
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::ROOT), 0));
    for (int i=0; i< n_groups; ++i) {
      ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::GROUP), -100 - i, 0));
    }

    for (int i=0; i < n_groups*n_disks_per_group; i++) {
      ASSERT_TRUE(sh.AddDisk(Disk(i + 1, ConfigStatus::kRW, ActiveStatus::kOnline, 1),
                             -100 - i / n_disks_per_group));
    }

  }
  auto cluster_data = mgr.GetClusterData();
  EXPECT_EQ(cluster_data->disks.size(), 32*16);
  EXPECT_EQ(cluster_data->buckets.size(), n_elements);
  auto root_bucket = cluster_data->buckets[0];
  EXPECT_EQ(root_bucket.items.size(), n_groups);
  for (auto it: root_bucket.items) {
    EXPECT_EQ(cluster_data->buckets.at(-it).items.size(), n_disks_per_group);
  }

  for (int i = 0; i < 1000; i++) {
    auto result = flat_scheduler.Schedule(cluster_data(), {2});
    ASSERT_TRUE(result);
    ASSERT_TRUE(result.IsValidPlacement(2));
  }
}

TEST(FlatScheduler, TLNoSiteExcludeFsids)
{
  using namespace eos::mgm::placement;
  eos::mgm::placement::ClusterMgr mgr;
  int n_elements = 1024;
  int n_disks_per_group = 16;
  int n_groups = 32;
  eos::mgm::placement::FlatScheduler flat_scheduler(2048);

  {

    auto sh = mgr.GetSnapshotBuilder(n_elements);
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::ROOT), 0));
    for (int i=0; i< n_groups; ++i) {
      ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::GROUP), -100 - i, 0));
    }

    for (int i=0; i < n_groups*n_disks_per_group; i++) {
      ASSERT_TRUE(sh.AddDisk(Disk(i + 1, ConfigStatus::kRW, ActiveStatus::kOnline, 1),
                             -100 - i / n_disks_per_group));
    }

  }
  auto cluster_data = mgr.GetClusterData();
  EXPECT_EQ(cluster_data->disks.size(), 32*16);
  EXPECT_EQ(cluster_data->buckets.size(), n_elements);
  auto root_bucket = cluster_data->buckets[0];
  EXPECT_EQ(root_bucket.items.size(), n_groups);
  for (auto it: root_bucket.items) {
    EXPECT_EQ(cluster_data->buckets.at(-it).items.size(), n_disks_per_group);
  }
  uint8_t n_replicas = 12;
  for (auto t: {PlacementStrategyT::kWeightedRoundRobin,
                PlacementStrategyT::kRoundRobin,
                PlacementStrategyT::kThreadLocalRoundRobin,
                PlacementStrategyT::kWeightedRandom,
                }) {

    PlacementArgs args{n_replicas};
    args.excludefs = {1};
    args.strategy = t;
    auto strategy_str = eos::mgm::placement::StrategyToStr(t);
    std::cerr << "\nTesting using strategy=" << strategy_str;
    for (int i = 0; i < 10000; i++) {
      if (i%500 == 0) {
        std::cerr << ".";
      }
      auto result = flat_scheduler.Schedule(cluster_data(), args);
      if (!result) {
        std::cerr << "Iteration " << i << " failed: err="
                  << result.err_msg.value_or("")
                  << " strategy=" << strategy_str
                    << " result=" << result << std::endl;
      }

      EXPECT_TRUE(result);
      EXPECT_TRUE(result.IsValidPlacement(n_replicas));

      for (int i = 0; i < n_replicas; ++i) {
        EXPECT_NE(result.ids[i],1);
      }
    }
  }
}

TEST(FlatScheduler, ForcedGroup)
{
  using namespace eos::mgm::placement;
  using namespace eos::mgm::placement;
  eos::mgm::placement::ClusterMgr mgr;
  int n_elements = 1024;
  int n_disks_per_group = 16;
  int n_groups = 32;
  eos::mgm::placement::FlatScheduler flat_scheduler(2048);

  {

    auto sh = mgr.GetSnapshotBuilder(n_elements);
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::ROOT), 0));
    std::vector<ItemIdT> group_ids;

    for (int i=0; i< n_groups; ++i) {
      // Registered by index, which is what a forced group index resolves
      // through; the identifier itself is the builder's business
      const ItemIdT group_id = sh.GetOrAddGroup(i);
      ASSERT_LT(group_id, 0);
      group_ids.push_back(group_id);
    }

    for (int i=0; i < n_groups*n_disks_per_group; i++) {
      ASSERT_TRUE(sh.AddDisk(Disk(i + 1, ConfigStatus::kRW, ActiveStatus::kOnline, 1),
                             group_ids[i / n_disks_per_group]));
    }

  }
  auto cluster_data = mgr.GetClusterData();
  for (int i=0; i<n_groups;i++) {
    for (auto strategy :{PlacementStrategyT::kRoundRobin,
                          PlacementStrategyT::kThreadLocalRoundRobin,
                          PlacementStrategyT::kRandom,
                          PlacementStrategyT::kWeightedRandom,
                          PlacementStrategyT::kWeightedRoundRobin}) {
      PlacementArgs args{2, ConfigStatus::kRW, strategy};
      args.forced_group_index = i;
      auto result = flat_scheduler.Schedule(cluster_data(), args);
      EXPECT_TRUE(result);
      EXPECT_TRUE(result.IsValidPlacement(2));
      auto bucket = cluster_data().buckets.at(-cluster_data().GetGroupBucketId(i));
      auto bucket_contains = [&bucket](int id) {
        return std::find(bucket.items.begin(), bucket.items.end(), id) != bucket.items.end();
      };
      for (int i = 0; i <2; i++) {
        EXPECT_TRUE(bucket_contains(result.ids[i]));
      }
    }
  }
}

TEST(FlatScheduler, ForcedGroupOutofRange)
{
  using namespace eos::mgm::placement;
  using namespace eos::mgm::placement;
  eos::mgm::placement::ClusterMgr mgr;
  int n_elements = 1024;
  int n_disks_per_group = 16;
  int n_groups = 32;
  eos::mgm::placement::FlatScheduler flat_scheduler(2048);

  {

    auto sh = mgr.GetSnapshotBuilder(n_elements);
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::ROOT), 0));
    std::vector<ItemIdT> group_ids;

    for (int i=0; i< n_groups; ++i) {
      // Registered by index, which is what a forced group index resolves
      // through; the identifier itself is the builder's business
      const ItemIdT group_id = sh.GetOrAddGroup(i);
      ASSERT_LT(group_id, 0);
      group_ids.push_back(group_id);
    }

    for (int i=0; i < n_groups*n_disks_per_group; i++) {
      ASSERT_TRUE(sh.AddDisk(Disk(i + 1, ConfigStatus::kRW, ActiveStatus::kOnline, 1),
                             group_ids[i / n_disks_per_group]));
    }

  }
  auto cluster_data = mgr.GetClusterData();
  for (auto strategy :{PlacementStrategyT::kRoundRobin,
                       PlacementStrategyT::kThreadLocalRoundRobin,
                       PlacementStrategyT::kRandom,
                       PlacementStrategyT::kWeightedRandom,
                       PlacementStrategyT::kWeightedRoundRobin}) {
    PlacementArgs args{2, ConfigStatus::kRW, strategy};
    args.forced_group_index = 4000;
    auto result = flat_scheduler.Schedule(cluster_data(), args);

    EXPECT_FALSE(result);
    EXPECT_EQ(result.ErrorString(), "Invalid forced group index");
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

    auto sh = mgr.GetSnapshotBuilder(n_elements);
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::ROOT), 0));
    for (int i=0; i< n_groups; ++i) {
      ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::GROUP), -100 - i, 0));
    }

    for (int i=0; i < n_groups*n_disks_per_group; i++) {
      ASSERT_TRUE(sh.AddDisk(Disk(i + 1, ConfigStatus::kRW, ActiveStatus::kOnline, 1),
                             -100 - i / n_disks_per_group));
    }

  }
  auto cluster_data = mgr.GetClusterData();
  EXPECT_EQ(cluster_data->disks.size(), 32*16);
  EXPECT_EQ(cluster_data->buckets.size(), n_elements);
  auto root_bucket = cluster_data->buckets[0];
  EXPECT_EQ(root_bucket.items.size(), n_groups);
  for (auto it: root_bucket.items) {
    EXPECT_EQ(cluster_data->buckets.at(-it).items.size(), n_disks_per_group);
  }

  for (int i = 0; i < 1000; i++) {
    auto result = flat_scheduler.Schedule(cluster_data(), {2});
    ASSERT_TRUE(result);
    ASSERT_TRUE(result.IsValidPlacement(2));
  }
}

TEST(FlatScheduler, TLNoSiteUniformWeightedRR)
{
  using namespace eos::mgm::placement;
  eos::mgm::placement::ClusterMgr mgr;
  int n_elements = 1024;
  int n_disks_per_group = 16;
  int n_groups = 32;
  eos::mgm::placement::FlatScheduler flat_scheduler(PlacementStrategyT::kWeightedRoundRobin,
                                                    2048);

  {

    auto sh = mgr.GetSnapshotBuilder(n_elements);
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::ROOT), 0));
    for (int i=0; i< n_groups; ++i) {
      ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::GROUP), -100 - i, 0));
    }

    for (int i=0; i < n_groups*n_disks_per_group; i++) {
      ASSERT_TRUE(sh.AddDisk(Disk(i + 1, ConfigStatus::kRW, ActiveStatus::kOnline, 1),
                             -100 - i / n_disks_per_group));
    }

  }
  auto cluster_data = mgr.GetClusterData();
  EXPECT_EQ(cluster_data->disks.size(), 32*16);
  EXPECT_EQ(cluster_data->buckets.size(), n_elements);
  auto root_bucket = cluster_data->buckets[0];
  EXPECT_EQ(root_bucket.items.size(), n_groups);
  for (auto it: root_bucket.items) {
    EXPECT_EQ(cluster_data->buckets.at(-it).items.size(), n_disks_per_group);
  }

  for (int i = 0; i < 1000; i++) {
    auto result = flat_scheduler.Schedule(cluster_data(), {2});
    ASSERT_TRUE(result);
    ASSERT_TRUE(result.IsValidPlacement(2));
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
  std::map<int, int> disk_wt_count;
  {
    std::vector<int> weights = {4, 8, 16, 32};
    auto sh = mgr.GetSnapshotBuilder(n_elements);
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::ROOT), 0));
    for (int i=0; i< n_groups; ++i) {
      ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::GROUP), -100 - i, 0));
    }

    for (int i=0; i < n_groups*n_disks_per_group; i++) {
      auto weight = eos::common::pickIndexRR(weights, i);
      disk_wt_map[i+1] = weight;
      disk_wt_count[weight]++;
      ASSERT_TRUE(
          sh.AddDisk(Disk(i + 1, ConfigStatus::kRW, ActiveStatus::kOnline, weight),
                     -100 - i / n_disks_per_group));
    }

  }
  auto cluster_data = mgr.GetClusterData();
  EXPECT_EQ(cluster_data->disks.size(), 32*32);
  EXPECT_EQ(cluster_data->buckets.size(), n_elements);
  auto root_bucket = cluster_data->buckets[0];
  EXPECT_EQ(root_bucket.items.size(), n_groups);
  for (auto it: root_bucket.items) {
    EXPECT_EQ(cluster_data->buckets.at(-it).items.size(), n_disks_per_group);
  }
  std::map<int, int> weight_counter;

  for (int i = 0; i < 1024; i++) {
    auto result = flat_scheduler.Schedule(cluster_data(), {2});
    ASSERT_TRUE(result);
    ASSERT_TRUE(result.IsValidPlacement(2));
    weight_counter[disk_wt_map[result.ids[0]]]++;
    weight_counter[disk_wt_map[result.ids[1]]]++;
  }
  ASSERT_TRUE(weight_counter[4] < weight_counter[8]);
  ASSERT_TRUE(weight_counter[8] < weight_counter[16]);
  ASSERT_TRUE(weight_counter[16] < weight_counter[32]);
  std::cout << "Cluster Disk weight count: " << std::endl;
  for (const auto &kv: disk_wt_count) {
    std::cout << kv.first << " : " << kv.second << std::endl;
  }

  std::cout << "Scheduling Disk Weight distribution: " << std::endl;
  for (const auto &kv: weight_counter) {
    std::cout << kv.first << " : " << kv.second << std::endl;
  }
}


TEST(FlatScheduler, TLNoSiteWeightedRR)
{
  using namespace eos::mgm::placement;
  eos::mgm::placement::ClusterMgr mgr;
  int n_elements = 1024;
  int n_disks_per_group = 32;
  int n_groups = 32;
  eos::mgm::placement::FlatScheduler flat_scheduler(PlacementStrategyT::kWeightedRoundRobin,
                                                    2048);
  std::map<int, int> disk_wt_map;
  std::map<int, int> disk_wt_count;
  {
    std::vector<int> weights = {4, 8, 16, 32};
    auto sh = mgr.GetSnapshotBuilder(n_elements);
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::ROOT), 0));
    for (int i=0; i< n_groups; ++i) {
      ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::GROUP), -100 - i, 0));
    }

    for (int i=0; i < n_groups*n_disks_per_group; i++) {
      auto weight = eos::common::pickIndexRR(weights, i);
      disk_wt_map[i+1] = weight;
      disk_wt_count[weight]++;
      ASSERT_TRUE(
          sh.AddDisk(Disk(i + 1, ConfigStatus::kRW, ActiveStatus::kOnline, weight),
                     -100 - i / n_disks_per_group));
    }

  }
  auto cluster_data = mgr.GetClusterData();
  EXPECT_EQ(cluster_data->disks.size(), 32*32);
  EXPECT_EQ(cluster_data->buckets.size(), n_elements);
  auto root_bucket = cluster_data->buckets[0];
  EXPECT_EQ(root_bucket.items.size(), n_groups);
  for (auto it: root_bucket.items) {
    EXPECT_EQ(cluster_data->buckets.at(-it).items.size(), n_disks_per_group);
  }
  std::map<int, int> weight_counter;
  // with Interleaved Weighted RR, you'd need at least weight*n_items to show the
  // distribution at lower numbers below the full wt of a category, you'd end up
  // with uniform ie. for the previous case of 1024 schedulings, you'd end up
  // with equal distribution as you've not finished a full round of each weights yet
  for (int i = 0; i < 60*256; i++) {
    auto result = flat_scheduler.Schedule(cluster_data(), {2});
    ASSERT_TRUE(result);
    ASSERT_TRUE(result.IsValidPlacement(2));
    weight_counter[disk_wt_map[result.ids[0]]]++;
    weight_counter[disk_wt_map[result.ids[1]]]++;
  }
  ASSERT_TRUE(weight_counter[4] < weight_counter[8]);
  ASSERT_TRUE(weight_counter[8] < weight_counter[16]);
  ASSERT_TRUE(weight_counter[16] < weight_counter[32]);

  std::cout << "Cluster Disk weight count: " << std::endl;
  for (const auto &kv: disk_wt_count) {
    std::cout << kv.first << " : " << kv.second << std::endl;
  }

  std::cout << "Scheduling Disk Weight distribution: " << std::endl;
  for (const auto &kv: weight_counter) {
    std::cout << kv.first << " : " << kv.second << std::endl;
  }

  // schedule one more for offby1 errors
  ASSERT_TRUE(flat_scheduler.Schedule(cluster_data(), {2}));
}

//------------------------------------------------------------------------------
// Build a cluster of one scheduling group holding one disk per given weight
//------------------------------------------------------------------------------
static void
MakeSkewedGroup(eos::mgm::placement::ClusterMgr& mgr, const std::vector<uint8_t>& weights)
{
  using namespace eos::mgm::placement;
  auto sh = mgr.GetSnapshotBuilder(16);
  ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::ROOT), 0));
  ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::GROUP), -1, 0));

  for (size_t i = 0; i < weights.size(); ++i) {
    ASSERT_TRUE(sh.AddDisk(
        Disk(i + 1, ConfigStatus::kRW, ActiveStatus::kOnline, weights[i]), -1));
  }
}

//------------------------------------------------------------------------------
// A bucket where one disk carries most of the weight of the group still fills
// every request. The walk over the weight space used to probe a fixed lattice
// of positions, one every total_weight / n_replicas, so a disk heavier than
// that spacing took several of the picks of one request and the placement came
// back one replica short - for these weights whatever the cursor was, which no
// retry could recover.
//------------------------------------------------------------------------------
TEST(FlatScheduler, WeightedRRSkewedWeights)
{
  using namespace eos::mgm::placement;

  for (const auto& [weights, n_replicas] :
       std::vector<std::pair<std::vector<uint8_t>, uint8_t>>{{{200, 30, 25}, 3},
                                                             {{200, 55}, 2},
                                                             {{60, 40}, 2},
                                                             {{250, 3, 2}, 3},
                                                             {{100, 1}, 2}}) {
    ClusterMgr mgr;
    MakeSkewedGroup(mgr, weights);
    FlatScheduler flat_scheduler(PlacementStrategyT::kWeightedRoundRobin, 64);
    auto cluster_data = mgr.GetClusterData();
    std::set<ItemIdT> seen;

    for (int i = 0; i < 200; ++i) {
      auto result = flat_scheduler.Schedule(cluster_data(), {n_replicas});
      ASSERT_TRUE(result) << result.ErrorString();
      ASSERT_TRUE(result.IsValidPlacement(n_replicas)) << result.ResultString();

      for (int j = 0; j < result.n_filled; ++j) {
        seen.insert(result.ids[j]);
      }
    }

    // Every disk of the bucket takes replicas, the heavy one does not starve
    // the light ones out of the picks
    EXPECT_EQ(seen.size(), weights.size());
  }
}

//------------------------------------------------------------------------------
// The heavy disks of a skewed bucket take more replicas than the light ones
//------------------------------------------------------------------------------
TEST(FlatScheduler, WeightedRRSkewedProportions)
{
  using namespace eos::mgm::placement;
  ClusterMgr mgr;
  MakeSkewedGroup(mgr, {200, 100, 50, 10});
  FlatScheduler flat_scheduler(PlacementStrategyT::kWeightedRoundRobin, 64);
  auto cluster_data = mgr.GetClusterData();
  std::map<ItemIdT, int> picks;

  for (int i = 0; i < 4000; ++i) {
    auto result = flat_scheduler.Schedule(cluster_data(), {2});
    ASSERT_TRUE(result) << result.ErrorString();
    ASSERT_TRUE(result.IsValidPlacement(2));

    for (int j = 0; j < result.n_filled; ++j) {
      ++picks[result.ids[j]];
    }
  }

  EXPECT_GT(picks[1], picks[2]);
  EXPECT_GT(picks[2], picks[3]);
  EXPECT_GT(picks[3], picks[4]);
}

//------------------------------------------------------------------------------
// Consecutive requests spread over the bucket instead of handing file after
// file the very same disks. The seeder counts items, not weight, so a cursor
// used raw as a position crawls a couple of units per request through a space
// of hundreds and stays inside the same pair of disks for dozens of files.
//------------------------------------------------------------------------------
TEST(FlatScheduler, WeightedRRConsecutiveRequestsSpread)
{
  using namespace eos::mgm::placement;
  ClusterMgr mgr;
  MakeSkewedGroup(mgr, std::vector<uint8_t>(8, 100));
  FlatScheduler flat_scheduler(PlacementStrategyT::kWeightedRoundRobin, 64);
  auto cluster_data = mgr.GetClusterData();
  std::set<std::pair<ItemIdT, ItemIdT>> pairs;
  int n_repeats = 0;
  std::pair<ItemIdT, ItemIdT> previous{0, 0};

  for (int i = 0; i < 100; ++i) {
    auto result = flat_scheduler.Schedule(cluster_data(), {2});
    ASSERT_TRUE(result) << result.ErrorString();
    ASSERT_TRUE(result.IsValidPlacement(2));
    const std::pair<ItemIdT, ItemIdT> pair{std::min(result.ids[0], result.ids[1]),
                                           std::max(result.ids[0], result.ids[1])};
    pairs.insert(pair);
    n_repeats += (pair == previous);
    previous = pair;
  }

  // 8 equal disks hold 28 distinct pairs; the strided walk over a crawling
  // cursor used to hand back 4 of them in runs of 50 identical placements
  EXPECT_GE(pairs.size(), 12u);
  EXPECT_LE(n_repeats, 10);
}

void printProcessMemoryUsage() {
  std::ifstream status_file("/proc/self/status");
  std::string line;
  while (std::getline(status_file, line)) {
    if (line.find("VmRSS") == 0 || line.find("VmSize") == 0) {
      std::cout << line << std::endl;
    }
  }
}

TEST(ClusterMgr, Concurrency)
{
  using namespace eos::mgm::placement;
  ClusterMgr mgr;
  using eos::mgm::placement::PlacementStrategyT;

  std::mutex log_mtx;
  printProcessMemoryUsage();
  eos::mgm::placement::FlatScheduler flat_scheduler(PlacementStrategyT::kRoundRobin,
                                                    2048);

  {
    auto sh = mgr.GetSnapshotBuilder(1024);
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::ROOT), 0));
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::SITE), -1, 0));
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::GROUP), -100, -1));

    ASSERT_TRUE(sh.AddDisk(Disk(1, ConfigStatus::kRW, ActiveStatus::kOnline, 1), -100));
    ASSERT_TRUE(sh.AddDisk(Disk(2, ConfigStatus::kRW, ActiveStatus::kOnline, 1), -100));
    ASSERT_TRUE(sh.AddDisk(Disk(3, ConfigStatus::kRW, ActiveStatus::kOnline, 1), -100));
    ASSERT_TRUE(sh.AddDisk(Disk(4, ConfigStatus::kRW, ActiveStatus::kOnline, 1), -100));
    ASSERT_TRUE(sh.AddDisk(Disk(5, ConfigStatus::kRW, ActiveStatus::kOnline, 1), -100));
  }

  printProcessMemoryUsage();
  auto mgr_ptr = &mgr;

  auto add_fn = [mgr_ptr, &log_mtx]() {
    for (int i=0; i < 10; i++) {
      std::cout << "Writer thread: " << std::this_thread::get_id() << " ctr"
                << i << std::endl;
      {
        auto sh = mgr_ptr->GetSnapshotBuilderWithData();
        auto group_id = -101 - i;
        std::cout << "Adding group with id=" << group_id << std::endl;
        ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::GROUP), group_id, -1));
        for (int k = 0; k < 10; k++) {
          ASSERT_TRUE(sh.AddDisk(
              Disk((i + 1) * 10 + k + 1, ConfigStatus::kRW, ActiveStatus::kOnline, 1),
              group_id));
        }
      }
    }
    {
      std::scoped_lock log_lock(log_mtx);
      printProcessMemoryUsage();
      std::cout << "Done with writer at " << std::this_thread::get_id() << std::endl;
    }
  };



  auto read_fn = [&flat_scheduler, mgr_ptr, &log_mtx]() {
    for (int i=0; i < 1000; i++) {
      auto data = mgr_ptr->GetClusterData();

      ASSERT_TRUE(data->buckets.size());
      ASSERT_TRUE(data->disks.size());
      auto result = flat_scheduler.Schedule(data(), {2});

      ASSERT_TRUE(result);
      ASSERT_TRUE(result.IsValidPlacement(2));
    }
    {
      std::scoped_lock log_lock(log_mtx);
      printProcessMemoryUsage();
      std::cout << "Done with reader at " << std::this_thread::get_id() << std::endl;
    }
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

// Regression for 1.3: a hole in the pre-allocated bucket array must be
// distinguishable from a real GROUP bucket. A default constructed hole carries
// the INVALID sentinel type, GetBucket returns nullptr for it, and a descent
// that starts from a hole id is rejected instead of recursing into a
// nonexistent bucket.
TEST_F(SimpleClusterF, HoleBucketsAreInvalid)
{
  using namespace eos::mgm::placement;
  auto data = mgr.GetClusterData();

  // Index 50 is inside the pre-allocated range but was never added: a hole
  ASSERT_LT((size_t)50, data->buckets.size());
  const auto& hole = data->buckets[50];
  EXPECT_EQ(hole.id, 0);
  EXPECT_EQ(hole.bucket_type, GetBucketType(BucketType::INVALID));
  EXPECT_NE(hole.bucket_type, GetBucketType(BucketType::GROUP))
      << "a hole must never be mistaken for a scheduling group";

  // GetBucket: real buckets resolve, holes / out-of-range / disk ids do not
  ASSERT_NE(data->GetBucket(0), nullptr); // root
  EXPECT_EQ(data->GetBucket(0)->bucket_type, GetBucketType(BucketType::ROOT));
  EXPECT_NE(data->GetBucket(-1), nullptr);   // a site
  EXPECT_NE(data->GetBucket(-100), nullptr); // a group
  EXPECT_EQ(data->GetBucket(-50), nullptr) << "hole in range";
  EXPECT_EQ(data->GetBucket(-5000), nullptr) << "out of range";
  EXPECT_EQ(data->GetBucket(5), nullptr) << "positive ids are disks";

  // GetDisk mirrors it for the fsid range
  EXPECT_NE(data->GetDisk(1), nullptr);
  EXPECT_EQ(data->GetDisk(0), nullptr);
  EXPECT_EQ(data->GetDisk(100000), nullptr);

  // A descent that starts from a hole id must be rejected, not recurse into
  // the nonexistent bucket
  FlatScheduler flat_scheduler(PlacementStrategyT::kRoundRobin, 2048);
  PlacementArgs args(static_cast<ItemIdT>(-50), static_cast<uint8_t>(2));
  EXPECT_FALSE(flat_scheduler.Schedule(data(), args));
}

// Regression for the inverted IsValidBucketId guard in FlatScheduler::Schedule:
// scheduling that starts from an explicit, valid (negative) bucket id must be
// accepted, while genuinely invalid ids (positive disk ids, out-of-range
// negatives) must be rejected cleanly (no crash / exception).
TEST_F(SimpleClusterF, ScheduleFromExplicitBucketId)
{
  using namespace eos::mgm::placement;
  FlatScheduler flat_scheduler(PlacementStrategyT::kRoundRobin, 2048);
  auto data = mgr.GetClusterData();

  // Start directly at group -100 (default placement). Must succeed and pick
  // 2 of that group's disks (ids 1..10).
  PlacementArgs args(static_cast<ItemIdT>(-100), static_cast<uint8_t>(2));
  auto result = flat_scheduler.Schedule(data(), args);
  ASSERT_TRUE(result) << result.ErrorString();
  ASSERT_TRUE(result.IsValidPlacement(2));
  for (int i = 0; i < result.n_replicas; ++i) {
    EXPECT_GE(result.ids[i], 1);
    EXPECT_LE(result.ids[i], 10);
  }

  // A positive id is a disk, not a valid starting bucket -> rejected.
  PlacementArgs disk_args(static_cast<ItemIdT>(5), static_cast<uint8_t>(2));
  EXPECT_FALSE(flat_scheduler.Schedule(data(), disk_args));

  // An out-of-range negative id -> rejected without descending into an
  // out-of-range bucket access.
  PlacementArgs oor_args(static_cast<ItemIdT>(-300), static_cast<uint8_t>(2));
  EXPECT_FALSE(flat_scheduler.Schedule(data(), oor_args));
}

//------------------------------------------------------------------------------
// Phase 1 regression tests: the access path must honour its constraints and
// report failure instead of silently handing back a stale index.
//------------------------------------------------------------------------------
class AccessClusterF : public ::testing::Test {
protected:
  void
  SetUp() override
  {
    using namespace eos::mgm::placement;
    auto sh = mMgr.GetSnapshotBuilder(64);
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::ROOT), 0));
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::GROUP), -10, 0));

    for (int i = 1; i <= 4; ++i) {
      ASSERT_TRUE(sh.AddDisk(
          eos::mgm::placement::Disk(i, ConfigStatus::kRW, ActiveStatus::kOnline, 1),
          -10));
    }
  }

  eos::mgm::placement::ClusterMgr mMgr;
  //! Replicas of the file under test, in location order
  const std::vector<uint32_t> mLocations{1, 2, 3, 4};
};

TEST_F(AccessClusterF, AccessSkipsOfflineReplicas)
{
  using namespace eos::mgm::placement;
  FlatScheduler flat_scheduler(PlacementStrategyT::kRoundRobin, 2048);
  auto data = mMgr.GetClusterData();
  // Take every replica but fsid 3 offline
  for (eos::mgm::placement::fsid_t id : {1u, 2u, 4u}) {
    ASSERT_TRUE(mMgr.SetDiskStatus(id, ActiveStatus::kOffline));
  }

  size_t index = 99;
  AccessArgs args(index, PlacementStrategyT::kRoundRobin, mLocations);
  ASSERT_EQ(flat_scheduler.Access(data(), args), 0);
  // The only online replica sits at position 2
  EXPECT_EQ(index, 2u);
}

TEST_F(AccessClusterF, AccessFailsWhenAllReplicasOffline)
{
  using namespace eos::mgm::placement;
  FlatScheduler flat_scheduler(PlacementStrategyT::kRoundRobin, 2048);
  auto data = mMgr.GetClusterData();

  for (eos::mgm::placement::fsid_t id : {1u, 2u, 3u, 4u}) {
    ASSERT_TRUE(mMgr.SetDiskStatus(id, ActiveStatus::kOffline));
  }

  size_t index = 99;
  AccessArgs args(index, PlacementStrategyT::kRoundRobin, mLocations);
  // Fewer replicas are reachable than the one required, so the request cannot
  // be served; it reports ENETUNREACH rather than pretending to have picked one
  EXPECT_EQ(flat_scheduler.Access(data(), args), ENETUNREACH);
  EXPECT_EQ(index, 99u);
}

TEST_F(AccessClusterF, RainFlagsDownStripesAndKeepsEnough)
{
  using namespace eos::mgm::placement;
  FlatScheduler flat_scheduler(PlacementStrategyT::kRoundRobin, 2048);
  auto data = mMgr.GetClusterData();
  // One stripe of the four is down, a 3-of-4 layout can still be read
  ASSERT_TRUE(mMgr.SetDiskStatus(2u, ActiveStatus::kOffline));

  std::vector<uint32_t> unavail;
  size_t index = 99;
  AccessArgs args(index, 0, PlacementStrategyT::kRoundRobin, "", &unavail, mLocations);
  args.n_replicas = 3;
  ASSERT_EQ(flat_scheduler.Access(data(), args), 0);
  // The down stripe is reported so the RAIN driver routes around it, and the
  // chosen head is never that stripe
  EXPECT_NE(index, 1u) << "the offline stripe must not be selected";
  EXPECT_EQ(unavail.size(), 1u);
  EXPECT_EQ(unavail.front(), 2u);
}

TEST_F(AccessClusterF, RainFailsWhenTooFewStripes)
{
  using namespace eos::mgm::placement;
  FlatScheduler flat_scheduler(PlacementStrategyT::kRoundRobin, 2048);
  auto data = mMgr.GetClusterData();
  // Two of the four stripes are down, a 3-of-4 layout can no longer be served
  ASSERT_TRUE(mMgr.SetDiskStatus(2u, ActiveStatus::kOffline));
  ASSERT_TRUE(mMgr.SetDiskStatus(3u, ActiveStatus::kOffline));

  std::vector<uint32_t> unavail;
  size_t index = 99;
  AccessArgs args(index, 0, PlacementStrategyT::kRoundRobin, "", &unavail, mLocations);
  args.n_replicas = 3;
  EXPECT_EQ(flat_scheduler.Access(data(), args), ENETUNREACH);
  EXPECT_EQ(index, 99u) << "nothing may be selected when the layout cannot be read";
  // Both down stripes are still reported, even though the request failed
  EXPECT_EQ(unavail.size(), 2u);
}

TEST_F(AccessClusterF, RainDoesNotDuplicateAlreadyFlaggedStripes)
{
  using namespace eos::mgm::placement;
  FlatScheduler flat_scheduler(PlacementStrategyT::kRoundRobin, 2048);
  auto data = mMgr.GetClusterData();
  ASSERT_TRUE(mMgr.SetDiskStatus(2u, ActiveStatus::kOffline));
  // fsid 2 is already flagged, e.g. from the tried-hosts translation
  std::vector<uint32_t> unavail{2};
  size_t index = 99;
  AccessArgs args(index, 0, PlacementStrategyT::kRoundRobin, "", &unavail, mLocations);
  args.n_replicas = 2;
  ASSERT_EQ(flat_scheduler.Access(data(), args), 0);
  EXPECT_EQ(std::count(unavail.begin(), unavail.end(), 2u), 1)
      << "an already flagged stripe must not be added twice";
}

TEST_F(AccessClusterF, ForcedFsidThatIsDownIsUnreachable)
{
  using namespace eos::mgm::placement;
  FlatScheduler flat_scheduler(PlacementStrategyT::kRoundRobin, 2048);
  auto data = mMgr.GetClusterData();
  ASSERT_TRUE(mMgr.SetDiskStatus(2u, ActiveStatus::kOffline));
  // Enough other stripes are up, but the client insisted on the one that is down
  std::vector<uint32_t> unavail;
  size_t index = 99;
  AccessArgs args(index, 0, PlacementStrategyT::kRoundRobin, "", &unavail, mLocations);
  args.forcedfsid = 2;
  args.n_replicas = 1;
  EXPECT_EQ(flat_scheduler.Access(data(), args), ENETUNREACH);
  EXPECT_EQ(index, 99u);
}

TEST_F(AccessClusterF, AccessHonoursForcedFsid)
{
  using namespace eos::mgm::placement;
  FlatScheduler flat_scheduler(PlacementStrategyT::kRoundRobin, 2048);
  auto data = mMgr.GetClusterData();

  for (int trial = 0; trial < 20; ++trial) {
    size_t index = 99;
    AccessArgs args(index, PlacementStrategyT::kRoundRobin, mLocations);
    args.forcedfsid = 4;
    ASSERT_EQ(flat_scheduler.Access(data(), args), 0);
    EXPECT_EQ(index, 3u) << "forced fsid 4 lives at position 3";
  }
}

TEST_F(AccessClusterF, AccessHonoursUnavailAndExclude)
{
  using namespace eos::mgm::placement;
  FlatScheduler flat_scheduler(PlacementStrategyT::kRoundRobin, 2048);
  auto data = mMgr.GetClusterData();
  std::vector<uint32_t> unavail{1, 2};
  const std::vector<uint32_t> exclude{4};

  for (int trial = 0; trial < 20; ++trial) {
    size_t index = 99;
    AccessArgs args(index, 0, PlacementStrategyT::kRoundRobin, "", &unavail, mLocations,
                    &exclude);
    ASSERT_EQ(flat_scheduler.Access(data(), args), 0);
    EXPECT_EQ(index, 2u) << "only fsid 3 survives unavail{1,2} + exclude{4}";
  }
}

TEST_F(AccessClusterF, AccessForWriteSkipsReadOnlyReplicas)
{
  using namespace eos::mgm::placement;
  FlatScheduler flat_scheduler(PlacementStrategyT::kRoundRobin, 2048);
  auto data = mMgr.GetClusterData();

  // Everything but fsid 3 is read-only or draining, which a read is happy with
  // but a write is not
  ASSERT_TRUE(mMgr.SetDiskStatus(1u, ConfigStatus::kRO));
  ASSERT_TRUE(mMgr.SetDiskStatus(2u, ConfigStatus::kDrain));
  ASSERT_TRUE(mMgr.SetDiskStatus(4u, ConfigStatus::kRO));

  for (int trial = 0; trial < 20; ++trial) {
    size_t index = 99;
    AccessArgs args(index, PlacementStrategyT::kRoundRobin, mLocations);
    args.status = ConfigStatus::kRW;
    ASSERT_EQ(flat_scheduler.Access(data(), args), 0);
    EXPECT_EQ(index, 2u) << "a write must land on the only writable replica";
  }

  // The same file systems still serve a read
  size_t index = 99;
  AccessArgs ro_args(index, PlacementStrategyT::kRoundRobin, mLocations);
  ro_args.status = ConfigStatus::kRO;
  ASSERT_EQ(flat_scheduler.Access(data(), ro_args), 0);
}

TEST_F(AccessClusterF, AccessForWriteFailsWhenNothingIsWritable)
{
  using namespace eos::mgm::placement;
  FlatScheduler flat_scheduler(PlacementStrategyT::kWeightedRandom, 2048);
  auto data = mMgr.GetClusterData();

  for (eos::mgm::placement::fsid_t id : {1u, 2u, 3u, 4u}) {
    ASSERT_TRUE(mMgr.SetDiskStatus(id, ConfigStatus::kRO));
  }

  size_t index = 99;
  AccessArgs args(index, PlacementStrategyT::kWeightedRandom, mLocations);
  args.status = ConfigStatus::kRW;
  // Better to fail here than to send the client to a file system that will
  // refuse the write at the FST; no writable stripe is left, so ENETUNREACH
  EXPECT_EQ(flat_scheduler.Access(data(), args), ENETUNREACH);
  EXPECT_EQ(index, 99u);
}

TEST_F(AccessClusterF, WeightedAccessHonoursForcedFsid)
{
  // The weighted access path used to ignore forcedfsid and send the client to
  // whichever replica the HRW score preferred
  using namespace eos::mgm::placement;
  FlatScheduler flat_scheduler(PlacementStrategyT::kWeightedRandom, 2048);
  auto data = mMgr.GetClusterData();

  for (int trial = 0; trial < 20; ++trial) {
    size_t index = 99;
    AccessArgs args(index, PlacementStrategyT::kWeightedRandom, mLocations);
    args.forcedfsid = 4;
    ASSERT_EQ(flat_scheduler.Access(data(), args), 0);
    EXPECT_EQ(index, 3u) << "forced fsid 4 lives at position 3";
  }

  // A forced file system that does not hold a replica cannot be honoured
  size_t index = 99;
  AccessArgs args(index, PlacementStrategyT::kWeightedRandom, mLocations);
  args.forcedfsid = 7;
  EXPECT_EQ(flat_scheduler.Access(data(), args), ENOENT);
  EXPECT_EQ(index, 99u);
}

TEST_F(AccessClusterF, WeightedAccessHonoursExclude)
{
  // The weighted access path used to ignore the exclusion list as well
  using namespace eos::mgm::placement;
  FlatScheduler flat_scheduler(PlacementStrategyT::kWeightedRandom, 2048);
  auto data = mMgr.GetClusterData();
  // Find the replica the HRW score prefers, then exclude it
  size_t index = 99;
  AccessArgs args(index, PlacementStrategyT::kWeightedRandom, mLocations);
  ASSERT_EQ(flat_scheduler.Access(data(), args), 0);
  const uint32_t winner = mLocations[index];
  const std::vector<uint32_t> exclude{winner};
  size_t other_index = 99;
  AccessArgs excl_args(other_index, 0, PlacementStrategyT::kWeightedRandom, "", nullptr,
                       mLocations, &exclude);
  ASSERT_EQ(flat_scheduler.Access(data(), excl_args), 0);
  EXPECT_NE(mLocations[other_index], winner) << "the excluded replica was chosen";

  // Excluding every replica leaves nothing to serve the request
  const std::vector<uint32_t> all(mLocations);
  size_t none_index = 99;
  AccessArgs none_args(none_index, 0, PlacementStrategyT::kWeightedRandom, "", nullptr,
                       mLocations, &all);
  EXPECT_EQ(flat_scheduler.Access(data(), none_args), ENOENT);
  EXPECT_EQ(none_index, 99u);
}

TEST_F(AccessClusterF, WeightedAccessServesZeroWeightReplicas)
{
  // A zero weight stops a disk from attracting new files, it must not make the
  // existing ones unreadable - the weighted access path used to skip such
  // disks and reported ENOENT when every replica sat on one
  using namespace eos::mgm::placement;
  FlatScheduler flat_scheduler(PlacementStrategyT::kWeightedRandom, 2048);
  auto data = mMgr.GetClusterData();

  for (eos::mgm::placement::fsid_t id : {1u, 2u, 3u, 4u}) {
    ASSERT_TRUE(mMgr.SetDiskWeight(id, 0));
  }

  size_t index = 99;
  AccessArgs args(index, PlacementStrategyT::kWeightedRandom, mLocations);
  EXPECT_EQ(flat_scheduler.Access(data(), args), 0);
  EXPECT_LT(index, mLocations.size());
}

TEST_F(AccessClusterF, AccessDefaultsToReadOnly)
{
  using namespace eos::mgm::placement;
  FlatScheduler flat_scheduler(PlacementStrategyT::kRoundRobin, 2048);
  auto data = mMgr.GetClusterData();
  ASSERT_TRUE(mMgr.SetDiskStatus(1u, ConfigStatus::kRO));
  ASSERT_TRUE(mMgr.SetDiskStatus(2u, ConfigStatus::kRO));
  ASSERT_TRUE(mMgr.SetDiskStatus(3u, ConfigStatus::kRO));
  ASSERT_TRUE(mMgr.SetDiskStatus(4u, ConfigStatus::kRO));

  size_t index = 99;
  AccessArgs args(index, PlacementStrategyT::kRoundRobin, mLocations);
  EXPECT_EQ(flat_scheduler.Access(data(), args), 0);
}

TEST_F(AccessClusterF, AccessIgnoresTheFillLevelOfAReplica)
{
  using namespace eos::mgm::placement;
  FlatScheduler flat_scheduler(PlacementStrategyT::kRoundRobin, 2048);
  auto data = mMgr.GetClusterData();

  // An existing replica is readable however full its disk is - the free space
  // check belongs to placement only
  for (eos::mgm::placement::fsid_t id : {1u, 2u, 3u, 4u}) {
    ASSERT_TRUE(mMgr.SetDiskPercentUsed(id, 100));
  }

  size_t index = 99;
  AccessArgs args(index, PlacementStrategyT::kRoundRobin, mLocations);
  EXPECT_EQ(flat_scheduler.Access(data(), args), 0);
}

TEST_F(AccessClusterF, AccessOnEmptyLocationsFails)
{
  using namespace eos::mgm::placement;
  FlatScheduler flat_scheduler(PlacementStrategyT::kRoundRobin, 2048);
  auto data = mMgr.GetClusterData();
  const std::vector<uint32_t> empty;
  size_t index = 99;
  AccessArgs args(index, PlacementStrategyT::kRoundRobin, empty);
  EXPECT_EQ(flat_scheduler.Access(data(), args), ENOENT);
}

TEST_F(AccessClusterF, WeightedPlacementReportsPartialAsEnospc)
{
  using namespace eos::mgm::placement;
  FlatScheduler flat_scheduler(PlacementStrategyT::kWeightedRandom, 2048);
  auto data = mMgr.GetClusterData();

  // Only one disk can take a replica, but three are requested
  for (eos::mgm::placement::fsid_t id : {2u, 3u, 4u}) {
    ASSERT_TRUE(mMgr.SetDiskStatus(id, ActiveStatus::kOffline));
  }

  PlacementArgs args(static_cast<ItemIdT>(-10), static_cast<uint8_t>(3));
  args.strategy = PlacementStrategyT::kWeightedRandom;
  auto result = flat_scheduler.Schedule(data(), args);
  EXPECT_FALSE(result);
  EXPECT_EQ(result.ret_code, ENOSPC);
  EXPECT_FALSE(result.ErrorString().empty()) << "a partial placement must say why";
}

TEST(ClusterMgr, SelfParentedBucketIsRejected)
{
  using namespace eos::mgm::placement;
  ClusterMgr mgr;
  {
    auto sh = mgr.GetSnapshotBuilder(64);
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::ROOT), 0));
    // A bucket parented to itself would create a cycle in the hierarchy
    sh.AddBucket(GetBucketType(BucketType::GROUP), -5, -5);
  }

  auto data = mgr.GetClusterData();
  const auto& bucket = data->buckets.at(5);
  EXPECT_TRUE(std::find(bucket.items.begin(), bucket.items.end(), -5) ==
              bucket.items.end())
      << "bucket -5 must not be its own child";
}

//------------------------------------------------------------------------------
// Phase 3: capacity and fill feedback. A filling disk must lose weight and
// eventually stop attracting replicas altogether.
//------------------------------------------------------------------------------
TEST(EffectiveWeight, DecaysWithFillLevel)
{
  using namespace eos::mgm::placement;
  Disk disk(1, ConfigStatus::kRW, ActiveStatus::kOnline, 100);
  const FillLimits limits;
  // Below the warning level the capacity weight is untouched
  disk.percent_used.store(0);
  EXPECT_EQ(GetEffectiveWeight(disk, limits), 100u);
  disk.percent_used.store(kDefaultFillWarnPercent);
  EXPECT_EQ(GetEffectiveWeight(disk, limits), 100u);

  // Inside the decay band it drops monotonically but stays usable
  uint32_t previous = GetEffectiveWeight(disk, limits);

  for (uint8_t used = kDefaultFillWarnPercent + 1; used < kDefaultFillCapPercent;
       ++used) {
    disk.percent_used.store(used);
    const uint32_t current = GetEffectiveWeight(disk, limits);
    EXPECT_LT(current, previous) << "weight must fall as used=" << (int)used;
    EXPECT_GT(current, 0u) << "still usable below the cap";
    previous = current;
  }

  // At and above the cap the disk takes no further replicas
  disk.percent_used.store(kDefaultFillCapPercent);
  EXPECT_EQ(GetEffectiveWeight(disk, limits), 0u);
  disk.percent_used.store(100);
  EXPECT_EQ(GetEffectiveWeight(disk, limits), 0u);
}

TEST(EffectiveWeight, ZeroCapacityStaysZero)
{
  using namespace eos::mgm::placement;
  Disk disk(1, ConfigStatus::kRW, ActiveStatus::kOnline, 0);
  disk.percent_used.store(0);
  EXPECT_EQ(GetEffectiveWeight(disk, FillLimits{}), 0u);
}

TEST(EffectiveWeight, HonoursConfiguredLimits)
{
  using namespace eos::mgm::placement;
  Disk disk(1, ConfigStatus::kRW, ActiveStatus::kOnline, 100);
  FillLimits limits;
  limits.cap.store(60);
  limits.warn.store(40);
  // The decay band moved: untouched at the new warning level, zero at the
  // new cap, decayed in between - all far below the compile time defaults
  disk.percent_used.store(40);
  EXPECT_EQ(GetEffectiveWeight(disk, limits), 100u);
  disk.percent_used.store(50);
  const uint32_t decayed = GetEffectiveWeight(disk, limits);
  EXPECT_GT(decayed, 0u);
  EXPECT_LT(decayed, 100u);
  disk.percent_used.store(60);
  EXPECT_EQ(GetEffectiveWeight(disk, limits), 0u);
}

TEST(FlatScheduler, FullDisksStopAttractingReplicas)
{
  using namespace eos::mgm::placement;
  ClusterMgr mgr;
  FlatScheduler flat_scheduler(PlacementStrategyT::kWeightedRandom, 2048);
  {
    auto sh = mgr.GetSnapshotBuilder(64);
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::ROOT), 0));
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::GROUP), -10, 0));

    for (int i = 1; i <= 4; ++i) {
      ASSERT_TRUE(sh.AddDisk(Disk(i, ConfigStatus::kRW, ActiveStatus::kOnline, 10), -10));
    }
  }

  // Fill disks 1 and 2 past the cap; they must never be selected again
  ASSERT_TRUE(mgr.SetDiskPercentUsed(1, 99));
  ASSERT_TRUE(mgr.SetDiskPercentUsed(2, kDefaultFillCapPercent));
  auto data = mgr.GetClusterData();

  for (int i = 0; i < 200; ++i) {
    PlacementArgs args(static_cast<ItemIdT>(-10), static_cast<uint8_t>(2));
    args.strategy = PlacementStrategyT::kWeightedRandom;
    args.fid = i + 1;
    auto result = flat_scheduler.Schedule(data(), args);
    ASSERT_TRUE(result) << result.ErrorString();

    for (int r = 0; r < result.n_filled; ++r) {
      EXPECT_GE(result.ids[r], 3) << "disks 1 and 2 are above the fill cap";
    }
  }
}

TEST(FlatScheduler, FillLevelSkewsThePlacementDistribution)
{
  using namespace eos::mgm::placement;
  ClusterMgr mgr;
  FlatScheduler flat_scheduler(PlacementStrategyT::kWeightedRandom, 2048);
  {
    auto sh = mgr.GetSnapshotBuilder(64);
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::ROOT), 0));
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::GROUP), -10, 0));

    for (int i = 1; i <= 4; ++i) {
      ASSERT_TRUE(
          sh.AddDisk(Disk(i, ConfigStatus::kRW, ActiveStatus::kOnline, 100), -10));
    }
  }

  // Identical capacity, but disk 1 sits inside the decay band
  ASSERT_TRUE(mgr.SetDiskPercentUsed(1, kDefaultFillCapPercent - 1));
  auto data = mgr.GetClusterData();
  std::map<ItemIdT, int> picks;

  for (int i = 0; i < 2000; ++i) {
    PlacementArgs args(static_cast<ItemIdT>(-10), static_cast<uint8_t>(1));
    args.strategy = PlacementStrategyT::kWeightedRandom;
    args.fid = i + 1;
    auto result = flat_scheduler.Schedule(data(), args);
    ASSERT_TRUE(result) << result.ErrorString();
    picks[result.ids[0]]++;
  }

  // The nearly full disk must be picked far less often than its equally sized
  // but empty peers
  EXPECT_LT(picks[1], picks[2] / 4);
  EXPECT_LT(picks[1], picks[3] / 4);
  EXPECT_LT(picks[1], picks[4] / 4);
}
