// ----------------------------------------------------------------------
// File: ClusterMapTests
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


#include "mgm/placement/ClusterMap.hh"
#include "gtest/gtest.h"

TEST(ClusterMgr, default)
{
  eos::mgm::placement::ClusterMgr mgr;
  EXPECT_EQ(mgr.getCurrentEpoch(), 0);
  EXPECT_EQ(mgr.getClusterData(), nullptr);
}

TEST(ClusterMgr, addDummyData)
{
  eos::mgm::placement::ClusterMgr mgr;
  eos::mgm::placement::ClusterData data;
  mgr.addClusterData(std::move(data));
  EXPECT_EQ(mgr.getCurrentEpoch(), 1);
  EXPECT_NE(mgr.getClusterData(), nullptr);
  auto d = mgr.getClusterData();
  EXPECT_EQ(d->buckets.size(), 0);
  EXPECT_EQ(d->disks.size(), 0);
}

TEST(ClusterMgr, addDummyDataTwice)
{
  eos::mgm::placement::ClusterMgr mgr;
  eos::mgm::placement::ClusterData data1, data2;
  mgr.addClusterData(std::move(data1));
  mgr.addClusterData(std::move(data2));
  EXPECT_EQ(mgr.getCurrentEpoch(), 2);
  EXPECT_NE(mgr.getClusterData(), nullptr);
  auto d = mgr.getClusterData();
  EXPECT_EQ(d->buckets.size(), 0);
  EXPECT_EQ(d->disks.size(), 0);
}

TEST(ClusterMgr, StorageHandlerSeq)
{

  using namespace eos::mgm::placement;
  eos::mgm::placement::ClusterMgr mgr;
  {
    auto sh = mgr.getStorageHandler();
    ASSERT_TRUE(sh.addBucket(get_bucket_type(StdBucketType::ROOT), 0));
    ASSERT_TRUE(sh.addBucket(get_bucket_type(StdBucketType::SITE), -1, 0));
    ASSERT_TRUE(sh.addBucket(get_bucket_type(StdBucketType::SITE), -2, 0));
    ASSERT_TRUE(sh.addBucket(get_bucket_type(StdBucketType::GROUP), -100, -1));
    ASSERT_TRUE(sh.addBucket(get_bucket_type(StdBucketType::GROUP), -101, -1));
    ASSERT_TRUE(sh.addBucket(get_bucket_type(StdBucketType::GROUP), -102, -2));

    ASSERT_TRUE(sh.addDiskSequential(Disk(1), -100));
    ASSERT_TRUE(sh.addDiskSequential(Disk(2), -100));
    ASSERT_TRUE(sh.addDiskSequential(Disk(3), -101));
    ASSERT_TRUE(sh.addDiskSequential(Disk(4), -101));
    ASSERT_TRUE(sh.addDiskSequential(Disk(5), -102));
  }

  ASSERT_EQ(mgr.getCurrentEpoch(), 1);

  auto cluster_data = mgr.getClusterData();

  EXPECT_EQ(cluster_data->disks.size(), 5);
  EXPECT_EQ(cluster_data->buckets.size(), 256);

  auto root_bucket = cluster_data->buckets[0];
  std::vector<int32_t> root_items {-1,-2};
  EXPECT_EQ(root_bucket.id, 0);
  EXPECT_EQ(root_bucket.bucket_type, get_bucket_type(StdBucketType::ROOT));
  EXPECT_EQ(root_bucket.items, root_items);

  auto site_bucket1 = cluster_data->buckets[1];
  std::vector<int32_t> site_items1 {-100,-101};
  EXPECT_EQ(site_bucket1.id, -1);
  EXPECT_EQ(site_bucket1.bucket_type, get_bucket_type(StdBucketType::SITE));
  EXPECT_EQ(site_bucket1.items, site_items1);

  auto site_bucket2 = cluster_data->buckets[2];
  std::vector<int32_t> site_items2 {-102};
  EXPECT_EQ(site_bucket2.id, -2);
  EXPECT_EQ(site_bucket2.bucket_type, get_bucket_type(StdBucketType::SITE));
  EXPECT_EQ(site_bucket2.items, site_items2);

  auto group_bucket1 = cluster_data->buckets[100];
  std::vector<int32_t> group_items1 {1,2};
  EXPECT_EQ(group_bucket1.id, -100);
  EXPECT_EQ(group_bucket1.bucket_type, get_bucket_type(StdBucketType::GROUP));
  EXPECT_EQ(group_bucket1.items, group_items1);

  auto group_bucket3 = cluster_data->buckets[102];
  std::vector<int32_t> group_items3 {5};
  EXPECT_EQ(group_bucket3.id, -102);
  EXPECT_EQ(group_bucket3.bucket_type, get_bucket_type(StdBucketType::GROUP));
  EXPECT_EQ(group_bucket3.items, group_items3);

  for (int i=0; i < 5; i++) {
    auto disk = cluster_data->disks[i];
    EXPECT_EQ(disk.id, i+1);
  }
}

TEST(ClusterMgr, StorageHandlerDiskInOrder)
{

  using namespace eos::mgm::placement;
  eos::mgm::placement::ClusterMgr mgr;
  {
    auto sh = mgr.getStorageHandler();
    ASSERT_TRUE(sh.addBucket(get_bucket_type(StdBucketType::ROOT), 0));
    ASSERT_TRUE(sh.addBucket(get_bucket_type(StdBucketType::SITE), -1, 0));
    ASSERT_TRUE(sh.addBucket(get_bucket_type(StdBucketType::SITE), -2, 0));
    ASSERT_TRUE(sh.addBucket(get_bucket_type(StdBucketType::GROUP), -100, -1));
    ASSERT_TRUE(sh.addBucket(get_bucket_type(StdBucketType::GROUP), -101, -1));
    ASSERT_TRUE(sh.addBucket(get_bucket_type(StdBucketType::GROUP), -102, -2));

    ASSERT_TRUE(sh.addDisk(Disk(1), -100));
    ASSERT_TRUE(sh.addDisk(Disk(2), -100));
    ASSERT_TRUE(sh.addDisk(Disk(3), -101));
    ASSERT_TRUE(sh.addDisk(Disk(4), -101));
    ASSERT_TRUE(sh.addDisk(Disk(5), -102));
  }

  ASSERT_EQ(mgr.getCurrentEpoch(), 1);

  auto cluster_data = mgr.getClusterData();

  EXPECT_EQ(cluster_data->disks.size(), 5);
  EXPECT_EQ(cluster_data->buckets.size(), 256);

  auto root_bucket = cluster_data->buckets[0];
  std::vector<int32_t> root_items {-1,-2};
  EXPECT_EQ(root_bucket.id, 0);
  EXPECT_EQ(root_bucket.bucket_type, get_bucket_type(StdBucketType::ROOT));
  EXPECT_EQ(root_bucket.items, root_items);

  auto site_bucket1 = cluster_data->buckets[1];
  std::vector<int32_t> site_items1 {-100,-101};
  EXPECT_EQ(site_bucket1.id, -1);
  EXPECT_EQ(site_bucket1.bucket_type, get_bucket_type(StdBucketType::SITE));
  EXPECT_EQ(site_bucket1.items, site_items1);

  auto site_bucket2 = cluster_data->buckets[2];
  std::vector<int32_t> site_items2 {-102};
  EXPECT_EQ(site_bucket2.id, -2);
  EXPECT_EQ(site_bucket2.bucket_type, get_bucket_type(StdBucketType::SITE));
  EXPECT_EQ(site_bucket2.items, site_items2);

  auto group_bucket1 = cluster_data->buckets[100];
  std::vector<int32_t> group_items1 {1,2};
  EXPECT_EQ(group_bucket1.id, -100);
  EXPECT_EQ(group_bucket1.bucket_type, get_bucket_type(StdBucketType::GROUP));
  EXPECT_EQ(group_bucket1.items, group_items1);

  auto group_bucket3 = cluster_data->buckets[102];
  std::vector<int32_t> group_items3 {5};
  EXPECT_EQ(group_bucket3.id, -102);
  EXPECT_EQ(group_bucket3.bucket_type, get_bucket_type(StdBucketType::GROUP));
  EXPECT_EQ(group_bucket3.items, group_items3);

  for (int i=0; i < 5; i++) {
    auto disk = cluster_data->disks[i];
    EXPECT_EQ(disk.id, i+1);
  }
}

TEST(ClusterMgr, StorageHandlerDisksOutOfOrder)
{

  using namespace eos::mgm::placement;
  eos::mgm::placement::ClusterMgr mgr;
  {
    auto sh = mgr.getStorageHandler();
    ASSERT_TRUE(sh.addBucket(get_bucket_type(StdBucketType::ROOT), 0));
    ASSERT_TRUE(sh.addBucket(get_bucket_type(StdBucketType::SITE), -1, 0));
    ASSERT_TRUE(sh.addBucket(get_bucket_type(StdBucketType::SITE), -2, 0));
    ASSERT_TRUE(sh.addBucket(get_bucket_type(StdBucketType::GROUP), -100, -1));
    ASSERT_TRUE(sh.addBucket(get_bucket_type(StdBucketType::GROUP), -101, -1));
    ASSERT_TRUE(sh.addBucket(get_bucket_type(StdBucketType::GROUP), -102, -2));

    ASSERT_TRUE(sh.addDisk(Disk(110), -100));
    ASSERT_TRUE(sh.addDisk(Disk(100), -100));
    ASSERT_TRUE(sh.addDisk(Disk(104), -101));
    ASSERT_TRUE(sh.addDisk(Disk(121), -101));
    ASSERT_TRUE(sh.addDisk(Disk(150), -102));
  }

  ASSERT_EQ(mgr.getCurrentEpoch(), 1);

  auto cluster_data = mgr.getClusterData();

  EXPECT_EQ(cluster_data->disks.size(), 150);
  EXPECT_EQ(cluster_data->buckets.size(), 256);

  auto root_bucket = cluster_data->buckets[0];
  std::vector<int32_t> root_items{-1, -2};
  EXPECT_EQ(root_bucket.id, 0);
  EXPECT_EQ(root_bucket.bucket_type, get_bucket_type(StdBucketType::ROOT));
  EXPECT_EQ(root_bucket.items, root_items);

  auto site_bucket1 = cluster_data->buckets[1];
  std::vector<int32_t> site_items1{-100, -101};
  EXPECT_EQ(site_bucket1.id, -1);
  EXPECT_EQ(site_bucket1.bucket_type, get_bucket_type(StdBucketType::SITE));
  EXPECT_EQ(site_bucket1.items, site_items1);

  auto site_bucket2 = cluster_data->buckets[2];
  std::vector<int32_t> site_items2{-102};
  EXPECT_EQ(site_bucket2.id, -2);
  EXPECT_EQ(site_bucket2.bucket_type, get_bucket_type(StdBucketType::SITE));
  EXPECT_EQ(site_bucket2.items, site_items2);

  auto group_bucket1 = cluster_data->buckets[100];
  std::vector<int32_t> group_items1{110,100};
  EXPECT_EQ(group_bucket1.id, -100);
  EXPECT_EQ(group_bucket1.bucket_type, get_bucket_type(StdBucketType::GROUP));
  EXPECT_EQ(group_bucket1.items, group_items1);

  auto group_bucket2 = cluster_data->buckets[101];
  std::vector<int32_t> group_items2{104,121};
  EXPECT_EQ(group_bucket2.id, -101);
  EXPECT_EQ(group_bucket2.bucket_type, get_bucket_type(StdBucketType::GROUP));
  EXPECT_EQ(group_bucket2.items, group_items2);

  auto group_bucket3 = cluster_data->buckets[102];
  std::vector<int32_t> group_items3{150};
  EXPECT_EQ(group_bucket3.id, -102);
  EXPECT_EQ(group_bucket3.bucket_type, get_bucket_type(StdBucketType::GROUP));
  EXPECT_EQ(group_bucket3.items, group_items3);
}