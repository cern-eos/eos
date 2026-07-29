// ----------------------------------------------------------------------
// File: ClusterMgrTests
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

#include "mgm/placement/ClusterMgr.hh"
#include "gtest/gtest.h"

TEST(ClusterMgr, default)
{
  eos::mgm::placement::ClusterMgr mgr;
  // EXPECT_EQ(mgr.GetCurrentEpoch(), 0);
  EXPECT_FALSE(mgr.GetClusterData());
}

TEST(ClusterMgr, liveSettersOnUncommittedSnapshot)
{
  using namespace eos::mgm::placement;
  // A ClusterMgr that never committed a snapshot (e.g. a space missing from
  // mSpaceGroupView) must not crash when the live update path reaches it; the
  // setters simply report failure and GetState yields nothing.
  ClusterMgr mgr;
  ASSERT_FALSE(mgr.GetClusterData());

  EXPECT_FALSE(mgr.SetDiskStatus(1, ConfigStatus::kRW));
  EXPECT_FALSE(mgr.SetDiskStatus(1, ActiveStatus::kOnline));
  EXPECT_FALSE(mgr.SetDiskWeight(1, 100));
  EXPECT_FALSE(mgr.SetDiskPercentUsed(1, 50));
  EXPECT_FALSE(mgr.SetDiskFreeSpace(1, 1024));
  EXPECT_TRUE(mgr.GetState("all").empty());
}

TEST(ClusterMgr, addDummyData)
{
  eos::mgm::placement::ClusterMgr mgr;
  // A handler pre-sized to zero buckets commits an empty snapshot on scope exit
  {
    auto sh = mgr.GetSnapshotBuilder(0);
  }
  // EXPECT_EQ(mgr.GetCurrentEpoch(), 1);
  EXPECT_TRUE(mgr.GetClusterData());
  auto d = mgr.GetClusterData();
  EXPECT_EQ(d->buckets.size(), 0);
  EXPECT_EQ(d->disks.size(), 0);
}

TEST(ClusterMgr, addDummyDataTwice)
{
  eos::mgm::placement::ClusterMgr mgr;
  {
    auto sh = mgr.GetSnapshotBuilder(0);
  }
  {
    auto sh = mgr.GetSnapshotBuilder(0);
  }
  // EXPECT_EQ(mgr.GetCurrentEpoch(), 2);
  EXPECT_TRUE(mgr.GetClusterData());
  auto d = mgr.GetClusterData();
  EXPECT_EQ(d->buckets.size(), 0);
  EXPECT_EQ(d->disks.size(), 0);
}

TEST(ClusterMgr, SnapshotBuilderCommitAbandon)
{
  using namespace eos::mgm::placement;
  ClusterMgr mgr;

  // Seed a snapshot holding a single disk
  {
    auto sh = mgr.GetSnapshotBuilder();
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::ROOT), 0));
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::GROUP), -10, 0));
    ASSERT_TRUE(sh.AddDiskSequential(
        Disk(1, ConfigStatus::kRW, ActiveStatus::kOnline, 1, 0, 100), -10));
  }
  ASSERT_TRUE(mgr.GetClusterData());
  const ClusterData* seeded = mgr.GetClusterData().operator->();
  ASSERT_EQ(mgr.GetClusterData()->disks.at(0).id, 1u);

  // Abandon: the draft (built on a copy) drops the disk, but discarding it
  // leaves the live snapshot untouched - same pointer, disk still present
  {
    auto sh = mgr.GetSnapshotBuilderWithData();
    ASSERT_TRUE(sh.RemoveDisk(1));
    sh.Abandon();
  }
  EXPECT_EQ(mgr.GetClusterData().operator->(), seeded);
  EXPECT_EQ(mgr.GetClusterData()->disks.at(0).id, 1u);

  // Commit: the same removal is published, so the disk's slot is now a hole
  // (id 0) in the live snapshot
  {
    auto sh = mgr.GetSnapshotBuilderWithData();
    ASSERT_TRUE(sh.RemoveDisk(1));
    sh.Commit();
  }
  EXPECT_EQ(mgr.GetClusterData()->disks.at(0).id, 0u);

  // An explicit Commit is idempotent: the destructor finds the build finalized
  // and does not publish a second snapshot on scope exit
  const ClusterData* committed = nullptr;
  {
    auto sh = mgr.GetSnapshotBuilderWithData();
    ASSERT_TRUE(sh.AddDiskSequential(
        Disk(2, ConfigStatus::kRW, ActiveStatus::kOnline, 1, 0, 5), -10));
    sh.Commit();
    committed = mgr.GetClusterData().operator->();
  }
  EXPECT_EQ(mgr.GetClusterData().operator->(), committed);
  EXPECT_EQ(mgr.GetClusterData()->disks.at(1).id, 2u);
}

TEST(ClusterMgr, WritableFreeGiBCache)
{
  using namespace eos::mgm::placement;
  using namespace std::chrono_literals;
  ClusterMgr mgr;
  // no snapshot yet reports zero rather than caching anything
  EXPECT_EQ(mgr.GetWritableFreeGiB(1h), 0u);
  {
    auto sh = mgr.GetSnapshotBuilder();
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::ROOT), 0));
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::GROUP), -10, 0));
    ASSERT_TRUE(sh.AddDiskSequential(
        Disk(1, ConfigStatus::kRW, ActiveStatus::kOnline, 1, 0, 100), -10));
  }
  // an epoch bump (the commit above) invalidates whatever was cached
  EXPECT_EQ(mgr.GetWritableFreeGiB(1h), 100u);
  // within the TTL the cached figure is served even though the disk moved
  {
    auto cluster_data = mgr.GetClusterData();
    cluster_data->disks[0].free_gib.store(50);
  }
  EXPECT_EQ(mgr.GetWritableFreeGiB(1h), 100u);
  // a zero TTL bypasses the cache and sees the move
  EXPECT_EQ(mgr.GetWritableFreeGiB(0ms), 50u);
  // and a topology change makes the next long-TTL call recompute immediately
  {
    auto sh = mgr.GetSnapshotBuilderWithData();
    ASSERT_TRUE(sh.AddDiskSequential(
        Disk(2, ConfigStatus::kRW, ActiveStatus::kOnline, 1, 0, 25), -10));
  }
  EXPECT_EQ(mgr.GetWritableFreeGiB(1h), 75u);
}

TEST(ClusterMgr, SnapshotBuilderSeq)
{

  using namespace eos::mgm::placement;
  eos::mgm::placement::ClusterMgr mgr;
  {
    auto sh = mgr.GetSnapshotBuilder();
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::ROOT), 0));
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::SITE), -1, 0));
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::SITE), -2, 0));
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::GROUP), -100, -1));
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::GROUP), -101, -1));
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::GROUP), -102, -2));

    ASSERT_TRUE(sh.AddDiskSequential(Disk(1), -100));
    ASSERT_TRUE(sh.AddDiskSequential(Disk(2), -100));
    ASSERT_TRUE(sh.AddDiskSequential(Disk(3), -101));
    ASSERT_TRUE(sh.AddDiskSequential(Disk(4), -101));
    ASSERT_TRUE(sh.AddDiskSequential(Disk(5), -102));
  }

  // ASSERT_EQ(mgr.GetCurrentEpoch(), 1);

  auto cluster_data = mgr.GetClusterData();

  EXPECT_EQ(cluster_data->disks.size(), 5);
  EXPECT_EQ(cluster_data->buckets.size(), 256);

  auto root_bucket = cluster_data->buckets[0];
  std::vector<int32_t> root_items {-1,-2};
  EXPECT_EQ(root_bucket.id, 0);
  EXPECT_EQ(root_bucket.bucket_type, GetBucketType(BucketType::ROOT));
  EXPECT_EQ(root_bucket.items, root_items);

  auto site_bucket1 = cluster_data->buckets[1];
  std::vector<int32_t> site_items1 {-100,-101};
  EXPECT_EQ(site_bucket1.id, -1);
  EXPECT_EQ(site_bucket1.bucket_type, GetBucketType(BucketType::SITE));
  EXPECT_EQ(site_bucket1.items, site_items1);

  auto site_bucket2 = cluster_data->buckets[2];
  std::vector<int32_t> site_items2 {-102};
  EXPECT_EQ(site_bucket2.id, -2);
  EXPECT_EQ(site_bucket2.bucket_type, GetBucketType(BucketType::SITE));
  EXPECT_EQ(site_bucket2.items, site_items2);

  auto group_bucket1 = cluster_data->buckets[100];
  std::vector<int32_t> group_items1 {1,2};
  EXPECT_EQ(group_bucket1.id, -100);
  EXPECT_EQ(group_bucket1.bucket_type, GetBucketType(BucketType::GROUP));
  EXPECT_EQ(group_bucket1.items, group_items1);

  auto group_bucket3 = cluster_data->buckets[102];
  std::vector<int32_t> group_items3 {5};
  EXPECT_EQ(group_bucket3.id, -102);
  EXPECT_EQ(group_bucket3.bucket_type, GetBucketType(BucketType::GROUP));
  EXPECT_EQ(group_bucket3.items, group_items3);

  for (int i=0; i < 5; i++) {
    auto disk = cluster_data->disks[i];
    EXPECT_EQ(disk.id, i+1);
  }
}

TEST(ClusterMgr, SnapshotBuilderDiskInOrder)
{

  using namespace eos::mgm::placement;
  eos::mgm::placement::ClusterMgr mgr;
  {
    auto sh = mgr.GetSnapshotBuilder();
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::ROOT), 0));
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::SITE), -1, 0));
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::SITE), -2, 0));
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::GROUP), -100, -1));
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::GROUP), -101, -1));
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::GROUP), -102, -2));

    ASSERT_TRUE(sh.AddDisk(Disk(1), -100));
    ASSERT_TRUE(sh.AddDisk(Disk(2), -100));
    ASSERT_TRUE(sh.AddDisk(Disk(3), -101));
    ASSERT_TRUE(sh.AddDisk(Disk(4), -101));
    ASSERT_TRUE(sh.AddDisk(Disk(5), -102));
  }

  // ASSERT_EQ(mgr.GetCurrentEpoch(), 1);

  auto cluster_data = mgr.GetClusterData();

  EXPECT_EQ(cluster_data->disks.size(), 5);
  EXPECT_EQ(cluster_data->buckets.size(), 256);

  auto root_bucket = cluster_data->buckets[0];
  std::vector<int32_t> root_items {-1,-2};
  EXPECT_EQ(root_bucket.id, 0);
  EXPECT_EQ(root_bucket.bucket_type, GetBucketType(BucketType::ROOT));
  EXPECT_EQ(root_bucket.items, root_items);

  auto site_bucket1 = cluster_data->buckets[1];
  std::vector<int32_t> site_items1 {-100,-101};
  EXPECT_EQ(site_bucket1.id, -1);
  EXPECT_EQ(site_bucket1.bucket_type, GetBucketType(BucketType::SITE));
  EXPECT_EQ(site_bucket1.items, site_items1);

  auto site_bucket2 = cluster_data->buckets[2];
  std::vector<int32_t> site_items2 {-102};
  EXPECT_EQ(site_bucket2.id, -2);
  EXPECT_EQ(site_bucket2.bucket_type, GetBucketType(BucketType::SITE));
  EXPECT_EQ(site_bucket2.items, site_items2);

  auto group_bucket1 = cluster_data->buckets[100];
  std::vector<int32_t> group_items1 {1,2};
  EXPECT_EQ(group_bucket1.id, -100);
  EXPECT_EQ(group_bucket1.bucket_type, GetBucketType(BucketType::GROUP));
  EXPECT_EQ(group_bucket1.items, group_items1);

  auto group_bucket3 = cluster_data->buckets[102];
  std::vector<int32_t> group_items3 {5};
  EXPECT_EQ(group_bucket3.id, -102);
  EXPECT_EQ(group_bucket3.bucket_type, GetBucketType(BucketType::GROUP));
  EXPECT_EQ(group_bucket3.items, group_items3);

  for (int i=0; i < 5; i++) {
    auto disk = cluster_data->disks[i];
    EXPECT_EQ(disk.id, i+1);
  }
}

TEST(ClusterMgr, SnapshotBuilderDisksOutOfOrder)
{

  using namespace eos::mgm::placement;
  eos::mgm::placement::ClusterMgr mgr;
  {
    auto sh = mgr.GetSnapshotBuilder();
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::ROOT), 0));
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::SITE), -1, 0));
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::SITE), -2, 0));
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::GROUP), -100, -1));
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::GROUP), -101, -1));
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::GROUP), -102, -2));

    ASSERT_TRUE(sh.AddDisk(Disk(110), -100));
    ASSERT_TRUE(sh.AddDisk(Disk(100), -100));
    ASSERT_TRUE(sh.AddDisk(Disk(104), -101));
    ASSERT_TRUE(sh.AddDisk(Disk(121), -101));
    ASSERT_TRUE(sh.AddDisk(Disk(150), -102));
  }

  // ASSERT_EQ(mgr.GetCurrentEpoch(), 1);

  auto cluster_data = mgr.GetClusterData();

  EXPECT_EQ(cluster_data->disks.size(), 150);
  EXPECT_EQ(cluster_data->buckets.size(), 256);

  auto root_bucket = cluster_data->buckets[0];
  std::vector<int32_t> root_items{-1, -2};
  EXPECT_EQ(root_bucket.id, 0);
  EXPECT_EQ(root_bucket.bucket_type, GetBucketType(BucketType::ROOT));
  EXPECT_EQ(root_bucket.items, root_items);

  auto site_bucket1 = cluster_data->buckets[1];
  std::vector<int32_t> site_items1{-100, -101};
  EXPECT_EQ(site_bucket1.id, -1);
  EXPECT_EQ(site_bucket1.bucket_type, GetBucketType(BucketType::SITE));
  EXPECT_EQ(site_bucket1.items, site_items1);

  auto site_bucket2 = cluster_data->buckets[2];
  std::vector<int32_t> site_items2{-102};
  EXPECT_EQ(site_bucket2.id, -2);
  EXPECT_EQ(site_bucket2.bucket_type, GetBucketType(BucketType::SITE));
  EXPECT_EQ(site_bucket2.items, site_items2);

  auto group_bucket1 = cluster_data->buckets[100];
  std::vector<int32_t> group_items1{110,100};
  EXPECT_EQ(group_bucket1.id, -100);
  EXPECT_EQ(group_bucket1.bucket_type, GetBucketType(BucketType::GROUP));
  EXPECT_EQ(group_bucket1.items, group_items1);

  auto group_bucket2 = cluster_data->buckets[101];
  std::vector<int32_t> group_items2{104,121};
  EXPECT_EQ(group_bucket2.id, -101);
  EXPECT_EQ(group_bucket2.bucket_type, GetBucketType(BucketType::GROUP));
  EXPECT_EQ(group_bucket2.items, group_items2);

  auto group_bucket3 = cluster_data->buckets[102];
  std::vector<int32_t> group_items3{150};
  EXPECT_EQ(group_bucket3.id, -102);
  EXPECT_EQ(group_bucket3.bucket_type, GetBucketType(BucketType::GROUP));
  EXPECT_EQ(group_bucket3.items, group_items3);
}

TEST(ClusterMgr, BM_Layout)
{
  using namespace eos::mgm::placement;
  eos::mgm::placement::ClusterMgr mgr;
  int n_elements = 1024;
  int n_disks_per_group = 16;
  int n_groups = 32;
  {

    auto sh = mgr.GetSnapshotBuilder(n_elements);
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::ROOT), 0));
    // ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::SITE), -1, 0));

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
}

TEST(ClusterMgr, GeoBucketsAreSharedAndAddressable)
{
  using namespace eos::mgm::placement;
  eos::mgm::placement::ClusterMgr mgr;
  {
    auto sh = mgr.GetSnapshotBuilder();
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::ROOT), 0));
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::GROUP), -100, 0));
    ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::GROUP), -101, 0));

    // The same atom below the same parent must resolve to the same bucket
    auto site = sh.GetOrAddGeoBucket(-100, "site", GetBucketType(BucketType::SITE));
    ASSERT_LT(site, 0);
    EXPECT_EQ(sh.GetOrAddGeoBucket(-100, "site", GetBucketType(BucketType::SITE)), site);

    // ... but the same atom below a different parent must not, every scheduling
    // group owns its own copy of the geo hierarchy
    auto other_site = sh.GetOrAddGeoBucket(-101, "site", GetBucketType(BucketType::SITE));
    ASSERT_LT(other_site, 0);
    EXPECT_NE(other_site, site);

    auto rack = sh.GetOrAddGeoBucket(site, "rack", GetBucketType(BucketType::RACK));
    ASSERT_LT(rack, 0);
    ASSERT_TRUE(sh.AddDisk(Disk(1, ConfigStatus::kRW, ActiveStatus::kOnline, 1), rack));

    // A geotag atom is never empty and a positive id is never a parent
    EXPECT_EQ(sh.GetOrAddGeoBucket(-100, "", GetBucketType(BucketType::SITE)), 0);
    EXPECT_EQ(sh.GetOrAddGeoBucket(1, "site", GetBucketType(BucketType::SITE)), 0);
  }

  auto cluster_data = mgr.GetClusterData();
  const auto site_id = cluster_data->FindGeoChild(-100, "site");
  ASSERT_LT(site_id, 0);
  const auto rack_id = cluster_data->FindGeoChild(site_id, "rack");
  ASSERT_LT(rack_id, 0);
  EXPECT_EQ(cluster_data->FindGeoChild(-100, "nosuchsite"), 0);

  // The hierarchy carries the geotag structurally, so it can be read back
  EXPECT_EQ(cluster_data->GetGeoTag(rack_id), "site::rack");
  EXPECT_EQ(cluster_data->GetGeoTag(site_id), "site");
  // A scheduling group is not part of the geo hierarchy
  EXPECT_EQ(cluster_data->GetGeoTag(-100), "");

  const auto& rack = cluster_data->buckets[-rack_id];
  EXPECT_EQ(rack.parent, site_id);
  EXPECT_EQ(rack.bucket_type, GetBucketType(BucketType::RACK));
  // root 0 -> group 1 -> site 2 -> rack 3
  EXPECT_EQ(rack.level, 3);
  EXPECT_TRUE(rack.HoldsDisks());
  EXPECT_FALSE(cluster_data->buckets[-site_id].HoldsDisks());
}
