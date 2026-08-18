//------------------------------------------------------------------------------
//! @file ClusterBuilderTests.cc
//! @author Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2026 CERN/Switzerland                                  *
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

#include "mgm/placement/ClusterBuilder.hh"
#include "mgm/placement/FlatScheduler.hh"
#include "gtest/gtest.h"
#include <set>

using namespace eos::mgm::placement;

//------------------------------------------------------------------------------
// Describe one file system, everything but the identity has a usable default
//------------------------------------------------------------------------------
static FsDescription
MakeFs(eos::mgm::placement::fsid_t fsid, unsigned int group_index,
       const std::string& geotag, uint64_t capacity = (1ULL << 40))
{
  FsDescription desc;
  desc.fsid = fsid;
  desc.group_index = group_index;
  desc.geotag = geotag;
  desc.capacity = capacity;
  // An empty disk unless a test says otherwise, so that a booking of a
  // realistic size does not fail for reasons the test is not about
  desc.free_bytes = capacity;
  desc.config_status = ConfigStatus::kRW;
  desc.active_status = ActiveStatus::kOnline;
  return desc;
}

//------------------------------------------------------------------------------
// Build a cluster of n_sites sites, each with n_rooms rooms of n_disks disks,
// replicated across n_groups scheduling groups
//------------------------------------------------------------------------------
static std::vector<FsDescription>
MakeCluster(unsigned int n_groups, unsigned int n_sites, unsigned int n_rooms,
            unsigned int n_disks)
{
  std::vector<FsDescription> fs_list;
  eos::mgm::placement::fsid_t fsid = 1;

  for (unsigned int g = 0; g < n_groups; ++g) {
    for (unsigned int s = 0; s < n_sites; ++s) {
      for (unsigned int r = 0; r < n_rooms; ++r) {
        for (unsigned int d = 0; d < n_disks; ++d) {
          fs_list.push_back(MakeFs(
              fsid++, g, "site" + std::to_string(s) + "::room" + std::to_string(r)));
        }
      }
    }
  }

  return fs_list;
}

//------------------------------------------------------------------------------
// Get the scheduling group a disk was placed in
//------------------------------------------------------------------------------
static ItemIdT
GroupOf(const ClusterData& cluster_data,
        const std::unordered_map<ItemIdT, ItemIdT>& parents, ItemIdT disk_id)
{
  ItemIdT id = parents.at(disk_id);

  while ((id < 0) &&
         (cluster_data.buckets[-id].bucket_type != GetBucketType(BucketType::GROUP))) {
    id = cluster_data.buckets[-id].parent;
  }

  return id;
}

TEST(SplitGeoTag, AtomsAndDegenerateForms)
{
  EXPECT_EQ(SplitGeoTag("site::room::rack"),
            (std::vector<std::string_view>{"site", "room", "rack"}));
  // a single atom, no delimiter at all
  EXPECT_EQ(SplitGeoTag("flatsite"), (std::vector<std::string_view>{"flatsite"}));
  // a trailing separator must not produce an empty trailing atom
  EXPECT_EQ(SplitGeoTag("site::rack::"), (std::vector<std::string_view>{"site", "rack"}));
  // nor must a leading or a doubled one
  EXPECT_EQ(SplitGeoTag("::site"), (std::vector<std::string_view>{"site"}));
  EXPECT_EQ(SplitGeoTag("site::::rack"), (std::vector<std::string_view>{"site", "rack"}));
  EXPECT_TRUE(SplitGeoTag("").empty());
  EXPECT_TRUE(SplitGeoTag("::").empty());
}

TEST(CapacityToWeight, OneUnitPerTerabyte)
{
  constexpr uint64_t tb = 1ULL << 40;
  // a disk smaller than the quantum still takes part in the placement
  EXPECT_EQ(CapacityToWeight(0), 1);
  EXPECT_EQ(CapacityToWeight(tb / 2), 1);
  EXPECT_EQ(CapacityToWeight(tb), 1);
  EXPECT_EQ(CapacityToWeight(16 * tb), 16);
  // the weight is a uint8_t, a very large disk saturates rather than wraps
  EXPECT_EQ(CapacityToWeight(1024 * tb), 255);
}

TEST(ClusterBuilder, HierarchyShape)
{
  ClusterMgr mgr;
  BuildClusterData(mgr, MakeCluster(2, 3, 4, 5));
  auto cluster_data = mgr.GetClusterData();

  EXPECT_EQ(cluster_data->disks.size(), 2 * 3 * 4 * 5);

  // The root holds the scheduling groups and nothing else
  const auto& root = cluster_data->buckets[0];
  ASSERT_EQ(root.items.size(), 2);
  EXPECT_EQ(root.level, 0);

  for (const auto group_id : root.items) {
    const auto& group = cluster_data->buckets[-group_id];
    EXPECT_EQ(group.bucket_type, GetBucketType(BucketType::GROUP));
    EXPECT_EQ(group.parent, 0);
    EXPECT_EQ(group.level, 1);
    EXPECT_FALSE(group.HoldsDisks());
    ASSERT_EQ(group.items.size(), 3); // one per site

    for (const auto site_id : group.items) {
      const auto& site = cluster_data->buckets[-site_id];
      EXPECT_EQ(site.bucket_type, GetBucketType(BucketType::SITE));
      EXPECT_EQ(site.parent, group_id);
      EXPECT_EQ(site.level, 2);
      EXPECT_FALSE(site.HoldsDisks());
      ASSERT_EQ(site.items.size(), 4); // one per room

      for (const auto room_id : site.items) {
        const auto& room = cluster_data->buckets[-room_id];
        EXPECT_EQ(room.bucket_type, GetBucketType(BucketType::ROOM));
        EXPECT_EQ(room.parent, site_id);
        EXPECT_EQ(room.level, 3);
        EXPECT_TRUE(room.HoldsDisks());
        EXPECT_EQ(room.items.size(), 5); // one per disk
      }
    }
  }
}

TEST(ClusterBuilder, GroupIdsAreStableAndGeoIdsDoNotCollide)
{
  ClusterMgr mgr;
  BuildClusterData(mgr, MakeCluster(4, 2, 2, 2));
  auto cluster_data = mgr.GetClusterData();

  // A scheduling group keeps the identifier derived from its index, which is
  // what a forced group index addresses
  std::set<ItemIdT> ids;

  for (unsigned int g = 0; g < 4; ++g) {
    const ItemIdT group_id = cluster_data->GetGroupBucketId(g);
    const auto& group = cluster_data->buckets[-group_id];
    EXPECT_EQ(group.id, group_id);
    EXPECT_EQ(group.bucket_type, GetBucketType(BucketType::GROUP));
  }

  // No two buckets may share an identifier: the geo buckets are numbered by an
  // allocator, the groups by their index, and the two must not overlap
  for (const auto& bucket : cluster_data->buckets) {
    if (bucket.bucket_type == GetBucketType(BucketType::INVALID)) {
      continue; // unused slot of the pre-allocated array
    }

    EXPECT_TRUE(ids.insert(bucket.id).second) << "duplicate bucket id " << bucket.id;
  }
}

TEST(ClusterBuilder, GeoTagsRoundTrip)
{
  ClusterMgr mgr;
  BuildClusterData(mgr, {MakeFs(1, 0, "site::room::rack::node"),
                         MakeFs(2, 0, "site::room::rack::node"),
                         MakeFs(3, 0, "site::room::otherrack")});
  auto cluster_data = mgr.GetClusterData();
  const auto parents = cluster_data->GetDiskParents();

  ASSERT_EQ(parents.size(), 3);
  EXPECT_EQ(cluster_data->GetGeoTag(parents.at(1)), "site::room::rack::node");
  // two disks with the same geotag share one bucket
  EXPECT_EQ(parents.at(1), parents.at(2));
  EXPECT_EQ(cluster_data->GetGeoTag(parents.at(3)), "site::room::otherrack");

  // the level names follow the site::room::rack::node convention
  EXPECT_EQ(cluster_data->buckets[-parents.at(1)].bucket_type,
            GetBucketType(BucketType::NODE));
  EXPECT_EQ(cluster_data->buckets[-parents.at(3)].bucket_type,
            GetBucketType(BucketType::RACK));
}

TEST(ClusterBuilder, DisksWithoutAGeoTagGetTheirOwnBucket)
{
  ClusterMgr mgr;
  // A bucket must never hold a mix of disks and sub-buckets, so an untagged
  // disk gets a placeholder bucket rather than hanging off its group
  BuildClusterData(mgr, {MakeFs(1, 0, ""), MakeFs(2, 0, "site::rack")});
  auto cluster_data = mgr.GetClusterData();

  const auto& group = cluster_data->buckets[-cluster_data->GetGroupBucketId(0)];
  EXPECT_FALSE(group.HoldsDisks());
  // the placeholder bucket of the untagged disk, and the site of the tagged one
  ASSERT_EQ(group.items.size(), 2);

  // No bucket anywhere may hold a mix of disks and sub-buckets, that is what
  // lets the descent decide what to do from the first child alone
  for (const auto& bucket : cluster_data->buckets) {
    if (bucket.items.empty()) {
      continue;
    }

    const bool holds_disks = bucket.items.front() > 0;

    for (const auto item_id : bucket.items) {
      EXPECT_EQ(item_id > 0, holds_disks)
          << "bucket " << bucket.id << " mixes disks and sub-buckets";
    }
  }

  const auto parents = cluster_data->GetDiskParents();
  EXPECT_EQ(cluster_data->GetGeoTag(parents.at(1)), kNoGeoTagBucket);
  EXPECT_EQ(cluster_data->GetGeoTag(parents.at(2)), "site::rack");
}

TEST(ClusterBuilder, ChildTypesFollowTheHierarchy)
{
  ClusterMgr mgr;
  // one tagged disk two levels deep and one untagged, which brings in the
  // placeholder bucket - between them every kind of level appears
  BuildClusterData(mgr, {MakeFs(1, 0, "site::room"), MakeFs(2, 1, "")});
  auto cluster_data = mgr.GetClusterData();

  const auto& root = cluster_data->buckets[0];
  EXPECT_EQ(root.child_type, GetChildType(ChildType::kGroups));

  for (const auto group_id : root.items) {
    const auto& group = cluster_data->buckets[-group_id];
    // a group never holds disks directly, not even for an untagged disk
    EXPECT_EQ(group.child_type, GetChildType(ChildType::kGeoBuckets));
  }

  const ItemIdT site_id =
      cluster_data->FindGeoChild(cluster_data->GetGroupBucketId(0), "site");
  ASSERT_LT(site_id, 0);
  EXPECT_EQ(cluster_data->buckets[-site_id].child_type,
            GetChildType(ChildType::kGeoBuckets));
  const ItemIdT room_id = cluster_data->FindGeoChild(site_id, "room");
  ASSERT_LT(room_id, 0);
  EXPECT_EQ(cluster_data->buckets[-room_id].child_type, GetChildType(ChildType::kDisks));

  const ItemIdT nogeo_id =
      cluster_data->FindGeoChild(cluster_data->GetGroupBucketId(1), kNoGeoTagBucket);
  ASSERT_LT(nogeo_id, 0);
  EXPECT_EQ(cluster_data->buckets[-nogeo_id].child_type, GetChildType(ChildType::kDisks));

  // and the recorded type agrees with what the children actually are
  for (const auto& bucket : cluster_data->buckets) {
    if (bucket.items.empty()) {
      EXPECT_EQ(bucket.child_type, GetChildType(ChildType::kNone))
          << "bucket " << bucket.id << " has a child type without children";
      continue;
    }

    EXPECT_EQ(bucket.HoldsDisks(), bucket.child_type == GetChildType(ChildType::kDisks))
        << "bucket " << bucket.id << " disagrees with its child type";
  }
}

TEST(ClusterBuilder, MixedChildrenAreRefused)
{
  ClusterMgr mgr;
  auto sh = mgr.GetSnapshotBuilder(256);
  ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::ROOT), 0));
  ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::GROUP), -10, 0));
  ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::GROUP), -11, 0));

  // A bucket that holds disks takes no sub-bucket ...
  ASSERT_TRUE(sh.AddDisk(Disk(1, ConfigStatus::kRW, ActiveStatus::kOnline, 1), -10));
  EXPECT_FALSE(sh.AddBucket(GetBucketType(BucketType::SITE), -20, -10));
  EXPECT_EQ(sh.GetOrAddGeoBucket(-10, "site", GetBucketType(BucketType::SITE)), 0);
  // ... and the refusal leaves the bucket untouched
  EXPECT_EQ(sh.GetBucketTypeOf(-20), -1);
  ASSERT_TRUE(sh.AddDisk(Disk(2, ConfigStatus::kRW, ActiveStatus::kOnline, 1), -10));

  // ... nor the other way around
  const ItemIdT site_id =
      sh.GetOrAddGeoBucket(-11, "site", GetBucketType(BucketType::SITE));
  ASSERT_LT(site_id, 0);
  EXPECT_FALSE(sh.AddDisk(Disk(3, ConfigStatus::kRW, ActiveStatus::kOnline, 1), -11));
  ASSERT_TRUE(sh.AddDisk(Disk(3, ConfigStatus::kRW, ActiveStatus::kOnline, 1), site_id));

  // The two kinds of sub-bucket do not mix either: only the root holds groups
  EXPECT_FALSE(sh.AddBucket(GetBucketType(BucketType::GROUP), -21, -11));
  sh.Commit();

  auto cluster_data = mgr.GetClusterData();
  EXPECT_EQ(cluster_data->buckets[10].items.size(), 2u); // the two disks
  EXPECT_EQ(cluster_data->buckets[11].items.size(), 1u); // the site alone
  EXPECT_EQ(cluster_data->disks.size(), 3u);
}

TEST(ClusterBuilder, FlatLeafViewMirrorsTheGroupDisks)
{
  ClusterMgr mgr;
  // fsids: g0 = 1-8 (site0 1-4, site1 5-8), g1 = 9-16
  BuildClusterData(mgr, MakeCluster(2, 2, 2, 2));
  auto cluster_data = mgr.GetClusterData();

  for (int g = 0; g < 2; ++g) {
    const ItemIdT group_id = cluster_data->GetGroupBucketId(g);
    const Bucket* group = cluster_data->GetBucket(group_id);
    ASSERT_NE(group, nullptr);
    ASSERT_LT(group->flat_view, 0);
    const Bucket* view = cluster_data->GetBucket(group->flat_view);
    ASSERT_NE(view, nullptr);
    EXPECT_EQ(view->bucket_type, GetBucketType(BucketType::FLATVIEW));
    EXPECT_EQ(view->parent, group_id);
    EXPECT_EQ(view->child_type, GetChildType(ChildType::kDisks));
    EXPECT_TRUE(view->geo_atom.empty());
    // the view is no geo branch of the group, the normal descent never sees it
    EXPECT_EQ(std::find(group->items.begin(), group->items.end(), group->flat_view),
              group->items.end());
    // it mirrors exactly the disks of the group's subtree, weight included
    std::vector<ItemIdT> expected;

    for (ItemIdT fsid = 1 + 8 * g; fsid <= 8 + 8 * g; ++fsid) {
      expected.push_back(fsid);
    }

    std::vector<ItemIdT> actual(view->items);
    std::sort(actual.begin(), actual.end());
    EXPECT_EQ(actual, expected);
    EXPECT_EQ(view->total_weight, group->total_weight);
  }
}

TEST(ClusterBuilder, FlatLeafViewFollowsIncrementalChanges)
{
  ClusterMgr mgr;
  // fsids 1,2 = site0::room0 and 3,4 = site1::room0, one group
  BuildClusterData(mgr, MakeCluster(1, 2, 1, 2));
  {
    auto sh = mgr.GetSnapshotBuilderWithData();
    ASSERT_TRUE(sh.RemoveDisk(2));
    ASSERT_TRUE(AddFsToCluster(sh, MakeFs(5, 0, "site2::room0")));
  }
  auto cluster_data = mgr.GetClusterData();
  const Bucket* group = cluster_data->GetBucket(cluster_data->GetGroupBucketId(0));
  ASSERT_NE(group, nullptr);
  ASSERT_LT(group->flat_view, 0);
  std::vector<ItemIdT> actual(cluster_data->GetBucket(group->flat_view)->items);
  std::sort(actual.begin(), actual.end());
  EXPECT_EQ(actual, (std::vector<ItemIdT>{1, 3, 4, 5}));
}

TEST(ClusterBuilder, DirectDiskGroupNeedsNoFlatView)
{
  ClusterMgr mgr;
  auto sh = mgr.GetSnapshotBuilder(64);
  ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::ROOT), 0));
  ASSERT_TRUE(sh.AddBucket(GetBucketType(BucketType::GROUP), -10, 0));
  ASSERT_TRUE(sh.AddDisk(Disk(1, ConfigStatus::kRW, ActiveStatus::kOnline, 1), -10));
  sh.Commit();
  auto cluster_data = mgr.GetClusterData();
  // the group holds its disks directly and already is its own leaf level
  EXPECT_EQ(cluster_data->GetBucket(-10)->flat_view, 0);
}

TEST(ClusterBuilder, DeepGeoTagIsTruncated)
{
  ClusterMgr mgr;
  std::string deep = "l0";

  for (int i = 1; i < kMaxGeoDepth + 4; ++i) {
    deep += "::l" + std::to_string(i);
  }

  BuildClusterData(mgr, {MakeFs(1, 0, deep)});
  auto cluster_data = mgr.GetClusterData();
  const auto parents = cluster_data->GetDiskParents();
  ASSERT_EQ(parents.size(), 1);

  // The group sits at level 1, so the deepest geo bucket is at kMaxGeoDepth + 1
  EXPECT_EQ(cluster_data->buckets[-parents.at(1)].level, kMaxGeoDepth + 1);
}

TEST(ClusterBuilder, WeightsAggregateUpTheHierarchy)
{
  ClusterMgr mgr;
  constexpr uint64_t tb = 1ULL << 40;
  BuildClusterData(mgr, {MakeFs(1, 0, "site::rack0", 2 * tb),
                         MakeFs(2, 0, "site::rack0", 3 * tb),
                         MakeFs(3, 0, "site::rack1", 5 * tb)});
  auto cluster_data = mgr.GetClusterData();

  const ItemIdT group_id = cluster_data->GetGroupBucketId(0);
  const ItemIdT site_id = cluster_data->FindGeoChild(group_id, "site");
  ASSERT_LT(site_id, 0);
  const ItemIdT rack0_id = cluster_data->FindGeoChild(site_id, "rack0");
  ASSERT_LT(rack0_id, 0);

  EXPECT_EQ(cluster_data->buckets[-rack0_id].total_weight, 5);
  EXPECT_EQ(cluster_data->buckets[-site_id].total_weight, 10);
  EXPECT_EQ(cluster_data->buckets[-group_id].total_weight, 10);
  EXPECT_EQ(cluster_data->buckets[0].total_weight, 10);
}

TEST(ClusterBuilder, SkipsZeroFsid)
{
  ClusterMgr mgr;
  BuildClusterData(mgr, {MakeFs(0, 0, "site::rack"), MakeFs(1, 0, "site::rack")});
  auto cluster_data = mgr.GetClusterData();
  EXPECT_EQ(cluster_data->disks.size(), 1);
}

TEST(ClusterBuilder, EmptySpace)
{
  ClusterMgr mgr;
  BuildClusterData(mgr, {});
  auto cluster_data = mgr.GetClusterData();
  EXPECT_TRUE(cluster_data->disks.empty());
  EXPECT_TRUE(cluster_data->buckets[0].items.empty());
}

//------------------------------------------------------------------------------
// Placement over a real geo hierarchy
//------------------------------------------------------------------------------
TEST(GeoHierarchyPlacement, ReplicasOfAnUntaggedClientStayInOneGroup)
{
  ClusterMgr mgr;
  BuildClusterData(mgr, MakeCluster(4, 3, 2, 4));
  auto cluster_data = mgr.GetClusterData();
  FlatScheduler scheduler(PlacementStrategyT::kRoundRobin, 1024);

  for (int i = 0; i < 100; ++i) {
    PlacementArgs args(3, ConfigStatus::kRW, PlacementStrategyT::kRoundRobin);
    args.fid = i;
    auto result = scheduler.Schedule(cluster_data(), args);
    ASSERT_TRUE(result) << result.ErrorString();
    ASSERT_TRUE(result.IsValidPlacement(3)) << result.ResultString();

    // Every replica must sit in the same scheduling group and on a disk of
    // its own. This client carries no geotag, so it is served from the
    // group's flat leaf view and its replicas are deliberately not spread
    // over the geo branches - the group is the failure domain, its disks sit
    // on distinct nodes by design. The spread is the geotagged path's job,
    // locked by GeoScheduler.ScatteredKeepsOneReplicaAtTheClient.
    std::set<ItemIdT> groups;
    std::set<ItemIdT> disks;
    const auto parents = cluster_data->GetDiskParents();

    for (int j = 0; j < result.n_filled; ++j) {
      disks.insert(result.ids[j]);
      groups.insert(GroupOf(cluster_data(), parents, result.ids[j]));
    }

    EXPECT_EQ(groups.size(), 1) << "replicas spread over several groups";
    EXPECT_EQ(disks.size(), 3) << "the same disk took more than one replica";
  }
}

TEST(GeoHierarchyPlacement, MoreReplicasThanSites)
{
  ClusterMgr mgr;
  // 2 sites, 4 replicas: the descent has to double up rather than give up
  BuildClusterData(mgr, MakeCluster(1, 2, 2, 4));
  auto cluster_data = mgr.GetClusterData();
  FlatScheduler scheduler(PlacementStrategyT::kRoundRobin, 1024);

  for (int i = 0; i < 50; ++i) {
    PlacementArgs args(4, ConfigStatus::kRW, PlacementStrategyT::kRoundRobin);
    args.fid = i;
    auto result = scheduler.Schedule(cluster_data(), args);
    ASSERT_TRUE(result) << result.ErrorString();
    ASSERT_TRUE(result.IsValidPlacement(4)) << result.ResultString();
  }
}

TEST(GeoHierarchyPlacement, ForcedGroupIndexIsHonoured)
{
  ClusterMgr mgr;
  BuildClusterData(mgr, MakeCluster(4, 2, 2, 4));
  auto cluster_data = mgr.GetClusterData();
  FlatScheduler scheduler(PlacementStrategyT::kRoundRobin, 1024);

  for (unsigned int forced = 0; forced < 4; ++forced) {
    PlacementArgs args(2, ConfigStatus::kRW, PlacementStrategyT::kRoundRobin);
    args.forced_group_index = forced;
    auto result = scheduler.Schedule(cluster_data(), args);
    ASSERT_TRUE(result) << result.ErrorString();
    ASSERT_TRUE(result.IsValidPlacement(2));

    const auto parents = cluster_data->GetDiskParents();

    for (int j = 0; j < result.n_filled; ++j) {
      EXPECT_EQ(GroupOf(cluster_data(), parents, result.ids[j]),
                cluster_data->GetGroupBucketId(forced));
    }
  }
}

TEST(GeoHierarchyPlacement, ForcedGroupIndexOutOfRange)
{
  ClusterMgr mgr;
  BuildClusterData(mgr, MakeCluster(2, 2, 2, 4));
  auto cluster_data = mgr.GetClusterData();
  FlatScheduler scheduler(PlacementStrategyT::kRoundRobin, 1024);

  PlacementArgs args(2, ConfigStatus::kRW, PlacementStrategyT::kRoundRobin);
  args.forced_group_index = 99;
  auto result = scheduler.Schedule(cluster_data(), args);
  EXPECT_FALSE(result);
  EXPECT_EQ(result.ret_code, EINVAL);
}

TEST(GeoHierarchyPlacement, UntaggedClusterBehavesLikeAFlatOne)
{
  ClusterMgr mgr;
  std::vector<FsDescription> fs_list;

  for (eos::mgm::placement::fsid_t i = 1; i <= 40; ++i) {
    fs_list.push_back(MakeFs(i, (i - 1) / 10, ""));
  }

  BuildClusterData(mgr, fs_list);
  auto cluster_data = mgr.GetClusterData();
  FlatScheduler scheduler(PlacementStrategyT::kRoundRobin, 1024);

  for (int i = 0; i < 50; ++i) {
    PlacementArgs args(3, ConfigStatus::kRW, PlacementStrategyT::kRoundRobin);
    args.fid = i;
    auto result = scheduler.Schedule(cluster_data(), args);
    ASSERT_TRUE(result) << result.ErrorString();
    ASSERT_TRUE(result.IsValidPlacement(3)) << result.ResultString();
  }
}

//------------------------------------------------------------------------------
// Geo-aware descent: the client geotag steers where the replicas land
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Get the site bucket a disk ended up in
//------------------------------------------------------------------------------
static ItemIdT
SiteOf(const ClusterData& cluster_data,
       const std::unordered_map<ItemIdT, ItemIdT>& parents, ItemIdT disk_id)
{
  // disk -> room -> site
  return cluster_data.buckets[-parents.at(disk_id)].parent;
}

//------------------------------------------------------------------------------
// Build the arguments of a geo-aware placement
//------------------------------------------------------------------------------
static PlacementArgs
GeoArgs(uint8_t n_replicas, std::string_view geolocation, uint8_t ncollocatedfs)
{
  PlacementArgs args(n_replicas, ConfigStatus::kRW, PlacementStrategyT::kGeoScheduler);
  args.geolocation = geolocation;
  args.ncollocatedfs = ncollocatedfs;
  return args;
}

TEST(GeoScheduler, IsRegistered)
{
  // Phase 5 registers a strategy for kGeoScheduler; before that the scheduler
  // rejected it outright and Scheduler.cc short-circuited to geotree
  FlatScheduler scheduler(1024);
  ClusterMgr mgr;
  BuildClusterData(mgr, MakeCluster(1, 2, 2, 4));
  auto cluster_data = mgr.GetClusterData();
  auto result = scheduler.Schedule(cluster_data(), GeoArgs(2, "", 0));
  ASSERT_TRUE(result) << result.ErrorString();
  EXPECT_TRUE(result.IsValidPlacement(2));
}

TEST(GeoScheduler, ScatteredKeepsOneReplicaAtTheClient)
{
  ClusterMgr mgr;
  BuildClusterData(mgr, MakeCluster(1, 4, 2, 6));
  auto cluster_data = mgr.GetClusterData();
  FlatScheduler scheduler(PlacementStrategyT::kGeoScheduler, 1024);
  const auto parents = cluster_data->GetDiskParents();
  const ItemIdT home_site =
      cluster_data->FindGeoChild(cluster_data->GetGroupBucketId(0), "site2");
  ASSERT_LT(home_site, 0);

  for (int i = 0; i < 100; ++i) {
    // kScattered with a client geotag: exactly one replica next to the client
    auto args = GeoArgs(3, "site2::room0", 1);
    args.fid = i;
    auto result = scheduler.Schedule(cluster_data(), args);
    ASSERT_TRUE(result) << result.ErrorString();
    ASSERT_TRUE(result.IsValidPlacement(3)) << result.ResultString();

    int n_home = 0;
    std::set<ItemIdT> sites;

    for (int j = 0; j < result.n_filled; ++j) {
      const ItemIdT site = SiteOf(cluster_data(), parents, result.ids[j]);
      sites.insert(site);

      if (site == home_site) {
        ++n_home;
      }
    }

    EXPECT_EQ(n_home, 1) << "expected exactly one replica in the client's site";
    EXPECT_EQ(sites.size(), 3) << "the other replicas did not scatter";
  }
}

TEST(GeoScheduler, GatheredKeepsEveryReplicaAtTheClient)
{
  ClusterMgr mgr;
  BuildClusterData(mgr, MakeCluster(1, 4, 2, 6));
  auto cluster_data = mgr.GetClusterData();
  FlatScheduler scheduler(PlacementStrategyT::kGeoScheduler, 1024);
  const auto parents = cluster_data->GetDiskParents();
  const ItemIdT home_site =
      cluster_data->FindGeoChild(cluster_data->GetGroupBucketId(0), "site1");
  ASSERT_LT(home_site, 0);

  for (int i = 0; i < 100; ++i) {
    // kGathered: ncollocatedfs == n_replicas, everything stays together
    auto args = GeoArgs(3, "site1::room1", 3);
    args.fid = i;
    auto result = scheduler.Schedule(cluster_data(), args);
    ASSERT_TRUE(result) << result.ErrorString();
    ASSERT_TRUE(result.IsValidPlacement(3)) << result.ResultString();

    for (int j = 0; j < result.n_filled; ++j) {
      EXPECT_EQ(SiteOf(cluster_data(), parents, result.ids[j]), home_site);
    }
  }
}

TEST(GeoScheduler, DeeperAtomsAreFollowed)
{
  ClusterMgr mgr;
  BuildClusterData(mgr, MakeCluster(1, 3, 3, 6));
  auto cluster_data = mgr.GetClusterData();
  FlatScheduler scheduler(PlacementStrategyT::kGeoScheduler, 1024);
  const auto parents = cluster_data->GetDiskParents();
  const ItemIdT home_site =
      cluster_data->FindGeoChild(cluster_data->GetGroupBucketId(0), "site0");
  ASSERT_LT(home_site, 0);
  const ItemIdT home_room = cluster_data->FindGeoChild(home_site, "room2");
  ASSERT_LT(home_room, 0);

  for (int i = 0; i < 100; ++i) {
    // the collocated replicas must follow the client past the site level
    auto args = GeoArgs(2, "site0::room2", 2);
    args.fid = i;
    auto result = scheduler.Schedule(cluster_data(), args);
    ASSERT_TRUE(result) << result.ErrorString();
    ASSERT_TRUE(result.IsValidPlacement(2));

    for (int j = 0; j < result.n_filled; ++j) {
      EXPECT_EQ(parents.at(result.ids[j]), home_room);
    }
  }
}

TEST(GeoScheduler, UnknownClientGeoTagFallsBackToSpreading)
{
  ClusterMgr mgr;
  BuildClusterData(mgr, MakeCluster(1, 3, 2, 4));
  auto cluster_data = mgr.GetClusterData();
  FlatScheduler scheduler(PlacementStrategyT::kGeoScheduler, 1024);

  for (int i = 0; i < 50; ++i) {
    // nothing in the topology matches, the descent must not fail over it
    auto args = GeoArgs(3, "nowhere::nothing", 1);
    args.fid = i;
    auto result = scheduler.Schedule(cluster_data(), args);
    ASSERT_TRUE(result) << result.ErrorString();
    EXPECT_TRUE(result.IsValidPlacement(3)) << result.ResultString();
  }
}

TEST(GeoScheduler, UndersizedHomeBranchSpillsToItsSiblings)
{
  // site0 has a single disk, so a gathered placement of 3 replicas cannot be
  // served where the client is. It has to spill rather than fail - this is the
  // up-root fallback.
  std::vector<FsDescription> fs_list{MakeFs(1, 0, "site0::room0")};

  for (eos::mgm::placement::fsid_t i = 2; i <= 9; ++i) {
    fs_list.push_back(MakeFs(i, 0, "site1::room0"));
  }

  ClusterMgr mgr;
  BuildClusterData(mgr, fs_list);
  auto cluster_data = mgr.GetClusterData();
  FlatScheduler scheduler(PlacementStrategyT::kGeoScheduler, 1024);

  for (int i = 0; i < 50; ++i) {
    auto args = GeoArgs(3, "site0::room0", 3);
    args.fid = i;
    auto result = scheduler.Schedule(cluster_data(), args);
    ASSERT_TRUE(result) << result.ErrorString();
    ASSERT_TRUE(result.IsValidPlacement(3)) << result.ResultString();

    // and the one disk that is at the client must not be handed out twice
    std::set<ItemIdT> unique(result.ids.begin(), result.ids.begin() + result.n_filled);
    EXPECT_EQ(unique.size(), 3u);
  }
}

TEST(GeoScheduler, ReplicasNeverCrossAGroupBoundary)
{
  // Only group 1 can serve 3 replicas; the descent must abandon a group that
  // falls short rather than spill the remainder into another one
  std::vector<FsDescription> fs_list{MakeFs(1, 0, "site0::room0")};

  for (eos::mgm::placement::fsid_t i = 2; i <= 9; ++i) {
    fs_list.push_back(MakeFs(i, 1, "site0::room0"));
  }

  ClusterMgr mgr;
  BuildClusterData(mgr, fs_list);
  auto cluster_data = mgr.GetClusterData();
  FlatScheduler scheduler(PlacementStrategyT::kGeoScheduler, 1024);
  const auto parents = cluster_data->GetDiskParents();

  for (int i = 0; i < 50; ++i) {
    auto args = GeoArgs(3, "site0::room0", 1);
    args.fid = i;
    auto result = scheduler.Schedule(cluster_data(), args);

    if (!result) {
      continue; // it picked the group that is too small, the caller retries
    }

    std::set<ItemIdT> groups;

    for (int j = 0; j < result.n_filled; ++j) {
      groups.insert(GroupOf(cluster_data(), parents, result.ids[j]));
    }

    EXPECT_EQ(groups.size(), 1u) << "replicas spread over several groups";
  }
}

//------------------------------------------------------------------------------
// Booking size / free space
//------------------------------------------------------------------------------
TEST(FreeSpace, ConversionSaturatesAndRoundsDown)
{
  EXPECT_EQ(FreeSpaceToGiB(0), 0u);
  // anything below a whole unit is not counted, which errs towards refusing
  EXPECT_EQ(FreeSpaceToGiB(kFreeSpaceUnit - 1), 0u);
  EXPECT_EQ(FreeSpaceToGiB(kFreeSpaceUnit), 1u);
  EXPECT_EQ(FreeSpaceToGiB(10 * kFreeSpaceUnit + 42), 10u);
  // a disk larger than the field can express is clamped, not wrapped
  EXPECT_EQ(FreeSpaceToGiB(std::numeric_limits<uint64_t>::max()),
            std::numeric_limits<uint32_t>::max());
}

TEST(FreeSpace, HasRoomForRoundsTheBookingUp)
{
  Disk disk(1, ConfigStatus::kRW, ActiveStatus::kOnline, 1, 0, 2);
  // no booking asks nothing of the disk
  EXPECT_TRUE(HasRoomFor(disk, 0));
  EXPECT_TRUE(HasRoomFor(disk, 1));
  EXPECT_TRUE(HasRoomFor(disk, 2 * kFreeSpaceUnit));
  EXPECT_FALSE(HasRoomFor(disk, 2 * kFreeSpaceUnit + 1));

  Disk empty(2, ConfigStatus::kRW, ActiveStatus::kOnline, 1, 0, 0);
  EXPECT_TRUE(HasRoomFor(empty, 0));
  EXPECT_FALSE(HasRoomFor(empty, 1));
}

TEST(FreeSpace, BuilderStoresFreeSpaceInGiB)
{
  auto fs = MakeFs(1, 0, "site0::room0");
  fs.free_bytes = 4 * kFreeSpaceUnit;
  ClusterMgr mgr;
  BuildClusterData(mgr, {fs});
  auto cluster_data = mgr.GetClusterData();
  ASSERT_EQ(cluster_data->disks.size(), 1u);
  EXPECT_EQ(cluster_data->disks[0].free_gib.load(), 4u);
}

TEST(FreeSpace, FullDisksAreNotPlacementCandidates)
{
  // Two disks with room, two without. A four replica request cannot be met and
  // a two replica one has exactly one answer. The disks with room are sized
  // for all fifty trials, since every successful placement prebooks its
  // 8 GiB and the debits add up.
  std::vector<FsDescription> fs_list;

  for (eos::mgm::placement::fsid_t i = 1; i <= 4; ++i) {
    auto fs = MakeFs(i, 0, "site0::room0");
    fs.free_bytes = (i <= 2) ? (1000 * kFreeSpaceUnit) : 0;
    fs_list.push_back(fs);
  }

  ClusterMgr mgr;
  BuildClusterData(mgr, fs_list);
  auto cluster_data = mgr.GetClusterData();
  FlatScheduler scheduler(PlacementStrategyT::kGeoScheduler, 1024);

  for (int i = 0; i < 50; ++i) {
    auto args = GeoArgs(2, "", 0);
    args.fid = i;
    args.bookingsize = 8 * kFreeSpaceUnit;
    auto result = scheduler.Schedule(cluster_data(), args);
    ASSERT_TRUE(result) << result.ErrorString();
    ASSERT_TRUE(result.IsValidPlacement(2)) << result.ResultString();
    // only the two disks with room can appear
    EXPECT_TRUE(result.Contains(1) && result.Contains(2)) << result.ResultString();
  }

  auto args = GeoArgs(3, "", 0);
  args.bookingsize = 8 * kFreeSpaceUnit;
  EXPECT_FALSE(scheduler.Schedule(cluster_data(), args));
}

TEST(FreeSpace, BookingIsIgnoredWhenZero)
{
  // A disk with no free space left still takes a file of unknown size, which is
  // what the callers that do not know it ask for
  std::vector<FsDescription> fs_list;

  for (eos::mgm::placement::fsid_t i = 1; i <= 2; ++i) {
    auto fs = MakeFs(i, 0, "site0::room0");
    fs.free_bytes = 0;
    fs_list.push_back(fs);
  }

  ClusterMgr mgr;
  BuildClusterData(mgr, fs_list);
  auto cluster_data = mgr.GetClusterData();
  FlatScheduler scheduler(PlacementStrategyT::kGeoScheduler, 1024);
  auto args = GeoArgs(2, "", 0);
  EXPECT_TRUE(scheduler.Schedule(cluster_data(), args));
}

TEST(FreeSpace, DeficitSpillsToASiblingWithRoom)
{
  // The client's site cannot hold the file, the other one can. The descent has
  // to fall back rather than fail, which is what the spill pass is for.
  std::vector<FsDescription> fs_list;

  for (eos::mgm::placement::fsid_t i = 1; i <= 4; ++i) {
    auto fs = MakeFs(i, 0, "site0::room0");
    fs.free_bytes = 0;
    fs_list.push_back(fs);
  }

  for (eos::mgm::placement::fsid_t i = 5; i <= 8; ++i) {
    auto fs = MakeFs(i, 0, "site1::room0");
    fs.free_bytes = 100 * kFreeSpaceUnit;
    fs_list.push_back(fs);
  }

  ClusterMgr mgr;
  BuildClusterData(mgr, fs_list);
  auto cluster_data = mgr.GetClusterData();
  FlatScheduler scheduler(PlacementStrategyT::kGeoScheduler, 1024);
  auto args = GeoArgs(2, "site0::room0", 1);
  args.bookingsize = 8 * kFreeSpaceUnit;
  auto result = scheduler.Schedule(cluster_data(), args);
  ASSERT_TRUE(result) << result.ErrorString();
  ASSERT_TRUE(result.IsValidPlacement(2)) << result.ResultString();

  for (int j = 0; j < result.n_filled; ++j) {
    EXPECT_GE(result.ids[j], 5) << "placed on a disk without room";
  }
}

//------------------------------------------------------------------------------
// Prebooking: a successful placement debits the free space it was promised, so
// that the placements between two FST publishes see each other's bookings, and
// a publish reconciles the bookings away again
//------------------------------------------------------------------------------
TEST(Prebooking, BookSpaceDebitsRoundsUpAndClampsAtZero)
{
  Disk disk(1, ConfigStatus::kRW, ActiveStatus::kOnline, 1, 0, 10);
  // a zero booking touches nothing
  BookSpace(disk, 0);
  EXPECT_EQ(disk.free_gib.load(), 10u);
  EXPECT_EQ(disk.booked_gib.load(), 0u);
  // a partial unit is rounded up to a whole one
  BookSpace(disk, 1);
  EXPECT_EQ(disk.free_gib.load(), 9u);
  EXPECT_EQ(disk.booked_gib.load(), 1u);
  BookSpace(disk, 3 * kFreeSpaceUnit);
  EXPECT_EQ(disk.free_gib.load(), 6u);
  EXPECT_EQ(disk.booked_gib.load(), 4u);
  // debiting past zero clamps rather than wraps
  BookSpace(disk, 100 * kFreeSpaceUnit);
  EXPECT_EQ(disk.free_gib.load(), 0u);
  EXPECT_EQ(disk.booked_gib.load(), 104u);
}

TEST(Prebooking, SuccessfulPlacementDebitsTheSelectedDisks)
{
  std::vector<FsDescription> fs_list;

  for (eos::mgm::placement::fsid_t i = 1; i <= 4; ++i) {
    auto fs = MakeFs(i, 0, "site0::room0");
    fs.free_bytes = 100 * kFreeSpaceUnit;
    fs_list.push_back(fs);
  }

  ClusterMgr mgr;
  BuildClusterData(mgr, fs_list);
  auto cluster_data = mgr.GetClusterData();
  FlatScheduler scheduler(PlacementStrategyT::kGeoScheduler, 1024);
  auto args = GeoArgs(2, "", 0);
  args.bookingsize = 8 * kFreeSpaceUnit;
  auto result = scheduler.Schedule(cluster_data(), args);
  ASSERT_TRUE(result) << result.ErrorString();
  ASSERT_TRUE(result.IsValidPlacement(2)) << result.ResultString();

  for (const auto& disk : cluster_data->disks) {
    const bool selected = result.Contains(disk.id);
    EXPECT_EQ(disk.free_gib.load(), selected ? 92u : 100u) << "fsid " << disk.id;
    EXPECT_EQ(disk.booked_gib.load(), selected ? 8u : 0u) << "fsid " << disk.id;
  }

  // a placement of unknown size books nothing
  auto no_booking = GeoArgs(2, "", 0);
  ASSERT_TRUE(scheduler.Schedule(cluster_data(), no_booking));
  uint64_t total_booked = 0;

  for (const auto& disk : cluster_data->disks) {
    total_booked += disk.booked_gib.load();
  }

  EXPECT_EQ(total_booked, 16u);
}

TEST(Prebooking, FailedPlacementBooksNothing)
{
  // Three replicas cannot be placed on two disks with room, and the failed
  // attempt must not leave a booking behind on the disks it did select
  std::vector<FsDescription> fs_list;

  for (eos::mgm::placement::fsid_t i = 1; i <= 4; ++i) {
    auto fs = MakeFs(i, 0, "site0::room0");
    fs.free_bytes = (i <= 2) ? (100 * kFreeSpaceUnit) : 0;
    fs_list.push_back(fs);
  }

  ClusterMgr mgr;
  BuildClusterData(mgr, fs_list);
  auto cluster_data = mgr.GetClusterData();
  FlatScheduler scheduler(PlacementStrategyT::kGeoScheduler, 1024);
  auto args = GeoArgs(3, "", 0);
  args.bookingsize = 8 * kFreeSpaceUnit;
  ASSERT_FALSE(scheduler.Schedule(cluster_data(), args));

  for (const auto& disk : cluster_data->disks) {
    EXPECT_EQ(disk.booked_gib.load(), 0u) << "fsid " << disk.id;
  }

  EXPECT_EQ(cluster_data->disks[0].free_gib.load(), 100u);
  EXPECT_EQ(cluster_data->disks[1].free_gib.load(), 100u);
}

TEST(Prebooking, PlacementsSeeEachOthersBookingsBeforeAnyPublish)
{
  // Two disks of 10 GiB: the first 8 GiB file books its space, so the second
  // one must be refused instead of over-committing the same last gigabytes -
  // which is the race the prebooking exists to close
  std::vector<FsDescription> fs_list;

  for (eos::mgm::placement::fsid_t i = 1; i <= 2; ++i) {
    auto fs = MakeFs(i, 0, "site0::room0");
    fs.free_bytes = 10 * kFreeSpaceUnit;
    fs_list.push_back(fs);
  }

  ClusterMgr mgr;
  BuildClusterData(mgr, fs_list);
  auto cluster_data = mgr.GetClusterData();
  FlatScheduler scheduler(PlacementStrategyT::kGeoScheduler, 1024);
  auto args = GeoArgs(2, "", 0);
  args.bookingsize = 8 * kFreeSpaceUnit;
  ASSERT_TRUE(scheduler.Schedule(cluster_data(), args));
  EXPECT_FALSE(scheduler.Schedule(cluster_data(), args));
}

TEST(Prebooking, PublishReconcilesEveryBookingExactlyOnce)
{
  auto fs = MakeFs(1, 0, "site0::room0");
  fs.free_bytes = 100 * kFreeSpaceUnit;
  ClusterMgr mgr;
  BuildClusterData(mgr, {fs});
  auto cluster_data = mgr.GetClusterData();
  ASSERT_TRUE(cluster_data->BookDiskSpace(1, 10 * kFreeSpaceUnit));
  EXPECT_EQ(cluster_data->disks[0].free_gib.load(), 90u);

  // The first publish cannot see the booked bytes yet, so the booking is
  // subtracted from it - and retired
  ASSERT_TRUE(mgr.SetDiskFreeSpace(1, 100 * kFreeSpaceUnit));
  EXPECT_EQ(cluster_data->disks[0].free_gib.load(), 90u);
  EXPECT_EQ(cluster_data->disks[0].booked_gib.load(), 0u);

  // The next publish is the truth again, whether the file was written or not
  ASSERT_TRUE(mgr.SetDiskFreeSpace(1, 100 * kFreeSpaceUnit));
  EXPECT_EQ(cluster_data->disks[0].free_gib.load(), 100u);

  // A publish smaller than the outstanding bookings clamps at zero instead of
  // resurrecting space the bookings already claimed
  cluster_data->BookDiskSpace(1, 10 * kFreeSpaceUnit);
  ASSERT_TRUE(mgr.SetDiskFreeSpace(1, 5 * kFreeSpaceUnit));
  EXPECT_EQ(cluster_data->disks[0].free_gib.load(), 0u);
  EXPECT_EQ(cluster_data->disks[0].booked_gib.load(), 0u);
}

//------------------------------------------------------------------------------
// Writable capacity: the flat equivalent of the geotree totalWritableSpace
// aggregate behind placementSpace - the free space on the disks a placement
// can actually use
//------------------------------------------------------------------------------
TEST(WritableCapacity, CountsOnlyPlacementCandidates)
{
  // Four disks of 10 GiB: one healthy, one read-only, one offline, one in
  // drain. Only the healthy one is a placement candidate.
  std::vector<FsDescription> fs_list;

  for (eos::mgm::placement::fsid_t i = 1; i <= 4; ++i) {
    auto fs = MakeFs(i, 0, "site0::room0");
    fs.free_bytes = 10 * kFreeSpaceUnit;
    fs_list.push_back(fs);
  }

  fs_list[1].config_status = ConfigStatus::kRO;
  fs_list[2].active_status = ActiveStatus::kOffline;
  fs_list[3].config_status = ConfigStatus::kDrain;
  ClusterMgr mgr;
  BuildClusterData(mgr, fs_list);
  auto cluster_data = mgr.GetClusterData();
  EXPECT_EQ(cluster_data->GetWritableFreeGiB(), 10u);
  // the unfiltered summary keeps seeing all four
  const auto summary = cluster_data->GetStateSummary();
  EXPECT_EQ(summary.free_gib, 40u);
  EXPECT_EQ(summary.writable_free_gib, 10u);
}

TEST(WritableCapacity, HonoursDisabledPlacementBranches)
{
  // Two sites of 10 GiB each; disabling one for placement halves the figure,
  // while an access-only rule leaves it alone - a replica there stays
  // readable, but no new one lands there
  ClusterMgr mgr;
  auto site0 = MakeFs(1, 0, "site0::room0");
  auto site1 = MakeFs(2, 0, "site1::room0");
  site0.free_bytes = site1.free_bytes = 10 * kFreeSpaceUnit;
  BuildClusterData(mgr, {site0, site1});
  auto cluster_data = mgr.GetClusterData();
  EXPECT_EQ(cluster_data->GetWritableFreeGiB(), 20u);
  cluster_data->ApplyDisabledBranches({{"site0", kDisabledAccess}});
  EXPECT_EQ(cluster_data->GetWritableFreeGiB(), 20u);
  cluster_data->ApplyDisabledBranches({{"site0", kDisabledPlct}});
  EXPECT_EQ(cluster_data->GetWritableFreeGiB(), 10u);
}

TEST(WritableCapacity, TracksBookingsAndPublishes)
{
  auto fs = MakeFs(1, 0, "site0::room0");
  fs.free_bytes = 100 * kFreeSpaceUnit;
  ClusterMgr mgr;
  BuildClusterData(mgr, {fs});
  auto cluster_data = mgr.GetClusterData();
  EXPECT_EQ(cluster_data->GetWritableFreeGiB(), 100u);
  // a booking debits the figure immediately, so concurrent capacity checks
  // do not all fit into the same last gigabytes
  ASSERT_TRUE(cluster_data->BookDiskSpace(1, 10 * kFreeSpaceUnit));
  EXPECT_EQ(cluster_data->GetWritableFreeGiB(), 90u);
  // and the second publish after it is the truth again, see Prebooking
  ASSERT_TRUE(mgr.SetDiskFreeSpace(1, 100 * kFreeSpaceUnit));
  ASSERT_TRUE(mgr.SetDiskFreeSpace(1, 100 * kFreeSpaceUnit));
  EXPECT_EQ(cluster_data->GetWritableFreeGiB(), 100u);
}

//------------------------------------------------------------------------------
// Geo-aware access: the replica closest to the client serves the request, the
// strategy only tie-breaks within the closest set
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// One group holding a file with a replica in every room of two sites
//------------------------------------------------------------------------------
static std::vector<FsDescription>
GeoAccessCluster()
{
  return {MakeFs(1, 0, "siteA::room1"), MakeFs(2, 0, "siteA::room2"),
          MakeFs(3, 0, "siteB::room1"), MakeFs(4, 0, "siteB::room2")};
}

TEST(GeoOverlap, CountsLeadingAtoms)
{
  ClusterMgr mgr;
  BuildClusterData(mgr,
                   {MakeFs(1, 0, "siteA::room1::rack1"), MakeFs(2, 0, "siteA::room2"),
                    MakeFs(3, 0, "siteB::room1"), MakeFs(4, 0, "")});
  auto cluster_data = mgr.GetClusterData();
  EXPECT_EQ(cluster_data->GetGeoOverlap(1, SplitGeoTag("siteA::room1::rack1")), 3);
  EXPECT_EQ(cluster_data->GetGeoOverlap(1, SplitGeoTag("siteA::room1")), 2);
  EXPECT_EQ(cluster_data->GetGeoOverlap(1, SplitGeoTag("siteA::room2")), 1);
  EXPECT_EQ(cluster_data->GetGeoOverlap(2, SplitGeoTag("siteA::room1")), 1);
  EXPECT_EQ(cluster_data->GetGeoOverlap(3, SplitGeoTag("siteA::room1")), 0);
  // room1 below siteB is not room1 below siteA, the whole path has to match
  EXPECT_EQ(cluster_data->GetGeoOverlap(3, SplitGeoTag("siteB::room1")), 2);
  // an untagged disk sits on the placeholder bucket and never matches
  EXPECT_EQ(cluster_data->GetGeoOverlap(4, SplitGeoTag("siteA")), 0);
  // no client atoms and an unknown disk both rank as no overlap
  EXPECT_EQ(cluster_data->GetGeoOverlap(1, {}), 0);
  EXPECT_EQ(cluster_data->GetGeoOverlap(99, SplitGeoTag("siteA")), 0);
}

TEST(GeoAccess, ClosestReplicaWins)
{
  ClusterMgr mgr;
  BuildClusterData(mgr, GeoAccessCluster());
  auto cluster_data = mgr.GetClusterData();
  FlatScheduler scheduler(PlacementStrategyT::kRoundRobin, 64);
  const std::vector<uint32_t> locations{1, 2, 3, 4};

  // Whatever the random tie-break does, a client in siteB::room1 always lands
  // on the one replica in that room
  for (int i = 0; i < 32; ++i) {
    size_t index = 99;
    AccessArgs args(index, 0, PlacementStrategyT::kRoundRobin, "siteB::room1", nullptr,
                    locations);
    ASSERT_EQ(scheduler.Access(cluster_data(), args), 0);
    EXPECT_EQ(index, 2u);
  }
}

TEST(GeoAccess, DeeperMatchBeatsShallower)
{
  ClusterMgr mgr;
  BuildClusterData(mgr, GeoAccessCluster());
  auto cluster_data = mgr.GetClusterData();
  FlatScheduler scheduler(PlacementStrategyT::kRoundRobin, 64);
  const std::vector<uint32_t> locations{1, 2, 3, 4};

  // fsid 1 shares the site with the client, fsid 2 also shares the room
  for (int i = 0; i < 32; ++i) {
    size_t index = 99;
    AccessArgs args(index, 0, PlacementStrategyT::kRoundRobin, "siteA::room2", nullptr,
                    locations);
    ASSERT_EQ(scheduler.Access(cluster_data(), args), 0);
    EXPECT_EQ(index, 1u);
  }
}

TEST(GeoAccess, FallsBackWhenTheClosestIsDown)
{
  ClusterMgr mgr;
  BuildClusterData(mgr, GeoAccessCluster());
  auto cluster_data = mgr.GetClusterData();
  ASSERT_TRUE(mgr.SetDiskStatus(3u, ActiveStatus::kOffline));
  FlatScheduler scheduler(PlacementStrategyT::kRoundRobin, 64);
  const std::vector<uint32_t> locations{1, 2, 3, 4};

  // The room replica is down, the remaining siteB one is the closest left
  for (int i = 0; i < 32; ++i) {
    size_t index = 99;
    AccessArgs args(index, 0, PlacementStrategyT::kRoundRobin, "siteB::room1", nullptr,
                    locations);
    ASSERT_EQ(scheduler.Access(cluster_data(), args), 0);
    EXPECT_EQ(index, 3u);
  }
}

TEST(GeoAccess, TieBreaksWithinTheClosestSet)
{
  ClusterMgr mgr;
  BuildClusterData(mgr, GeoAccessCluster());
  auto cluster_data = mgr.GetClusterData();
  FlatScheduler scheduler(PlacementStrategyT::kRoundRobin, 64);
  const std::vector<uint32_t> locations{1, 2, 3, 4};
  std::set<size_t> seen;

  // A site-only client geotag ties the two siteA replicas: the filter demotes
  // the far ones and leaves the choice within the site to the strategy
  for (int i = 0; i < 64; ++i) {
    size_t index = 99;
    AccessArgs args(index, 0, PlacementStrategyT::kRoundRobin, "siteA", nullptr,
                    locations);
    ASSERT_EQ(scheduler.Access(cluster_data(), args), 0);
    ASSERT_LE(index, 1u);
    seen.insert(index);
  }

  EXPECT_EQ(seen.size(), 2u) << "the tie never reached one of the tied replicas";
}

TEST(GeoAccess, NoGeoMatchChangesNothing)
{
  ClusterMgr mgr;
  BuildClusterData(mgr, GeoAccessCluster());
  auto cluster_data = mgr.GetClusterData();
  FlatScheduler scheduler(PlacementStrategyT::kRoundRobin, 64);
  const std::vector<uint32_t> locations{1, 2, 3, 4};

  // A client nowhere near the topology, and one with no geotag at all, keep
  // every replica a candidate
  for (const std::string_view geotag : {"elsewhere::nowhere", ""}) {
    std::set<size_t> seen;

    for (int i = 0; i < 128; ++i) {
      size_t index = 99;
      AccessArgs args(index, 0, PlacementStrategyT::kRoundRobin, geotag, nullptr,
                      locations);
      ASSERT_EQ(scheduler.Access(cluster_data(), args), 0);
      seen.insert(index);
    }

    EXPECT_EQ(seen.size(), 4u) << "geotag \"" << geotag << "\" narrowed the choice";
  }
}

TEST(GeoAccess, ForcedFsidBeatsProximity)
{
  ClusterMgr mgr;
  BuildClusterData(mgr, GeoAccessCluster());
  auto cluster_data = mgr.GetClusterData();
  FlatScheduler scheduler(PlacementStrategyT::kRoundRobin, 64);
  const std::vector<uint32_t> locations{1, 2, 3, 4};

  // The client sits next to fsid 3, but the caller forces fsid 1
  size_t index = 99;
  AccessArgs args(index, 0, PlacementStrategyT::kRoundRobin, "siteB::room1", nullptr,
                  locations);
  args.forcedfsid = 1;
  ASSERT_EQ(scheduler.Access(cluster_data(), args), 0);
  EXPECT_EQ(index, 0u);
}

TEST(GeoAccess, DemotionsDoNotLeakIntoUnavail)
{
  ClusterMgr mgr;
  BuildClusterData(mgr, GeoAccessCluster());
  auto cluster_data = mgr.GetClusterData();
  FlatScheduler scheduler(PlacementStrategyT::kRoundRobin, 64);
  const std::vector<uint32_t> locations{1, 2, 3, 4};

  // The far replicas are demoted for the pick, but they are reachable and must
  // not be reported to the RAIN driver as unavailable stripes
  std::vector<uint32_t> unavail;
  size_t index = 99;
  AccessArgs args(index, 0, PlacementStrategyT::kRoundRobin, "siteB::room1", &unavail,
                  locations);
  ASSERT_EQ(scheduler.Access(cluster_data(), args), 0);
  EXPECT_EQ(index, 2u);
  EXPECT_TRUE(unavail.empty());
}

TEST(GeoAccess, GeoBeatsTheHrwTieBreak)
{
  ClusterMgr mgr;
  BuildClusterData(mgr, GeoAccessCluster());
  auto cluster_data = mgr.GetClusterData();
  FlatScheduler scheduler(PlacementStrategyT::kWeightedRandom, 64);
  const std::vector<uint32_t> locations{1, 2, 3, 4};

  // Whatever replica the weighted rendezvous hash would prefer for a given
  // inode, the client's room replica wins
  for (uint64_t inode = 1; inode <= 64; ++inode) {
    size_t index = 99;
    AccessArgs args(index, inode, PlacementStrategyT::kWeightedRandom, "siteA::room1",
                    nullptr, locations);
    ASSERT_EQ(scheduler.Access(cluster_data(), args), 0);
    EXPECT_EQ(index, 0u) << "inode " << inode;
  }
}

//------------------------------------------------------------------------------
// Disabled branches: an administratively disabled geotag branch takes no new
// replicas and serves no reads, the flat equivalent of the geotree
// addDisabledBranch
//------------------------------------------------------------------------------

TEST(DisabledBranches, RulesResolveOntoEveryGroupAndClear)
{
  ClusterMgr mgr;
  // fsids: g0 = site0(1-4: room0 1,2 room1 3,4) site1(5-8),
  //        g1 = site0(9-12) site1(13-16)
  BuildClusterData(mgr, MakeCluster(2, 2, 2, 2));
  auto cluster_data = mgr.GetClusterData();
  cluster_data->ApplyDisabledBranches({{"site0", kDisabledPlct}});
  int n_disabled = 0;

  for (const auto& bucket : cluster_data->buckets) {
    if (bucket.IsDisabledFor(kDisabledAll)) {
      ++n_disabled;
    }
  }

  // the geo hierarchy repeats below every scheduling group, so one rule flags
  // one bucket per group - and nothing below or beside it
  EXPECT_EQ(n_disabled, 2);
  // disks below the branch report it, for the disabled operation only
  EXPECT_TRUE(cluster_data->IsBranchDisabled(1, kDisabledPlct));
  EXPECT_FALSE(cluster_data->IsBranchDisabled(1, kDisabledAccess));
  EXPECT_TRUE(cluster_data->IsBranchDisabled(9, kDisabledPlct));
  EXPECT_FALSE(cluster_data->IsBranchDisabled(5, kDisabledPlct));

  // a deeper rule flags the room and leaves the sibling room alone
  cluster_data->ApplyDisabledBranches({{"site0::room1", kDisabledAll}});
  EXPECT_FALSE(cluster_data->IsBranchDisabled(1, kDisabledAll));
  EXPECT_TRUE(cluster_data->IsBranchDisabled(3, kDisabledPlct));
  EXPECT_TRUE(cluster_data->IsBranchDisabled(3, kDisabledAccess));
  EXPECT_TRUE(cluster_data->IsBranchDisabled(11, kDisabledAll));

  // a rule naming no existing branch flags nothing
  cluster_data->ApplyDisabledBranches({{"nowhere", kDisabledAll}});

  for (const auto& bucket : cluster_data->buckets) {
    EXPECT_FALSE(bucket.IsDisabledFor(kDisabledAll));
  }

  // and so does an empty rule set - the rules are always the complete set,
  // never additive
  cluster_data->ApplyDisabledBranches({{"site0", kDisabledAll}});
  cluster_data->ApplyDisabledBranches({});
  EXPECT_FALSE(cluster_data->IsBranchDisabled(1, kDisabledAll));
}

TEST(DisabledBranches, PlctFlagTracksTheRules)
{
  ClusterMgr mgr;
  BuildClusterData(mgr, MakeCluster(1, 2, 1, 2));
  auto cluster_data = mgr.GetClusterData();
  EXPECT_FALSE(cluster_data->HasPlctDisabledBranches());
  // an access rule closes nothing for placement
  cluster_data->ApplyDisabledBranches({{"site0", kDisabledAccess}});
  EXPECT_FALSE(cluster_data->HasPlctDisabledBranches());
  cluster_data->ApplyDisabledBranches({{"site0", kDisabledPlct}});
  EXPECT_TRUE(cluster_data->HasPlctDisabledBranches());
  // even a rule naming no existing branch keeps the flag up - erring towards
  // the full descent is the safe direction
  cluster_data->ApplyDisabledBranches({{"nowhere", kDisabledPlct}});
  EXPECT_TRUE(cluster_data->HasPlctDisabledBranches());
  cluster_data->ApplyDisabledBranches({});
  EXPECT_FALSE(cluster_data->HasPlctDisabledBranches());
}

TEST(DisabledBranches, PlacementAvoidsADisabledSite)
{
  ClusterMgr mgr;
  BuildClusterData(mgr, MakeCluster(2, 3, 2, 4));
  auto cluster_data = mgr.GetClusterData();
  cluster_data->ApplyDisabledBranches({{"site0", kDisabledPlct}});
  FlatScheduler scheduler(PlacementStrategyT::kRoundRobin, 1024);
  const auto parents = cluster_data->GetDiskParents();

  // 3 replicas over 3 sites of which one is disabled: the descent has to
  // double up on the surviving sites rather than fail
  for (int i = 0; i < 100; ++i) {
    PlacementArgs args(3, ConfigStatus::kRW, PlacementStrategyT::kRoundRobin);
    args.fid = i;
    auto result = scheduler.Schedule(cluster_data(), args);
    ASSERT_TRUE(result) << result.ErrorString();
    ASSERT_TRUE(result.IsValidPlacement(3)) << result.ResultString();

    for (int j = 0; j < result.n_filled; ++j) {
      const auto geotag = cluster_data->GetGeoTag(parents.at(result.ids[j]));
      EXPECT_NE(geotag.rfind("site0::", 0), 0u)
          << "replica " << result.ids[j] << " landed in the disabled site";
    }
  }
}

TEST(DisabledBranches, EveryBranchDisabledFailsPlacement)
{
  ClusterMgr mgr;
  BuildClusterData(mgr, MakeCluster(1, 2, 1, 4));
  auto cluster_data = mgr.GetClusterData();
  cluster_data->ApplyDisabledBranches(
      {{"site0", kDisabledPlct}, {"site1", kDisabledPlct}});
  FlatScheduler scheduler(PlacementStrategyT::kRoundRobin, 1024);
  PlacementArgs args(2, ConfigStatus::kRW, PlacementStrategyT::kRoundRobin);
  auto result = scheduler.Schedule(cluster_data(), args);
  ASSERT_FALSE(result);
  EXPECT_EQ(result.ret_code, EACCES);
}

TEST(DisabledBranches, DisabledHomeBranchPushesReplicasAway)
{
  ClusterMgr mgr;
  BuildClusterData(mgr, MakeCluster(1, 3, 1, 4));
  auto cluster_data = mgr.GetClusterData();
  cluster_data->ApplyDisabledBranches({{"site0", kDisabledPlct}});
  FlatScheduler scheduler(PlacementStrategyT::kGeoScheduler, 1024);
  const auto parents = cluster_data->GetDiskParents();

  // The client sits in the disabled site: the gathered placement must treat
  // it as no home at all and settle elsewhere, not bounce off the guard
  for (int i = 0; i < 50; ++i) {
    auto args = GeoArgs(2, "site0::room0", 2);
    args.fid = i;
    auto result = scheduler.Schedule(cluster_data(), args);
    ASSERT_TRUE(result) << result.ErrorString();
    ASSERT_TRUE(result.IsValidPlacement(2)) << result.ResultString();

    for (int j = 0; j < result.n_filled; ++j) {
      const auto geotag = cluster_data->GetGeoTag(parents.at(result.ids[j]));
      EXPECT_NE(geotag.rfind("site0::", 0), 0u)
          << "replica " << result.ids[j] << " landed in the disabled site";
    }
  }
}

TEST(DisabledBranches, AccessSkipsADisabledBranch)
{
  ClusterMgr mgr;
  BuildClusterData(mgr, GeoAccessCluster());
  auto cluster_data = mgr.GetClusterData();
  cluster_data->ApplyDisabledBranches({{"siteB", kDisabledAccess}});
  FlatScheduler scheduler(PlacementStrategyT::kRoundRobin, 64);
  const std::vector<uint32_t> locations{1, 2, 3, 4};

  // The client's room replica sits in the disabled site, so a siteA replica
  // serves the read instead
  for (int i = 0; i < 32; ++i) {
    size_t index = 99;
    AccessArgs args(index, 0, PlacementStrategyT::kRoundRobin, "siteB::room1", nullptr,
                    locations);
    ASSERT_EQ(scheduler.Access(cluster_data(), args), 0);
    EXPECT_LE(index, 1u);
  }
}

TEST(DisabledBranches, AccessFailsWhenEveryReplicaIsDisabled)
{
  ClusterMgr mgr;
  BuildClusterData(mgr, GeoAccessCluster());
  auto cluster_data = mgr.GetClusterData();
  cluster_data->ApplyDisabledBranches(
      {{"siteA", kDisabledAccess}, {"siteB", kDisabledAccess}});
  FlatScheduler scheduler(PlacementStrategyT::kRoundRobin, 64);
  const std::vector<uint32_t> locations{1, 2, 3, 4};
  std::vector<uint32_t> unavail;
  size_t index = 99;
  AccessArgs args(index, 0, PlacementStrategyT::kRoundRobin, "", &unavail, locations);
  EXPECT_EQ(scheduler.Access(cluster_data(), args), ENETUNREACH);
  // the disabled replicas are reported like unreachable ones, which is what
  // tells the RAIN driver it cannot reconstruct from them
  EXPECT_EQ(unavail.size(), 4u);
}

TEST(DisabledBranches, PlctDisabledBranchStillServesReads)
{
  ClusterMgr mgr;
  BuildClusterData(mgr, GeoAccessCluster());
  auto cluster_data = mgr.GetClusterData();
  cluster_data->ApplyDisabledBranches({{"siteB", kDisabledPlct}});
  FlatScheduler scheduler(PlacementStrategyT::kRoundRobin, 64);
  const std::vector<uint32_t> locations{1, 2, 3, 4};

  // Closing a branch for placement must not make its existing replicas
  // unreadable - the two operations are independent
  for (int i = 0; i < 32; ++i) {
    size_t index = 99;
    AccessArgs args(index, 0, PlacementStrategyT::kRoundRobin, "siteB::room1", nullptr,
                    locations);
    ASSERT_EQ(scheduler.Access(cluster_data(), args), 0);
    EXPECT_EQ(index, 2u);
  }
}

//------------------------------------------------------------------------------
// Flat leaf view placement: a client without any geo preference is served
// straight from the group's flat view, skipping the geo levels - the group is
// the failure domain, its disks sit on distinct nodes by design
//------------------------------------------------------------------------------

TEST(FlatLeafView, NoGeoTagClientSpreadsOverTheWholeGroup)
{
  ClusterMgr mgr;
  // one group of 8 disks: site0 = 1-4, site1 = 5-8
  BuildClusterData(mgr, MakeCluster(1, 2, 2, 2));
  auto cluster_data = mgr.GetClusterData();
  FlatScheduler scheduler(PlacementStrategyT::kRoundRobin, 1024);
  const auto parents = cluster_data->GetDiskParents();
  std::map<ItemIdT, int> counts;
  bool same_site_pair = false;

  for (int i = 0; i < 8; ++i) {
    PlacementArgs args(2, ConfigStatus::kRW, PlacementStrategyT::kRoundRobin);
    args.fid = i;
    auto result = scheduler.Schedule(cluster_data(), args);
    ASSERT_TRUE(result) << result.ErrorString();
    ASSERT_TRUE(result.IsValidPlacement(2)) << result.ResultString();

    for (int j = 0; j < result.n_filled; ++j) {
      ++counts[result.ids[j]];
    }

    const auto tag0 = cluster_data->GetGeoTag(parents.at(result.ids[0]));
    const auto tag1 = cluster_data->GetGeoTag(parents.at(result.ids[1]));
    same_site_pair = same_site_pair ||
                     (tag0.substr(0, tag0.find("::")) == tag1.substr(0, tag1.find("::")));
  }

  // the round-robin cursor walks the whole flat view: 8 placements of 2 over
  // 8 disks land on every disk exactly twice
  ASSERT_EQ(counts.size(), 8u);

  for (const auto& [disk_id, n] : counts) {
    EXPECT_EQ(n, 2) << "disk " << disk_id;
  }

  // and a pair is free to share a site: the geo levels below the group impose
  // nothing on a client without a geotag
  EXPECT_TRUE(same_site_pair);
}

TEST(FlatLeafView, GeoTaggedClientOnANonGeoStrategyStillUsesTheView)
{
  ClusterMgr mgr;
  // one group of 8 disks: site0 = 1-4, site1 = 5-8
  BuildClusterData(mgr, MakeCluster(1, 2, 2, 2));
  auto cluster_data = mgr.GetClusterData();
  FlatScheduler scheduler(PlacementStrategyT::kRoundRobin, 1024);
  const auto parents = cluster_data->GetDiskParents();
  std::map<ItemIdT, int> counts;
  bool same_site_pair = false;

  for (int i = 0; i < 8; ++i) {
    // A client that does carry a geotag, and asks for a replica next to it -
    // but over a strategy that is not the geo scheduler, so the preference is
    // not honoured and the descent never reaches the geo levels
    PlacementArgs args(2, ConfigStatus::kRW, PlacementStrategyT::kRoundRobin);
    args.geolocation = "site0::room0";
    args.ncollocatedfs = 1;
    args.fid = i;
    auto result = scheduler.Schedule(cluster_data(), args);
    ASSERT_TRUE(result) << result.ErrorString();
    ASSERT_TRUE(result.IsValidPlacement(2)) << result.ResultString();

    for (int j = 0; j < result.n_filled; ++j) {
      ++counts[result.ids[j]];
    }

    const auto tag0 = cluster_data->GetGeoTag(parents.at(result.ids[0]));
    const auto tag1 = cluster_data->GetGeoTag(parents.at(result.ids[1]));
    same_site_pair = same_site_pair ||
                     (tag0.substr(0, tag0.find("::")) == tag1.substr(0, tag1.find("::")));
  }

  // Same outcome as the untagged client: the flat view is walked end to end,
  // every disk of the group takes exactly two of the sixteen replicas
  ASSERT_EQ(counts.size(), 8u);

  for (const auto& [disk_id, n] : counts) {
    EXPECT_EQ(n, 2) << "disk " << disk_id;
  }

  // and pairs share a site, which the geo aware descent would never allow with
  // ncollocatedfs == 1 - see GeoScheduler.ScatteredKeepsOneReplicaAtTheClient
  EXPECT_TRUE(same_site_pair);
}

TEST(FlatLeafView, PlctRuleIsHonouredThroughTheView)
{
  ClusterMgr mgr;
  // one group of 8 disks, two per room: site0::room0 = 1-2, site0::room1 = 3-4,
  // site1::room0 = 5-6, site1::room1 = 7-8
  BuildClusterData(mgr, MakeCluster(1, 2, 2, 2));
  auto cluster_data = mgr.GetClusterData();
  FlatScheduler scheduler(PlacementStrategyT::kRoundRobin, 1024);
  const auto parents = cluster_data->GetDiskParents();
  // One room of one site, so that the six survivors still straddle both sites
  cluster_data->ApplyDisabledBranches({{"site0::room0", kDisabledPlct}});
  std::set<ItemIdT> served;
  bool same_site_pair = false;

  // The interior buckets are never visited - the group is served from its flat
  // view - so it is the disks below the rule that have to refuse it
  for (int i = 0; i < 24; ++i) {
    PlacementArgs args(2, ConfigStatus::kRW, PlacementStrategyT::kRoundRobin);
    args.fid = i;
    auto result = scheduler.Schedule(cluster_data(), args);
    ASSERT_TRUE(result) << result.ErrorString();
    ASSERT_TRUE(result.IsValidPlacement(2)) << result.ResultString();

    for (int j = 0; j < result.n_filled; ++j) {
      const auto geotag = cluster_data->GetGeoTag(parents.at(result.ids[j]));
      ASSERT_NE(geotag, "site0::room0")
          << "replica " << result.ids[j] << " landed in the disabled room";
      served.insert(result.ids[j]);
    }

    const auto tag0 = cluster_data->GetGeoTag(parents.at(result.ids[0]));
    const auto tag1 = cluster_data->GetGeoTag(parents.at(result.ids[1]));
    same_site_pair = same_site_pair ||
                     (tag0.substr(0, tag0.find("::")) == tag1.substr(0, tag1.find("::")));
  }

  // Every disk the rule does not name stays in play - a rule on one room must
  // not cost the group the rest of its site
  EXPECT_EQ(served.size(), 6u);

  // and the placement is still the flat one: the full descent would have split
  // the pair one replica per site and never collocated one
  EXPECT_TRUE(same_site_pair);

  // Clearing the rule puts the room back in service
  cluster_data->ApplyDisabledBranches({});
  bool room0_seen = false;

  for (int i = 0; i < 8; ++i) {
    PlacementArgs args(2, ConfigStatus::kRW, PlacementStrategyT::kRoundRobin);
    args.fid = i;
    auto result = scheduler.Schedule(cluster_data(), args);
    ASSERT_TRUE(result) << result.ErrorString();

    for (int j = 0; j < result.n_filled; ++j) {
      room0_seen = room0_seen ||
                   (cluster_data->GetGeoTag(parents.at(result.ids[j])) == "site0::room0");
    }
  }

  EXPECT_TRUE(room0_seen);
}

TEST(IncrementalTopology, NewGroupRegistersIntoALiveSnapshot)
{
  ClusterMgr mgr;
  // fsids 1,2 in group 0, whose geo buckets are already allocated
  BuildClusterData(mgr, MakeCluster(1, 1, 1, 2));
  ASSERT_EQ(mgr.GetClusterData()->GetGroupBucketId(1), 0);
  {
    // A group the snapshot has never seen: its bucket used to be addressed
    // arithmetically and the identifier was already owned by a geo bucket of
    // the build above, so the registration was refused
    auto sh = mgr.GetSnapshotBuilderWithData();
    ASSERT_TRUE(AddFsToCluster(sh, MakeFs(3, 1, "site0::room0")));
    ASSERT_TRUE(AddFsToCluster(sh, MakeFs(4, 1, "site1::room0")));
  }
  auto cluster_data = mgr.GetClusterData();
  const ItemIdT group_id = cluster_data->GetGroupBucketId(1);
  ASSERT_LT(group_id, 0);
  const Bucket* group = cluster_data->GetBucket(group_id);
  ASSERT_NE(group, nullptr);
  EXPECT_EQ(group->bucket_type, GetBucketType(BucketType::GROUP));
  EXPECT_EQ(group->group_index, 1u);
  EXPECT_EQ(group->parent, 0);
  // the root took it as a child, so the descent can reach it
  EXPECT_NE(std::find(cluster_data->buckets[0].items.begin(),
                      cluster_data->buckets[0].items.end(), group_id),
            cluster_data->buckets[0].items.end());
  // and it carries the two disks under their own geo branches
  EXPECT_EQ(group->items.size(), 2u);
  const Bucket* view = cluster_data->GetBucket(group->flat_view);
  ASSERT_NE(view, nullptr);
  std::vector<ItemIdT> disks(view->items);
  std::sort(disks.begin(), disks.end());
  EXPECT_EQ(disks, (std::vector<ItemIdT>{3, 4}));

  // placement reaches the new group, and a forced index resolves to it
  FlatScheduler scheduler(PlacementStrategyT::kRoundRobin, 1024);
  PlacementArgs args(2, ConfigStatus::kRW, PlacementStrategyT::kRoundRobin);
  args.forced_group_index = 1;
  auto result = scheduler.Schedule(cluster_data(), args);
  ASSERT_TRUE(result) << result.ErrorString();
  ASSERT_TRUE(result.IsValidPlacement(2)) << result.ResultString();
  const auto parents = cluster_data->GetDiskParents();

  for (int i = 0; i < result.n_filled; ++i) {
    EXPECT_EQ(GroupOf(cluster_data(), parents, result.ids[i]), group_id);
  }
}

TEST(IncrementalTopology, GroupIdentifiersDoNotCollideWithGeoBuckets)
{
  ClusterMgr mgr;
  BuildClusterData(mgr, MakeCluster(2, 2, 2, 2));
  std::set<ItemIdT> geo_ids;
  {
    auto before = mgr.GetClusterData();

    for (const auto& bucket : before->buckets) {
      if (!bucket.geo_atom.empty()) {
        geo_ids.insert(bucket.id);
      }
    }
  }
  ASSERT_FALSE(geo_ids.empty());
  {
    auto sh = mgr.GetSnapshotBuilderWithData();

    for (unsigned int g = 2; g < 6; ++g) {
      ASSERT_TRUE(AddFsToCluster(sh, MakeFs(100 + g, g, "site0::room0")));
    }
  }
  auto cluster_data = mgr.GetClusterData();

  // every new group got an identifier of its own, below everything in use
  for (unsigned int g = 2; g < 6; ++g) {
    const ItemIdT group_id = cluster_data->GetGroupBucketId(g);
    ASSERT_LT(group_id, 0) << "group " << g;
    EXPECT_EQ(geo_ids.count(group_id), 0u)
        << "group " << g << " landed on a geo bucket identifier";
    EXPECT_EQ(cluster_data->GetBucket(group_id)->bucket_type,
              GetBucketType(BucketType::GROUP));
  }
}

//------------------------------------------------------------------------------
// Engine capacity: the topology is built long after the engine and keeps
// growing, so the per bucket state of the strategies follows the snapshot
//------------------------------------------------------------------------------

TEST(EngineCapacity, TopologyLargerThanTheEngineStillPlaces)
{
  ClusterMgr mgr;
  // 4 groups x 2 sites x 2 rooms x 2 disks needs well over 8 buckets: 1 root,
  // 4 groups, 4 flat views and 4 x (2 sites + 4 rooms) geo buckets
  BuildClusterData(mgr, MakeCluster(4, 2, 2, 2));
  auto cluster_data = mgr.GetClusterData();
  ASSERT_GT(cluster_data->buckets.size(), 8u);

  // Every strategy that keeps per bucket cursors used to refuse a topology
  // wider than the number it was built with
  for (const auto strategy :
       {PlacementStrategyT::kRoundRobin, PlacementStrategyT::kThreadLocalRoundRobin,
        PlacementStrategyT::kRandom, PlacementStrategyT::kFidRandom,
        PlacementStrategyT::kWeightedRoundRobin, PlacementStrategyT::kWeightedRandom}) {
    FlatScheduler scheduler(strategy, 8);

    for (int i = 0; i < 8; ++i) {
      PlacementArgs args(2, ConfigStatus::kRW, strategy);
      args.fid = i;
      auto result = scheduler.Schedule(cluster_data(), args);
      ASSERT_TRUE(result) << "strategy " << static_cast<int>(strategy) << ": "
                          << result.ErrorString();
      ASSERT_TRUE(result.IsValidPlacement(2)) << result.ResultString();
    }
  }
}

TEST(EngineCapacity, GrowsWithATopologyThatGainsBuckets)
{
  ClusterMgr mgr;
  BuildClusterData(mgr, MakeCluster(1, 1, 1, 2));
  FlatScheduler scheduler(PlacementStrategyT::kRoundRobin, 8);
  {
    auto cluster_data = mgr.GetClusterData();
    PlacementArgs args(2, ConfigStatus::kRW, PlacementStrategyT::kRoundRobin);
    ASSERT_TRUE(scheduler.Schedule(cluster_data(), args));
  }

  const size_t n_buckets_before = mgr.GetClusterData()->buckets.size();
  // The same engine now meets a topology grown past what it was built for,
  // which is what an incremental registration does in production. The disks
  // join the existing group - a brand new group cannot be added incrementally,
  // its identifier is already owned by a geo bucket, see AddFsToCluster.
  {
    auto sh = mgr.GetSnapshotBuilderWithData();

    for (unsigned int fsid = 3; fsid <= 20; ++fsid) {
      ASSERT_TRUE(
          AddFsToCluster(sh, MakeFs(fsid, 0, "site" + std::to_string(fsid) + "::room0")));
    }
  }
  auto cluster_data = mgr.GetClusterData();
  ASSERT_GT(cluster_data->buckets.size(), n_buckets_before);

  for (int i = 0; i < 8; ++i) {
    PlacementArgs args(2, ConfigStatus::kRW, PlacementStrategyT::kRoundRobin);
    args.fid = i;
    auto result = scheduler.Schedule(cluster_data(), args);
    ASSERT_TRUE(result) << result.ErrorString();
    ASSERT_TRUE(result.IsValidPlacement(2)) << result.ResultString();
  }
}

//------------------------------------------------------------------------------
// Incremental topology updates: one file system inserted into or removed from
// an existing snapshot, the machinery behind the FsView registration hooks
//------------------------------------------------------------------------------

TEST(IncrementalTopology, RemoveDiskLeavesAHole)
{
  ClusterMgr mgr;
  // fsids 1,2 = site0::room0 and 3,4 = site1::room0, one group
  BuildClusterData(mgr, MakeCluster(1, 2, 1, 2));
  {
    auto sh = mgr.GetSnapshotBuilderWithData();
    EXPECT_FALSE(sh.RemoveDisk(0));
    EXPECT_FALSE(sh.RemoveDisk(99));
    EXPECT_TRUE(sh.RemoveDisk(2));
    // already a hole
    EXPECT_FALSE(sh.RemoveDisk(2));
  }
  auto cluster_data = mgr.GetClusterData();
  // the arrays show the hole shape of an unassigned fsid
  EXPECT_EQ(cluster_data->disks[1].id, 0u);
  EXPECT_EQ(cluster_data->disk_parents[1], 0);
  const auto parents = cluster_data->GetDiskParents();
  EXPECT_EQ(parents.count(2), 0u);
  // the parent bucket no longer lists it, its sibling is untouched
  const ItemIdT room_id = parents.at(1);
  const auto& items = cluster_data->buckets[-room_id].items;
  EXPECT_EQ(std::count(items.begin(), items.end(), 2), 0);
  EXPECT_EQ(std::count(items.begin(), items.end(), 1), 1);
  // and placement keeps working without ever returning the hole
  FlatScheduler scheduler(PlacementStrategyT::kRoundRobin, 256);

  for (int i = 0; i < 32; ++i) {
    PlacementArgs args(2, ConfigStatus::kRW, PlacementStrategyT::kRoundRobin);
    args.fid = i;
    auto result = scheduler.Schedule(cluster_data(), args);
    ASSERT_TRUE(result) << result.ErrorString();

    for (int j = 0; j < result.n_filled; ++j) {
      EXPECT_NE(result.ids[j], 2);
    }
  }
}

TEST(IncrementalTopology, AddFsGrowsTheHierarchy)
{
  ClusterMgr mgr;
  // two disks in one site, one group
  BuildClusterData(mgr, MakeCluster(1, 1, 1, 2));
  {
    auto sh = mgr.GetSnapshotBuilderWithData();
    // a new disk in a brand new site of the same group
    EXPECT_TRUE(AddFsToCluster(sh, MakeFs(3, 0, "site9::room0")));
  }
  auto cluster_data = mgr.GetClusterData();
  const auto parents = cluster_data->GetDiskParents();
  // the new geo chain exists and reads back
  EXPECT_EQ(cluster_data->GetGeoTag(parents.at(3)), "site9::room0");
  // and the freshly allocated buckets did not overwrite the existing ones
  EXPECT_EQ(cluster_data->GetGeoTag(parents.at(1)), "site0::room0");
  // the new disk takes part in placement
  FlatScheduler scheduler(PlacementStrategyT::kRoundRobin, 256);
  bool seen = false;

  for (int i = 0; (i < 64) && !seen; ++i) {
    PlacementArgs args(2, ConfigStatus::kRW, PlacementStrategyT::kRoundRobin);
    args.fid = i;
    auto result = scheduler.Schedule(cluster_data(), args);
    ASSERT_TRUE(result) << result.ErrorString();

    for (int j = 0; j < result.n_filled; ++j) {
      seen = seen || (result.ids[j] == 3);
    }
  }

  EXPECT_TRUE(seen) << "the inserted disk never got a replica";
}

TEST(IncrementalTopology, NewGroupDoesNotDisturbExistingBuckets)
{
  ClusterMgr mgr;
  BuildClusterData(mgr, MakeCluster(1, 1, 1, 1));
  ItemIdT room_id = 0;
  {
    auto before = mgr.GetClusterData();
    room_id = before->GetDiskParents().at(1);
    ASSERT_LT(room_id, 0);
    ASSERT_EQ(before->GetGeoTag(room_id), "site0::room0");
  }
  {
    // Group indices the snapshot has never seen. Their buckets used to be
    // addressed arithmetically, which aimed them straight at identifiers the
    // geo buckets of the build above already held, and the insert had to
    // refuse rather than overwrite. They are allocated now, so both inserts
    // go through and nothing existing moves.
    auto sh = mgr.GetSnapshotBuilderWithData();
    EXPECT_TRUE(AddFsToCluster(sh, MakeFs(2, 2, "site0::room0")));
    EXPECT_TRUE(AddFsToCluster(sh, MakeFs(3, 5, "site0::room0")));
  }
  auto cluster_data = mgr.GetClusterData();
  // the original room bucket survived untouched and still holds disk 1
  const auto parents = cluster_data->GetDiskParents();
  EXPECT_EQ(parents.at(1), room_id);
  EXPECT_EQ(cluster_data->GetGeoTag(room_id), "site0::room0");
  // and each new disk sits in the group its index names, under a geo branch
  // of its own - the hierarchy repeats below every group
  EXPECT_EQ(cluster_data->GetGeoTag(parents.at(2)), "site0::room0");
  EXPECT_EQ(cluster_data->GetGeoTag(parents.at(3)), "site0::room0");
  EXPECT_EQ(GroupOf(cluster_data(), parents, 2), cluster_data->GetGroupBucketId(2));
  EXPECT_EQ(GroupOf(cluster_data(), parents, 3), cluster_data->GetGroupBucketId(5));
  EXPECT_NE(GroupOf(cluster_data(), parents, 1), GroupOf(cluster_data(), parents, 2));
  EXPECT_NE(GroupOf(cluster_data(), parents, 2), GroupOf(cluster_data(), parents, 3));
}
