//------------------------------------------------------------------------------
//! @file ClusterMgr.cc
//! @author Abhishek Lekshmanan - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2023 CERN/Switzerland                                  *
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
#include "mgm/placement/ClusterDataFormatter.hh"

#include <algorithm>
#include <limits>
#include <sstream>
#include <xxhash.h>

namespace eos::mgm::placement
{

//------------------------------------------------------------------------------
// Get a handler building a brand new topology snapshot
//------------------------------------------------------------------------------
ClusterMgr::SnapshotBuilder
ClusterMgr::GetSnapshotBuilder(size_t max_buckets)
{
  return SnapshotBuilder(*this, max_buckets);
}

//------------------------------------------------------------------------------
// Get an RCU read guarded handle to the current snapshot
//------------------------------------------------------------------------------
ClusterMgr::ClusterDataPtr
ClusterMgr::GetClusterData()
{
  return {mClusterData.get(), mClusterMgrRcu};
}

//------------------------------------------------------------------------------
// Swap in a new topology snapshot and drop the old one
//------------------------------------------------------------------------------
void
ClusterMgr::AddClusterData(ClusterData&& data)
{
  auto* fresh = new ClusterData(std::move(data));
  // mClusterData is an atomic unique ptr, so reset returns a ptr
  // whose deletion we need to do outside the lock
  ClusterData* old_ptr {nullptr};
  {
    // Stamp the authoritative configuration onto the snapshot before it goes
    // live, serialized against the config setters. This ClusterMgr is the sole
    // owner of the fill limits and disabled branches, so a rebuild no longer
    // needs any external re-stamping: every committed snapshot leaves here
    // already carrying the configured values instead of the bare defaults.
    std::lock_guard<std::mutex> cfg_lock(mConfigMutex);
    fresh->SetFillLimits(mConfiguredFillLimits.cap, mConfiguredFillLimits.warn);

    if (!mDisabledBranches.empty()) {
      fresh->ApplyDisabledBranches(mDisabledBranches);
    }

    {
      std::unique_lock l(mClusterMgrRcu);
      old_ptr = mClusterData.reset(fresh);
      mCurrentEpoch.fetch_add(1, std::memory_order_release);
    }
  }
  delete old_ptr;
}

//------------------------------------------------------------------------------
// Update the configuration status of a disk in place
//------------------------------------------------------------------------------
bool
ClusterMgr::SetDiskStatus(fsid_t disk_id, ConfigStatus status)
{
  eos::common::RCUReadLock rlock(mClusterMgrRcu);

  if (!mClusterData) {
    return false;
  }

  return mClusterData->SetDiskStatus(disk_id, status);
}

//------------------------------------------------------------------------------
// Update the active status of a disk in place
//------------------------------------------------------------------------------
bool
ClusterMgr::SetDiskStatus(fsid_t disk_id, ActiveStatus status)
{
  eos::common::RCUReadLock rlock(mClusterMgrRcu);

  if (!mClusterData) {
    return false;
  }

  return mClusterData->SetDiskStatus(disk_id, status);
}

//------------------------------------------------------------------------------
// Update the weight of a disk in place
//------------------------------------------------------------------------------
bool
ClusterMgr::SetDiskWeight(fsid_t disk_id, uint8_t weight)
{
  eos::common::RCUReadLock rlock(mClusterMgrRcu);

  if (!mClusterData) {
    return false;
  }

  // A weight change is an in-place atomic edit like status/space: it reweights
  // placement selection but does not alter writable capacity (the only epoch
  // consumer, via the GetWritableFreeGiB cache), so it must not bump the epoch.
  return mClusterData->SetDiskWeight(disk_id, weight);
}

//------------------------------------------------------------------------------
// Update the fill level of a disk in place
//------------------------------------------------------------------------------
bool
ClusterMgr::SetDiskPercentUsed(fsid_t disk_id, uint8_t percent_used)
{
  eos::common::RCUReadLock rlock(mClusterMgrRcu);

  if (!mClusterData) {
    return false;
  }

  return mClusterData->SetDiskPercentUsed(disk_id, percent_used);
}

//------------------------------------------------------------------------------
// Update the free space of a disk in place from an FST publish
//------------------------------------------------------------------------------
bool
ClusterMgr::SetDiskFreeSpace(fsid_t disk_id, uint64_t free_bytes)
{
  eos::common::RCUReadLock rlock(mClusterMgrRcu);

  if (!mClusterData) {
    return false;
  }

  return mClusterData->SetDiskFreeSpace(disk_id, free_bytes);
}

//------------------------------------------------------------------------------
// Set the configured fill thresholds of this space
//------------------------------------------------------------------------------
void
ClusterMgr::SetConfiguredFillLimits(uint8_t cap, uint8_t warn)
{
  std::lock_guard<std::mutex> cfg_lock(mConfigMutex);
  mConfiguredFillLimits = FillLimitsConfig{cap, warn};
  // Apply to the current snapshot right away; every future snapshot picks the
  // value up when AddClusterData stamps it.
  eos::common::RCUReadLock rlock(mClusterMgrRcu);

  if (mClusterData) {
    mClusterData->SetFillLimits(cap, warn);
  }
}

//------------------------------------------------------------------------------
// Get the configured fill thresholds of this space
//------------------------------------------------------------------------------
FillLimitsConfig
ClusterMgr::GetConfiguredFillLimits() const
{
  std::lock_guard<std::mutex> cfg_lock(mConfigMutex);
  return mConfiguredFillLimits;
}

//------------------------------------------------------------------------------
// Re-resolve the configured disabled branch rules onto the current snapshot
//------------------------------------------------------------------------------
void
ClusterMgr::ReapplyDisabledBranchesLocked()
{
  eos::common::RCUReadLock rlock(mClusterMgrRcu);

  if (mClusterData) {
    // The complete rule set is passed every time: ApplyDisabledBranches clears
    // all masks first, so an empty set correctly re-enables everything.
    mClusterData->ApplyDisabledBranches(mDisabledBranches);
  }
}

//------------------------------------------------------------------------------
// Disable a geotag branch for the given operations
//------------------------------------------------------------------------------
bool
ClusterMgr::AddDisabledBranch(const std::string& canonical, uint8_t op_mask)
{
  std::lock_guard<std::mutex> cfg_lock(mConfigMutex);
  mDisabledBranches[canonical] |= op_mask;
  ReapplyDisabledBranchesLocked();
  return true;
}

//------------------------------------------------------------------------------
// Re-enable a previously disabled geotag branch for the given operations
//------------------------------------------------------------------------------
bool
ClusterMgr::RmDisabledBranch(const std::string& canonical, uint8_t op_mask)
{
  std::lock_guard<std::mutex> cfg_lock(mConfigMutex);
  auto it = mDisabledBranches.find(canonical);

  if ((it == mDisabledBranches.end()) || !(it->second & op_mask)) {
    return false;
  }

  it->second &= ~op_mask;

  if (it->second == 0) {
    mDisabledBranches.erase(it);
  }

  ReapplyDisabledBranchesLocked();
  return true;
}

//------------------------------------------------------------------------------
// Get the configured disabled branch rules of this space
//------------------------------------------------------------------------------
DisabledBranchesT
ClusterMgr::GetDisabledBranches() const
{
  std::lock_guard<std::mutex> cfg_lock(mConfigMutex);
  return mDisabledBranches;
}

//------------------------------------------------------------------------------
// Set the configured placement strategy override of this space
//------------------------------------------------------------------------------
void
ClusterMgr::SetConfiguredStrategy(PlacementStrategyT strategy)
{
  mConfiguredStrategy.store(static_cast<int16_t>(strategy), std::memory_order_release);
}

//------------------------------------------------------------------------------
// Get the configured placement strategy override of this space
//------------------------------------------------------------------------------
std::optional<PlacementStrategyT>
ClusterMgr::GetConfiguredStrategy() const
{
  const int16_t raw = mConfiguredStrategy.load(std::memory_order_acquire);

  if (raw < 0) {
    return std::nullopt;
  }

  return static_cast<PlacementStrategyT>(raw);
}

//------------------------------------------------------------------------------
// Get a handler seeded with a copy of the current topology snapshot
//------------------------------------------------------------------------------
ClusterMgr::SnapshotBuilder
ClusterMgr::GetSnapshotBuilderWithData()
{
  if (!mClusterData) {
    return GetSnapshotBuilder();
  }

  auto cluster_data = GetClusterData();
  ClusterData cluster_data_copy(cluster_data());
  return SnapshotBuilder(*this, std::move(cluster_data_copy));
}

//------------------------------------------------------------------------------
// Get a human readable dump of the current snapshot
//------------------------------------------------------------------------------
std::string
ClusterMgr::GetState(std::string_view type)
{
  using namespace std::string_view_literals;
  std::stringstream ss;
  eos::common::RCUReadLock rlock(mClusterMgrRcu);

  if (!mClusterData) {
    return ss.str();
  }

  if (type == "bucket"sv || type == "all"sv) {
    ss << GetBucketsAsString(*mClusterData);
  }
  if (type == "disk"sv || type == "all"sv) {
    const auto& limits = mClusterData->fill_limits;
    ss << "fill limits: warn="
       << static_cast<uint16_t>(limits.warn.load(std::memory_order_relaxed))
       << "% cap=" << static_cast<uint16_t>(limits.cap.load(std::memory_order_relaxed))
       << "%\n";
    ss << GetDisksAsString(*mClusterData);
  }

  return ss.str();
}

//------------------------------------------------------------------------------
// Get the aggregate health picture of the current snapshot, epoch included
//------------------------------------------------------------------------------
ClusterStateSummary
ClusterMgr::GetStateSummary()
{
  eos::common::RCUReadLock rlock(mClusterMgrRcu);

  if (!mClusterData) {
    return {};
  }

  auto summary = mClusterData->GetStateSummary();
  summary.epoch = GetCurrentEpoch();
  return summary;
}

//------------------------------------------------------------------------------
// Get the writable placement capacity of the current snapshot, cached
//------------------------------------------------------------------------------
uint64_t
ClusterMgr::GetWritableFreeGiB(std::chrono::milliseconds ttl)
{
  using namespace std::chrono;
  const int64_t now =
      duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count();
  const EpochIdT epoch = mCurrentEpoch.load(std::memory_order_acquire);

  // Serve the cached figure while it is fresh and the topology it was
  // computed from is still the current one. The cache stores when the figure
  // was computed, not when it expires: the TTL is the caller's freshness
  // requirement and is judged at read time, so a lowered TTL takes effect
  // immediately instead of waiting out expiries stamped under the old one.
  if ((mCapCacheEpoch.load(std::memory_order_acquire) == epoch) &&
      (now - mCapCacheTime.load(std::memory_order_acquire) <
       duration_cast<nanoseconds>(ttl).count())) {
    return mCapCacheGiB.load(std::memory_order_relaxed);
  }

  eos::common::RCUReadLock rlock(mClusterMgrRcu);

  if (!mClusterData) {
    return 0;
  }

  const uint64_t total = mClusterData->GetWritableFreeGiB();
  // Value first, timestamp last with release: a reader accepting the new
  // timestamp is guaranteed to read a figure at least as fresh. Concurrent
  // refreshes compute equivalent figures, so the last writer winning is fine.
  mCapCacheGiB.store(total, std::memory_order_relaxed);
  mCapCacheEpoch.store(epoch, std::memory_order_relaxed);
  mCapCacheTime.store(now, std::memory_order_release);
  return total;
}

//------------------------------------------------------------------------------
// Get the type of the bucket registered under the given identifier
//------------------------------------------------------------------------------
int
ClusterMgr::SnapshotBuilder::GetBucketTypeOf(ItemIdT bucket_id) const
{
  // GetBucket is the one validity definition: it rejects positive ids, out of
  // range ids, holes (INVALID sentinel type) and a slot whose stored id does
  // not match, while returning the root for id 0
  const Bucket* bucket = mData.GetBucket(bucket_id);
  return (bucket != nullptr) ? bucket->bucket_type : -1;
}

//------------------------------------------------------------------------------
// Remove a disk from the accumulated topology
//------------------------------------------------------------------------------
bool
ClusterMgr::SnapshotBuilder::RemoveDisk(fsid_t fsid)
{
  if ((fsid == 0) || (fsid > mData.disks.size()) || (mData.disks[fsid - 1].id == 0)) {
    return false;
  }

  const ItemIdT parent_id = mData.disk_parents[fsid - 1];

  if ((parent_id < 0) && ((size_t)(-parent_id) < mData.buckets.size())) {
    auto& bucket = mData.buckets[-parent_id];
    bucket.items.erase(
        std::remove(bucket.items.begin(), bucket.items.end(), static_cast<ItemIdT>(fsid)),
        bucket.items.end());
    // Keep the running weight sane the way AddDisk maintains it; the
    // committed value is recomputed from scratch by AggregateBucketWeights
    const uint8_t weight = mData.disks[fsid - 1].weight.load(std::memory_order_relaxed);
    bucket.total_weight =
        (bucket.total_weight >= weight) ? bucket.total_weight - weight : 0;
    UntrackDiskFromFlatView(parent_id, static_cast<ItemIdT>(fsid), weight);
  }

  mData.disks[fsid - 1] = Disk();
  mData.disk_parents[fsid - 1] = 0;
  return true;
}

//------------------------------------------------------------------------------
// Add a bucket to the hierarchy
//------------------------------------------------------------------------------
bool
ClusterMgr::SnapshotBuilder::AddBucket(uint8_t bucket_type, ItemIdT bucket_id,
                                       ItemIdT parent_bucket_id)
{
  if (bucket_id > 0 || parent_bucket_id > 0) {
    return false;
  }

  int32_t index = -bucket_id;
  int32_t parent_index = -parent_bucket_id;

  // This cast is safe, we'd already checked that the value is +ve
  if ((size_t)index >= mData.buckets.size()) {
    mData.buckets.resize(index + 1);
  }

  if ((size_t)parent_index >= mData.buckets.size()) {
    return false;
  }

  // A bucket holds children of one kind only: refuse before anything is
  // touched, so a rejected append leaves the parent as it was. The root is its
  // own parent and is not a child of anything, hence the guard.
  if (parent_bucket_id != bucket_id) {
    const ChildType child_type = (bucket_type == GetBucketType(BucketType::GROUP))
                                     ? ChildType::kGroups
                                     : ChildType::kGeoBuckets;

    if (!mData.buckets[parent_index].RecordChildType(child_type)) {
      return false;
    }
  }

  // The root is its own parent and sits at level 0, everything else is one
  // level below the bucket it hangs from
  uint8_t level = 0;

  if (parent_bucket_id != bucket_id) {
    level = mData.buckets[parent_index].level + 1;
  }

  Bucket& bucket = mData.buckets.at(index);
  // A bucket the snapshot already carries is re-registered, not added afresh:
  // two writers racing to register the same scheduling group both reach here
  // with the same identifier. Holes carry the INVALID sentinel and a real
  // bucket stores its own id, which is how ClusterData::GetBucket tells them
  // apart.
  const bool exists = (bucket.bucket_type != GetBucketType(BucketType::INVALID)) &&
                      (bucket.id == bucket_id);

  if (!exists) {
    bucket = Bucket(bucket_id, bucket_type, parent_bucket_id, {}, level);

    // Handle special case when the parent is the root && we're adding root
    if (parent_bucket_id != bucket_id) {
      mData.buckets[parent_index].items.push_back(bucket_id);
    }
  } else {
    // Keep the children it already holds and leave the parent's child list
    // alone, so a repeated add leaves the topology as if it had been added
    // once. Only a bucket actually being hung somewhere else moves.
    if ((parent_bucket_id != bucket_id) && (bucket.parent != parent_bucket_id)) {
      const ItemIdT old_parent = bucket.parent;

      if ((old_parent < 0) && ((size_t)(-old_parent) < mData.buckets.size())) {
        auto& old_bucket = mData.buckets[-old_parent];
        old_bucket.items.erase(
            std::remove(old_bucket.items.begin(), old_bucket.items.end(), bucket_id),
            old_bucket.items.end());
      }

      mData.buckets[parent_index].items.push_back(bucket_id);
    }

    bucket.bucket_type = bucket_type;
    bucket.parent = parent_bucket_id;
    bucket.level = level;
  }

  mLowestBucketId = std::min(mLowestBucketId, bucket_id);
  return true;
}

//------------------------------------------------------------------------------
// Reserve an identifier for a bucket the caller does not name itself
//------------------------------------------------------------------------------
ItemIdT
ClusterMgr::SnapshotBuilder::AllocBucketId()
{
  return --mLowestBucketId;
}

//------------------------------------------------------------------------------
// Get the bucket of the scheduling group with the given index
//------------------------------------------------------------------------------
ItemIdT
ClusterMgr::SnapshotBuilder::GetGroupBucketId(unsigned int group_index) const
{
  return mData.GetGroupBucketId(group_index);
}

//------------------------------------------------------------------------------
// Get the bucket of the scheduling group with the given index, creating it
//------------------------------------------------------------------------------
ItemIdT
ClusterMgr::SnapshotBuilder::GetOrAddGroup(unsigned int group_index)
{
  if (const ItemIdT existing = mData.GetGroupBucketId(group_index); existing != 0) {
    return existing;
  }

  const ItemIdT bucket_id = AllocBucketId();

  if (!AddBucket(GetBucketType(BucketType::GROUP), bucket_id, 0)) {
    return 0;
  }

  if (group_index >= mData.group_buckets.size()) {
    mData.group_buckets.resize(group_index + 1, 0);
  }

  mData.group_buckets[group_index] = bucket_id;
  mData.buckets[-bucket_id].group_index = group_index;
  return bucket_id;
}

//------------------------------------------------------------------------------
// Get the bucket naming one geotag atom below the given parent
//------------------------------------------------------------------------------
ItemIdT
ClusterMgr::SnapshotBuilder::GetOrAddGeoBucket(ItemIdT parent_bucket_id,
                                               std::string_view atom, uint8_t bucket_type)
{
  if ((parent_bucket_id > 0) || atom.empty()) {
    return 0;
  }

  if ((size_t)(-parent_bucket_id) >= mData.buckets.size()) {
    return 0;
  }

  const uint64_t key = GeoChildKey(parent_bucket_id, HashGeoAtom(atom));

  if (auto it = mData.geotag_index.find(key); it != mData.geotag_index.end()) {
    // Reuse only if it really is the same atom; a 64 bit hash collision with a
    // different atom falls through to a distinct bucket, which stays reachable
    // by GetGeoTag even though the colliding index slot is already taken
    if (mData.buckets[-it->second].geo_atom == atom) {
      return it->second;
    }
  }

  const ItemIdT bucket_id = AllocBucketId();

  if (!AddBucket(bucket_type, bucket_id, parent_bucket_id)) {
    return 0;
  }

  mData.buckets[-bucket_id].geo_atom = atom;
  mData.geotag_index.emplace(key, bucket_id);
  return bucket_id;
}

//------------------------------------------------------------------------------
// Add a disk to the given bucket
//------------------------------------------------------------------------------
bool
ClusterMgr::SnapshotBuilder::AddDisk(Disk disk, ItemIdT bucket_id)
{
  if (disk.id == mData.disks.size() + 1)  {
    return AddDiskSequential(disk, bucket_id);
  }

  // A disk hangs from a real sub-bucket, i.e. a negative id that GetBucket
  // resolves (in range, not a hole); the root is never a disk's parent
  if ((bucket_id >= 0) || (mData.GetBucket(bucket_id) == nullptr) || (disk.id == 0)) {
    return false;
  }

  // A bucket holding sub-buckets takes no disk, see Bucket::RecordChildType.
  // Refused before anything is touched, so the topology stays as it was.
  if (!mData.buckets[-bucket_id].RecordChildType(ChildType::kDisks)) {
    return false;
  }

  // A disk the snapshot already carries is re-registered, not added afresh, so
  // detach it from wherever it currently hangs first. That keeps the child list
  // and the running weight of the bucket as if it had been added once, and also
  // covers a disk named into a different bucket than the one it sat in.
  if ((disk.id <= mData.disks.size()) && (mData.disks[disk.id - 1].id != 0)) {
    RemoveDisk(disk.id);
  }

  size_t insert_pos = disk.id - 1;

  if (disk.id > mData.disks.size()) {
    mData.disks.resize(disk.id);
    mData.disk_parents.resize(disk.id, 0);
  }

  mData.disks[insert_pos] = disk;
  mData.disk_parents[insert_pos] = bucket_id;
  mData.buckets[-bucket_id].items.push_back(disk.id);
  mData.buckets[-bucket_id].total_weight += disk.weight;
  TrackDiskInFlatView(bucket_id, disk.id, disk.weight);
  return true;
}

//------------------------------------------------------------------------------
// Add a disk whose id continues the disks array
//------------------------------------------------------------------------------
bool
ClusterMgr::SnapshotBuilder::AddDiskSequential(Disk disk, ItemIdT bucket_id)
{
  // A disk hangs from a real sub-bucket, i.e. a negative id that GetBucket
  // resolves (in range, not a hole); the root is never a disk's parent
  if ((bucket_id >= 0) || (mData.GetBucket(bucket_id) == nullptr) || (disk.id == 0)) {
    return false;
  }

  // A bucket holding sub-buckets takes no disk, see Bucket::RecordChildType.
  // Refused before anything is touched, so the topology stays as it was.
  if (!mData.buckets[-bucket_id].RecordChildType(ChildType::kDisks)) {
    return false;
  }

  mData.disks.push_back(disk);
  mData.disk_parents.push_back(bucket_id);
  mData.buckets[-bucket_id].items.push_back(disk.id);
  mData.buckets[-bucket_id].total_weight += disk.weight;
  TrackDiskInFlatView(bucket_id, disk.id, disk.weight);
  return true;
}

//------------------------------------------------------------------------------
// Get the scheduling group a bucket sits below
//------------------------------------------------------------------------------
ItemIdT
ClusterMgr::SnapshotBuilder::FindOwningGroup(ItemIdT bucket_id) const
{
  ItemIdT id = bucket_id;

  // Bounded rather than "until the root" so that a malformed parent chain
  // cannot spin here, matching the parent walks of ClusterData
  for (uint8_t i = 0; (i <= kMaxGeoDepth) && (id < 0); ++i) {
    if ((size_t)(-id) >= mData.buckets.size()) {
      break;
    }

    const auto& bucket = mData.buckets[-id];

    if (bucket.bucket_type == GetBucketType(BucketType::GROUP)) {
      // A disk attached to its group directly has no bucket above it to fold
      // into a view, the group already is its own leaf level
      return (id == bucket_id) ? 0 : id;
    }

    id = bucket.parent;
  }

  return 0;
}

//------------------------------------------------------------------------------
// Register a disk in the flat leaf view of the group owning its bucket
//------------------------------------------------------------------------------
void
ClusterMgr::SnapshotBuilder::TrackDiskInFlatView(ItemIdT bucket_id, ItemIdT disk_id,
                                                 uint8_t weight)
{
  const ItemIdT group_id = FindOwningGroup(bucket_id);

  if (group_id == 0) {
    return;
  }

  ItemIdT view_id = mData.buckets[-group_id].flat_view;

  if (view_id == 0) {
    view_id = AllocBucketId();
    const size_t index = static_cast<size_t>(-view_id);

    if (index >= mData.buckets.size()) {
      mData.buckets.resize(index + 1);
    }

    // Written into its slot directly, never pushed into anyone's items: the
    // normal descent must not see the view as a geo branch of the group
    mData.buckets[index] = Bucket(view_id, GetBucketType(BucketType::FLATVIEW), group_id,
                                  {}, mData.buckets[-group_id].level + 1);
    mData.buckets[index].child_type = GetChildType(ChildType::kDisks);
    mData.buckets[-group_id].flat_view = view_id;
  }

  auto& view = mData.buckets[-view_id];
  view.items.push_back(disk_id);
  view.total_weight += weight;
}

//------------------------------------------------------------------------------
// Remove a disk from the flat leaf view of the group owning its bucket
//------------------------------------------------------------------------------
void
ClusterMgr::SnapshotBuilder::UntrackDiskFromFlatView(ItemIdT bucket_id, ItemIdT disk_id,
                                                     uint8_t weight)
{
  const ItemIdT group_id = FindOwningGroup(bucket_id);

  if (group_id == 0) {
    return;
  }

  const ItemIdT view_id = mData.buckets[-group_id].flat_view;

  if (view_id == 0) {
    return;
  }

  auto& view = mData.buckets[-view_id];
  view.items.erase(std::remove(view.items.begin(), view.items.end(), disk_id),
                   view.items.end());
  // Keep the running weight sane the way RemoveDisk maintains it; the
  // committed value is recomputed from scratch by AggregateBucketWeights
  view.total_weight = (view.total_weight >= weight) ? view.total_weight - weight : 0;
}

//------------------------------------------------------------------------------
// Sum the weight of one bucket subtree and store it on the bucket
//------------------------------------------------------------------------------
uint64_t
ClusterMgr::SnapshotBuilder::WeighSubtree(ItemIdT bucket_id, std::vector<bool>& visited)
{
  const size_t index = -bucket_id;

  if ((bucket_id > 0) || (index >= mData.buckets.size()) || visited[index]) {
    return 0;
  }

  visited[index] = true;
  uint64_t total = 0;

  for (const auto item_id : mData.buckets[index].items) {
    if (item_id > 0) {
      if ((size_t)item_id <= mData.disks.size()) {
        // Raw capacity weight, matching how AddDisk/RemoveDisk maintain the
        // running total. Fill decay is deliberately NOT folded in here: it is
        // applied live at disk level (GetEffectiveWeight) when a leaf is
        // actually weighed, so total_weight stays a deterministic capacity
        // figure that does not go stale as percent_used drifts.
        total += mData.disks[item_id - 1].weight.load(std::memory_order_relaxed);
      }
    } else {
      total += WeighSubtree(item_id, visited);
    }
  }

  // total_weight is a uint32_t; 8k disks of weight 255 stay far below the cap,
  // but clamp rather than wrap should the topology ever grow past it
  mData.buckets[index].total_weight = static_cast<uint32_t>(
      std::min<uint64_t>(total, std::numeric_limits<uint32_t>::max()));
  return total;
}

//------------------------------------------------------------------------------
// Recompute Bucket::total_weight for the whole hierarchy, bottom up
//------------------------------------------------------------------------------
void
ClusterMgr::SnapshotBuilder::AggregateBucketWeights()
{
  if (mData.buckets.empty()) {
    return;
  }

  std::vector<bool> visited(mData.buckets.size(), false);
  // The root is bucket id 0; anything not reachable from it is weighed on its
  // own so that a detached subtree still gets a sane value
  WeighSubtree(0, visited);

  for (size_t i = 1; i < mData.buckets.size(); ++i) {
    if (!visited[i] && (mData.buckets[i].id != 0)) {
      WeighSubtree(mData.buckets[i].id, visited);
    }
  }
}

} // namespace eos::mgm::placement
