//------------------------------------------------------------------------------
//! @file ClusterBuilder.cc
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
#include "common/Logging.hh"
#include "common/utils/ContainerUtils.hh"
#include <algorithm>
#include <set>

namespace eos::mgm::placement {

//------------------------------------------------------------------------------
// Get the placement weight of a disk from its raw capacity
//------------------------------------------------------------------------------
uint8_t
CapacityToWeight(uint64_t capacity)
{
  constexpr uint64_t one_tb = 1ULL << 40;
  const uint64_t weight = capacity / one_tb;

  if (weight == 0) {
    return 1;
  }

  return static_cast<uint8_t>(std::min<uint64_t>(weight, 255));
}

//------------------------------------------------------------------------------
// Build the topology of one space and commit it to the given cluster manager
//------------------------------------------------------------------------------
void
BuildClusterData(ClusterMgr& mgr, const std::vector<FsDescription>& fs_list)
{
  std::set<unsigned int> group_indices;

  for (const auto& fs : fs_list) {
    group_indices.insert(fs.group_index);
  }

  // Root plus one bucket per group; the geo buckets that follow grow the array
  // further, this only saves the first few reallocations
  auto builder = mgr.GetSnapshotBuilder(common::next_power2(group_indices.size() + 1));

  if (!builder.AddBucket(GetBucketType(BucketType::ROOT), 0)) {
    eos_static_crit("%s", "msg=\"Failed to add root bucket\"");
    // A snapshot without a root is useless; abandon the draft so the manager
    // keeps whatever it had rather than going live with a broken topology.
    builder.Abandon();
    return;
  }

  for (const auto group_index : group_indices) {
    if (builder.GetOrAddGroup(group_index) == 0) {
      eos_static_crit("msg=\"Failed to add group bucket\" group_index=%u", group_index);
    }
  }

  // Adding the disks in fsid order keeps every one of them on the appending
  // path of AddDisk instead of growing the array one disk at a time
  std::vector<const FsDescription*> sorted_fs;
  sorted_fs.reserve(fs_list.size());

  for (const auto& fs : fs_list) {
    sorted_fs.push_back(&fs);
  }

  std::sort(
      sorted_fs.begin(), sorted_fs.end(),
      [](const FsDescription* l, const FsDescription* r) { return l->fsid < r->fsid; });

  for (const auto* fs : sorted_fs) {
    AddFsToCluster(builder, *fs);
  }

  // A rebuild tolerates individual filesystem failures (each logged above) and
  // publishes what it built; make that commit point explicit rather than
  // leaving it to scope exit.
  builder.Commit();
}

//------------------------------------------------------------------------------
// Add one file system to a topology under construction
//------------------------------------------------------------------------------
bool
AddFsToCluster(ClusterMgr::SnapshotBuilder& builder, const FsDescription& fs)
{
  if (fs.fsid == 0) {
    eos_static_warning("%s", "msg=\"Skipping file system with a zero id\"");
    return false;
  }

  // The bulk builder adds the root and the group buckets up front; an
  // incremental insert has to make sure they exist
  if (builder.GetBucketTypeOf(0) != GetBucketType(BucketType::ROOT)) {
    if (!builder.AddBucket(GetBucketType(BucketType::ROOT), 0)) {
      eos_static_crit("%s", "msg=\"Failed to add root bucket\"");
      return false;
    }
  }

  // A group the snapshot has not seen yet is created here, identifier and all,
  // so registering a file system into a brand new scheduling group needs no
  // full rebuild behind it
  const ItemIdT group_id = builder.GetOrAddGroup(fs.group_index);

  if (group_id == 0) {
    eos_static_crit("msg=\"Failed to add group bucket\" fsid=%u group_index=%u", fs.fsid,
                    fs.group_index);
    return false;
  }

  auto atoms = SplitGeoTag(fs.geotag);

  if (atoms.empty()) {
    // A disk without a geotag still gets a bucket of its own rather than
    // hanging off its group directly, so that a bucket never holds a mix of
    // disks and sub-buckets
    atoms.push_back(kNoGeoTagBucket);
  } else if (atoms.size() > kMaxGeoDepth) {
    eos_static_warning("msg=\"Truncating geotag deeper than %d levels\" "
                       "fsid=%u geotag=\"%s\"",
                       kMaxGeoDepth, fs.fsid, fs.geotag.c_str());
    atoms.resize(kMaxGeoDepth);
  }

  ItemIdT parent_id = group_id;

  for (size_t i = 0; i < atoms.size(); ++i) {
    const auto bucket_type = GetBucketType(GeoLevelToBucketType(i + 1));
    const ItemIdT bucket_id = builder.GetOrAddGeoBucket(parent_id, atoms[i], bucket_type);

    if (bucket_id == 0) {
      eos_static_crit("msg=\"Failed to add geo bucket\" fsid=%u parent_id=%d "
                      "atom=\"%s\"",
                      fs.fsid, parent_id, std::string(atoms[i]).c_str());
      return false;
    }

    parent_id = bucket_id;
  }

  const Disk disk(fs.fsid, fs.ops, fs.active_status, CapacityToWeight(fs.capacity),
                  fs.percent_used, FreeSpaceToGiB(fs.free_bytes));

  if (!builder.AddDisk(disk, parent_id)) {
    eos_static_crit("msg=\"Failed to add disk\" fsid=%u bucket_id=%d", fs.fsid,
                    parent_id);
    return false;
  }

  return true;
}

} // namespace eos::mgm::placement
