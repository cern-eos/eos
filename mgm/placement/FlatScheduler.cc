//------------------------------------------------------------------------------
//! @file FlatScheduler.cc
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

#include "mgm/placement/FlatScheduler.hh"
#include "common/utils/RandUtils.hh"
#include "mgm/placement/InlinedVector.hh"
#include "mgm/placement/RoundRobinStrategy.hh"
#include "mgm/placement/WeightedRandomStrategy.hh"
#include "mgm/placement/WeightedRoundRobinStrategy.hh"

namespace eos::mgm::placement {

//------------------------------------------------------------------------------
// Create the selection strategy implementing the given type
//------------------------------------------------------------------------------
std::unique_ptr<SelectionStrategy>
MakeSelectionStrategy(PlacementStrategyT type, size_t max_buckets)
{
  switch (type) {
  case PlacementStrategyT::kRoundRobin: [[fallthrough]];
  case PlacementStrategyT::kThreadLocalRoundRobin: [[fallthrough]];
  case PlacementStrategyT::kRandom: [[fallthrough]];
  case PlacementStrategyT::kFidRandom:
    return std::make_unique<RoundRobinStrategy>(type, max_buckets);
  case PlacementStrategyT::kWeightedRandom:
    return std::make_unique<WeightedRandomStrategy>(type, max_buckets);
  case PlacementStrategyT::kWeightedRoundRobin:
    return std::make_unique<WeightedRoundRobinStrategy>(type, max_buckets);
  case PlacementStrategyT::kGeoScheduler:
    // Geo awareness lives in the descent, which follows the client's geotag
    // down the hierarchy whatever strategy picks the items at each level. What
    // the geo scheduler names is therefore only the picker, and a capacity
    // aware one is the right default.
    return std::make_unique<WeightedRandomStrategy>(type, max_buckets);
  default:
    return nullptr;
  }

}

//------------------------------------------------------------------------------
// Constructor building every known selection strategy
//------------------------------------------------------------------------------
FlatScheduler::FlatScheduler(size_t max_buckets)
    : mCapacity(max_buckets)
{
  for (size_t i = 0; i < TOTAL_PLACEMENT_STRATEGIES; i++) {
    mSelectionStrategy[i] =
        MakeSelectionStrategy(static_cast<PlacementStrategyT>(i), max_buckets);
  }
}

//------------------------------------------------------------------------------
// Constructor building only the given selection strategy
//------------------------------------------------------------------------------
FlatScheduler::FlatScheduler(PlacementStrategyT strategy, size_t max_buckets)
    : mDefaultStrategy(strategy)
    , mCapacity(max_buckets)
{
  mSelectionStrategy[static_cast<int>(strategy)] =
      MakeSelectionStrategy(strategy, max_buckets);
}

//------------------------------------------------------------------------------
// Grow the per bucket state of every strategy to the size of the topology
//------------------------------------------------------------------------------
void
FlatScheduler::EnsureCapacity(size_t n_buckets)
{
  if (n_buckets <= mCapacity.load(std::memory_order_acquire)) {
    return;
  }

  for (auto& strategy : mSelectionStrategy) {
    if (strategy != nullptr) {
      strategy->EnsureCapacity(n_buckets);
    }
  }

  // Raised only after every strategy has been told, so a concurrent placement
  // that skips the growth on the memo is looking at state already grown. Never
  // lowered, so racing callers settle on the largest topology seen.
  size_t current = mCapacity.load(std::memory_order_relaxed);

  while ((current < n_buckets) &&
         !mCapacity.compare_exchange_weak(current, n_buckets, std::memory_order_release,
                                          std::memory_order_relaxed)) {
  }
}

//------------------------------------------------------------------------------
// Record the first failure of a descent
//------------------------------------------------------------------------------
void
FlatScheduler::SetError(PlacementResult& result, int ret_code, std::string err_msg)
{
  if (result.err_msg.has_value()) {
    return;
  }

  result.ret_code = ret_code;
  result.err_msg = std::move(err_msg);
}

//------------------------------------------------------------------------------
// Ask the selection strategy of the request to pick children of one bucket
//------------------------------------------------------------------------------
PlacementResult
FlatScheduler::RunStrategy(const ClusterData& cluster_data, PlacementArgs& args,
                           ItemIdT bucket_id, uint8_t n_replicas) const
{
  args.bucket_id = bucket_id;
  args.n_replicas = n_replicas;
  return mSelectionStrategy[StrategyIndex(args.strategy)]->Placement(cluster_data, args);
}

//------------------------------------------------------------------------------
// Place a number of replicas inside one bucket
//------------------------------------------------------------------------------
void
FlatScheduler::PlaceInBucket(const ClusterData& cluster_data, PlacementArgs& args,
                             const DescentContext& ctx, ItemIdT bucket_id,
                             uint8_t n_replicas, size_t atom_index, uint8_t depth,
                             PlacementResult& result)
{
  if (n_replicas == 0) {
    return;
  }

  // The starting bucket is checked by the caller, but the ones handed back by
  // the strategy are not, and a cycle in the hierarchy would otherwise spin
  // here forever under an RCU read lock.
  const Bucket* bucket_ptr = cluster_data.GetBucket(bucket_id);

  if (bucket_ptr == nullptr) {
    SetError(result, ERANGE, "Bucket id out of range during descent");
    return;
  }

  if (depth > MAX_PLACEMENT_DEPTH) {
    SetError(result, ELOOP,
             "Bucket hierarchy deeper than " + std::to_string(MAX_PLACEMENT_DEPTH) +
                 " levels, possible cycle");
    return;
  }

  const auto& bucket = *bucket_ptr;

  // An administratively disabled branch takes no new replicas. Every route
  // into a bucket funnels through here - strategy picks, forced starts and
  // spilled shortfalls alike - and the spill pass re-routes what a disabled
  // branch refuses to the surviving ones.
  if (bucket.IsDisabledFor(kDisabledPlct)) {
    SetError(result, EACCES,
             "Bucket " + std::to_string(bucket_id) + " branch is disabled");
    return;
  }

  if (bucket.items.empty()) {
    SetError(result, ENOENT, "Bucket " + std::to_string(bucket_id) + " holds no items");
    return;
  }

  // The kind of children decides the rule of this level. The topology builder
  // keeps child_type in step with the children and refuses a mixed bucket, so
  // no child has to be looked at to dispatch.
  switch (bucket.child_type) {
  case GetChildType(ChildType::kDisks):
    PlaceOnDisks(cluster_data, args, bucket_id, n_replicas, result);
    return;

  case GetChildType(ChildType::kGroups):
    PlaceInGroup(cluster_data, args, ctx, bucket_id, n_replicas, atom_index, depth,
                 result);
    return;

  case GetChildType(ChildType::kGeoBuckets):
    // A descent with no geo constraint to honour - either the request is not
    // geo scheduled at all or the client carries no geotag, see Schedule - has
    // no reason to walk the geo levels, and the scheduling group is the failure
    // domain by design, its disks sit on distinct nodes. So a group (the only
    // kind of bucket carrying a flat view) is served from that view directly:
    // one strategy pick over every disk of the group instead of one per geo
    // level. A disabled-branch rule survives the shortcut without the interior
    // buckets being visited, because the disks below it answer for themselves,
    // see SelectionStrategy::ValidPlacementDisk.
    if ((bucket.flat_view != 0) && ctx.geo_atoms.empty()) {
      PlaceInBucket(cluster_data, args, ctx, bucket.flat_view, n_replicas, atom_index,
                    depth + 1, result);
      return;
    }

    PlaceAcrossGeoBranches(cluster_data, args, ctx, bucket, bucket_id, n_replicas,
                           atom_index, depth, result);
    return;

  default:
    // A non-empty bucket always has a child type, so this is a corrupted
    // snapshot rather than a reachable state
    SetError(result, EINVAL,
             "Bucket " + std::to_string(bucket_id) + " holds children of unknown kind");
    return;
  }
}

//------------------------------------------------------------------------------
// Check whether every disk of a bucket sits below a disabled branch
//------------------------------------------------------------------------------
static bool
AllDisksBranchDisabled(const ClusterData& cluster_data, const Bucket& bucket)
{
  for (const auto item_id : bucket.items) {
    if ((item_id > 0) && !cluster_data.IsBranchDisabled(item_id, kDisabledPlct)) {
      return false;
    }
  }

  return true;
}

//------------------------------------------------------------------------------
// Place replicas on a bucket of disks: the strategy picks the whole quota in
// one go
//------------------------------------------------------------------------------
void
FlatScheduler::PlaceOnDisks(const ClusterData& cluster_data, PlacementArgs& args,
                            ItemIdT bucket_id, uint8_t n_replicas,
                            PlacementResult& result)
{
  auto plct_result = RunStrategy(cluster_data, args, bucket_id, n_replicas);

  if (plct_result.n_filled == 0) {
    // "Out of space" is the wrong story to tell when the disks are there and a
    // rule is all that keeps them out of reach. Only a flat leaf view can get
    // here in that state - anywhere else the descent refused the disabled
    // bucket on its way in - and the walk is only ever paid on this failure
    // path, which has already given up on placing anything.
    if (cluster_data.HasPlctDisabledBranches() &&
        AllDisksBranchDisabled(cluster_data, cluster_data.buckets[-bucket_id])) {
      SetError(result, EACCES,
               "Every disk of bucket " + std::to_string(bucket_id) +
                   " sits below a disabled branch");
      return;
    }

    SetError(result, plct_result ? ENOSPC : plct_result.ret_code,
             plct_result.ErrorString());
    return;
  }

  for (int i = 0; i < plct_result.n_filled; ++i) {
    const ItemIdT disk_id = plct_result.ids[i];

    if ((disk_id > 0) && !result.Contains(disk_id)) {
      result.Add(disk_id);
    }
  }
}

//------------------------------------------------------------------------------
// Place replicas at the level of the scheduling groups
//------------------------------------------------------------------------------
void
FlatScheduler::PlaceInGroup(const ClusterData& cluster_data, PlacementArgs& args,
                            const DescentContext& ctx, ItemIdT bucket_id,
                            uint8_t n_replicas, size_t atom_index, uint8_t depth,
                            PlacementResult& result)
{
  if (args.forced_group_index >= 0) {
    // The index names a group of this topology or nothing at all; there is no
    // arithmetic left that could land it on some unrelated bucket
    const ItemIdT forced_id = cluster_data.GetGroupBucketId(args.forced_group_index);

    if ((forced_id == 0) || (cluster_data.GetBucket(forced_id) == nullptr)) {
      SetError(result, EINVAL, "Invalid forced group index");
      return;
    }

    PlaceInBucket(cluster_data, args, ctx, forced_id, n_replicas, atom_index, depth + 1,
                  result);
    return;
  }

  // The group level is the one place where the descent must not spread: every
  // replica of a file belongs to the same group, so exactly one is chosen and
  // handed the whole quota. No spill on a shortfall either - offering it to a
  // second group would split the file across two of them - so a group that
  // cannot take the whole file is abandoned as a whole, which is what the
  // retry loop in FsScheduler::Schedule does with a fresh salt.
  auto plct_result = RunStrategy(cluster_data, args, bucket_id, 1);

  if (plct_result.n_filled == 0) {
    SetError(result, plct_result ? ENOSPC : plct_result.ret_code,
             plct_result.ErrorString());
    return;
  }

  // A scheduling group is not a geo level, so descending into one leaves the
  // client's geotag where it is
  PlaceInBucket(cluster_data, args, ctx, plct_result.ids[0], n_replicas, atom_index,
                depth + 1, result);
}

//------------------------------------------------------------------------------
// Place replicas at a level of the geo hierarchy: split the quota between the
// branch the client sits in and the rest of them
//------------------------------------------------------------------------------
void
FlatScheduler::PlaceAcrossGeoBranches(const ClusterData& cluster_data,
                                      PlacementArgs& args, const DescentContext& ctx,
                                      const Bucket& bucket, ItemIdT bucket_id,
                                      uint8_t n_replicas, size_t atom_index,
                                      uint8_t depth, PlacementResult& result)
{
  const int target = result.n_filled + n_replicas;
  ItemIdT home_id = 0;
  uint8_t n_home = 0;

  if (atom_index < ctx.geo_atoms.size()) {
    // FindGeoChild is keyed by (parent, atom), so this can only ever return a
    // child of this very bucket
    home_id = cluster_data.FindGeoChild(bucket_id, ctx.geo_atoms[atom_index]);

    // A disabled home branch is no home at all: the quota flows to the away
    // branches directly instead of bouncing off the descent guard
    if ((home_id != 0) && cluster_data.buckets[-home_id].IsDisabledFor(kDisabledPlct)) {
      home_id = 0;
    }

    n_home = std::min<uint8_t>(n_replicas, ctx.n_collocated);
  }

  if ((home_id != 0) && (n_home > 0)) {
    PlaceInBucket(cluster_data, args, ctx, home_id, n_home, atom_index + 1, depth + 1,
                  result);
  } else {
    home_id = 0;
    n_home = 0;
  }

  const uint8_t n_away = n_replicas - n_home;
  // Inline scratch: a bucket rarely holds more than a handful of branches, so
  // walking a level does not have to touch the allocator
  InlinedVector<ItemIdT, 16> branches;

  if (n_away > 0) {
    // Ask for one branch more than needed so that the home one, which the
    // strategy knows nothing about, can be dropped without running short
    const size_t n_wanted = (size_t)n_away + (home_id != 0);
    auto plct_result = RunStrategy(
        cluster_data, args, bucket_id,
        static_cast<uint8_t>(std::min<size_t>(n_wanted, bucket.items.size())));

    if ((plct_result.n_filled == 0) && (result.n_filled < target)) {
      SetError(result, plct_result ? ENOSPC : plct_result.ret_code,
               plct_result.ErrorString());
      return;
    }

    for (int i = 0; (i < plct_result.n_filled) && (branches.size() < n_away); ++i) {
      if (plct_result.ids[i] != home_id) {
        branches.push_back(plct_result.ids[i]);
      }
    }

    // A branch chosen *away* from the client is by definition off the path of
    // its geotag and gives up on it. Hand every chosen branch its share, the
    // first ones taking the remainder of an uneven split.
    const int n_branches = branches.size();

    for (int i = 0; i < n_branches; ++i) {
      const uint8_t quota = n_away / n_branches + ((i < (n_away % n_branches)) ? 1 : 0);
      PlaceInBucket(cluster_data, args, ctx, branches[i], quota, ctx.geo_atoms.size(),
                    depth + 1, result);
    }
  }

  // Whatever the chosen branches could not take is offered to every branch of
  // this bucket, before the level above hears about a shortfall. This is what
  // keeps an undersized branch from failing the whole placement, and it doubles
  // as the up-root fallback: a quota that cannot be met where the client is
  // gets met next to it rather than not at all.
  if (result.n_filled < target) {
    // The branches that were not asked yet come first, the home one among them:
    // a fresh branch beats doubling up on one that already gave what it could
    InlinedVector<ItemIdT, 16> fallback;
    fallback.reserve(bucket.items.size());

    for (const auto item_id : bucket.items) {
      if (std::find(branches.begin(), branches.end(), item_id) == branches.end()) {
        fallback.push_back(item_id);
      }
    }

    for (const auto branch_id : branches) {
      fallback.push_back(branch_id);
    }
    // Everything already placed has to be off the table, otherwise a branch
    // being revisited would just hand back the disks it gave us the first time
    const size_t n_excluded = args.excludefs.size();

    for (int i = 0; i < result.n_filled; ++i) {
      args.excludefs.push_back(result.ids[i]);
    }

    for (const auto branch_id : fallback) {
      if (result.n_filled >= target) {
        break;
      }

      const uint8_t deficit = static_cast<uint8_t>(target - result.n_filled);
      const int before = result.n_filled;
      // No geo preference on a spill, the point of it is to look elsewhere
      PlaceInBucket(cluster_data, args, ctx, branch_id, deficit, ctx.geo_atoms.size(),
                    depth + 1, result);

      for (int i = before; i < result.n_filled; ++i) {
        args.excludefs.push_back(result.ids[i]);
      }
    }

    args.excludefs.resize(n_excluded);
  }
}

//------------------------------------------------------------------------------
// Select the disks holding the replicas of a new file
//------------------------------------------------------------------------------
PlacementResult
FlatScheduler::Schedule(const ClusterData& cluster_data, PlacementArgs args)
{
  PlacementResult result;

  // A rejected request carries an errno of its own, never the default -1: the
  // retry loop in FsScheduler::Schedule only keeps trying on ENOSPC, and a zero
  // replica request would otherwise be handed back with n_filled == n_replicas,
  // which reads as a valid placement.
  if (args.n_replicas == 0) {
    SetError(result, EINVAL, "Zero replicas requested");
    return result;
  } else if (args.bucket_id != 0 && cluster_data.GetBucket(args.bucket_id) == nullptr) {
    // A valid starting point is either the root (bucket_id 0) or an existing
    // bucket (negative id in range, not a hole). Reject positive (disk) ids and
    // negative ids that are out of range or name a hole.
    SetError(result, ERANGE, "Bucket id out of range");
    return result;
  }

  if (!IsValidPlacementStrategy(args.strategy)) {
    args.strategy = mDefaultStrategy;
  }

  if (!IsValidPlacementStrategy(args.strategy) ||
      mSelectionStrategy[StrategyIndex(args.strategy)] == nullptr) {
    result.err_msg = "Not a valid SelectionStrategy";
    result.ret_code = EINVAL;
    return result;
  }

  // The topology this snapshot carries may have outgrown the one the engine
  // was built for, see EnsureCapacity
  EnsureCapacity(cluster_data.buckets.size());
  const uint8_t n_final_replicas = args.n_replicas;
  result.n_replicas = n_final_replicas;
  DescentContext ctx;

  // Geo aware placement is what the geo scheduler strategy names, so any other
  // strategy descends without a preferred branch at any level - as does a
  // client with no geolocation to point at one, or a policy that asks for
  // nothing to be collocated. An empty ctx.geo_atoms is what the rest of the
  // descent reads as "no geo constraint to honour", and it is also what lets a
  // scheduling group be served from its flat leaf view, see PlaceInBucket.
  if ((args.strategy == PlacementStrategyT::kGeoScheduler) && !args.geolocation.empty() &&
      (args.ncollocatedfs > 0)) {
    ctx.geo_atoms = SplitGeoTag(args.geolocation);
    ctx.n_collocated = args.ncollocatedfs;
  }

  PlaceInBucket(cluster_data, args, ctx, args.bucket_id, n_final_replicas, 0, 0, result);

  if (result.n_filled == n_final_replicas) {
    result.ret_code = 0;
    result.err_msg.reset();

    // Book the space of the file on every selected disk. Only a returned
    // placement books, a failed descent leaves nothing behind, so the retry
    // loop in FsScheduler::Schedule needs no rollback. The bookings are
    // reconciled away by the FST publishes, see ClusterData::SetDiskFreeSpace.
    if (args.bookingsize > 0) {
      for (int i = 0; i < result.n_filled; ++i) {
        if (result.ids[i] > 0) {
          cluster_data.BookDiskSpace(result.ids[i], args.bookingsize);
        }
      }
    }
  } else if (!result.err_msg.has_value()) {
    SetError(result, ENOSPC, "Could not find enough items to place replicas");
  }

  return result;
}

//------------------------------------------------------------------------------
// Select which of the existing replicas of a file should be accessed, picking
// one of the reachable ones uniformly at random.
//------------------------------------------------------------------------------
static int
AccessRandom(const ClusterData& cluster_data, AccessArgs& args)
{
  if (args.selectedfs.empty()) {
    return ENOENT;
  }

  static const std::vector<uint32_t> no_exclusions;
  const auto& unavailfs = args.unavailfs ? *args.unavailfs : no_exclusions;
  // Collect the positions of the replicas that can actually serve the request.
  // Inline scratch: a file has a handful of replicas, so this never allocates.
  InlinedVector<size_t, 32> candidates;
  candidates.reserve(args.selectedfs.size());

  for (size_t i = 0; i < args.selectedfs.size(); ++i) {
    if (SelectionStrategy::IsAccessCandidate(args.selectedfs[i], cluster_data, args,
                                             unavailfs)) {
      candidates.push_back(i);
    }
  }

  if (candidates.empty()) {
    return ENOENT;
  }

  args.selectedIndex = candidates[common::getRandom((size_t)0, candidates.size() - 1)];
  return 0;
}

//------------------------------------------------------------------------------
// Flag the replicas that cannot serve the request and count the ones that can
//------------------------------------------------------------------------------
static int
MarkUnavailableReplicas(const ClusterData& cluster_data, AccessArgs& args)
{
  if (args.selectedfs.empty()) {
    return ENOENT;
  }

  static const std::vector<uint32_t> no_exclusions;
  const auto& unavail = args.unavailfs ? *args.unavailfs : no_exclusions;
  size_t n_available = 0;

  for (const auto fsid : args.selectedfs) {
    // A replica that can serve the request stays a candidate. Anything else -
    // a zero sentinel, an offline or wrongly configured disk, one below an
    // access disabled branch, or one already flagged - is recorded so the
    // caller, and the RAIN driver in particular, knows which stripes to route
    // around. Passing the unavailable list as the exclusion set means an
    // already flagged replica reads back as invalid and is not counted twice.
    if ((fsid != 0) && !cluster_data.IsBranchDisabled(fsid, kDisabledAccess) &&
        SelectionStrategy::ValidDisk(fsid, cluster_data, unavail, args.status)) {
      ++n_available;
    } else if ((fsid != 0) && args.unavailfs &&
               (std::find(args.unavailfs->begin(), args.unavailfs->end(), fsid) ==
                args.unavailfs->end())) {
      args.unavailfs->push_back(fsid);
    }
  }

  // A RAIN layout can only be read if enough of its stripes are up; a single
  // reachable stripe is not a usable answer the way it is for a plain replica
  if (n_available < args.n_replicas) {
    return ENETUNREACH;
  }

  // A forced target that is itself unreachable cannot be honoured
  if ((args.forcedfsid != 0) && args.unavailfs &&
      (std::find(args.unavailfs->begin(), args.unavailfs->end(), args.forcedfsid) !=
       args.unavailfs->end())) {
    return ENETUNREACH;
  }

  return 0;
}

//------------------------------------------------------------------------------
// Select which of the existing replicas of a file should be accessed
//------------------------------------------------------------------------------
int
FlatScheduler::Access(const ClusterData& cluster_data, AccessArgs& args)
{
  if (!IsValidPlacementStrategy(args.strategy) ||
      mSelectionStrategy[StrategyIndex(args.strategy)] == nullptr) {
    return EINVAL;
  }

  // Flag the unreachable replicas and make sure enough are left before picking
  // one, so the strategy only ever chooses among stripes that can actually
  // serve the request
  if (int rc = MarkUnavailableReplicas(cluster_data, args); rc != 0) {
    return rc;
  }

  // Prefer the replicas closest to the client: the strategy tie-breaks among
  // the ones sharing the most leading geotag atoms with it, which is the flat
  // equivalent of the geotree closest-replica-first ordering. A forced file
  // system beats proximity, so the filter is skipped when one is set.
  if (!args.geolocation.empty() && (args.forcedfsid == 0)) {
    static const std::vector<uint32_t> no_exclusions;
    const auto& unavail = args.unavailfs ? *args.unavailfs : no_exclusions;
    const auto client_atoms = SplitGeoTag(args.geolocation);

    // Overlap of every replica that can serve the request, -1 for the rest.
    // The candidate check has to agree with the one the strategy applies, and
    // in particular honour the exclusion list: a replica the caller excluded
    // must not set the proximity bar, otherwise excluding the closest replicas
    // demotes the reachable far ones and the access fails instead of falling
    // back to them.
    std::vector<int> overlaps(args.selectedfs.size(), -1);
    int max_overlap = 0;

    for (size_t i = 0; i < args.selectedfs.size(); ++i) {
      const auto fsid = args.selectedfs[i];

      if (SelectionStrategy::IsAccessCandidate(fsid, cluster_data, args, unavail)) {
        overlaps[i] = cluster_data.GetGeoOverlap(fsid, client_atoms);
        max_overlap = std::max(max_overlap, overlaps[i]);
      }
    }

    // With no geo match anywhere there is nothing to prefer and the strategy
    // sees the same candidates it always did
    if (max_overlap > 0) {
      // Demote the farther replicas for the strategy call only: they are
      // reachable, so they must not leak into the caller's unavailfs, which
      // reports genuinely unreachable stripes back to the RAIN driver
      std::vector<uint32_t> demoted(unavail);

      for (size_t i = 0; i < args.selectedfs.size(); ++i) {
        if ((overlaps[i] >= 0) && (overlaps[i] < max_overlap)) {
          demoted.push_back(args.selectedfs[i]);
        }
      }

      auto* caller_unavail = args.unavailfs;
      args.unavailfs = &demoted;
      const int rc = AccessRandom(cluster_data, args);
      args.unavailfs = caller_unavail;
      return rc;
    }
  }

  return AccessRandom(cluster_data, args);
}
} // namespace eos::mgm::placement
