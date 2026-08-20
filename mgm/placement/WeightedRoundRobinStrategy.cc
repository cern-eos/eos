//------------------------------------------------------------------------------
//! @file WeightedRoundRobinStrategy.cc
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

#include "mgm/placement/WeightedRoundRobinStrategy.hh"
#include "common/Logging.hh"
#include "mgm/placement/InlinedVector.hh"
#include <algorithm>

namespace eos::mgm::placement {

//! Step the round-robin cursor is scaled by to turn the item counting units of
//! the seeder into a hop through the weight space. Knuth's multiplicative
//! constant, a prime near 2^32 / phi, so it shares no factor with any total
//! weight a bucket can reach: successive requests cycle through the whole
//! space instead of crawling a couple of units at a time and handing
//! consecutive files the very same disks. The long run stays proportional to
//! the weights - every position is visited equally often - and the cursor
//! wrapping around uint64 does not disturb that.
static constexpr uint64_t kWeightHop = 2654435761ULL;

//------------------------------------------------------------------------------
//! Take one item out of the weight space, keeping the ids and the cumulative
//! weights in step so that the next pick walks the reduced space
//!
//! @param ids candidate items
//! @param cumulative running weight totals, one per candidate
//! @param index position of the item to drop
//! @param weight weight of the item to drop
//------------------------------------------------------------------------------
template <typename IdsT, typename CumulativeT>
static void
DropItem(IdsT& ids, CumulativeT& cumulative, size_t index, uint64_t weight)
{
  for (size_t i = index + 1; i < ids.size(); ++i) {
    ids[i - 1] = ids[i];
    cumulative[i - 1] = cumulative[i] - weight;
  }

  ids.pop_back();
  cumulative.pop_back();
}

//------------------------------------------------------------------------------
// Select the disks holding the replicas of a new file
//------------------------------------------------------------------------------
PlacementResult
WeightedRoundRobinStrategy::Placement(const ClusterData& cluster_data,
                                      const PlacementArgs& args) const
{
  PlacementResult result(args.n_replicas);

  if (!ValidateArgs(cluster_data, args, result)) {
    return result;
  }

  // The scheduler grows the seeder to the topology before descending, so this
  // only catches a hierarchy past what the seeder can ever hold. Reporting it
  // is the point: the cursor lookup below would otherwise throw out of the
  // placement path.
  if (cluster_data.buckets.size() > mSeed->GetNumSeeds()) {
    result.err_msg =
        "More buckets than random seeds! seeds=" + std::to_string(mSeed->GetNumSeeds()) +
        " buckets=" + std::to_string(cluster_data.buckets.size());
    result.ret_code = ERANGE;
    return result;
  }

  // Build the cumulative weight table of the candidates that can actually take
  // a replica. Selecting a position in that table and resolving it back to an
  // item picks proportionally to the weights, and derives everything from the
  // snapshot in front of us rather than from counters kept between calls.
  const int32_t bucket_index = -args.bucket_id;
  const auto& bucket = cluster_data.buckets[bucket_index];
  // Per-visit scratch: inline for the common small bucket, spilling to the heap
  // only for an unusually large one, so the hot path never hits the allocator
  InlinedVector<ItemIdT, 64> ids;
  InlinedVector<uint64_t, 64> cumulative;
  ids.reserve(bucket.items.size());
  cumulative.reserve(bucket.items.size());
  uint64_t total_weight = 0;

  for (const auto item_id : bucket.items) {
    uint32_t weight = 0;

    if (item_id > 0) {
      // An id past the end of the disks array means the bucket contents and the
      // disks array of the same snapshot disagree - a hard error, not a skip.
      if ((size_t)item_id > cluster_data.disks.size()) {
        result.err_msg = "Disk ID unknown!";
        result.ret_code = ERANGE;
        return result;
      }

      if (!ValidDisk(item_id, cluster_data, args.excludefs, args.op, args.bookingsize)) {
        continue;
      }

      weight =
          GetEffectiveWeight(cluster_data.disks[item_id - 1], cluster_data.fill_limits);
    } else {
      weight = cluster_data.buckets.at(-item_id).total_weight;
    }

    if (weight == 0) {
      // a zero weight item can never take a replica
      continue;
    }

    total_weight += weight;
    ids.push_back(item_id);
    cumulative.push_back(total_weight);
  }

  if (total_weight == 0) {
    result.err_msg = "No item with a non-zero weight to place on";
    result.ret_code = ENOSPC;
    return result;
  }

  // Walk the weight space one pick at a time, taking the chosen item out of it
  // before the next pick. Drawing without replacement is what makes a request
  // that has enough candidates always come back full: probing a fixed lattice
  // of positions instead - one every total_weight / n_replicas - lets an item
  // heavier than that spacing swallow several probes, and the request ends
  // short of its replicas however many attempts it is given, for some weight
  // distributions whatever the cursor is, which no retry can recover.
  // Removing an item also shrinks what is left to walk, hence the stride
  // recomputed against the remaining weight and the replicas still owed.
  uint64_t cursor = mSeed->Get(bucket_index, args.n_replicas, args.fid) * kWeightHop;

  while (!ids.empty() && (total_weight > 0)) {
    // pos < total_weight == cumulative.back(), so this never runs off the end
    const uint64_t pos = cursor % total_weight;
    const size_t index =
        std::upper_bound(cumulative.begin(), cumulative.end(), pos) - cumulative.begin();
    const uint64_t weight = cumulative[index] - ((index > 0) ? cumulative[index - 1] : 0);

    if (!result.Add(ids[index])) {
      break;
    }

    DropItem(ids, cumulative, index, weight);
    total_weight -= weight;
    const int remaining = args.n_replicas - result.n_filled;

    if (remaining <= 0) {
      break;
    }

    cursor += std::max<uint64_t>(1, total_weight / static_cast<uint64_t>(remaining));
  }

  if (result.n_filled != args.n_replicas) {
    result.err_msg = "Could not find enough items to place replicas, added=" +
                     std::to_string(result.n_filled) +
                     " requested=" + std::to_string(args.n_replicas);
    result.ret_code = ENOSPC;
    return result;
  }

  result.ret_code = 0;
  return result;
}
} // namespace eos::mgm::placement
