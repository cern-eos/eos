//------------------------------------------------------------------------------
//! @file RoundRobinStrategy.cc
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

#include "mgm/placement/RoundRobinStrategy.hh"
#include "common/utils/ContainerUtils.hh"

namespace eos::mgm::placement {

//------------------------------------------------------------------------------
// Create the seeder backing the given round-robin flavour
//------------------------------------------------------------------------------
std::unique_ptr<RRSeeder>
MakeRRSeeder(PlacementStrategyT strategy, size_t max_buckets)
{
  if (strategy == PlacementStrategyT::kThreadLocalRoundRobin) {
    return std::make_unique<ThreadLocalRRSeeder>(max_buckets);
  } else if (strategy == PlacementStrategyT::kRandom) {
    return std::make_unique<RandomSeeder>(max_buckets);
  } else if (strategy == PlacementStrategyT::kFidRandom) {
    return std::make_unique<FidSeeder>(max_buckets);
  }
  return std::make_unique<GlobalRRSeeder>(max_buckets);
}

//------------------------------------------------------------------------------
// Select the disks holding the replicas of a new file
//------------------------------------------------------------------------------
PlacementResult
RoundRobinStrategy::Placement(const ClusterData& cluster_data,
                              const PlacementArgs& args) const
{

  PlacementResult result(args.n_replicas);
  if (!ValidateArgs(cluster_data, args, result)) {
    return result;
  }

  int32_t bucket_index = -args.bucket_id;
  auto bucket_sz = cluster_data.buckets.size();

  if (bucket_sz > mSeed->GetNumSeeds()) {
    result.err_msg =
        "More buckets than random seeds! seeds=" + std::to_string(mSeed->GetNumSeeds()) +
        " buckets=" + std::to_string(bucket_sz);
    result.ret_code = ERANGE;
    return result;
  }

  const auto& bucket = cluster_data.buckets[bucket_index];
  auto rr_seed = mSeed->Get(bucket_index, args.n_replicas, args.fid);
  // Visit every item of the bucket exactly once: a round-robin scan from the
  // seed covers each position in bucket.items.size() steps, and the snapshot
  // is immutable, so a second pass could never turn up a candidate the first
  // one missed.
  const size_t attempts = bucket.items.size();

  for (size_t i = 0; (result.n_filled < args.n_replicas) && (i < attempts); i++) {

    auto id = eos::common::pickIndexRR(bucket.items, rr_seed + i);

    // While it is highly unlikely that we'll get a duplicate with RR placement,
    // random seed gen can still generate the same seed twice.
    if (result.Contains(id)) {
      continue;
    }

    ItemIdT item_id = id;
    if (id > 0) {
      // we are dealing with a disk! check if it is usable
      if ((size_t)id > cluster_data.disks.size()) {
        result.err_msg = "Disk ID unknown!";
        result.ret_code = ERANGE;
        return result;
      }

      if (!SelectionStrategy::ValidPlacementDisk(item_id, cluster_data, args.excludefs,
                                                 args.status, args.bookingsize)) {
        continue;
      }
    }
    result.Add(item_id);
  }

  if (result.n_filled != args.n_replicas) {
    result.err_msg = "Could not find enough items to place replicas";
    result.ret_code = ENOSPC;
    return result;
  }

  result.ret_code = 0;
  return result;
}

} // namespace eos::mgm::placement
