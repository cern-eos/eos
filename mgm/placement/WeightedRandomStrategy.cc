//------------------------------------------------------------------------------
//! @file WeightedRandomStrategy.cc
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

#include "mgm/placement/WeightedRandomStrategy.hh"
#include "mgm/placement/InlinedVector.hh"
#include <algorithm>
#include <cmath>
#include <limits>
#include <random>

namespace eos::mgm::placement
{

namespace {

//------------------------------------------------------------------------------
//! Struct ScoredItem - candidate together with its rendezvous score
//------------------------------------------------------------------------------
struct ScoredItem {
  ItemIdT id;   ///< Item identifier
  double score; ///< Rendezvous score, lower wins

  //----------------------------------------------------------------------------
  //! Less than operator, orders by score
  //!
  //! @param other right hand side
  //!
  //! @return true if this item sorts before other, otherwise false
  //----------------------------------------------------------------------------
  bool
  operator<(const ScoredItem& other) const
  {
    return score < other.score;
  }
};

//------------------------------------------------------------------------------
//! Draw a uniform value in (0, 1) out of a 64 bit hash
//!
//! @param h hash value
//!
//! @return uniform value, never exactly 0 so that its logarithm is finite
//------------------------------------------------------------------------------
double
Uniform01(uint64_t h)
{
  const double u = (h >> 11) * 0x1.0p-53;
  return (u > 0.0) ? u : std::numeric_limits<double>::min();
}

} // anonymous namespace

//------------------------------------------------------------------------------
// Select the disks holding the replicas of a new file
//------------------------------------------------------------------------------
PlacementResult
WeightedRandomStrategy::Placement(const ClusterData& data,
                                  const PlacementArgs& args) const
{
  PlacementResult result(args.n_replicas);

  if (!ValidateArgs(data, args, result)) {
    return result;
  }

  // Weighted rendezvous hashing (Efraimidis-Spirakis): draw u in (0, 1) from a
  // hash of (fid, item, salt) and score the item by -log(u) / weight. The
  // n_replicas lowest scores are the selection, distributed proportionally to
  // the weights. Every candidate is inspected exactly once, so unlike sampling
  // there is no attempt loop, no duplicate to skip, and no state to carry
  // between calls - the result depends only on the snapshot and the arguments.
  // A request without a file identity cannot be placed deterministically: every
  // such file would hash to the same disks. Fall back to a per-call random draw
  // so that those callers still get a weighted spread.
  uint64_t identity = args.fid;

  if (identity == 0) {
    static thread_local std::mt19937_64 gen(std::random_device{}());
    identity = gen();
  }

  const auto& bucket = data.buckets[-args.bucket_id];
  // Per-visit scratch: inline for the common small bucket, spilling to the heap
  // only for an unusually large one, so the hot path never hits the allocator
  InlinedVector<ScoredItem, 64> ranked;
  ranked.reserve(bucket.items.size());

  for (const auto item_id : bucket.items) {
    uint32_t weight = 0;

    if (item_id > 0) {
      // An id past the end of the disks array means the bucket contents and the
      // disks array of the same snapshot disagree - a hard error, not a skip.
      if ((size_t)item_id > data.disks.size()) {
        result.err_msg = "Disk ID out of range";
        result.ret_code = ERANGE;
        return result;
      }

      if (!ValidPlacementDisk(item_id, data, args.excludefs, args.status,
                              args.bookingsize)) {
        continue;
      }

      weight = GetEffectiveWeight(data.disks[item_id - 1], data.fill_limits);
    } else {
      weight = data.buckets.at(-item_id).total_weight;
    }

    if (weight == 0) {
      // a zero weight item can never be selected, and would divide by zero
      continue;
    }

    const double u =
        Uniform01(HashFid(identity, static_cast<uint64_t>(item_id), args.salt));
    ranked.push_back({item_id, -std::log(u) / weight});
  }

  const size_t n_wanted = std::min(static_cast<size_t>(args.n_replicas), ranked.size());
  std::partial_sort(ranked.begin(), ranked.begin() + n_wanted, ranked.end());

  for (size_t i = 0; i < n_wanted; ++i) {
    result.Add(ranked[i].id);
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
