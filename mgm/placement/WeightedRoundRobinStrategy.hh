//------------------------------------------------------------------------------
//! @file WeightedRoundRobinStrategy.hh
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

#pragma once
#include "mgm/placement/RoundRobinStrategy.hh"
#include "mgm/placement/SelectionStrategy.hh"

namespace eos::mgm::placement {

//------------------------------------------------------------------------------
//! Class WeightedRoundRobinStrategy - places files round-robin while
//! honouring the item weights, which currently reflect the disk capacities. Per
//! item weight counters are decremented as items get chosen and refilled once
//! depleted. Access requests do not go through the strategy at all; a reachable
//! replica is picked uniformly at random by a free function in FlatScheduler.cc.
//------------------------------------------------------------------------------
class WeightedRoundRobinStrategy : public SelectionStrategy {
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param strategy placement strategy type
  //! @param max_buckets maximum number of buckets in the hierarchy
  //----------------------------------------------------------------------------
  WeightedRoundRobinStrategy(PlacementStrategyT strategy, size_t max_buckets)
      : mSeed(MakeRRSeeder(PlacementStrategyT::kRoundRobin, max_buckets))
  {
  }

  //----------------------------------------------------------------------------
  //! Select the disks holding the replicas of a new file
  //!
  //! @param data topology snapshot to place on
  //! @param args placement arguments
  //!
  //! @return placement result, convertible to false if placement failed
  //----------------------------------------------------------------------------
  virtual PlacementResult Placement(const ClusterData& data,
                                    const PlacementArgs& args) const override;

  //----------------------------------------------------------------------------
  //! Grow the seeder so that it can serve a topology of the given size
  //!
  //! @param n_buckets number of buckets in the topology
  //----------------------------------------------------------------------------
  void
  EnsureCapacity(size_t n_buckets) override
  {
    mSeed->EnsureCapacity(n_buckets);
  }

private:
  //! Per bucket round-robin cursor. This is the only state the strategy keeps
  //! and it is deliberately not derived from any topology snapshot, so it stays
  //! valid across snapshot swaps. The seeder synchronises itself.
  std::unique_ptr<RRSeeder> mSeed;
};

} // namespace eos::mgm::placement
