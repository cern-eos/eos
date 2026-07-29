//------------------------------------------------------------------------------
//! @file FlatScheduler.hh
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
#include "mgm/placement/ClusterDataTypes.hh"
#include "mgm/placement/SelectionStrategy.hh"
#include <algorithm>
#include <atomic>
#include <optional>

namespace eos::mgm::placement {

//------------------------------------------------------------------------------
//! Create the selection strategy implementing the given type
//!
//! @param type placement strategy type
//! @param max_buckets maximum number of buckets the strategy must handle
//!
//! @return strategy object or nullptr if the type has no implementation
//------------------------------------------------------------------------------
std::unique_ptr<SelectionStrategy> MakeSelectionStrategy(PlacementStrategyT type,
                                                         size_t max_buckets);

//------------------------------------------------------------------------------
//! Class FlatScheduler - the placement engine. Holds one object per placement
//! strategy and descends the bucket hierarchy of a ClusterData snapshot. It is
//! space agnostic, the caller decides which snapshot to operate on.
//------------------------------------------------------------------------------
class FlatScheduler {
public:
  //----------------------------------------------------------------------------
  //! Constructor - builds every known placement strategy
  //!
  //! @param max_buckets maximum number of buckets in the hierarchy
  //----------------------------------------------------------------------------
  FlatScheduler(size_t max_buckets);

  //----------------------------------------------------------------------------
  //! Constructor - builds only the given placement strategy, which also becomes
  //! the default one
  //!
  //! @param strategy placement strategy type
  //! @param max_buckets maximum number of buckets in the hierarchy
  //----------------------------------------------------------------------------
  FlatScheduler(PlacementStrategyT strategy, size_t max_buckets);

  //----------------------------------------------------------------------------
  //! Select the disks holding the replicas of a new file by descending the
  //! bucket hierarchy
  //!
  //! @param cluster_data topology snapshot to schedule on
  //! @param args placement arguments
  //!
  //! @return placement result, convertible to false if placement failed
  //----------------------------------------------------------------------------
  PlacementResult Schedule(const ClusterData& cluster_data, PlacementArgs args);

  //----------------------------------------------------------------------------
  //! Select which of the existing replicas of a file should be accessed
  //!
  //! @param cluster_data topology snapshot to select from
  //! @param args access arguments, updated with the selected index
  //!
  //! @return 0 if successful, otherwise an errno value
  //----------------------------------------------------------------------------
  int Access(const ClusterData& cluster_data, AccessArgs& args);

private:
  //----------------------------------------------------------------------------
  //! Struct DescentContext - what a descent needs to carry from level to level
  //! on top of the placement arguments
  //----------------------------------------------------------------------------
  struct DescentContext {
    //! Geolocation of the client, split into atoms. One atom is consumed per
    //! geo level, and it names the branch the collocated replicas go to.
    std::vector<std::string_view> geo_atoms;
    //! Number of replicas to keep in the branch the client points at
    uint8_t n_collocated{0};
  };

  //----------------------------------------------------------------------------
  //! Place a number of replicas inside one bucket, recursing into the
  //! sub-buckets it spreads them over
  //!
  //! Every route into a bucket funnels through here - strategy picks, forced
  //! starts and spilled shortfalls alike - so this is where the common guards
  //! live: an id handed back by a strategy is validated, the depth bounds a
  //! possible cycle, and a disabled or empty bucket takes nothing. The rule of
  //! the level is then decided by the kind of children the bucket holds, see
  //! PlaceOnDisks, PlaceInGroup and PlaceAcrossGeoBranches - except that a
  //! scheduling group entered by a client without any geo preference is served
  //! straight from its flat leaf view, skipping the geo levels, unless a
  //! disabled-branch placement rule is active, see Bucket::flat_view.
  //!
  //! @param cluster_data topology snapshot to schedule on
  //! @param args placement arguments, bucket_id and n_replicas are overwritten
  //! @param ctx geolocation and collocation budget of this descent
  //! @param bucket_id bucket to place in
  //! @param n_replicas number of replicas to place inside this bucket
  //! @param atom_index geotag atom naming the preferred child of this bucket,
  //!        past the end of ctx.geo_atoms once there is no preference left
  //! @param depth current depth of the descent, guards against a cycle
  //! @param result result accumulating the selected disks
  //----------------------------------------------------------------------------
  void PlaceInBucket(const ClusterData& cluster_data, PlacementArgs& args,
                     const DescentContext& ctx, ItemIdT bucket_id, uint8_t n_replicas,
                     size_t atom_index, uint8_t depth, PlacementResult& result);

  //----------------------------------------------------------------------------
  //! Ask the selection strategy of the request to pick children of one bucket
  //!
  //! @param cluster_data topology snapshot to pick from
  //! @param args placement arguments, bucket_id and n_replicas are overwritten
  //! @param bucket_id bucket whose children are picked among
  //! @param n_replicas number of children to pick
  //!
  //! @return result of the strategy, convertible to false if the pick failed
  //----------------------------------------------------------------------------
  PlacementResult RunStrategy(const ClusterData& cluster_data, PlacementArgs& args,
                              ItemIdT bucket_id, uint8_t n_replicas) const;

  //----------------------------------------------------------------------------
  //! Place replicas on a bucket of disks, ie. a leaf level: the strategy picks
  //! the whole quota in one go
  //!
  //! @param cluster_data topology snapshot to schedule on
  //! @param args placement arguments, bucket_id and n_replicas are overwritten
  //! @param bucket_id bucket holding the disks
  //! @param n_replicas number of replicas to place on them
  //! @param result result accumulating the selected disks
  //----------------------------------------------------------------------------
  void PlaceOnDisks(const ClusterData& cluster_data, PlacementArgs& args,
                    ItemIdT bucket_id, uint8_t n_replicas, PlacementResult& result);

  //----------------------------------------------------------------------------
  //! Place replicas at the level of the scheduling groups: exactly one group -
  //! the forced one if the request names one - is chosen and handed the whole
  //! quota, because every replica of a file belongs to the same group
  //!
  //! @param cluster_data topology snapshot to schedule on
  //! @param args placement arguments, bucket_id and n_replicas are overwritten
  //! @param ctx geolocation and collocation budget of this descent
  //! @param bucket_id bucket holding the groups
  //! @param n_replicas number of replicas to place, all inside the chosen group
  //! @param atom_index geotag atom the descent stands at, passed on unconsumed
  //!        because a group is not a geo level
  //! @param depth current depth of the descent, guards against a cycle
  //! @param result result accumulating the selected disks
  //----------------------------------------------------------------------------
  void PlaceInGroup(const ClusterData& cluster_data, PlacementArgs& args,
                    const DescentContext& ctx, ItemIdT bucket_id, uint8_t n_replicas,
                    size_t atom_index, uint8_t depth, PlacementResult& result);

  //----------------------------------------------------------------------------
  //! Place replicas at a level of the geo hierarchy: the quota is split between
  //! the branch the client's geolocation points at and the rest of the
  //! branches, and whatever the chosen branches could not take spills over
  //! every branch of the bucket, the ones not asked yet first. The spill is
  //! what keeps an undersized branch from failing the whole placement, and it
  //! doubles as the up-root fallback: a quota that cannot be met where the
  //! client is gets met next to it rather than not at all.
  //!
  //! @param cluster_data topology snapshot to schedule on
  //! @param args placement arguments, bucket_id and n_replicas are overwritten
  //! @param ctx geolocation and collocation budget of this descent
  //! @param bucket bucket holding the geo branches
  //! @param bucket_id id of that bucket
  //! @param n_replicas number of replicas to spread over the branches
  //! @param atom_index geotag atom naming the preferred branch of this bucket,
  //!        past the end of ctx.geo_atoms once there is no preference left
  //! @param depth current depth of the descent, guards against a cycle
  //! @param result result accumulating the selected disks
  //----------------------------------------------------------------------------
  void PlaceAcrossGeoBranches(const ClusterData& cluster_data, PlacementArgs& args,
                              const DescentContext& ctx, const Bucket& bucket,
                              ItemIdT bucket_id, uint8_t n_replicas, size_t atom_index,
                              uint8_t depth, PlacementResult& result);

  //----------------------------------------------------------------------------
  //! Record the first failure of a descent, later ones are consequences of it
  //!
  //! @param result result to record into
  //! @param ret_code errno value
  //! @param err_msg error description
  //----------------------------------------------------------------------------
  static void SetError(PlacementResult& result, int ret_code, std::string err_msg);

  //----------------------------------------------------------------------------
  //! Grow the per bucket state of every strategy to the size of the topology
  //! about to be descended. The engine is built long before the topology it
  //! ends up serving exists - the MGM constructs it at boot, ahead of the
  //! FsView - and the hierarchy keeps growing afterwards, as spaces gain
  //! groups, geo branches and their flat views. Driving the capacity off the
  //! snapshot in hand is what keeps that from turning into a placement failure,
  //! and it cannot be forgotten the way a call at every topology mutation site
  //! could be.
  //!
  //! @param n_buckets number of buckets in the topology
  //----------------------------------------------------------------------------
  void EnsureCapacity(size_t n_buckets);

  //! Selection strategies indexed by PlacementStrategyT
  std::array<std::unique_ptr<SelectionStrategy>, TOTAL_PLACEMENT_STRATEGIES>
      mSelectionStrategy;
  //! Strategy used when the request does not specify a valid one
  PlacementStrategyT mDefaultStrategy{PlacementStrategyT::Count};
  //! Largest topology the strategies have been grown for, see EnsureCapacity.
  //! Only a memo of what they were told, so that the frequent case - a topology
  //! that has not grown - is one relaxed load on the scheduling path.
  std::atomic<size_t> mCapacity{0};
};

} // namespace eos::mgm::placement
