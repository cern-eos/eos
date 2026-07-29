//------------------------------------------------------------------------------
//! @file WeightedRandomStrategy.hh
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
#include "mgm/placement/SelectionStrategy.hh"

namespace eos::mgm::placement {

//------------------------------------------------------------------------------
//! Class WeightedRandomStrategy - places files by weighted rendezvous hashing
//! (Efraimidis-Spirakis): every candidate is scored once from a hash of
//! (fid, item, salt) and its weight, which currently reflects the disk
//! capacity, and the lowest scores win. The access path uses weighted
//! rendezvous (HRW) hashing as well, so that a given file consistently
//! prefers the same replica.
//------------------------------------------------------------------------------
class WeightedRandomStrategy : public SelectionStrategy {
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param strategy placement strategy type
  //! @param max_buckets maximum number of buckets in the hierarchy
  //!
  //! @note both are unused, the strategy holds no state; the signature is kept
  //!       uniform across strategies for MakeSelectionStrategy()
  //----------------------------------------------------------------------------
  WeightedRandomStrategy(PlacementStrategyT strategy, size_t max_buckets) {}

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
};

} // namespace eos::mgm::placement
