// ----------------------------------------------------------------------
//! @file: WeightedRoundRobinPlacementStrategy.hh
//! @author: Abhishek Lekshmanan - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2023 CERN/Switzerland                           *
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
#include "mgm/placement/PlacementStrategy.hh"
namespace eos::mgm::placement {

/**
 * A placement strategy that places files on nodes based on a weighted
 * random distribution. The weights are currently based on the disk sizes
 */
class WeightedRoundRobinPlacement : public PlacementStrategy {
public:
  WeightedRoundRobinPlacement(PlacementStrategyT strategy, size_t max_buckets);
  virtual PlacementResult placeFiles(const ClusterData& data,
                                     Args args) override;
  virtual int access(const ClusterData& data, AccessArguments args) override;
  ~WeightedRoundRobinPlacement();
private:
  struct Impl;
  std::unique_ptr<Impl> mImpl;
};

} // namespace eos::mgm::placement
