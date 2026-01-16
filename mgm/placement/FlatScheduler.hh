// ----------------------------------------------------------------------
// File: Scheduler
// Author: Abhishek Lekshmanan - CERN
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

#include "mgm/placement/ClusterDataTypes.hh"
#include "mgm/placement/PlacementStrategy.hh"
#include <algorithm>
#include <optional>

namespace eos::mgm::placement {

std::unique_ptr<PlacementStrategy> makePlacementStrategy(PlacementStrategyT type,
                      size_t max_buckets);
// We really need a more creative name?
class FlatScheduler {
public:
  FlatScheduler(size_t max_buckets);
  FlatScheduler(PlacementStrategyT strategy, size_t max_buckets);

  PlacementResult schedule(const ClusterData& cluster_data,
                           PlacementArguments args);

  int access(const ClusterData& cluster_data,
             AccessArguments& args);

  static constexpr size_t
  accessStategyIndex(PlacementStrategyT strategy);
private:
  PlacementResult scheduleDefault(const ClusterData& cluster_data,
                                  PlacementArguments args);

  std::array<std::unique_ptr<PlacementStrategy>, TOTAL_PLACEMENT_STRATEGIES>
      mPlacementStrategy;
  PlacementStrategyT mDefaultStrategy{PlacementStrategyT::Count};
};
} // namespace eos::mgm::placement





