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
#include "mgm/placement/RoundRobinPlacementStrategy.hh"
#include <algorithm>
#include <optional>

namespace eos::mgm::placement {

template <typename... Args>
std::unique_ptr<PlacementStrategy> makePlacementStrategy(PlacementStrategyT type,
                      Args&&... args) {
  switch (type) {
  case PlacementStrategyT::kRoundRobin: [[fallthrough]];
  case PlacementStrategyT::kThreadLocalRoundRobin: [[fallthrough]];
  case PlacementStrategyT::kRandom:
    return std::make_unique<RoundRobinPlacement>(type, std::forward<Args>(args)...);
  default:
    return nullptr;
  }
}
// We really need a more creative name?
class FlatScheduler {
public:
  struct PlacementArguments {
    item_id_t bucket_id = 0;
    uint8_t n_replicas;
    ConfigStatus status = ConfigStatus::kRW;
    uint64_t fid;
    bool default_placement = true;
    selection_rules_t rules = kDefault2Replica;
    PlacementStrategyT strategy = PlacementStrategyT::kRoundRobin;

    PlacementArguments(uint8_t n_replicas, ConfigStatus _status)
        : bucket_id(0), n_replicas(n_replicas), status(_status), fid(0),
          rules(kDefault2Replica), default_placement(true)
    {
    }

    PlacementArguments(uint8_t n_replicas, ConfigStatus _status, PlacementStrategyT _strategy)
      : bucket_id(0), n_replicas(n_replicas), status(_status), fid(0),
        rules(kDefault2Replica), default_placement(true), strategy(_strategy)
    {
    }


    PlacementArguments(uint8_t n_replicas)
        : PlacementArguments(n_replicas, ConfigStatus::kRW)
    {
    }

    PlacementArguments(item_id_t bucket_id, uint8_t n_replicas,
                       ConfigStatus status, uint64_t fid,
                       selection_rules_t rules)
        : bucket_id(bucket_id), n_replicas(n_replicas), status(status),
          fid(fid), rules(rules), default_placement(false)
    {
    }
  };

  FlatScheduler(size_t max_buckets)
  {
    for (size_t i = 0; i < TOTAL_PLACEMENT_STRATEGIES; i++) {
      mPlacementStrategy[i] = makePlacementStrategy(
          static_cast<PlacementStrategyT>(i), max_buckets);
    }
  }

  template <typename... Args>
  FlatScheduler(PlacementStrategyT strategy, Args&&... args)
      : mDefaultStrategy(strategy)
  {
    mPlacementStrategy[static_cast<int>(strategy)] =
        makePlacementStrategy(strategy, std::forward<Args>(args)...);
  }

  PlacementResult schedule(const ClusterData& cluster_data,
                           PlacementArguments args);

private:
  PlacementResult scheduleDefault(const ClusterData& cluster_data,
                                  PlacementArguments args);

  std::array<std::unique_ptr<PlacementStrategy>, TOTAL_PLACEMENT_STRATEGIES>
      mPlacementStrategy;
  PlacementStrategyT mDefaultStrategy{PlacementStrategyT::Count};
};
} // namespace eos::mgm::placement





