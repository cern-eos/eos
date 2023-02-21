// ----------------------------------------------------------------------
// File: FsViewUpdater.cc
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
#include "mgm/placement/ClusterMap.hh"
#include "mgm/placement/Scheduler.hh"

namespace eos::mgm::placement {

class FSScheduler {
public:
  FSScheduler(PlacementStrategyT strategy, size_t max_buckets) :
    cluster_mgr(std::make_unique<ClusterMgr>()),
    scheduler(std::make_unique<FlatScheduler>(strategy, max_buckets))
  {}

  FSScheduler() : FSScheduler(PlacementStrategyT::kThreadLocalRoundRobin,
                              1024) {}


  PlacementResult schedule(uint8_t n_replicas);
  void updateClusterData(const std::string& spaceName);
private:
  std::unique_ptr<ClusterMgr> cluster_mgr;
  std::unique_ptr<FlatScheduler> scheduler;
  std::atomic<bool> initialized;
};



} // eos::mgm::placement
