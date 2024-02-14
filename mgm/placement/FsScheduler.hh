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
#include "mgm/placement/FlatScheduler.hh"

namespace eos::mgm::placement {

using ClusterMapT = std::map<std::string, std::unique_ptr<ClusterMgr>>;

struct ClusterMgrHandler {
  virtual ClusterMapT make_cluster_mgr()=0;
  virtual std::unique_ptr<ClusterMgr> make_cluster_mgr(const std::string& spaceName)=0;
  virtual ~ClusterMgrHandler() = default;
};

struct EosClusterMgrHandler : public ClusterMgrHandler
{
  ClusterMapT make_cluster_mgr() override;
  std::unique_ptr<ClusterMgr> make_cluster_mgr(const std::string& spaceName) override;
};


class FSScheduler {
public:
  using RCUMutexT = eos::common::VersionedRCUDomain;
  using ClusterMapPtrT = eos::common::atomic_unique_ptr<ClusterMapT>;
  using SpaceStrategyMapT = std::map<std::string, PlacementStrategyT>;
  using SpaceStrategyMapPtrT = eos::common::atomic_unique_ptr<SpaceStrategyMapT>;

  FSScheduler(size_t max_buckets,
              std::unique_ptr<ClusterMgrHandler>&& _handler) :
    scheduler(std::make_unique<FlatScheduler>(max_buckets)),
    cluster_handler(std::move(_handler)),
    placement_strategy(placement::PlacementStrategyT::kGeoScheduler)
  {}

  FSScheduler() : FSScheduler(1024,
                              std::make_unique<EosClusterMgrHandler>()) {}


  PlacementResult schedule(const std::string& spaceName, uint8_t n_replicas);
  PlacementResult schedule(const std::string& spaceName, PlacementArguments args);
  void updateClusterData();
  bool setDiskStatus(const std::string& spaceName, fsid_t disk_id,
                     ConfigStatus status);
  bool setDiskStatus(const std::string& spaceName, fsid_t disk_id,
                     ActiveStatus status, eos::common::BootStatus bstatus);

  bool setDiskWeight(const std::string& spaceName, fsid_t disk_id,
                     uint8_t weight);

  void setPlacementStrategy(std::string_view strategy_sv);
  void setPlacementStrategy(const std::string& spacename,
                            std::string_view strategy_sv);

  PlacementStrategyT getPlacementStrategy();
  PlacementStrategyT getPlacementStrategy(const std::string& spacename);
  std::string getStateStr(const std::string& spacename, std::string_view type_sv);
private:

  ClusterMgr* get_cluster_mgr(const std::string& spaceName);

  std::unique_ptr<FlatScheduler> scheduler;
  std::unique_ptr<ClusterMgrHandler> cluster_handler;
  ClusterMapPtrT cluster_mgr_map;
  std::atomic<PlacementStrategyT> placement_strategy;
  SpaceStrategyMapPtrT space_strategy_map;
  RCUMutexT cluster_rcu_mutex;
};



} // eos::mgm::placement
