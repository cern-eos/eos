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
#include "mgm/placement/RRSeed.hh"
#include "mgm/placement/ThreadLocalRRSeed.hh"
#include <algorithm>

namespace eos::mgm::placement {

struct PlacementResult {
  std::vector<item_id_t> ids;
  int ret_code;
  std::string err_msg;

  PlacementResult() : ret_code(-1) {}

  operator bool() const {
    return ret_code == 0;
  }


  bool is_valid_placement(uint8_t n_replicas) const {
    return ids.size() == n_replicas &&
    (std::all_of(ids.cbegin(), ids.cend(),[](item_id_t id) {
      return id > 0;
    }));
  }

  friend std::ostream& operator<< (std::ostream& os, const PlacementResult r) {
    for (const auto& id: r.ids) {
      os << id << " ";
    }
    os << "\n";
    return os;
  }

};

enum class PlacementStrategyT {
  kRoundRobin,
  kThreadLocalRoundRobin,
};

struct PlacementStrategy {
  struct Args {
    item_id_t bucket_id;
    uint8_t n_replicas;
    DiskStatus status=DiskStatus::kRO;
    uint64_t fid=0;

    Args(item_id_t bucket_id, uint8_t n_replicas,
         DiskStatus status=DiskStatus::kRO, uint64_t fid=0) :
      bucket_id(bucket_id), n_replicas(n_replicas),
      status(status), fid(fid) {}
  };

  virtual PlacementResult chooseItems(const ClusterData& cluster_data,
                                      Args args) = 0;

  virtual ~PlacementStrategy() = default;
};

struct RRSeeder {
  virtual ~RRSeeder() = default;
  virtual size_t get(size_t index, size_t num_items) = 0;
  virtual size_t getNumSeeds() = 0;
};

struct GlobalRRSeeder : public RRSeeder {
  explicit GlobalRRSeeder(size_t max_buckets) : mSeed(max_buckets) {}

  size_t get(size_t index, size_t num_items) override {
    return mSeed.get(index, num_items);
  }

  size_t getNumSeeds() override {
    return mSeed.getNumSeeds();
  }
private:
  RRSeed<size_t> mSeed;
};

struct ThreadLocalRRSeeder : public RRSeeder {
  explicit ThreadLocalRRSeeder(size_t max_buckets) {
    ThreadLocalRRSeed::init(max_buckets);
  }

  size_t get(size_t index, size_t num_items) override {
    return ThreadLocalRRSeed::get(index, num_items);
  }

  size_t getNumSeeds() override {
    return ThreadLocalRRSeed::getNumSeeds();
  }
};


std::unique_ptr<RRSeeder> makeRRSeeder(PlacementStrategyT strategy,
                                       size_t max_buckets);

class RoundRobinPlacement : public PlacementStrategy {
public:
  explicit
  RoundRobinPlacement(PlacementStrategyT strategy,
                      size_t max_buckets) : mSeed(makeRRSeeder(strategy, max_buckets)) {}

  PlacementResult chooseItems(const ClusterData& cluster_data,
                              Args args) override;
private:
  std::unique_ptr<RRSeeder> mSeed;
};

template <typename... Args>
std::unique_ptr<PlacementStrategy> makePlacementStrategy(PlacementStrategyT type,
                                                         Args&&... args) {
  switch (type) {
  case PlacementStrategyT::kRoundRobin: [[fallthrough]];
  case PlacementStrategyT::kThreadLocalRoundRobin:
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
    DiskStatus status = DiskStatus::kRW;
    uint64_t fid;
    bool default_placement = true;
    selection_rules_t rules=kDefault2Replica;


    PlacementArguments(uint8_t n_replicas, DiskStatus _status): bucket_id(0), n_replicas(n_replicas),
        status(_status), fid(0), rules(kDefault2Replica), default_placement(true) {}

    PlacementArguments(uint8_t n_replicas) :
        PlacementArguments(n_replicas, DiskStatus::kRW)
    {}

    PlacementArguments(item_id_t bucket_id, uint8_t n_replicas,
                       DiskStatus status, uint64_t fid, selection_rules_t rules):
      bucket_id(bucket_id), n_replicas(n_replicas), status(status), fid(fid),
      rules(rules), default_placement(false) {}
  };

  template <typename... Args>
  FlatScheduler(PlacementStrategyT strategy, Args&&... args) :
      mPlacementStrategy(makePlacementStrategy(strategy, std::forward<Args>(args)...)) {}

  PlacementResult schedule(const ClusterData& cluster_data,
                           PlacementArguments args);

private:
  PlacementResult scheduleDefault(const ClusterData& cluster_data,
                                  PlacementArguments args);


  std::unique_ptr<PlacementStrategy> mPlacementStrategy;

};

static_assert(sizeof(FlatScheduler) == 8);

} // namespace eos::mgm::placement





