//------------------------------------------------------------------------------
//! @file PlacementStrategy.hh
//! @author Abhishek Lekshmanan <abhishek.lekshmanan@cern.ch>
//-----------------------------------------------------------------------------

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
#include <algorithm>
#include <optional>
#include <xxhash.h>

namespace eos::mgm::placement
{

struct PlacementResult {
  std::array<item_id_t, 32> ids {0};
  int ret_code;
  int n_replicas;
  std::optional<std::string> err_msg;

  PlacementResult() :  ret_code(-1), n_replicas(0) {}
  PlacementResult(int n_rep):  ret_code(-1), n_replicas(n_rep) {}

  operator bool() const
  {
    return ret_code == 0;
  }


  bool is_valid_placement(uint8_t _n_replicas) const
  {
    return _n_replicas == n_replicas &&
           (std::all_of(ids.cbegin(), ids.cbegin() + n_replicas,
    [](item_id_t id) {
      return id > 0;
    }));
  }

  friend std::ostream& operator<< (std::ostream& os, const PlacementResult r)
  {
    for (int i = 0; i < r.n_replicas; ++i) {
      os << r.ids[i] << " ";
    }

    return os;
  }

  // Simple helper to convert to string
  std::string result_string() const
  {
    std::stringstream ss;
    ss << *this;
    return ss.str();
  }

  std::string error_string() const
  {
    return err_msg.value_or("");
  }

  bool contains(item_id_t item) const
  {
    return std::find(ids.cbegin(),
                     ids.cbegin() + n_replicas,
                     item) != ids.cbegin() + n_replicas;
  }
};

enum class PlacementStrategyT : uint8_t {
  kRoundRobin = 0,
  kThreadLocalRoundRobin,
  kRandom,
  kFidRandom,
  kWeightedRandom,
  kWeightedRoundRobin,
  kGeoScheduler,
  Count
};

// Determining placement of replicas for a file
// We need to understand how many storage elements we select at each level
// of the hierarchy, for example for a 2 replica file, with 2 sites,
// we'd select 1 per site, and then going further down the hierarchy, we'd have
// to select 1 per room etc. until we reach our last abstraction at the group
// where we'd need to select as many replicas as we have left, in this case 2.
// we really don't want a tree that's more than 16 levels deep?
constexpr uint8_t MAX_PLACEMENT_HEIGHT = 16;
using selection_rules_t = std::array<int8_t, MAX_PLACEMENT_HEIGHT>;
static selection_rules_t kDefault2Replica {-1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1};

constexpr size_t TOTAL_PLACEMENT_STRATEGIES = static_cast<size_t>
    (PlacementStrategyT::Count);
constexpr uint8_t MAX_PLACEMENT_ATTEMPTS = 100;

inline constexpr bool is_valid_placement_strategy(PlacementStrategyT strategy)
{
  return strategy != PlacementStrategyT::Count;
}

inline size_t strategy_index(PlacementStrategyT strategy)
{
  return static_cast<size_t>(strategy);
}

constexpr PlacementStrategyT strategy_from_str(std::string_view strategy_sv)
{
  using namespace std::string_view_literals;

  if (strategy_sv == "roundrobin"sv ||
      strategy_sv == "rr"sv) {
    return PlacementStrategyT::kRoundRobin;
  } else if (strategy_sv == "threadlocalroundrobin"sv ||
             strategy_sv == "threadlocalrr"sv ||
             strategy_sv == "tlrr"sv) {
    return PlacementStrategyT::kThreadLocalRoundRobin;
  } else if (strategy_sv == "random"sv) {
    return PlacementStrategyT::kRandom;
  } else if (strategy_sv == "fid"sv ||
             strategy_sv == "fidrandom"sv) {
    return PlacementStrategyT::kFidRandom;
  } else if (strategy_sv == "weightedrandom"sv) {
    return PlacementStrategyT::kWeightedRandom;
  } else if (strategy_sv == "weightedroundrobin"sv ||
             strategy_sv == "weightedrr"sv) {
    return PlacementStrategyT::kWeightedRoundRobin;
  } else if (strategy_sv == "geoscheduler"sv ||
             strategy_sv == "geo"sv) {
    return PlacementStrategyT::kGeoScheduler;
  }

  // default to geoscheduler!
  return PlacementStrategyT::kGeoScheduler;
}

inline std::string strategy_to_str(PlacementStrategyT strategy)
{
  switch (strategy) {
  case PlacementStrategyT::kRoundRobin:
    return "roundrobin";

  case PlacementStrategyT::kThreadLocalRoundRobin:
    return "threadlocalroundrobin";

  case PlacementStrategyT::kRandom:
    return "random";

  case PlacementStrategyT::kFidRandom:
    return "fidrandom";

  case PlacementStrategyT::kWeightedRandom:
    return "weightedrandom";

  case PlacementStrategyT::kWeightedRoundRobin:
    return "weightedroundrobin";

  case PlacementStrategyT::kGeoScheduler:
    return "geoscheduler";

  default:
    return "unknown";
  }
}

struct PlacementArguments {
  item_id_t bucket_id = 0;
  uint8_t n_replicas;
  ConfigStatus status = ConfigStatus::kRW;
  uint64_t fid;
  bool default_placement = true;
  selection_rules_t rules = kDefault2Replica;
  PlacementStrategyT strategy = PlacementStrategyT::Count;
  std::vector<uint32_t> excludefs;
  int64_t forced_group_index = -1;

  PlacementArguments(item_id_t bucket_id, uint8_t n_replicas,
                     ConfigStatus status, uint64_t fid,
                     selection_rules_t rules)
    : bucket_id(bucket_id), n_replicas(n_replicas), status(status),
      fid(fid), default_placement(false), rules(rules)
  {
  }

  PlacementArguments(item_id_t bucket_id, uint8_t n_replicas,
                     ConfigStatus status, uint64_t fid)
    : bucket_id(bucket_id), n_replicas(n_replicas), status(status),
      fid(fid)
  {
  }

  PlacementArguments(uint8_t n_replicas, ConfigStatus _status,
                     PlacementStrategyT _strategy)
    : bucket_id(0), n_replicas(n_replicas), status(_status), fid(0),
      default_placement(true), rules(kDefault2Replica), strategy(_strategy)
  {
  }


  PlacementArguments(uint8_t n_replicas, ConfigStatus _status)
    : PlacementArguments(0, n_replicas, _status, 0)
  {
  }



  PlacementArguments(uint8_t n_replicas)
    : PlacementArguments(n_replicas, ConfigStatus::kRW)
  {
  }



  PlacementArguments(item_id_t bucket_id, uint8_t n_replicas, ConfigStatus status)
    : bucket_id(bucket_id), n_replicas(n_replicas), status(status),
      fid(0), default_placement(true), rules(kDefault2Replica)
  {
  }

  PlacementArguments(item_id_t bucket_id, uint8_t n_replicas) :
    PlacementArguments(bucket_id, n_replicas, ConfigStatus::kRW)
  {
  }

};

  struct AccessArguments {
    size_t n_replicas;
    size_t& selectedIndex;
    ino64_t inode;
    PlacementStrategyT strategy;

    std::string_view geolocation;
    std::vector<uint32_t>* unavailfs;
    const std::vector<uint32_t>& selectedfs;
  };

struct PlacementStrategy {
  using Args = PlacementArguments;

  virtual PlacementResult placeFiles(const ClusterData& cluster_data,
                                     Args args) = 0;

  virtual int access(const ClusterData& cluster_data,
                     AccessArguments args) = 0;

  bool validateArgs(const ClusterData& cluster_data, const Args& args,
                    PlacementResult& result) const
  {
    if (args.n_replicas == 0) {
      result.ret_code = EINVAL;
      result.err_msg = "Zero replicas requested";
      return false;
    }

    int32_t bucket_index = -args.bucket_id;
    auto bucket_sz = cluster_data.buckets.size();

    if (bucket_sz < args.n_replicas) {
      result.err_msg = "More replicas than bucket size!";
      result.ret_code = ERANGE;
      return false;
    }

    try {
      const auto& bucket = cluster_data.buckets.at(bucket_index);

      if (bucket.items.size() < args.n_replicas) {
        result.err_msg = "Bucket " + std::to_string(bucket.id) +
                         "does not contain enough elements!";
        result.ret_code = ENOENT;
        return false;
      }
    } catch (std::out_of_range& e) {
      result.err_msg = "Bucket ID" + std::to_string(bucket_index) + "is invalid!";
      result.ret_code = ERANGE;
      return false;
    }

    return true;
  }

  static bool validDiskPlct(item_id_t disk_id,
                            const ClusterData& cluster_data,
                            Args args)
  {
    if (disk_id <= 0) {
      return false;
    }

    if (std::find(args.excludefs.begin(),
                  args.excludefs.end(),
                  disk_id) != args.excludefs.end()) {
      return false;
    }

    auto disk_config_status = cluster_data.disks[disk_id - 1].config_status.load(
                                std::memory_order_acquire);
    auto disk_active_status = cluster_data.disks[disk_id - 1].active_status.load(
                                std::memory_order_acquire);
    return disk_active_status == eos::common::ActiveStatus::kOnline &&
           disk_config_status >= args.status;
  }

  virtual ~PlacementStrategy() = default;

  /**
   * Calculates the maximum topological overlap between a candidate and existing replicas.
   * Lower score is better.
   * * @param candidate_id The disk ID we are considering adding.
   * @param data The cluster data containing the GeoTag vectors.
   * @param current_result The list of replicas already selected for this file.
   * @param items_added How many items in current_result are valid.
   * @return The number of shared hierarchy levels with the NEAREST existing replica.
   */
  size_t calculateMaxGeoOverlap(item_id_t candidate_id,
                                const ClusterData& data,
                                const PlacementResult& current_result,
                                int items_added) const;

  PlacementResult placeWithGeoFilter(const ClusterData& cluster_data,
                                     const Args& args,
                                     const std::vector<item_id_t>& sorted_candidates);
};

static inline uint64_t hashFid(uint64_t fid, uint64_t fsid, uint64_t salt=0) {
// Using XXH3 as it provides good distribution and performance
// ensure little-endian encoding for cross platform consistency
  uint64_t buf[3] = {
    htole64(fid),
    htole64(fsid),
    htole64(salt)
  };
  return XXH3_64bits(buf, sizeof(buf));
}
// Simple helper struct to help sort items based on score
struct RankedItem {
  item_id_t id;
  uint64_t score;

  bool operator<(const RankedItem& other) const
  {
    return score < other.score;
  }
};

} // namespace eos::mgm::placement
