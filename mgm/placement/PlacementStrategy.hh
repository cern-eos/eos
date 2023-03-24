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
#include "mgm/placement/ThreadLocalRRSeed.hh"
#include <algorithm>
#include <optional>
#include <errno.h>

namespace eos::mgm::placement {

struct PlacementResult {
  std::array<item_id_t, 32> ids {0};
  int ret_code;
  int n_replicas;
  std::optional<std::string> err_msg;

  PlacementResult() :  ret_code(-1), n_replicas(0) {}
  PlacementResult(int n_rep):  ret_code(-1), n_replicas(n_rep) {}

  operator bool() const {
    return ret_code == 0;
  }


  bool is_valid_placement(uint8_t _n_replicas) const {
    return _n_replicas == n_replicas &&
      (std::all_of(ids.cbegin(), ids.cbegin() + n_replicas,
                   [](item_id_t id) {
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

  std::string error_string() const {
    return err_msg.value_or("");
  }
};

enum class PlacementStrategyT : uint8_t {
  kRoundRobin=0,
  kThreadLocalRoundRobin,
  Count
};

constexpr size_t TOTAL_PLACEMENT_STRATEGIES=static_cast<size_t>(PlacementStrategyT::Count);
constexpr uint8_t MAX_PLACEMENT_ATTEMPTS = 20;

inline constexpr bool is_valid_placement_strategy(PlacementStrategyT strategy) {
  return strategy != PlacementStrategyT::Count;
}

inline size_t strategy_index(PlacementStrategyT strategy) {
  return static_cast<size_t>(strategy);
}

constexpr PlacementStrategyT strategy_from_str(std::string_view strategy_sv) {
  using namespace std::string_view_literals;
  if (strategy_sv == "threadlocalroundrobin"sv||
      strategy_sv == "threadlocalrr"sv ||
      strategy_sv == "tlrr"sv) {
    return PlacementStrategyT::kThreadLocalRoundRobin;
  }
  return PlacementStrategyT::kRoundRobin;
}

inline std::string strategy_to_str(PlacementStrategyT strategy) {
  switch (strategy) {
  case PlacementStrategyT::kRoundRobin:
    return "roundrobin";
  case PlacementStrategyT::kThreadLocalRoundRobin:
    return "threadlocalroundrobin";
  default:
    return "unknown";
  }
}

struct PlacementStrategy {
  struct Args {
    item_id_t bucket_id;
    uint8_t n_replicas;
    ConfigStatus status= ConfigStatus::kRO;
    uint64_t fid=0;

    Args(item_id_t bucket_id, uint8_t n_replicas,
         ConfigStatus status= ConfigStatus::kRO, uint64_t fid=0) :
      bucket_id(bucket_id), n_replicas(n_replicas),
      status(status), fid(fid) {}
  };

  virtual PlacementResult placeFiles(const ClusterData& cluster_data,
                                      Args args) = 0;

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
        if (bucket.items.empty()) {
          result.err_msg = "Bucket " + std::to_string(bucket.id) + "is empty!";
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

  virtual ~PlacementStrategy() = default;
};


} // namespace eos::mgm::placement
