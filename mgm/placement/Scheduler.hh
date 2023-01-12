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

namespace eos::mgm::placement {

struct PlacementResult {
  std::vector<item_id_t> ids;
  int ret_code;
  std::string err_msg;

  PlacementResult() : ret_code(-1) {}

  operator bool() const {
    return ret_code == 0;
  }

  friend std::ostream& operator<< (std::ostream& os, const PlacementResult r) {
    for (const auto& id: r.ids) {
      os << id << " ";
    }
    os << "\n";
    return os;
  }

};

struct PlacementStrategy {
  struct Args {
    item_id_t bucket_id;
    uint8_t n_replicas;
    DiskStatus status=DiskStatus::kRO;
    uint64_t fid=0;

    Args(item_id_t bucket_id, uint8_t n_replicas,
         DiskStatus status=DiskStatus::kRO, uint64_t fid=0) :
      bucket_id(bucket_id), n_replicas(n_replicas), status(status), fid(fid) {}
  };

  virtual PlacementResult chooseItems(const ClusterData& cluster_data,
                                      Args args) = 0;
};


class RoundRobinPlacement : PlacementStrategy {
public:
  explicit
  RoundRobinPlacement(size_t max_buckets) : mSeed(max_buckets) {}

  PlacementResult chooseItems(const ClusterData& cluster_data,
                              Args args) override;
private:
  RRSeed<size_t> mSeed;
};


} // namespace eos::mgm::placement

