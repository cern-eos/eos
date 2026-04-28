// ----------------------------------------------------------------------
//! @file: RoundRobinPlacementStrategy.hh
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

#include "common/Logging.hh"
#include "mgm/placement/ClusterDataTypes.hh"
#include "mgm/placement/PlacementStrategy.hh"
#include "mgm/placement/RRSeed.hh"
#include "mgm/placement/ThreadLocalRRSeed.hh"
#include "utils/RandUtils.hh"

namespace eos::mgm::placement {

struct RRSeeder {
  virtual ~RRSeeder() = default;
  virtual size_t get(size_t index, size_t num_items, size_t fid) = 0;
  virtual size_t getNumSeeds() = 0;
};

struct GlobalRRSeeder : public RRSeeder {
  explicit GlobalRRSeeder(size_t max_buckets) : mSeed(max_buckets) {}

  size_t
  get(size_t index, size_t num_items, size_t) override
  {
    return mSeed.get(index, num_items);
  }

  size_t
  getNumSeeds() override
  {
    return mSeed.getNumSeeds();
  }

private:
  RRSeed<size_t> mSeed;
};

struct ThreadLocalRRSeeder : public RRSeeder {
  explicit ThreadLocalRRSeeder(size_t max_buckets)
  {
    ThreadLocalRRSeed::init(max_buckets);
  }

  size_t
  get(size_t index, size_t num_items, size_t) override
  {
    return ThreadLocalRRSeed::get(index, num_items);
  }

  size_t
  getNumSeeds() override
  {
    return ThreadLocalRRSeed::getNumSeeds();
  }
};

struct RandomSeeder: public RRSeeder {
  explicit RandomSeeder(size_t max_buckets)
      : mMaxBuckets(max_buckets)
  {
  }

  size_t
  get(size_t index, size_t, size_t) override
  {
    if (index > mMaxBuckets) {
      eos_static_err("msg=\"RandomSeeder index > MaxBuckets\" index=%lu mMaxBuckets=%lu",
                     index, mMaxBuckets);
      return eos::common::getRandom(0ul, mMaxBuckets - 1) + index - mMaxBuckets;
    }
    return eos::common::getRandom(0ul, mMaxBuckets - 1);
  }

  size_t
  getNumSeeds() override
  {
    return mMaxBuckets;
  }

private:
  size_t mMaxBuckets;
};

struct FidSeeder: public RRSeeder {
  explicit FidSeeder(size_t _max_buckets) : max_buckets(_max_buckets) {}

  size_t get(size_t index, size_t replicas, size_t fid) {
    return index ^ replicas ^ fid;
  }

  size_t getNumSeeds() override {
    return max_buckets;
  }
private:
  size_t max_buckets;
};

std::unique_ptr<RRSeeder>
makeRRSeeder(PlacementStrategyT strategy, size_t max_buckets);


class RoundRobinPlacement : public PlacementStrategy {
public:
  explicit RoundRobinPlacement(PlacementStrategyT strategy, size_t max_buckets)
      : mSeed(makeRRSeeder(strategy, max_buckets))
  {
  }

  PlacementResult placeFiles(const ClusterData& cluster_data,
                             Args args) override;

  int access(const ClusterData& cluster_data,
             AccessArguments& args) override;

private:
  std::unique_ptr<RRSeeder> mSeed;
};
} // namespace eos::mgm::placementq
