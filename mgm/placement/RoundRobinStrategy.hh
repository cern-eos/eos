//------------------------------------------------------------------------------
//! @file RoundRobinStrategy.hh
//! @author Abhishek Lekshmanan - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2023 CERN/Switzerland                                  *
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
#include "mgm/placement/RRSeed.hh"
#include "mgm/placement/SelectionStrategy.hh"
#include "mgm/placement/ThreadLocalRRSeed.hh"
#include "utils/RandUtils.hh"

namespace eos::mgm::placement {

//------------------------------------------------------------------------------
//! Struct RRSeeder - interface handing out the round-robin cursor of a bucket.
//! It is the knob differentiating the round-robin flavours from one another.
//------------------------------------------------------------------------------
struct RRSeeder {
  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~RRSeeder() = default;

  //----------------------------------------------------------------------------
  //! Get the seed of the given bucket
  //!
  //! @param index bucket index
  //! @param num_items number of items about to be picked
  //! @param fid file identifier, only used by the deterministic seeders
  //!
  //! @return seed value
  //----------------------------------------------------------------------------
  virtual size_t Get(size_t index, size_t num_items, size_t fid) = 0;

  //----------------------------------------------------------------------------
  //! Get the number of seeds held
  //!
  //! @return number of seeds
  //----------------------------------------------------------------------------
  virtual size_t GetNumSeeds() = 0;

  //----------------------------------------------------------------------------
  //! Grow the seeder so that it can serve a topology of the given size. Called
  //! off the snapshot a placement is about to descend, so that a hierarchy
  //! that gained buckets - a new geo branch, a new group and its flat view -
  //! is served instead of being reported as out of range.
  //!
  //! @param max_items number of seeds the topology needs
  //----------------------------------------------------------------------------
  virtual void EnsureCapacity(size_t max_items) = 0;
};

//------------------------------------------------------------------------------
//! Struct GlobalRRSeeder - shared atomic counters giving strong global
//! fairness at the cost of contention between threads
//------------------------------------------------------------------------------
struct GlobalRRSeeder : public RRSeeder {
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param max_buckets number of seeds to hold
  //----------------------------------------------------------------------------
  explicit GlobalRRSeeder(size_t max_buckets) : mSeed(max_buckets) {}

  //----------------------------------------------------------------------------
  //! Get the seed of the given bucket
  //!
  //! @param index bucket index
  //! @param num_items number of items about to be picked
  //!
  //! @return seed value
  //----------------------------------------------------------------------------
  size_t
  Get(size_t index, size_t num_items, size_t) override
  {
    return mSeed.Get(index, num_items);
  }

  //----------------------------------------------------------------------------
  //! Get the number of seeds held
  //!
  //! @return number of seeds
  //----------------------------------------------------------------------------
  size_t
  GetNumSeeds() override
  {
    return mSeed.GetNumSeeds();
  }

  //----------------------------------------------------------------------------
  //! Grow the counters to serve a topology of the given size
  //!
  //! @param max_items number of seeds the topology needs
  //----------------------------------------------------------------------------
  void
  EnsureCapacity(size_t max_items) override
  {
    mSeed.EnsureCapacity(max_items);
  }

private:
  RRSeed<size_t> mSeed; ///< Shared atomic round-robin counters
};

//------------------------------------------------------------------------------
//! Struct ThreadLocalRRSeeder - per thread counters, lock and atomic free and
//! therefore faster, at the cost of only per-thread fairness
//------------------------------------------------------------------------------
struct ThreadLocalRRSeeder : public RRSeeder {
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param max_buckets number of seeds to hold
  //----------------------------------------------------------------------------
  explicit ThreadLocalRRSeeder(size_t max_buckets)
  {
    ThreadLocalRRSeed::Init(max_buckets);
  }

  //----------------------------------------------------------------------------
  //! Get the seed of the given bucket
  //!
  //! @param index bucket index
  //! @param num_items number of items about to be picked
  //!
  //! @return seed value
  //----------------------------------------------------------------------------
  size_t
  Get(size_t index, size_t num_items, size_t) override
  {
    return ThreadLocalRRSeed::Get(index, num_items);
  }

  //----------------------------------------------------------------------------
  //! Get the number of seeds held
  //!
  //! @return number of seeds
  //----------------------------------------------------------------------------
  size_t
  GetNumSeeds() override
  {
    return ThreadLocalRRSeed::GetNumSeeds();
  }

  //----------------------------------------------------------------------------
  //! Grow the per thread counters to serve a topology of the given size
  //!
  //! @param max_items number of seeds the topology needs
  //----------------------------------------------------------------------------
  void
  EnsureCapacity(size_t max_items) override
  {
    ThreadLocalRRSeed::EnsureCapacity(max_items);
  }
};

//------------------------------------------------------------------------------
//! Raise a counter to at least the given value, never lowering it, so that
//! callers racing on a growing topology settle on the largest request
//!
//! @param counter counter to raise
//! @param value smallest value the caller needs
//------------------------------------------------------------------------------
inline void
RaiseAtLeast(std::atomic<size_t>& counter, size_t value)
{
  size_t current = counter.load(std::memory_order_acquire);

  while ((current < value) &&
         !counter.compare_exchange_weak(current, value, std::memory_order_release,
                                        std::memory_order_acquire)) {
  }
}

//------------------------------------------------------------------------------
//! Struct RandomSeeder - uniformly random seeds, no round-robin memory at all
//------------------------------------------------------------------------------
struct RandomSeeder: public RRSeeder {
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param max_buckets number of buckets the seeder must handle
  //----------------------------------------------------------------------------
  explicit RandomSeeder(size_t max_buckets)
      : mMaxBuckets(max_buckets)
  {
  }

  //----------------------------------------------------------------------------
  //! Get the seed of the given bucket
  //!
  //! @param index bucket index
  //!
  //! @return seed value
  //----------------------------------------------------------------------------
  size_t
  Get(size_t index, size_t, size_t) override
  {
    const size_t max_buckets = mMaxBuckets.load(std::memory_order_acquire);

    if (index > max_buckets) {
      // Out of range only means we cannot honour this specific bucket index;
      // a uniform seeder has no per-bucket memory anyway, so fall back to a
      // valid random seed rather than to out-of-range index arithmetic.
      eos_static_err("msg=\"RandomSeeder index > MaxBuckets\" index=%lu mMaxBuckets=%lu",
                     index, max_buckets);
    }

    return eos::common::getRandom(0ul, max_buckets - 1);
  }

  //----------------------------------------------------------------------------
  //! Get the number of seeds held
  //!
  //! @return number of seeds
  //----------------------------------------------------------------------------
  size_t
  GetNumSeeds() override
  {
    return mMaxBuckets.load(std::memory_order_acquire);
  }

  //----------------------------------------------------------------------------
  //! Grow the handled range to serve a topology of the given size
  //!
  //! @param max_items number of seeds the topology needs
  //----------------------------------------------------------------------------
  void
  EnsureCapacity(size_t max_items) override
  {
    RaiseAtLeast(mMaxBuckets, max_items);
  }

private:
  std::atomic<size_t> mMaxBuckets; ///< Number of buckets handled
};

//------------------------------------------------------------------------------
//! Struct FidSeeder - seed derived from the file identifier, so that placement
//! of a given file is deterministic and reproducible
//------------------------------------------------------------------------------
struct FidSeeder: public RRSeeder {
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param max_buckets number of buckets the seeder must handle
  //----------------------------------------------------------------------------
  explicit FidSeeder(size_t max_buckets)
      : mMaxBuckets(max_buckets)
  {
  }

  //----------------------------------------------------------------------------
  //! Get the seed of the given bucket
  //!
  //! @param index bucket index
  //! @param replicas number of replicas about to be placed
  //! @param fid file identifier
  //!
  //! @return seed value
  //----------------------------------------------------------------------------
  size_t
  Get(size_t index, size_t replicas, size_t fid) override
  {
    return index ^ replicas ^ fid;
  }

  //----------------------------------------------------------------------------
  //! Get the number of seeds held
  //!
  //! @return number of seeds
  //----------------------------------------------------------------------------
  size_t
  GetNumSeeds() override
  {
    return mMaxBuckets.load(std::memory_order_acquire);
  }

  //----------------------------------------------------------------------------
  //! Grow the handled range to serve a topology of the given size
  //!
  //! @param max_items number of seeds the topology needs
  //----------------------------------------------------------------------------
  void
  EnsureCapacity(size_t max_items) override
  {
    RaiseAtLeast(mMaxBuckets, max_items);
  }

private:
  std::atomic<size_t> mMaxBuckets; ///< Number of buckets handled
};

//------------------------------------------------------------------------------
//! Create the seeder backing the given round-robin flavour
//!
//! @param strategy placement strategy type
//! @param max_buckets number of buckets the seeder must handle
//!
//! @return seeder object
//------------------------------------------------------------------------------
std::unique_ptr<RRSeeder> MakeRRSeeder(PlacementStrategyT strategy, size_t max_buckets);

//------------------------------------------------------------------------------
//! Class RoundRobinStrategy - backs the round-robin, thread local round-robin,
//! random and fid-random strategies, which differ only by the RRSeeder they
//! are built with
//------------------------------------------------------------------------------
class RoundRobinStrategy : public SelectionStrategy {
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param strategy placement strategy type, selects the seeder
  //! @param max_buckets maximum number of buckets in the hierarchy
  //----------------------------------------------------------------------------
  explicit RoundRobinStrategy(PlacementStrategyT strategy, size_t max_buckets)
      : mSeed(MakeRRSeeder(strategy, max_buckets))
  {
  }

  //----------------------------------------------------------------------------
  //! Select the disks holding the replicas of a new file
  //!
  //! @param cluster_data topology snapshot to place on
  //! @param args placement arguments
  //!
  //! @return placement result, convertible to false if placement failed
  //----------------------------------------------------------------------------
  PlacementResult Placement(const ClusterData& cluster_data,
                            const PlacementArgs& args) const override;

  //----------------------------------------------------------------------------
  //! Grow the seeder so that it can serve a topology of the given size
  //!
  //! @param n_buckets number of buckets in the topology
  //----------------------------------------------------------------------------
  void
  EnsureCapacity(size_t n_buckets) override
  {
    mSeed->EnsureCapacity(n_buckets);
  }

private:
  std::unique_ptr<RRSeeder> mSeed; ///< Seeder handing out the RR cursors
};

} // namespace eos::mgm::placement
