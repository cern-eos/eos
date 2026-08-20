// ----------------------------------------------------------------------
// File: BM_FlatScheduler.cc
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
#include "benchmark/benchmark.h"
#include "common/utils/ContainerUtils.hh"
#include "mgm/placement/ClusterBuilder.hh"
#include "mgm/placement/ClusterMgr.hh"
#include "mgm/placement/FlatScheduler.hh"
#include "mgm/placement/SelectionStrategy.hh"

static void BM_Scheduler(benchmark::State& state) {
  using namespace eos::mgm::placement;
  auto n_groups = state.range(0);
  auto n_elements = n_groups + 101;
  const int n_disks_per_group = 16;
  ClusterMgr mgr;
  {

    auto sh = mgr.GetSnapshotBuilder(n_elements);
    sh.AddBucket(GetBucketType(BucketType::ROOT), 0);
    sh.AddBucket(GetBucketType(BucketType::SITE), -1, 0);

    for (int i=0; i< n_groups; ++i) {
      sh.AddBucket(GetBucketType(BucketType::GROUP), -100 - i, -1);
    }

    for (int i=0; i < n_groups*n_disks_per_group; i++) {
      sh.AddDisk(Disk(i + 1, kMaskAll, ActiveStatus::kOnline, 1),
                 -100 - i / n_disks_per_group);
    }

  }
  FlatScheduler flat_scheduler(PlacementStrategyT::kRoundRobin, n_elements);


  for (auto _: state) {
    auto cluster_data_ptr = mgr.GetClusterData();
    benchmark::DoNotOptimize(flat_scheduler.Schedule(cluster_data_ptr(), state.range(1)));
  }
  state.counters["frequency"] = benchmark::Counter(state.iterations(),
                                                   benchmark::Counter::kIsRate);
}

static void BM_ThreadLocalRRScheduler(benchmark::State& state) {
  using namespace eos::mgm::placement;
  auto n_groups = state.range(0);
  auto n_elements = 1024;
  const int n_disks_per_group = 16;
  ClusterMgr mgr;
  {

    auto sh = mgr.GetSnapshotBuilder(n_elements);
    sh.AddBucket(GetBucketType(BucketType::ROOT), 0);
    // sh.AddBucket(GetBucketType(BucketType::SITE), -1, 0);

    for (int i=0; i< n_groups; ++i) {
      sh.AddBucket(GetBucketType(BucketType::GROUP), -100 - i, 0);
    }

    for (int i=0; i < n_groups*n_disks_per_group; i++) {
      sh.AddDisk(Disk(i + 1, kMaskAll, ActiveStatus::kOnline, 1),
                 -100 - i / n_disks_per_group);
    }

  }
  FlatScheduler flat_scheduler(PlacementStrategyT::kThreadLocalRoundRobin, n_elements);


  for (auto _: state) {
    auto cluster_data_ptr = mgr.GetClusterData();
    benchmark::DoNotOptimize(flat_scheduler.Schedule(cluster_data_ptr(), state.range(1)));
  }
  state.counters["frequency"] = benchmark::Counter(state.iterations(),
                                                   benchmark::Counter::kIsRate);
}

static void BM_RandomScheduler(benchmark::State& state) {
  using namespace eos::mgm::placement;
  auto n_groups = state.range(0);
  auto n_elements = 1024;
  const int n_disks_per_group = 16;
  ClusterMgr mgr;
  {

    auto sh = mgr.GetSnapshotBuilder(n_elements);
    sh.AddBucket(GetBucketType(BucketType::ROOT), 0);
    // sh.AddBucket(GetBucketType(BucketType::SITE), -1, 0);

    for (int i=0; i< n_groups; ++i) {
      sh.AddBucket(GetBucketType(BucketType::GROUP), -100 - i, 0);
    }

    for (int i=0; i < n_groups*n_disks_per_group; i++) {
      sh.AddDisk(Disk(i + 1, kMaskAll, ActiveStatus::kOnline, 1),
                 -100 - i / n_disks_per_group);
    }

  }
  FlatScheduler flat_scheduler(PlacementStrategyT::kRandom, n_elements);


  for (auto _: state) {
    auto cluster_data_ptr = mgr.GetClusterData();
    benchmark::DoNotOptimize(flat_scheduler.Schedule(cluster_data_ptr(), state.range(1)));
  }
  state.counters["frequency"] = benchmark::Counter(state.iterations(),
                                                   benchmark::Counter::kIsRate);
}

static void BM_FidScheduler(benchmark::State& state) {
  using namespace eos::mgm::placement;
  auto n_groups = state.range(0);
  auto n_elements = 1024;
  const int n_disks_per_group = 16;
  ClusterMgr mgr;
  {

    auto sh = mgr.GetSnapshotBuilder(n_elements);
    sh.AddBucket(GetBucketType(BucketType::ROOT), 0);
    // sh.AddBucket(GetBucketType(BucketType::SITE), -1, 0);

    for (int i=0; i< n_groups; ++i) {
      sh.AddBucket(GetBucketType(BucketType::GROUP), -100 - i, 0);
    }

    for (int i=0; i < n_groups*n_disks_per_group; i++) {
      sh.AddDisk(Disk(i + 1, kMaskAll, ActiveStatus::kOnline, 1),
                 -100 - i / n_disks_per_group);
    }

  }
  FlatScheduler flat_scheduler(PlacementStrategyT::kFidRandom, n_elements);

  PlacementArgs args(state.range(1));
  args.fid=1;
  for (auto _: state) {
    auto cluster_data_ptr = mgr.GetClusterData();
    benchmark::DoNotOptimize(flat_scheduler.Schedule(cluster_data_ptr(), args));
    args.fid++;
  }
  state.counters["frequency"] = benchmark::Counter(state.iterations(),
                                                   benchmark::Counter::kIsRate);
}

static void BM_WeightedRandomScheduler(benchmark::State& state) {
  using namespace eos::mgm::placement;
  auto n_groups = state.range(0);
  auto n_elements = 1024;
  const int n_disks_per_group = 16;
  std::vector<int> weights = {4,8,16};
  ClusterMgr mgr;
  {

    auto sh = mgr.GetSnapshotBuilder(n_elements);
    sh.AddBucket(GetBucketType(BucketType::ROOT), 0);
    // sh.AddBucket(GetBucketType(BucketType::SITE), -1, 0);

    for (int i=0; i< n_groups; ++i) {
      sh.AddBucket(GetBucketType(BucketType::GROUP), -100 - i, 0);
    }

    for (int i=0; i < n_groups*n_disks_per_group; i++) {
      sh.AddDisk(Disk(i + 1, kMaskAll, ActiveStatus::kOnline,
                      eos::common::pickIndexRR(weights, i)),
                 -100 - i / n_disks_per_group);
    }

  }
  FlatScheduler flat_scheduler(PlacementStrategyT::kWeightedRandom, n_elements);

  PlacementArgs args(state.range(1));
  args.fid=1;
  for (auto _: state) {
    auto cluster_data_ptr = mgr.GetClusterData();
    benchmark::DoNotOptimize(flat_scheduler.Schedule(cluster_data_ptr(), args));
    args.fid++;
  }
  state.counters["frequency"] = benchmark::Counter(state.iterations(),
                                                   benchmark::Counter::kIsRate);
}

static void BM_WeightedRRScheduler(benchmark::State& state) {
  using namespace eos::mgm::placement;
  auto n_groups = state.range(0);
  auto n_elements = 1024;
  const int n_disks_per_group = 16;
  std::vector<int> weights = {4,8,16};
  ClusterMgr mgr;
  {

    auto sh = mgr.GetSnapshotBuilder(n_elements);
    sh.AddBucket(GetBucketType(BucketType::ROOT), 0);
    // sh.AddBucket(GetBucketType(BucketType::SITE), -1, 0);

    for (int i=0; i< n_groups; ++i) {
      sh.AddBucket(GetBucketType(BucketType::GROUP), -100 - i, 0);
    }

    for (int i=0; i < n_groups*n_disks_per_group; i++) {
      sh.AddDisk(Disk(i + 1, kMaskAll, ActiveStatus::kOnline,
                      eos::common::pickIndexRR(weights, i)),
                 -100 - i / n_disks_per_group);
    }

  }
  FlatScheduler flat_scheduler(PlacementStrategyT::kWeightedRoundRobin, n_elements);

  PlacementArgs args(state.range(1));
  args.fid=1;
  for (auto _: state) {
    auto cluster_data_ptr = mgr.GetClusterData();
    benchmark::DoNotOptimize(flat_scheduler.Schedule(cluster_data_ptr(), args));
    args.fid++;
  }
  state.counters["frequency"] = benchmark::Counter(state.iterations(),
                                                   benchmark::Counter::kIsRate);
}

//------------------------------------------------------------------------------
// The shape the topology builder produces for a cluster whose disks carry no
// geotag: every group holds a single <nogeotag> placeholder bucket, which in
// turn holds the disks - a disk is never attached to its group directly, see
// ClusterBuilder.cc. The descent therefore walks one more level than the
// benchmarks above, which is the common case in production.
//------------------------------------------------------------------------------
static void
BM_NoGeoTagScheduler(benchmark::State& state)
{
  using namespace eos::mgm::placement;
  auto n_groups = state.range(0);
  const int n_disks_per_group = 16;
  // root, then one group, its placeholder bucket and its flat leaf view per
  // group - undersizing this trips the buckets-vs-seeds guard of the round
  // robin strategies and the benchmark would time failures instead
  const size_t n_elements = 3 * n_groups + 256;
  ClusterMgr mgr;
  {

    auto sh = mgr.GetSnapshotBuilder(n_elements);
    sh.AddBucket(GetBucketType(BucketType::ROOT), 0);

    // Every group id has to be taken before the first geo bucket is allocated,
    // the builder hands out ids continuing below the lowest one in use
    for (int i = 0; i < n_groups; ++i) {
      sh.AddBucket(GetBucketType(BucketType::GROUP), -100 - i, 0);
    }

    for (int i = 0; i < n_groups; ++i) {
      auto nogeo_id = sh.GetOrAddGeoBucket(-100 - i, kNoGeoTagBucket,
                                           GetBucketType(BucketType::SITE));

      for (int j = 0; j < n_disks_per_group; ++j) {
        sh.AddDisk(
            Disk(i * n_disks_per_group + j + 1, kMaskAll, ActiveStatus::kOnline, 1),
            nogeo_id);
      }
    }
  }
  FlatScheduler flat_scheduler(PlacementStrategyT::kThreadLocalRoundRobin, n_elements);

  for (auto _ : state) {
    auto cluster_data_ptr = mgr.GetClusterData();
    auto result = flat_scheduler.Schedule(cluster_data_ptr(), state.range(1));

    // A failed placement must fail the benchmark, not get timed as if it were
    // one - an undersized topology otherwise goes unnoticed
    if (!result) {
      state.SkipWithError(result.ErrorString().c_str());
      break;
    }

    benchmark::DoNotOptimize(result);
  }
  state.counters["frequency"] =
      benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate);
}

//------------------------------------------------------------------------------
// A geo hierarchy of 2 sites x 2 rooms x 2 racks below every group, scheduled
// for a client sitting in one of the racks: this is what times the home/away
// split and the spill pass of the descent.
//------------------------------------------------------------------------------
static void
BM_GeoScheduler(benchmark::State& state)
{
  using namespace eos::mgm::placement;
  auto n_groups = state.range(0);
  const int n_disks_per_group = 16;
  const int n_sites = 2, n_rooms = 2, n_racks = 2;
  const int n_disks_per_rack = n_disks_per_group / (n_sites * n_rooms * n_racks);
  // root, one group and 2 sites, 4 rooms and 8 racks below each of them
  const size_t n_elements = 16 * n_groups + 256;
  ClusterMgr mgr;
  {

    auto sh = mgr.GetSnapshotBuilder(n_elements);
    sh.AddBucket(GetBucketType(BucketType::ROOT), 0);

    // Every group id has to be taken before the first geo bucket is allocated,
    // the builder hands out ids continuing below the lowest one in use
    for (int i = 0; i < n_groups; ++i) {
      sh.AddBucket(GetBucketType(BucketType::GROUP), -100 - i, 0);
    }

    uint32_t fsid = 1;

    for (int i = 0; i < n_groups; ++i) {
      for (int s = 0; s < n_sites; ++s) {
        auto site_id = sh.GetOrAddGeoBucket(-100 - i, "site" + std::to_string(s),
                                            GetBucketType(BucketType::SITE));

        for (int r = 0; r < n_rooms; ++r) {
          auto room_id = sh.GetOrAddGeoBucket(site_id, "room" + std::to_string(r),
                                              GetBucketType(BucketType::ROOM));

          for (int k = 0; k < n_racks; ++k) {
            auto rack_id = sh.GetOrAddGeoBucket(room_id, "rack" + std::to_string(k),
                                                GetBucketType(BucketType::RACK));

            for (int j = 0; j < n_disks_per_rack; ++j) {
              sh.AddDisk(Disk(fsid++, kMaskAll, ActiveStatus::kOnline, 1), rack_id);
            }
          }
        }
      }
    }
  }
  FlatScheduler flat_scheduler(PlacementStrategyT::kGeoScheduler, n_elements);

  const std::string geolocation = "site0::room0::rack0";
  PlacementArgs args(state.range(1), kClientCreate, PlacementStrategyT::kGeoScheduler);
  args.geolocation = geolocation;
  args.ncollocatedfs = 2;
  args.fid = 1;
  for (auto _ : state) {
    auto cluster_data_ptr = mgr.GetClusterData();
    auto result = flat_scheduler.Schedule(cluster_data_ptr(), args);

    // A failed placement must fail the benchmark, not get timed as if it were
    // one - an undersized topology otherwise goes unnoticed
    if (!result) {
      state.SkipWithError(result.ErrorString().c_str());
      break;
    }

    benchmark::DoNotOptimize(result);
    args.fid++;
  }
  state.counters["frequency"] =
      benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate);
}

BENCHMARK(BM_Scheduler)->Threads(1)->Threads(8)->Threads(64)->Threads(128)->Threads(256)
    ->ArgsProduct({{32, 64, 128, 256, 512},
                   {2,3,6}})->UseRealTime();

BENCHMARK(BM_ThreadLocalRRScheduler)->Threads(1)->Threads(8)->Threads(64)->Threads(128)->Threads(256)
    ->ArgsProduct({{32, 64, 128, 256, 512},
                   {2,3,6}})->UseRealTime();

BENCHMARK(BM_RandomScheduler)->Threads(1)->Threads(8)->Threads(64)->Threads(128)->Threads(256)
    ->ArgsProduct({{32, 64, 128, 256, 512},
                   {2,3,6}})->UseRealTime();

BENCHMARK(BM_FidScheduler)->Threads(1)->Threads(8)->Threads(64)->Threads(128)->Threads(256)
->ArgsProduct({{32, 64, 128, 256, 512},
               {2,3,6}})->UseRealTime();

BENCHMARK(BM_WeightedRandomScheduler)->Threads(1)->Threads(8)->Threads(64)->Threads(128)->Threads(256)
->ArgsProduct({{32, 64, 128, 256, 512},
               {2,3,6}})->UseRealTime();

BENCHMARK(BM_WeightedRRScheduler)->Threads(1)->Threads(8)->Threads(64)->Threads(128)->Threads(256)
->ArgsProduct({{32, 64, 128, 256, 512},
               {2,3,6}})->UseRealTime();

//------------------------------------------------------------------------------
// The same geo hierarchy as BM_GeoScheduler, scheduled for a client without a
// geotag: no collocation constraint to honour, so the descent is served from
// the flat leaf view of the chosen group instead of walking the geo levels.
// This is the production case of an untagged client on a geo-organized space.
//------------------------------------------------------------------------------
static void
BM_NoGeoTagGeoTopology(benchmark::State& state)
{
  using namespace eos::mgm::placement;
  auto n_groups = state.range(0);
  const int n_disks_per_group = 16;
  const int n_sites = 2, n_rooms = 2, n_racks = 2;
  const int n_disks_per_rack = n_disks_per_group / (n_sites * n_rooms * n_racks);
  // root, one group and 2 sites, 4 rooms and 8 racks below each of them
  const size_t n_elements = 16 * n_groups + 256;
  ClusterMgr mgr;
  {

    auto sh = mgr.GetSnapshotBuilder(n_elements);
    sh.AddBucket(GetBucketType(BucketType::ROOT), 0);

    // Every group id has to be taken before the first geo bucket is allocated,
    // the builder hands out ids continuing below the lowest one in use
    for (int i = 0; i < n_groups; ++i) {
      sh.AddBucket(GetBucketType(BucketType::GROUP), -100 - i, 0);
    }

    uint32_t fsid = 1;

    for (int i = 0; i < n_groups; ++i) {
      for (int s = 0; s < n_sites; ++s) {
        auto site_id = sh.GetOrAddGeoBucket(-100 - i, "site" + std::to_string(s),
                                            GetBucketType(BucketType::SITE));

        for (int r = 0; r < n_rooms; ++r) {
          auto room_id = sh.GetOrAddGeoBucket(site_id, "room" + std::to_string(r),
                                              GetBucketType(BucketType::ROOM));

          for (int k = 0; k < n_racks; ++k) {
            auto rack_id = sh.GetOrAddGeoBucket(room_id, "rack" + std::to_string(k),
                                                GetBucketType(BucketType::RACK));

            for (int j = 0; j < n_disks_per_rack; ++j) {
              sh.AddDisk(Disk(fsid++, kMaskAll, ActiveStatus::kOnline, 1), rack_id);
            }
          }
        }
      }
    }
  }
  FlatScheduler flat_scheduler(PlacementStrategyT::kGeoScheduler, n_elements);

  PlacementArgs args(state.range(1), kClientCreate, PlacementStrategyT::kGeoScheduler);
  args.fid = 1;
  for (auto _ : state) {
    auto cluster_data_ptr = mgr.GetClusterData();
    auto result = flat_scheduler.Schedule(cluster_data_ptr(), args);

    // A failed placement must fail the benchmark, not get timed as if it were
    // one - an undersized topology otherwise goes unnoticed
    if (!result) {
      state.SkipWithError(result.ErrorString().c_str());
      break;
    }

    benchmark::DoNotOptimize(result);
    args.fid++;
  }
  state.counters["frequency"] =
      benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate);
}

BENCHMARK(BM_NoGeoTagScheduler)
    ->Threads(1)
    ->Threads(8)
    ->ArgsProduct({{64, 256}, {2, 3}})
    ->UseRealTime();

BENCHMARK(BM_GeoScheduler)
    ->Threads(1)
    ->Threads(8)
    ->ArgsProduct({{64, 256}, {2, 3}})
    ->UseRealTime();

BENCHMARK(BM_NoGeoTagGeoTopology)
    ->Threads(1)
    ->Threads(8)
    ->ArgsProduct({{64, 256}, {2, 3}})
    ->UseRealTime();

BENCHMARK_MAIN();
