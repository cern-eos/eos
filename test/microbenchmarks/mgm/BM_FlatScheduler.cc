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

#include "mgm/placement/PlacementStrategy.hh"
#include "benchmark/benchmark.h"
#include "mgm/placement/ClusterMap.hh"
#include "mgm/placement/FlatScheduler.hh"

static void BM_Scheduler(benchmark::State& state) {
  using namespace eos::mgm::placement;
  auto n_groups = state.range(0);
  auto n_elements = n_groups + 101;
  const int n_disks_per_group = 16;
  ClusterMgr mgr;
  {

    auto sh = mgr.getStorageHandler(n_elements);
    sh.addBucket(get_bucket_type(StdBucketType::ROOT), 0);
    sh.addBucket(get_bucket_type(StdBucketType::SITE), -1, 0);

    for (int i=0; i< n_groups; ++i) {
      sh.addBucket(get_bucket_type(StdBucketType::GROUP), -100-i, -1);
    }

    for (int i=0; i < n_groups*n_disks_per_group; i++) {
      sh.addDisk(Disk(i+1, ConfigStatus::kRW, ActiveStatus::kOnline, 1),
                 -100 - i/n_disks_per_group);
    }

  }
  FlatScheduler flat_scheduler(PlacementStrategyT::kRoundRobin, n_elements);


  for (auto _: state) {
    auto cluster_data_ptr = mgr.getClusterData();
    benchmark::DoNotOptimize(flat_scheduler.schedule(cluster_data_ptr(),state.range(1)));
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

    auto sh = mgr.getStorageHandler(n_elements);
    sh.addBucket(get_bucket_type(StdBucketType::ROOT), 0);
    //sh.addBucket(get_bucket_type(StdBucketType::SITE), -1, 0);

    for (int i=0; i< n_groups; ++i) {
      sh.addBucket(get_bucket_type(StdBucketType::GROUP), -100-i, 0);
    }

    for (int i=0; i < n_groups*n_disks_per_group; i++) {
      sh.addDisk(Disk(i+1, ConfigStatus::kRW, ActiveStatus::kOnline, 1),
                 -100 - i/n_disks_per_group);
    }

  }
  FlatScheduler flat_scheduler(PlacementStrategyT::kThreadLocalRoundRobin, n_elements);


  for (auto _: state) {
    auto cluster_data_ptr = mgr.getClusterData();
    benchmark::DoNotOptimize(flat_scheduler.schedule(cluster_data_ptr(),state.range(1)));
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

    auto sh = mgr.getStorageHandler(n_elements);
    sh.addBucket(get_bucket_type(StdBucketType::ROOT), 0);
    //sh.addBucket(get_bucket_type(StdBucketType::SITE), -1, 0);

    for (int i=0; i< n_groups; ++i) {
      sh.addBucket(get_bucket_type(StdBucketType::GROUP), -100-i, 0);
    }

    for (int i=0; i < n_groups*n_disks_per_group; i++) {
      sh.addDisk(Disk(i+1, ConfigStatus::kRW, ActiveStatus::kOnline, 1),
                 -100 - i/n_disks_per_group);
    }

  }
  FlatScheduler flat_scheduler(PlacementStrategyT::kRandom, n_elements);


  for (auto _: state) {
    auto cluster_data_ptr = mgr.getClusterData();
    benchmark::DoNotOptimize(flat_scheduler.schedule(cluster_data_ptr(),state.range(1)));
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

    auto sh = mgr.getStorageHandler(n_elements);
    sh.addBucket(get_bucket_type(StdBucketType::ROOT), 0);
    //sh.addBucket(get_bucket_type(StdBucketType::SITE), -1, 0);

    for (int i=0; i< n_groups; ++i) {
      sh.addBucket(get_bucket_type(StdBucketType::GROUP), -100-i, 0);
    }

    for (int i=0; i < n_groups*n_disks_per_group; i++) {
      sh.addDisk(Disk(i+1, ConfigStatus::kRW, ActiveStatus::kOnline, 1),
                 -100 - i/n_disks_per_group);
    }

  }
  FlatScheduler flat_scheduler(PlacementStrategyT::kFidRandom, n_elements);


  FlatScheduler::PlacementArguments args(state.range(1));
  args.fid=1;
  for (auto _: state) {
    auto cluster_data_ptr = mgr.getClusterData();
    benchmark::DoNotOptimize(flat_scheduler.schedule(cluster_data_ptr(), args));
    args.fid++;
  }
  state.counters["frequency"] = benchmark::Counter(state.iterations(),
                                                   benchmark::Counter::kIsRate);
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


BENCHMARK_MAIN();
