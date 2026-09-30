// ----------------------------------------------------------------------
// File: BM_RRSeed.cc
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
#include "mgm/placement/RRSeed.hh"
#include <random>

using benchmark::Counter;
using eos::mgm::placement::RRSeed;

//------------------------------------------------------------------------------
// Every thread on the same counter: true sharing, the worst case
//------------------------------------------------------------------------------
static void BM_RRSeed(benchmark::State& state) {
  static RRSeed seed(1024);
  for (auto _ : state) {
    benchmark::DoNotOptimize(seed.Get(1, 1));
  }
  state.counters["frequency"] = Counter(state.iterations(), benchmark::Counter::kIsRate);
}

//------------------------------------------------------------------------------
// Every thread on a counter of its own: only false sharing between
// neighbouring counters could slow this down
//------------------------------------------------------------------------------
static void
BM_RRSeedDistinctIndex(benchmark::State& state)
{
  static RRSeed seed(1024);
  const size_t index = 1 + state.thread_index();
  for (auto _ : state) {
    benchmark::DoNotOptimize(seed.Get(index, 1));
  }
  state.counters["frequency"] = Counter(state.iterations(), benchmark::Counter::kIsRate);
}

//------------------------------------------------------------------------------
// What a placement does: advance the root counter, then the counter of one of
// 96 groups
//------------------------------------------------------------------------------
static void
BM_RRSeedPlacementMix(benchmark::State& state)
{
  static RRSeed seed(1024);
  std::minstd_rand rng(state.thread_index() + 1);
  for (auto _ : state) {
    benchmark::DoNotOptimize(seed.Get(0, 1));
    benchmark::DoNotOptimize(seed.Get(1 + rng() % 96, 2));
  }
  state.counters["frequency"] = Counter(state.iterations(), benchmark::Counter::kIsRate);
}

BENCHMARK(BM_RRSeed)->ThreadRange(1,64)->UseRealTime();
BENCHMARK(BM_RRSeedDistinctIndex)->ThreadRange(1, 64)->UseRealTime();
BENCHMARK(BM_RRSeedPlacementMix)->ThreadRange(1, 64)->UseRealTime();
BENCHMARK_MAIN();
