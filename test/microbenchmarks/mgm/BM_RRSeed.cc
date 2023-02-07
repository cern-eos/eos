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


#include "mgm/placement/RRSeed.hh"
#include "benchmark/benchmark.h"

static void BM_RRSeed(benchmark::State& state) {
  eos::mgm::placement::RRSeed seed(10);
  for (auto _ : state) {
    for (int i=0;i<10; ++i)
    benchmark::DoNotOptimize(seed.get(1,0));
  }
  state.counters["frequency"] = Counter(state.iterations()*10,
                                        benchmark::Counter::kIsRate);
}

BENCHMARK(BM_RRSeed)->ThreadRange(1,64)->UseRealTime();
