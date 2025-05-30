//------------------------------------------------------------------------------
// File: BM_IdMap.cc
// Author: Abhishek Lekshmanan - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2022 CERN/Switzerland                           *
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

#include "common/Mapping.hh"
#include <XrdSec/XrdSecEntity.hh>
#include "benchmark/benchmark.h"
#include <sstream>
#include <thread>

using benchmark::Counter;

class MappingFixture: public benchmark::Fixture
{
public:
  void SetUp(const ::benchmark::State& state)
  {
    eos::common::Mapping::Init();
  }

  void TearDown(const ::benchmark::State& state)
  {
    eos::common::Mapping::Reset();
  }

};

static void BM_IdMap(benchmark::State& state)
{
  using namespace eos::common;
  std::atomic<uint64_t> ctr = 0;

  if (state.thread_index == 0) {
    eos::common::Mapping::Reset();
    eos::common::Mapping::Init();
    eos::common::Mapping::gVirtualUidMap["sss:\"<pwd>\":uid"] = 0;
    eos::common::Mapping::gVirtualGidMap["sss:\"<pwd>\":gid"] = 0;
  }

  for (auto _ : state) {
    state.PauseTiming();
    XrdSecEntity client("test");
    eos::common::VirtualIdentity vid;
    vid.prot = "sss";
    client.tident = "root";
    std::stringstream base_ss;
    base_ss << "foo.bar:baz@bar" << std::this_thread::get_id();
    std::string tident_base = base_ss.str();
    std::string client_name = "client" + std::to_string(ctr);
    vid.uid = ctr % 2147483646;
    vid.gid = ctr % 2147483646;
    client.name = client_name.data();
    std::string tident = tident_base + std::to_string(ctr);
    state.ResumeTiming();
    eos::common::Mapping::IdMap(&client, nullptr, tident.c_str(), vid);
    state.PauseTiming();
    ctr++;
  }

  if (state.thread_index == 0) {
    eos::common::Mapping::Reset();
  }
}

static void BM_ReduceTident(benchmark::State& state)
{
  for (auto _ : state) {
    //state.PauseTiming();
    for (int j = 0; j < state.range(0); ++j) {
      std::string tident = "foo.bar:baz@bar" + std::to_string(j);
      std::string wildcardtident, mytident, myhost;
      // state.ResumeTiming();
      mytident = eos::common::Mapping::ReduceTident(tident, wildcardtident, myhost);
    }
  }

  state.counters["frequency"] = Counter(state.iterations(),
                                        benchmark::Counter::kIsRate);
}

static void BM_ReduceTidentXrd(benchmark::State& state)
{
  for (auto _ : state) {
    //state.PauseTiming();
    for (int j = 0; j < state.range(0); ++j) {
      std::string tident = "foo.bar:baz@bar" + std::to_string(j);
      XrdOucString tident_xrd(tident.c_str());
      XrdOucString wildcardtident, mytident, myhost;
      // state.ResumeTiming();
      eos::common::Mapping::ReduceTident(tident_xrd, wildcardtident, mytident,
                                         myhost);
    }
  }

  state.counters["frequency"] = Counter(state.iterations(),
                                        benchmark::Counter::kIsRate);
}
BENCHMARK(BM_IdMap)
->Range(1 << 10, 1 << 20)->ThreadRange(1, 128)->UseRealTime()
->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_ReduceTident)->Range(1, 1 << 20);
BENCHMARK(BM_ReduceTidentXrd)->Range(1, 1 << 20);
BENCHMARK_MAIN();
