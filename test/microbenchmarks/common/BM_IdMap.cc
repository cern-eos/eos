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
#include "XrdSec/XrdSecEntity.hh"
#include "benchmark/benchmark.h"
#include <sstream>
#include <thread>

class MappingFixture: public benchmark::Fixture {
public:
  void SetUp(const ::benchmark::State& state) {
    eos::common::Mapping::Init();
  }

  void TearDown(const ::benchmark::State& state) {
    eos::common::Mapping::Reset();
  }

};

static void BM_IdMap(benchmark::State& state) {
  XrdSecEntity client("test");
  eos::common::Mapping::Init();
  auto vid = eos::common::VirtualIdentity::Nobody();
  vid.prot="sss";
  client.tident = "root";
  std::string client_name = "foobar";
  std::stringstream base_ss;
  base_ss << "foo.bar:baz@bar" << std::this_thread::get_id();
  std::string tident_base = base_ss.str();
  for (auto _ : state) {
    for (int j=0; j < state.range(0); ++j) {
      std::string client_name = "client" + std::to_string(j);
      client.name = client_name.data();
      std::string tident = tident_base + std::to_string(j);
      eos::common::Mapping::IdMap(&client, nullptr, tident.c_str(), vid);
    }
  }
}

//BENCHMARK_REGISTER_F(MappingFixture, IdMap);
BENCHMARK(BM_IdMap)->Range(1<<10,1<<20)->ThreadRange(1, 128)->UseRealTime()->Unit(benchmark::kMillisecond);
BENCHMARK_MAIN();
