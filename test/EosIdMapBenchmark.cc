//------------------------------------------------------------------------------
// File: EosIdMapBenchmark.cc
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
#include "common/Logging.hh"
#include <XrdSec/XrdSecEntity.hh>
#include <chrono>

void IdMapClient(int n, int cache_factor=1){
  auto vid = eos::common::VirtualIdentity::Nobody();
  XrdSecEntity client("sss");
  char name[24] = "foobar";
  client.tident = "root";
  client.name = name;
  std::stringstream base_ss;
  base_ss << "foo.bar:baz@bar" << std::this_thread::get_id();
  std::string tident_base = base_ss.str();
  for (int j=0; j < cache_factor; ++j) {
    for (int i=0; i < n/cache_factor; ++i) {
      std::string client_name = "testuser" + std::to_string(i);
      client.name = client_name.data();
      std::string tident = tident_base + std::to_string(i);
      eos::common::Mapping::IdMap(&client, nullptr, tident.c_str(), vid);
    }
  }
}

int main(int argc, const char* argv[]) {
  if (argc < 2) {
    std::cerr << "Usage: " << argv[0] << " <num-entries> [num-threads] [cache_factor]" << std::endl;
    return 1;
  }
  std::chrono::steady_clock::time_point init = std::chrono::steady_clock::now();

  eos::common::Mapping::Init();
  eos::common::Mapping::gVirtualUidMap["sss:\"<pwd>\":uid"]=0;
  eos::common::Mapping::gVirtualGidMap["sss:\"<pwd>\":gid"]=0;
  int n_clients = 1;
  int num_threads = 50;
  int cache_factor = 1;

  switch (argc) {
  case 4:
    cache_factor = atoi(argv[3]);
    // fallthrough
  case 3:
    num_threads = atoi(argv[2]);
    // fallthrough
  case 2:
    n_clients = atoi(argv[1]);
    // fallthrough
  }

  /*
  auto& g_logger = eos::common::Logging::GetInstance();
  g_logger.SetLogPriority(LOG_INFO);
  g_logger.SetUnit("EOSFileMD");
*/
  std::vector<std::thread> threads;
  threads.reserve(num_threads);
  std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();

  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back(IdMapClient, n_clients, cache_factor);
  }

  for (auto& t : threads) {
    t.join();
  }

  std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
  auto init_time = std::chrono::duration_cast<std::chrono::milliseconds>(begin - init).count();
  auto ms_elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();
  std::cout << "Init Time = " << init_time << " ms"
            << " Time difference = " << ms_elapsed << " [ms] frequency = "
            << (n_clients*num_threads)/ms_elapsed << " [kHz]"
            << "\n";

  eos::common::Mapping::Reset();
  std::chrono::steady_clock::time_point reset = std::chrono::steady_clock::now();
  auto reset_time = std::chrono::duration_cast<std::chrono::milliseconds>(reset - end).count();
  std::cout << "Reset time=" << reset_time << " ms" << "\n";
  return 0;
}
