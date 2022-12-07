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
#include "XrdSec/XrdSecEntity.hh"
#include <chrono>

thread_local std::vector<uint64_t> time_records;
std::vector<uint64_t> g_time_records;
std::mutex g_time_record_mtx;

struct SampleStats
{
  double mean;
  double stddev;

  SampleStats(double mean_, double stddev_ ) : mean(mean_), stddev(stddev_) {};

};

template <typename C>
SampleStats calculate_stats(const C& c)
{
  size_t count = g_time_records.size();
  double sum = std::accumulate(c.begin(), c.end(), 0.0);
  double mean = sum/count;
  double accum = std::accumulate(c.begin(), c.end(), 0.0,
                             [mean](double accum, double val) {
    return accum + (val - mean)*(val - mean);
  });
  double stddev = std::sqrt(accum/count);
  return {mean, stddev};
}

template <typename It>
void print_range(const It& begin, const It& end) {
  for (auto it = begin;
       it != end; ++it) {
    std::cout << *it << ", ";
  }
  std::cout << "\n";
}

template <typename C>
void write_to_file(const std::string& filename,
                   const C& data) {
  std::ofstream out(filename);

  using value_t = typename C::value_type;
  std::ostream_iterator<value_t> output_iterator(out, "\n");
  std::copy(data.begin(), data.end(), output_iterator);
}

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
      std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
      eos::common::Mapping::IdMap(&client, nullptr, tident.c_str(), vid);
      std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
      time_records.emplace_back(std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count());
    }
  }
  std::scoped_lock lk(g_time_record_mtx);
  g_time_records.insert(g_time_records.end(),
                        std::make_move_iterator(time_records.begin()),
                        std::make_move_iterator(time_records.end()));
}

int main(int argc, const char* argv[]) {
  if (argc < 2) {
    std::cerr << "Usage: " << argv[0]
              << " <num-entries> [num-threads] [cache_factor] [filename]" << std::endl;
    return 1;
  }
  std::chrono::steady_clock::time_point init = std::chrono::steady_clock::now();

  eos::common::Mapping::Init();
  eos::common::Mapping::gVirtualUidMap["sss:\"<pwd>\":uid"]=0;
  eos::common::Mapping::gVirtualGidMap["sss:\"<pwd>\":gid"]=0;
  int n_clients = 1;
  int num_threads = 50;
  int cache_factor = 1;
  std::string filename = "benchmark.csv";
  switch (argc) {
  case 5:
    filename = argv[4];
  case 4:
    cache_factor = atoi(argv[3]);
  case 3:
    num_threads = atoi(argv[2]);
  case 2:
    n_clients = atoi(argv[1]);
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

  std::sort(g_time_records.begin(), g_time_records.end());
  auto stats = calculate_stats(g_time_records);
  std::cout << "Average idmap times=" << stats.mean << "us stddev=" << stats.stddev << " us\n"
            << "Top 10 times Min, Max\n";
  print_range(g_time_records.begin(), g_time_records.begin()+10);
  print_range(g_time_records.end()-10, g_time_records.end());
  std::cout << "Writing per idmap time to file " << filename << "\n";
  write_to_file(filename, g_time_records);

  eos::common::Mapping::Reset();
  std::chrono::steady_clock::time_point reset = std::chrono::steady_clock::now();
  auto reset_time = std::chrono::duration_cast<std::chrono::milliseconds>(reset - end).count();
  std::cout << "Reset time=" << reset_time << " ms" << "\n";
  return 0;
}
