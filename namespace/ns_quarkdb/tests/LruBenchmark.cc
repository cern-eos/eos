//------------------------------------------------------------------------------
// @file LruBenchmark.cc
// @author Elvin-Alin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2020 CERN/Switzerland                                  *
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

#include "common/CLI11.hpp"
#include "namespace/ns_quarkdb/LRU.hh"
#include <random>

//! Global synchronization primitives
std::mutex gMutex;
std::condition_variable gCondVar;
std::atomic<unsigned long> gDoneWork {0};

uint64_t randint(uint64_t start, uint64_t end)
{
  thread_local std::mt19937 engine(std::random_device{}());
  std::uniform_int_distribution<> dist(start,end);
  return dist(engine);
}

//------------------------------------------------------------------------------
//! Dummy struct used to populate the LRU
//------------------------------------------------------------------------------
struct Entry {
  explicit Entry(std::uint64_t id) : id_(id) {}

  ~Entry() = default;

  std::uint64_t
  getId() const
  {
    return id_;
  }

  std::uint64_t id_;
};

//------------------------------------------------------------------------------
//! Populate LRU with the desired number of entries
//------------------------------------------------------------------------------
void Populate(eos::LRU<std::uint64_t, Entry>& lru, uint64_t size)
{
  for (std::uint64_t id = 1; id <= size; ++id) {
    lru.put(id, std::make_shared<Entry>(id));
  }
}

//------------------------------------------------------------------------------
//! Work done by each individual thread
//------------------------------------------------------------------------------
void WokerThread(eos::LRU<std::uint64_t, Entry>& lru, std::uint64_t num_req,
                 std::uint64_t max_size)
{

  std::random_device rd;
  std::mt19937 gen(rd());

  // Pick a random start location between [1, max_size]

  unsigned long long random_start =
    randint(1ull, (unsigned long long) max_size);
  // Wait for notification from the main thread
  std::unique_lock<std::mutex> lock(gMutex);
  gCondVar.wait(lock);
  lock.unlock();

  while (num_req) {
    lru.get(random_start);
    random_start = (random_start + 1) % max_size + 1;
    --num_req;
  }

  ++gDoneWork;
  gCondVar.notify_one();
}

//------------------------------------------------------------------------------
// Main programm
//------------------------------------------------------------------------------
int main(int argc, char* argv[])
{
  CLI::App app{"LRU benchmark tool"};
  std::uint64_t max_size = 1000000;
  std::uint32_t num_threads = 1;
  std::uint64_t num_requests = max_size / 10;
  app.add_option("-s,--size", max_size, "max size of the LRU");
  app.add_option("-t,--num_threads", num_threads,
                 "number of threads for access operations");
  app.add_option("-r,--num_requests", num_requests,
                 "number of requests per thread");
  CLI11_PARSE(app, argc, argv);
  eos::LRU<std::uint64_t, Entry> lru{max_size + 10};
  Populate(lru, max_size);
  std::list<std::thread> workers;

  for (auto i = 0ull; i < num_threads; ++i) {
    workers.emplace_back(WokerThread, std::ref(lru), num_requests, max_size);
  }

  // Sleep a bit to allow all threads to start
  std::this_thread::sleep_for(std::chrono::seconds(2));
  auto start_ts = std::chrono::system_clock::now();
  gCondVar.notify_all();
  // Wait for all threads to finish
  {
    std::unique_lock<std::mutex> lock(gMutex);
    gCondVar.wait(lock, [&] {return (gDoneWork == num_threads);});
  }
  auto end_ts = std::chrono::system_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>
                  (end_ts - start_ts);
  std::uint64_t total_req = num_threads * num_requests;
  std::cout << "Rate : " << ((total_req * 1000000) / duration.count()) / 100 <<
            " kHz\n";

  for (auto& thread : workers) {
    thread.join();
  }

  return 0;
}
