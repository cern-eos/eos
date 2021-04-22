#include "common/RWMutex.hh"
#include <map>
#include <thread>
#include <benchmark/benchmark.h>

/*
 A simple concurrent map that uses std::map & the various eos rwlocks to
 simulate some concurrent processing issues seen in the MGM, primarily used to
 simulate clients reading and writing caps for eg.
*/
class SimpleConcMap : public eos::common::RWMutex
{
public:
  using key_type = std::string;
  using val_type = uint64_t;
  using cmap_t = std::map<key_type, val_type>;

  SimpleConcMap(): eos::common::RWMutex()
  {
    mBlocking=true;
  }

  // all the following methods have a thread-safe and an unsafe version
  // just to do some basic RWMutex benchmarks
  void add(key_type key) {
    cmap[key]=static_cast<uint64_t>(time(NULL));
  }

  val_type read(key_type key) {
    auto it = cmap.find(key);
    return it != cmap.end() ? it->second : val_type {};
  }

  bool remove(key_type key) {
    return cmap.erase(key);
  }

  void addTS(key_type key) {
    eos::common::RWMutexWriteLock wLock(*this);
    add(key);
  }


  val_type readTS(key_type key) {
    eos::common::RWMutexReadLock rLock(*this);
    return read(key);
  }

  bool remove_TS(key_type key) {
    eos::common::RWMutexWriteLock wLock(*this);
    return remove(key);
  }

  cmap_t copy_allTS(int delay=0);
  size_t get_sizeTS() {
    eos::common::RWMutexReadLock rLock(*this);
    return cmap.size();
  }

private:
  cmap_t cmap;
};

SimpleConcMap::cmap_t SimpleConcMap::copy_allTS(int delay)
{
  cmap_t result;
  eos::common::RWMutexReadLock rLock(*this);
  for (const auto& kv: cmap) {
    result.emplace(kv.first, kv.second);
  }
  return result;
}

static void BM_KeyWrite(benchmark::State& state) {
  for (auto _: state) {
    SimpleConcMap cm;
    for(int64_t i = state.range(0); --i;) {
      cm.add(std::to_string(i));
    }
  }
}

static void BM_KeyWriteTS(benchmark::State& state) {
  for (auto _: state) {
    SimpleConcMap cm;
    for(int64_t i = state.range(0); --i;) {
      cm.add(std::to_string(i));
    }
  }
}


static void BM_SingleRWTest(benchmark::State& state) {
  SimpleConcMap cmap;
  const int map_count = 10000;
  const int delay = 10;

  std::thread writer([&cmap]() {
    for (int i =0; i< map_count; i++) {
      cmap.addTS(std::to_string(i));
    }
  });
  std::thread reader([&cmap]() {
    size_t sz = 0;
    while(sz < map_count)
      {
        cmap.copy_allTS(delay);
      }
  });

  if (state.thread_index == 0) {
  }

  for (auto _: state) {
    reader.join();
    writer.join();
  }

}

uint64_t start = 1<<7;
uint64_t end = 1<<24ULL;
BENCHMARK(BM_KeyWrite)->Range(start,end)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_KeyWriteTS)->Range(start,end)->ThreadRange(1,8)->Unit(benchmark::kMillisecond);
BENCHMARK_MAIN();
