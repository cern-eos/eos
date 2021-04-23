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

  void clear() { cmap.clear(); }
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
      cm.addTS(std::to_string(i));
    }
  }
}

struct CMFixture: public ::benchmark::Fixture {
  SimpleConcMap cm;
  void Setup(benchmark::State& state) {
    for(int64_t i = state.range(0); --i;) {
      cm.add(std::to_string(i));
    }
  }
  void TearDown(benchmark::State&) {
    cm.clear();
  }
};

struct CMFixedFixture: public ::benchmark::Fixture {
  SimpleConcMap cm;
  void Setup(benchmark::State& state) {
    for(int64_t i = state.range(1)*1000; --i;) {
      cm.add(std::to_string(i));
    }
  };
  void TearDown(benchmark::State&) {
    cm.clear();
  }
};

BENCHMARK_DEFINE_F(CMFixture, BM_ReadTS)(benchmark::State& state) {
  const int64_t sz = static_cast<int64_t>(state.range(0));
  for (auto _: state) {
    for (int i=0; i < sz; i++) {
      cm.readTS(std::to_string(std::rand() % sz));
    }
  }
}

// prepopulate some keys so that initial reads have some hit rate
BENCHMARK_DEFINE_F(CMFixture, BM_ReadWriteTS)(benchmark::State& state) {
  const int64_t sz = static_cast<int64_t>(state.range(0));

  for (auto _:state) {
    std::thread writer([&]() {
      for (int i =0; i< sz; i++) {
        cm.addTS(std::to_string(i));
      }
    });
    std::thread reader([&]() {
      for (int i=0; i < sz; i++) {
        cm.readTS(std::to_string(std::rand() % sz));
      }
    });
    reader.join();
    writer.join();
  }
}

BENCHMARK_DEFINE_F(CMFixedFixture, BM_ReadWriteMultiTS)(benchmark::State& state) {
  const int64_t sz = static_cast<int64_t>(state.range(0));
  const int64_t w_thread_sz = static_cast<int64_t>(state.range(1));
  const int64_t r_thread_sz = static_cast<int64_t>(state.range(2));

  for (auto _:state) {
    std::vector<std::thread> reader_threads;
    std::vector<std::thread> writer_threads;

    for (int i=0; i< w_thread_sz; i++) {
      writer_threads.emplace_back(std::thread([&]() {
        for (int i =0; i < sz; i++) {
          cm.addTS(std::to_string(i));
        }
      }));
    }
    for (int i=0; i < r_thread_sz; i++) {
      reader_threads.emplace_back(std::thread([&]() {
        for (int i=0; i < sz; i++) {
          cm.readTS(std::to_string(std::rand() % sz));
        }
      }));
    }

    for (auto& rth: reader_threads) rth.join();
    for (auto& wth: writer_threads) wth.join();
  }
}

uint64_t start = 1<<7;
uint64_t end = 4<<20UL;
BENCHMARK(BM_KeyWrite)->Range(start,end)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_KeyWriteTS)->Range(start,end)->ThreadRange(1,8)->Unit(benchmark::kMillisecond);
BENCHMARK_REGISTER_F(CMFixture, BM_ReadTS)->Range(start,end)->ThreadRange(1,8)->Unit(benchmark::kMillisecond);
BENCHMARK_REGISTER_F(CMFixture, BM_ReadWriteTS)->Range(start,end)->Unit(benchmark::kMillisecond)->UseRealTime();
BENCHMARK_REGISTER_F(CMFixedFixture, BM_ReadWriteMultiTS)->Ranges({{start,end},{1,2},{1,2}})->Unit(benchmark::kMillisecond)->UseRealTime();
BENCHMARK_MAIN();
