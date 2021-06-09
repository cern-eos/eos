#include "mgm/FuseServer/Caps.hh"
#include <benchmark/benchmark.h>

using namespace eos::mgm::FuseServer;

static void BM_GetTS(benchmark::State& state) {
  Caps c;
  std::string key {"invalid"};
  for (auto _: state) {
    c.GetTS(key);
  }
}

static void BM_Get(benchmark::State& state) {
  Caps c;
  std::string key {"invalid"};
  for (auto _: state) {
    c.Get(key);
  }
}


static void BM_GetRaw(benchmark::State& state){
  Caps c;
  std::string key {"invalid"};
  for (auto _ :state) {
    c.GetRaw(key);
  }
}

static void BM_GetUnsafe(benchmark::State& state) {
  Caps c;
  std::string key {"invalid"};
  for (auto _ :state) {
    c.GetUnsafe(key);
  }
}

BENCHMARK(BM_GetTS);
BENCHMARK(BM_Get);
BENCHMARK(BM_GetRaw);
BENCHMARK(BM_GetUnsafe);
BENCHMARK_MAIN();
