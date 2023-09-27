#include "benchmark/benchmark.h"
#include "common/utils/RandUtils.hh"
#include <stdlib.h>
using benchmark::Counter;

static constexpr uint32_t MAX_RAND = 60;

static void BM_CRand(benchmark::State& state)
{
  uint32_t x;
  for (auto _: state) {
    benchmark::DoNotOptimize(x = rand() % MAX_RAND);
    benchmark::ClobberMemory();
  }
  state.counters["frequency"] = Counter(state.iterations(),
                                        benchmark::Counter::kIsRate);
}

static void BM_getRandom(benchmark::State& state)
{
  uint64_t x;
  for (auto _: state) {
    benchmark::DoNotOptimize(x = eos::common::getRandom((uint32_t)0,MAX_RAND));
    benchmark::ClobberMemory();
  }
  state.counters["frequency"] = Counter(state.iterations(),
                                        benchmark::Counter::kIsRate);
}

BENCHMARK(BM_CRand)->ThreadRange(1,256)->UseRealTime();
BENCHMARK(BM_getRandom)->ThreadRange(1,256)->UseRealTime();

BENCHMARK_MAIN();
