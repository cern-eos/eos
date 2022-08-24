#include "benchmark/benchmark.h"
#include "common/StringUtils.hh"

using benchmark::Counter;

static void BM_StringToNumeric(benchmark::State& state)
{
  int val;
  std::string s = std::to_string(state.range(0));
  using namespace eos::common;
  // Since the call is very small, do a few cycles to avoid jitters
  for (auto _: state) {
    for (int i=0; i<100; ++i) {
      benchmark::DoNotOptimize(StringToNumeric(s, val));
    }
  }
  state.counters["frequency"] = Counter(100*state.iterations(),
                                        benchmark::Counter::kIsRate);
}


static void BM_atoi(benchmark::State& state)
{
  int val;
  std::string s = std::to_string(state.range(0));
  for (auto _: state) {
    for (int i=0; i<100; ++i) {
      benchmark::DoNotOptimize(val = atoi(s.c_str()));
    }
  }
  state.counters["frequency"] = Counter(100*state.iterations(),
                                        benchmark::Counter::kIsRate);

}

int64_t start = 8;
int64_t end = 1UL<<24;
BENCHMARK(BM_StringToNumeric)->Range(start,end);
BENCHMARK(BM_atoi)->Range(start,end);
