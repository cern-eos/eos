#include <XrdOuc/XrdOucString.hh>
#include <string>
#include <cstring>
#include "benchmark/benchmark.h"

using benchmark::Counter;

static void BM_StringCreate(benchmark::State& state)
{
  std::string _s(state.range(0), 'a');
  const char* s = _s.c_str();

  for (auto _ : state) {
    benchmark::DoNotOptimize(std::string(s));
  }

  state.counters["frequency"] = Counter(state.iterations(),
                                        benchmark::Counter::kIsRate);
}

static void BM_XrdStringCreate(benchmark::State& state)
{
  std::string _s(state.range(0), 'a');
  const char* s = _s.c_str();

  for (auto _ : state) {
    benchmark::DoNotOptimize(XrdOucString(s));
  }

  state.counters["frequency"] = Counter(state.iterations(),
                                        benchmark::Counter::kIsRate);
}


static void BM_StringAppend(benchmark::State& state)
{
  std::string s("This is a line");

  for (auto _ : state) {
    for (auto i = 0; i < state.range(0); ++i) {
      benchmark::DoNotOptimize(s += "a");
    }
  }

  state.counters["frequency"] = Counter(state.iterations(),
                                        benchmark::Counter::kIsRate);
}

static void BM_XrdStringAppend(benchmark::State& state)
{
  XrdOucString s("This is a line");

  for (auto _ : state) {
    for (auto i = 0; i < state.range(0); ++i) {
      benchmark::DoNotOptimize(s += "a");
    }
  }

  state.counters["frequency"] = Counter(state.iterations(),
                                        benchmark::Counter::kIsRate);
}


BENCHMARK(BM_StringCreate)->RangeMultiplier(2)->Range(8, 1 << 9);
BENCHMARK(BM_XrdStringCreate)->RangeMultiplier(2)->Range(8, 1 << 9);
BENCHMARK(BM_StringAppend)->RangeMultiplier(2)->Range(8, 1 << 9);
BENCHMARK(BM_XrdStringAppend)->RangeMultiplier(2)->Range(8, 1 << 9);
