#include <thread>
#include <sys/syscall.h>
#include <unistd.h>
#include "common/concurrency/ThreadEpochCounter.hh"
#include "benchmark/benchmark.h"

using benchmark::Counter;

static void BM_ThreadId(benchmark::State& state)
{
  for (auto _: state) {
    benchmark::DoNotOptimize(std::this_thread::get_id());
  }
  state.counters["frequency"]=Counter(state.iterations(),
                                      benchmark::Counter::kIsRate);
}

static void BM_SysTID(benchmark::State& state)
{
  for (auto _: state) {
    benchmark::DoNotOptimize(syscall(SYS_gettid));
  }
  state.counters["frequency"]=Counter(state.iterations(),
                                      benchmark::Counter::kIsRate);

}

static void BM_tlTID(benchmark::State& state)
{
  for (auto _: state) {
    benchmark::DoNotOptimize(eos::common::experimental::tlocalID.get());
  }
  state.counters["freqeuncy"]=Counter(state.iterations(),
                                      benchmark::Counter::kIsRate);
}
BENCHMARK(BM_ThreadId)->ThreadRange(1,4096)->UseRealTime();
BENCHMARK(BM_SysTID)->ThreadRange(1,4096)->UseRealTime();
BENCHMARK(BM_tlTID)->ThreadRange(1,4096)->UseRealTime();
BENCHMARK_MAIN();
