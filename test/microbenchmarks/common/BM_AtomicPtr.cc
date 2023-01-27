#include "common/concurrency/AtomicUniquePtr.h"
#include "common/concurrency/RCULite.hh"
#include "common/RWMutex.hh"
#include "benchmark/benchmark.h"
#include <memory>
#include <shared_mutex>
#include <mutex>

using benchmark::Counter;

static void BM_AtomicUniquePtrGet(benchmark::State& state)
{
  eos::common::atomic_unique_ptr<std::string> p(new std::string("foobar"));
  std::string *x;
  for (auto _ : state) {
    benchmark::DoNotOptimize(x = p.get());
    benchmark::ClobberMemory();
  }
  state.counters["frequency"] = Counter(state.iterations(),
                                        benchmark::Counter::kIsRate);
}

static void BM_UniquePtrGet(benchmark::State& state)
{
  std::unique_ptr<int> p(new int(1));
  int *x;
  for (auto _ : state) {
    benchmark::DoNotOptimize( x = p.get());
    benchmark::ClobberMemory();
  }
  state.counters["frequency"] = Counter(state.iterations(),
                                        benchmark::Counter::kIsRate);
}

static void BM_SharedPtrCopy(benchmark::State& state)
{
  std::shared_ptr<std::string> p(new std::string("foobar"));
  for (auto _ : state) {
    std::shared_ptr<std::string> p_copy=p;
    benchmark::ClobberMemory();
  }
  state.counters["frequency"] = Counter(state.iterations(),
                                        benchmark::Counter::kIsRate);
}

static void BM_AtomicSharedPtrGet(benchmark::State& state)
{
  std::shared_ptr<std::string> p(new std::string("foobar"));
  for (auto _ : state) {
    std::shared_ptr<std::string> p_copy = std::atomic_load_explicit(&p, std::memory_order_acquire);
    benchmark::ClobberMemory();
  }
  state.counters["frequency"] = Counter(state.iterations(),
                                        benchmark::Counter::kIsRate);
}

static void BM_MutexLock(benchmark::State& state)
{
  std::mutex m;
  std::unique_ptr<std::string> p(new std::string("foobar"));
  std::string *x;
  for (auto _ : state) {
    std::lock_guard<std::mutex> lock(m);
    benchmark::DoNotOptimize(x=p.get());
  }
  state.counters["frequency"] = Counter(state.iterations(),
                                        benchmark::Counter::kIsRate);
}

static void BM_SharedMutexLock(benchmark::State& state)
{
  std::shared_mutex m;
  std::unique_ptr<std::string> p(new std::string("foobar"));
  std::string *x;
  for (auto _ : state) {
    std::shared_lock<std::shared_mutex> lock(m);
    benchmark::DoNotOptimize(x=p.get());
  }
  state.counters["frequency"] = Counter(state.iterations(),
                                        benchmark::Counter::kIsRate);
}

static void BM_RCUVersionReadLock(benchmark::State& state)
{
  eos::common::VersionedRCUDomain rcu_domain;
  std::unique_ptr<std::string> p(new std::string("foobar"));
  std::string *x;
  for (auto _ : state) {
    eos::common::RCUReadLock rlock(rcu_domain);
    benchmark::DoNotOptimize(x=p.get());
  }
  state.counters["frequency"] = Counter(state.iterations(),
                                        benchmark::Counter::kIsRate);

}

static void BM_RCUEpochReadLock(benchmark::State& state)
{
  eos::common::EpochRCUDomain rcu_domain;
  std::unique_ptr<std::string> p(new std::string("foobar"));
  std::string *x;
  for (auto _ : state) {
    eos::common::RCUReadLock rlock(rcu_domain);
    benchmark::DoNotOptimize(x=p.get());
  }
  state.counters["frequency"] = Counter(state.iterations(),
                                        benchmark::Counter::kIsRate);

}



static void BM_EOSReadLock(benchmark::State& state)
{
  eos::common::RWMutex m;
  std::unique_ptr<std::string> p(new std::string("foobar"));
  std::string *x;
  for (auto _ : state) {
    eos::common::RWMutexReadLock lock(m);
    benchmark::DoNotOptimize(x=p.get());
  }
  state.counters["frequency"] = Counter(state.iterations(),
                                        benchmark::Counter::kIsRate);
}

static void BM_MutexRWLock(benchmark::State& state)
{
  std::mutex m;
  std::unique_ptr<std::string> p(new std::string("foobar"));
  std::string *x;
  auto writer_fn = [&p, &m] {
    for (int i = 0; i < 10000; i++) {
      std::lock_guard<std::mutex> lock(m);
      p.reset(new std::string("foobar2"));
    }
  };

  std::thread writer;
  if (state.thread_index() == 0) {
    writer = std::thread(writer_fn);
  }

  for (auto _ : state) {
    std::lock_guard<std::mutex> lock(m);
    benchmark::DoNotOptimize(x=p.get());
  }

  if (state.thread_index() == 0) {
    writer.join();
  }
  state.counters["frequency"] = Counter(state.iterations(),
                                        benchmark::Counter::kIsRate);
}

static void BM_SharedMutexRWLock(benchmark::State& state)
{
  std::shared_mutex m;
  std::unique_ptr<std::string> p(new std::string("foobar"));
  std::string *x;
  auto writer_fn = [&p, &m] {
    for (int i = 0; i < 10000; i++) {
      std::unique_lock<std::shared_mutex> lock(m);
      p.reset(new std::string("foobar2"));
    }
  };

  std::thread writer;
  if (state.thread_index() == 0) {
    writer = std::thread(writer_fn);
  }

  for (auto _ : state) {
    std::shared_lock<std::shared_mutex> lock(m);
    benchmark::DoNotOptimize(x=p.get());
  }

  if (state.thread_index() == 0) {
    writer.join();
  }

  state.counters["frequency"] = Counter(state.iterations(),
                                        benchmark::Counter::kIsRate);
}

static void BM_RCUVersionedReadWriteLock(benchmark::State& state)
{
  eos::common::VersionedRCUDomain rcu_domain;
  eos::common::atomic_unique_ptr<std::string> p(new std::string("foobar"));
  std::string *x;
  auto writer_fn = [&p, &rcu_domain] {
    for (int i=0; i < 10000; ++i) {
      rcu_domain.rcu_write_lock();
      auto x = p.reset(new std::string("foobar2"));
      rcu_domain.rcu_synchronize();
      delete x;

      eos::common::ScopedRCUWrite w(rcu_domain, p,
                                    new std::string("foobar" + std::to_string(i)));
    }
  };

  std::thread writer;
  if (state.thread_index()==0) {
    writer = std::thread(writer_fn);
  }

  for (auto _ : state) {
    eos::common::RCUReadLock rlock(rcu_domain);
    benchmark::DoNotOptimize(x=p.get());
  }

  if (state.thread_index()==0) {
    if (writer.joinable())
      writer.join();
  }

  state.counters["frequency"] = Counter(state.iterations(),
                                        benchmark::Counter::kIsRate);
}

static void BM_RCUEpochReadWriteLock(benchmark::State& state)
{
  eos::common::EpochRCUDomain rcu_domain;
  eos::common::atomic_unique_ptr<std::string> p(new std::string("foobar"));
  std::string *x;
  auto writer_fn = [&p, &rcu_domain] {
    for (int i=0; i < 10000; ++i) {
      rcu_domain.rcu_write_lock();
      auto x = p.reset(new std::string("foobar2"));
      rcu_domain.rcu_synchronize();
      delete x;

      eos::common::ScopedRCUWrite w(rcu_domain, p,
                                    new std::string("foobar" + std::to_string(i)));
    }
  };

  std::thread writer;
  if (state.thread_index() == 0) {
    writer = std::thread(writer_fn);
  }

  for (auto _ : state) {
    eos::common::RCUReadLock rlock(rcu_domain);
    benchmark::DoNotOptimize(x=p.get());
  }

  if (state.thread_index() == 0) {
    if (writer.joinable())
      writer.join();
  }

  state.counters["frequency"] = Counter(state.iterations(),
                                        benchmark::Counter::kIsRate);
}

static void BM_EOSReadWriteLock(benchmark::State& state)
{
  eos::common::RWMutex mtx;
  std::unique_ptr<std::string> p(new std::string("foobar"));
  std::string *x;
  auto writer_fn = [&p, &mtx] {
    for (int i = 0; i < 10000; i++) {
      eos::common::RWMutexWriteLock wlock(mtx);
      p.reset(new std::string("foobar2"));
    }
  };

  std::thread writer;
  if (state.thread_index() == 0) {
    writer = std::thread(writer_fn);
  }

  for (auto _ : state) {
    eos::common::RWMutexReadLock rlock(mtx);
    benchmark::DoNotOptimize(x=p.get());
  }

  if (state.thread_index() == 0) {
    writer.join();
  }

  state.counters["frequency"] = Counter(state.iterations(),
                                        benchmark::Counter::kIsRate);
}


BENCHMARK(BM_AtomicUniquePtrGet)->ThreadRange(1, 256)->UseRealTime();
BENCHMARK(BM_UniquePtrGet)->ThreadRange(1, 256)->UseRealTime();
BENCHMARK(BM_SharedPtrCopy)->ThreadRange(1, 256)->UseRealTime();
BENCHMARK(BM_AtomicSharedPtrGet)->ThreadRange(1, 256)->UseRealTime();
BENCHMARK(BM_MutexLock)->ThreadRange(1, 256)->UseRealTime();
BENCHMARK(BM_SharedMutexLock)->ThreadRange(1, 256)->UseRealTime();
BENCHMARK(BM_RCUVersionReadLock)->ThreadRange(1,256)->UseRealTime();
BENCHMARK(BM_RCUEpochReadLock)->ThreadRange(1,256)->UseRealTime();
BENCHMARK(BM_EOSReadLock)->ThreadRange(1, 256)->UseRealTime();

BENCHMARK(BM_MutexRWLock)->ThreadRange(1, 256)->UseRealTime();
BENCHMARK(BM_SharedMutexRWLock)->ThreadRange(1, 256)->UseRealTime();
BENCHMARK(BM_RCUVersionedReadWriteLock)->ThreadRange(1,256)->UseRealTime();
BENCHMARK(BM_RCUEpochReadWriteLock)->ThreadRange(1,256)->UseRealTime();
BENCHMARK(BM_EOSReadWriteLock)->ThreadRange(1, 256)->UseRealTime();
BENCHMARK_MAIN();
