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

// Adapted from Abseil's Mutex benchmarks, which is under an Apache License
// While the benchmarks above benchmarked the pure cost of a lock/unlock operation
// unless the critical section is exceedingly small, this won't benchmark the
// contention caused which usually happens when you have multiple threads trying to
// get a lock and when the activity inside the lock is more or less realistic.
static void DelayNs(int64_t _ns, int* data) {
  using namespace std::chrono_literals;
  auto end = std::chrono::system_clock::now() + std::chrono::nanoseconds(_ns);
  int l;
  while (std::chrono::system_clock::now() < end) {
    l = (*data)+1;
    (void) l; // make compiler happy
    benchmark::DoNotOptimize(*data);
  }
}

template <typename MutexType>
class RaiiLocker {
public:
  explicit RaiiLocker(MutexType* mu) : mu_(mu) { mu_->lock(); }
  ~RaiiLocker() { mu_->unlock(); }
private:
  MutexType* mu_;
};

template <>
class RaiiLocker<std::shared_mutex> {
public:
  explicit RaiiLocker(std::shared_mutex* mu) : mu_(mu) { mu_->lock_shared(); }
  ~RaiiLocker() { mu_->unlock_shared(); }
private:
  std::shared_mutex* mu_;
};

template <>
class RaiiLocker<eos::common::EpochRCUDomain>
{
public:
  explicit RaiiLocker(eos::common::EpochRCUDomain* domain): domain_(domain) {
    epoch = domain_->get_current_epoch();
    tag = domain_->rcu_read_lock(epoch);
  }

  ~RaiiLocker() {
    domain_->rcu_read_unlock(epoch, tag);
  }
 private:
  eos::common::EpochRCUDomain* domain_;
  uint64_t epoch;
  uint64_t tag;
};

template <>
class RaiiLocker<eos::common::RWMutex>
{
public:
  explicit RaiiLocker(eos::common::RWMutex* mutex): mutex_(mutex) {
    mutex_->LockRead();
  }

  ~RaiiLocker() {
    mutex_->UnLockRead();
  }
private:
  eos::common::RWMutex* mutex_;
};

template <typename MutexType>
void BM_Contended(benchmark::State& state) {

  struct Shared {
    MutexType mu;
    int data = 0;
  };
  static auto* shared = new Shared;
  int local = 0;
  for (auto _ : state) {
    // Here we model both local work outside of the critical section as well as
    // some work inside of the critical section. The idea is to capture some
    // more or less realisitic contention levels.
    // If contention is too low, the benchmark won't measure anything useful.
    // If contention is unrealistically high, the benchmark will favor
    // bad mutex implementations that block and otherwise distract threads
    // from the mutex and shared state for as much as possible.
    // To achieve this amount of local work is multiplied by number of threads
    // to keep ratio between local work and critical section approximately
    // equal regardless of number of threads.
    DelayNs(100 * state.threads(), &local);
    RaiiLocker<MutexType> locker(&shared->mu);
    DelayNs(state.range(0), &shared->data);
  }
  state.counters["frequency"] = Counter(state.iterations(),
                                        benchmark::Counter::kIsRate);

}

void SetupBenchmarkArgs(benchmark::internal::Benchmark* bm) {
  bm->UseRealTime()
      // ThreadPerCpu poorly handles non-power-of-two CPU counts.
      ->Threads(1)
      ->Threads(2)
      ->Threads(4)
      ->Threads(6)
      ->Threads(8)
      ->Threads(12)
      ->Threads(16)
      ->Threads(24)
      ->Threads(32)
      ->Threads(48)
      ->Threads(64)
      ->Threads(96)
      ->Threads(128)
      ->Threads(192)
      ->Threads(256)
    ->ArgNames({"cs_ns"});
  // Some empirically chosen amounts of work in critical section.
  // 1 is low contention, 2000 is high contention and few values in between.
  for (int critical_section_ns : {1, 20, 50, 200, 2000}) {
      bm->Arg(critical_section_ns);
  }
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

BENCHMARK_TEMPLATE(BM_Contended, std::mutex)
->Apply([](benchmark::internal::Benchmark* bm) {
  SetupBenchmarkArgs(bm);
 });
BENCHMARK_TEMPLATE(BM_Contended, std::shared_mutex)
->Apply([](benchmark::internal::Benchmark* bm) {
  SetupBenchmarkArgs(bm);
 });
BENCHMARK_TEMPLATE(BM_Contended, eos::common::RWMutex)
->Apply([](benchmark::internal::Benchmark* bm) {
  SetupBenchmarkArgs(bm);
 });
BENCHMARK_TEMPLATE(BM_Contended, eos::common::EpochRCUDomain)
->Apply([](benchmark::internal::Benchmark* bm) {
  SetupBenchmarkArgs(bm);
 });

BENCHMARK_MAIN();
