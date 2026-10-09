// /************************************************************************
//  * EOS - the CERN Disk Storage System                                   *
//  * Copyright (C) 2023 CERN/Switzerland                           *
//  *                                                                      *
//  * This program is free software: you can redistribute it and/or modify *
//  * it under the terms of the GNU General Public License as published by *
//  * the Free Software Foundation, either version 3 of the License, or    *
//  * (at your option) any later version.                                  *
//  *                                                                      *
//  * This program is distributed in the hope that it will be useful,      *
//  * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
//  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
//  * GNU General Public License for more details.                         *
//  *                                                                      *
//  * You should have received a copy of the GNU General Public License    *
//  * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
//  ************************************************************************
//

#include "common/concurrency/AtomicUniquePtr.h"
#include "common/concurrency/RCULite.hh"
#include "gtest/gtest.h"
#include <functional>
#include <memory>
#include <shared_mutex>
#include <type_traits>
#include <vector>

namespace {

template <typename CounterT>
class RegistrationCounter : public eos::common::detail::stateful_trait_base<CounterT> {
public:
  inline static std::function<void(uint64_t)> beforeRegistration;
  inline static std::function<void(uint64_t, size_t)> beforeUnlock;

  size_t
  increment(uint64_t epoch) noexcept
  {
    if (beforeRegistration) {
      beforeRegistration(epoch);
    }

    return mCounter.increment(epoch);
  }

  void
  decrement(uint64_t epoch, size_t tag) noexcept
  {
    if (beforeUnlock) {
      beforeUnlock(epoch, tag);
    }

    mCounter.decrement(epoch, tag);
  }

  void
  decrement() noexcept
  {
    mCounter.decrement();
  }

  bool
  epochHasReaders(uint64_t epoch) noexcept
  {
    return mCounter.epochHasReaders(epoch);
  }

private:
  CounterT mCounter;
};

using ThreadRegistrationCounter = RegistrationCounter<eos::common::ThreadEpochCounter>;
using VersionRegistrationCounter =
    RegistrationCounter<eos::common::experimental::VersionEpochCounter<4>>;
using ThreadRegistrationDomain = eos::common::RCUDomain<ThreadRegistrationCounter>;
using VersionRegistrationDomain = eos::common::RCUDomain<VersionRegistrationCounter>;

class RCURegistrationTest : public ::testing::Test {
protected:
  void
  TearDown() override
  {
    ThreadRegistrationCounter::beforeRegistration = {};
    ThreadRegistrationCounter::beforeUnlock = {};
    VersionRegistrationCounter::beforeRegistration = {};
    VersionRegistrationCounter::beforeUnlock = {};
  }
};

using Mutex = eos::common::RCUMutexT<>;
using ReadGuard = eos::common::RCUReadLock<Mutex>;
using WriteGuard = eos::common::RCUWriteLock<eos::common::EpochRCUDomain>;
using ScopedGuard =
    eos::common::ScopedRCUWrite<Mutex, eos::common::atomic_unique_ptr<int>>;
static_assert(!std::is_copy_constructible_v<ReadGuard> &&
              !std::is_move_constructible_v<ReadGuard>);
static_assert(!std::is_copy_constructible_v<WriteGuard> &&
              !std::is_move_constructible_v<WriteGuard>);
static_assert(!std::is_copy_constructible_v<ScopedGuard> &&
              !std::is_move_constructible_v<ScopedGuard>);

TEST_F(RCURegistrationTest, MutexReaderRetriesAfterPublicationBeforeRegistration)
{
  using namespace eos::common;
  // Thread counters are large; keep the domain off the test thread's stack.
  auto mutex = std::make_unique<RCUMutexT<ThreadRegistrationDomain>>();
  atomic_unique_ptr<int> ptr(new int(0));
  std::vector<uint64_t> registrations;
  ThreadRegistrationCounter::beforeRegistration = [&](uint64_t epoch) {
    registrations.push_back(epoch);

    if (registrations.size() == 1) {
      int* retired;
      {
        std::unique_lock lock(*mutex);
        retired = ptr.reset(new int(1));
      }
      delete retired;
    }
  };
  {
    RCUReadLock guard(*mutex);
    EXPECT_EQ((std::vector<uint64_t>{0, 1}), registrations);
    EXPECT_EQ(1, *ptr.get());
  }
}

TEST_F(RCURegistrationTest, ExplicitLockReturnsAcceptedEpochAfterStaleCandidate)
{
  auto domain = std::make_unique<VersionRegistrationDomain>();
  domain->rcu_write_lock();
  domain->rcu_write_unlock();
  std::vector<uint64_t> registrations, unlocks;
  VersionRegistrationCounter::beforeRegistration = [&](uint64_t epoch) {
    registrations.push_back(epoch);
  };
  VersionRegistrationCounter::beforeUnlock = [&](uint64_t epoch, size_t) {
    unlocks.push_back(epoch);
  };
  uint64_t epoch = 0;
  auto tag = domain->rcu_read_lock(epoch);
  EXPECT_EQ(1u, epoch);
  EXPECT_EQ((std::vector<uint64_t>{0, 1}), registrations);
  domain->rcu_read_unlock(epoch, tag);
  EXPECT_EQ((std::vector<uint64_t>{0, 1}), unlocks);
}

TEST_F(RCURegistrationTest, StatefulGuardUnlocksAcceptedRegistration)
{
  using namespace eos::common;
  auto domain = std::make_unique<VersionRegistrationDomain>();
  atomic_unique_ptr<int> ptr(new int(0));
  std::vector<uint64_t> registrations, unlocks;
  VersionRegistrationCounter::beforeRegistration = [&](uint64_t epoch) {
    registrations.push_back(epoch);

    if (registrations.size() == 1) {
      int* retired;
      {
        RCUWriteLock guard(*domain);
        retired = ptr.reset(new int(1));
      }
      delete retired;
    }
  };
  VersionRegistrationCounter::beforeUnlock = [&](uint64_t epoch, size_t) {
    unlocks.push_back(epoch);
  };
  {
    RCUReadLock guard(*domain);
    EXPECT_EQ((std::vector<uint64_t>{0, 1}), registrations);
    EXPECT_EQ(1, *ptr.get());
  }
  EXPECT_EQ((std::vector<uint64_t>{0, 1}), unlocks);
}

} // namespace

TEST(RCUTests, Basic)
{
  using namespace eos::common;
  // Test that we can create an RCU object
  RCUDomain<ThreadEpochCounter> rcu_domain;
  atomic_unique_ptr<int> ptr(new int(0));
  int i{0};
  // Test that we can create an RCU read lock
  auto read_fn = [&rcu_domain, &ptr](int index) {
    auto tid = tlocalID.get();
    std::cout << "Starting reader at index=" << index << "tid=" << tid
              <<  std::endl;

    for (int j = 0; j < 100; ++j) {
      RCUReadLock rlock(rcu_domain);
      ASSERT_TRUE(ptr);
    }

    std::cout << "Done with reader at index= " << index << " tid=" << tid <<
              std::endl;
  };
  std::thread writer([&rcu_domain, &ptr, &i]() {
    std::cout << "Starting writer";

    for (int j = 0; j < 5000; ++j) {
      int* old_ptr(nullptr);
      {
        RCUWriteLock wlock(rcu_domain);
        old_ptr = ptr.reset(new int(i++));
      }
      std::cout << ".";
      delete old_ptr;
      std::this_thread::sleep_for(std::chrono::nanoseconds(1));
    }
  });
  std::vector<std::thread> readers;

  for (int k = 0; k < 100; ++k) {
    readers.emplace_back(read_fn, k);
  }

  for (int i = 0; i < 100; ++i) {
    readers[i].join();
  }

  writer.join();
}

TEST(RCUTests, BasicVersionCounter)
{
  using namespace eos::common;
  // Test that we can create an RCU object
  VersionedRCUDomain rcu_domain;
  atomic_unique_ptr<int> ptr(new int(0));
  int i{0};
  // Test that we can create an RCU read lock
  auto read_fn = [&rcu_domain, &ptr](int index) {
    auto tid = std::hash<std::thread::id> {}(std::this_thread::get_id()) % 4096;
    std::cout << "Starting reader at index=" << index << "tid=" << tid
              <<  std::endl;

    for (int j = 0; j < 100; ++j) {
      RCUReadLock rlock(rcu_domain);
      ASSERT_TRUE(ptr);
    }

    std::cout << "Done with reader at index= " << index << " tid=" << tid <<
              std::endl;
  };
  std::thread writer([&rcu_domain, &ptr, &i]() {
    std::cout << "Starting writer";

    for (int j = 0; j < 5000; ++j) {
      rcu_domain.rcu_write_lock();
      auto old_ptr = ptr.reset(new int(i++));
      rcu_domain.rcu_write_unlock();
      std::cout << ".";
      delete old_ptr;
      std::this_thread::sleep_for(std::chrono::nanoseconds(1));
    }
  });
  std::vector<std::thread> readers;

  for (int k = 0; k < 100; ++k) {
    readers.emplace_back(read_fn, k);
  }

  for (int i = 0; i < 100; ++i) {
    readers[i].join();
  }
  std::cout << "Joining writer" << std::endl;
  writer.join();
} // namespace eos::common
