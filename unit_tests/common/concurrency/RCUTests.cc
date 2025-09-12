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

#include "common/concurrency/RCULite.hh"
#include "common/concurrency/AtomicUniquePtr.h"
#include "gtest/gtest.h"
#include <shared_mutex>

TEST(RCUTests, Basic)
{
  using namespace eos::common;
  // Test that we can create an RCU object
  RCUDomain<ThreadEpochCounter, 1> rcu_domain;
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
      rcu_domain.rcu_synchronize();
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
} // namespace eos::common
