// ----------------------------------------------------------------------
// File: BM_BulkNSLocking.cc
// Author: Cedric Caffy - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2025 CERN/Switzerland                           *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include "benchmark/benchmark.h"
#include "namespace/MDLocking.hh"
#include "namespace/locking/BulkNsObjectLocker.hh"
#include "namespace/ns_quarkdb/tests/NsTests.hh"
#include "namespace/ns_quarkdb/views/HierarchicalView.hh"

// Fixture for initializing services.
class BulkNSObjectLockFixture : public benchmark::Fixture {
public:
  BulkNSObjectLockFixture() {

  }
  void SetUp(const benchmark::State &) override {

  }
  void TearDown(const benchmark::State &) override {

  }
  ~BulkNSObjectLockFixture() {
  }
};

void simulateWork(size_t iterations) {
  volatile size_t dummy = 0;
  for (size_t i = 0; i < iterations; ++i) {
    dummy += i;
  }
}


std::unique_ptr<eos::ns::testing::NsTests> nsTests;
std::shared_ptr<eos::IContainerMD> container1;
std::shared_ptr<eos::IContainerMD> container2;
std::shared_ptr<eos::IFileMD> file;

BENCHMARK_DEFINE_F(BulkNSObjectLockFixture, ContainerMDLock)(benchmark::State& state)
{
  if (state.thread_index() == 0) {
    nsTests = std::make_unique<eos::ns::testing::NsTests>();
    container1 = nsTests->view()->createContainer("/test",true);
  }
  for (auto _ : state) {
    eos::MDLocking::ContainerWriteLock contLock(container1);
    container1->setAttribute("test1","test2");
    nsTests->view()->updateContainerStore(container1.get());
  }
  if (state.thread_index() == 0) {
    nsTests.reset();
  }
}


BENCHMARK_DEFINE_F(BulkNSObjectLockFixture, BulkNSObjectLocker)(benchmark::State& state)
{
  if (state.thread_index() == 0) {
    nsTests = std::make_unique<eos::ns::testing::NsTests>();
    container1 = nsTests->view()->createContainer("/test",true);
    container2 = nsTests->view()->createContainer("/test/test2",true);
    file = nsTests->view()->createFile("/test/test1");
  }
  for (auto _ : state) {
    eos::MDLocking::BulkMDWriteLock bulkLocker;
    bulkLocker.add(container1);
    bulkLocker.add(file);
    bulkLocker.add(container2);
    auto locks = bulkLocker.lockAll();
    // Simulate work while holding the locks.
    simulateWork(500000);
  }
  if (state.thread_index() == 0) {
    nsTests.reset();
  }
}

BENCHMARK_REGISTER_F(BulkNSObjectLockFixture, ContainerMDLock)->ThreadRange(1,5000)->UseRealTime();
BENCHMARK_REGISTER_F(BulkNSObjectLockFixture, BulkNSObjectLocker)->ThreadRange(1,5000)->UseRealTime();
BENCHMARK_MAIN();

