//------------------------------------------------------------------------------
// File: ContainerAccountingTest.cc
// Author: Cedric Caffy <cedric.caffy@cern.ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2026 CERN/Switzerland                                  *
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

#include "TestUtils.hh"
#include "namespace/MDLocking.hh"
#include "namespace/interface/IContainerMDSvc.hh"
#include "namespace/interface/IFileMDSvc.hh"
#include "namespace/interface/IView.hh"
#include "namespace/ns_quarkdb/accounting/ContainerAccounting.hh"
#include <atomic>
#include <gtest/gtest.h>
#include <thread>
#include <vector>

class ContainerAccountingF : public eos::ns::testing::NsTestsFixture {};

//------------------------------------------------------------------------------
// Flush() must synchronously apply all the tree info updates queued so far,
// without waiting for the periodic propagation interval
//------------------------------------------------------------------------------
TEST_F(ContainerAccountingF, FlushAppliesQueuedUpdatesImmediately)
{
  view()->createContainer("/test/d1", true);

  for (int i = 0; i < 10; ++i) {
    auto fmd = view()->createFile("/test/d1/f" + std::to_string(i));
    fmd->setSize(1000);
    view()->updateFileStore(fmd.get());
  }

  auto* accounting = namespaceGroupPtr->getQuarkContainerAccounting();
  ASSERT_NE(accounting, nullptr);
  // No sleep: the flush alone must make the tree values exact
  accounting->Flush();
  auto cont = view()->getContainer("/test/d1");
  ASSERT_EQ(10 * 1000, cont->getTreeSize());
  ASSERT_EQ(10, cont->getTreeFiles());
  ASSERT_EQ(0, cont->getTreeContainers());
  cont = view()->getContainer("/test");
  ASSERT_EQ(10 * 1000, cont->getTreeSize());
  ASSERT_EQ(10, cont->getTreeFiles());
  ASSERT_EQ(1, cont->getTreeContainers());
}

//------------------------------------------------------------------------------
// Flush() must also work when the asynchronous update threads are disabled
// (update_interval = 0): the queue is then drained by the calling thread
//------------------------------------------------------------------------------
TEST_F(ContainerAccountingF, FlushWithAsyncUpdatesDisabled)
{
  auto cont = view()->createContainer("/test/d1", true);
  const auto initialTreeSize = cont->getTreeSize();
  // Standalone accounting sharing the container service, not registered as
  // a file MD change listener, hence fed manually
  eos::QuarkContainerAccounting accounting(containerSvc(), 0);
  accounting.QueueForUpdate(cont->getId(), eos::TreeInfos{4444, 0, 0});
  accounting.Flush();
  ASSERT_EQ(initialTreeSize + 4444, view()->getContainer("/test/d1")->getTreeSize());
  ASSERT_EQ(initialTreeSize + 4444, view()->getContainer("/test")->getTreeSize());
}

//------------------------------------------------------------------------------
// Flush() concurrent with update queueing and with the periodic propagation
// thread: whenever the queueing stops, a last flush must leave the tree
// values exact
//------------------------------------------------------------------------------
TEST_F(ContainerAccountingF, FlushConcurrentWithUpdates)
{
  // Large enough initial size so that no interleaving of the +100/-100
  // updates below can bring the (unsigned) file size below zero
  constexpr uint64_t initialSize = 1000000;
  view()->createContainer("/test/d1", true);
  auto fmd = view()->createFile("/test/d1/f1");
  fmd->setSize(initialSize);
  view()->updateFileStore(fmd.get());
  auto* accounting = namespaceGroupPtr->getQuarkContainerAccounting();
  ASSERT_NE(accounting, nullptr);
  std::atomic<bool> done = false;
  std::vector<std::thread> workers;

  for (int i = 0; i < 4; ++i) {
    workers.emplace_back([this, i]() {
      auto fmd = view()->getFile("/test/d1/f1");

      for (int j = 0; j < 100; ++j) {
        eos::MDLocking::FileWriteLock fmdLock(fmd.get());
        // Even workers grow the file, odd workers shrink it back
        fmd->setSize(fmd->getSize() + ((i % 2 == 0) ? 100 : -100));
        view()->updateFileStore(fmd.get());
      }
    });
  }

  workers.emplace_back([accounting, &done]() {
    while (!done) {
      accounting->Flush();
    }
  });

  for (size_t i = 0; i < workers.size() - 1; ++i) {
    workers[i].join();
  }

  done = true;
  workers.back().join();
  // All updates are queued now, one flush makes the values exact
  accounting->Flush();
  ASSERT_EQ(initialSize, view()->getFile("/test/d1/f1")->getSize());
  auto cont = view()->getContainer("/test/d1");
  ASSERT_EQ(initialSize, cont->getTreeSize());
  ASSERT_EQ(1, cont->getTreeFiles());
}

//------------------------------------------------------------------------------
// Recording of updated container ids: nothing is recorded while disabled;
// while enabled, applying a delta records the origin container and all its
// ancestors (excluding the root, which the accounting never updates); taking
// the set clears it; stopping drops and disables the recording
//------------------------------------------------------------------------------
TEST_F(ContainerAccountingF, RecordingUpdatedContIdsWithAsyncUpdatesDisabled)
{
  auto d1 = view()->createContainer("/test/d1", true);
  const auto d1Id = d1->getId();
  const auto testId = view()->getContainer("/test")->getId();
  // Standalone accounting sharing the container service, fed manually
  eos::QuarkContainerAccounting accounting(containerSvc(), 0);
  // Not recording: updated ids are not collected
  accounting.QueueForUpdate(d1Id, eos::TreeInfos{100, 0, 0});
  accounting.Flush();
  ASSERT_TRUE(accounting.TakeUpdatedContIds().empty());
  // Recording: the origin container and its ancestors are collected
  accounting.StartRecordingUpdatedContIds();
  accounting.QueueForUpdate(d1Id, eos::TreeInfos{100, 0, 0});
  accounting.Flush();
  auto updated = accounting.TakeUpdatedContIds();
  ASSERT_EQ(2, updated.size());
  ASSERT_EQ(1, updated.count(d1Id));
  ASSERT_EQ(1, updated.count(testId));
  // Taking the set clears it
  ASSERT_TRUE(accounting.TakeUpdatedContIds().empty());
  // Stopping drops the set and disables the recording
  accounting.QueueForUpdate(d1Id, eos::TreeInfos{100, 0, 0});
  accounting.StopRecordingUpdatedContIds();
  accounting.Flush();
  ASSERT_TRUE(accounting.TakeUpdatedContIds().empty());
}

//------------------------------------------------------------------------------
// Starting a recording clears any residue from a previous one
//------------------------------------------------------------------------------
TEST_F(ContainerAccountingF, StartRecordingUpdatedContIdsClearsResidue)
{
  auto d1 = view()->createContainer("/test/d1", true);
  eos::QuarkContainerAccounting accounting(containerSvc(), 0);
  accounting.StartRecordingUpdatedContIds();
  accounting.QueueForUpdate(d1->getId(), eos::TreeInfos{100, 0, 0});
  accounting.Flush();
  // Not taken: a new recording must not inherit the previously recorded ids
  accounting.StartRecordingUpdatedContIds();
  ASSERT_TRUE(accounting.TakeUpdatedContIds().empty());
}

//------------------------------------------------------------------------------
// Recording with the asynchronous update threads running, through the RAII
// recording scope
//------------------------------------------------------------------------------
TEST_F(ContainerAccountingF, RecordingUpdatedContIdsWithLiveThreads)
{
  view()->createContainer("/test/d1", true);
  auto* accounting = namespaceGroupPtr->getQuarkContainerAccounting();
  ASSERT_NE(accounting, nullptr);
  const auto d1Id = view()->getContainer("/test/d1")->getId();
  const auto testId = view()->getContainer("/test")->getId();
  {
    eos::QuarkContainerAccounting::UpdatedContIdsRecordingScope recording(*accounting);
    auto fmd = view()->createFile("/test/d1/f1");
    fmd->setSize(1000);
    view()->updateFileStore(fmd.get());
    accounting->Flush();
    auto updated = accounting->TakeUpdatedContIds();
    ASSERT_EQ(1, updated.count(d1Id));
    ASSERT_EQ(1, updated.count(testId));
  }
  // Scope exited: activity is not recorded anymore
  auto fmd = view()->createFile("/test/d1/f2");
  fmd->setSize(1000);
  view()->updateFileStore(fmd.get());
  accounting->Flush();
  ASSERT_TRUE(accounting->TakeUpdatedContIds().empty());
}
