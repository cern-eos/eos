//------------------------------------------------------------------------------
// File: TreeSizeAccountingFenceTest.cc
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

#include "common/AssistedThread.hh"
#include "common/ConcurrentQueue.hh"
#include "namespace/interface/IContainerMDSvc.hh"
#include "namespace/interface/IView.hh"
#include "namespace/ns_quarkdb/accounting/tree_size/TreeSizeAccountingFence.hh"
#include "namespace/ns_quarkdb/accounting/tree_size/TreeSizeJournal.hh"
#include "namespace/ns_quarkdb/accounting/tree_size/TreeSizeJournalCaptureScope.hh"
#include "namespace/ns_quarkdb/tests/TestUtils.hh"
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <gtest/gtest.h>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

// Hack to expose the pending-batch internals needed to seed a deterministic
// in-flight propagation window.
#define private public
#include "namespace/ns_quarkdb/accounting/ContainerAccounting.hh"
#undef private

namespace {

class TreeSizeAccountingFenceF : public eos::ns::testing::NsTestsFixture {};

class BlockingContainerMDSvc : public eos::IContainerMDSvc {
public:
  BlockingContainerMDSvc(eos::IContainerMDSvc* backend,
                         eos::IContainerMD::id_t blocked_id)
      : mBackend(backend)
      , mBlockedId(blocked_id)
  {
  }

  bool
  WaitUntilBlocked(std::chrono::milliseconds timeout)
  {
    std::unique_lock<std::mutex> lock(mMutex);
    return mCv.wait_for(lock, timeout, [&]() { return mBlocked; });
  }

  void
  Release()
  {
    {
      std::lock_guard<std::mutex> lock(mMutex);
      mReleased = true;
    }

    mCv.notify_all();
  }

  void
  initialize() override
  {
    mBackend->initialize();
  }

  void
  configure(const std::map<std::string, std::string>& config) override
  {
    mBackend->configure(config);
  }

  void
  finalize() override
  {
    mBackend->finalize();
  }

  folly::Future<eos::IContainerMDPtr>
  getContainerMDFut(eos::IContainerMD::id_t id) override
  {
    return mBackend->getContainerMDFut(id);
  }

  std::shared_ptr<eos::IContainerMD>
  getContainerMD(eos::IContainerMD::id_t id) override
  {
    WaitIfBlocked(id);
    return mBackend->getContainerMD(id);
  }

  std::shared_ptr<eos::IContainerMD>
  getContainerMD(eos::IContainerMD::id_t id, uint64_t* clock) override
  {
    WaitIfBlocked(id);
    return mBackend->getContainerMD(id, clock);
  }

  bool
  dropCachedContainerMD(eos::ContainerIdentifier id) override
  {
    return mBackend->dropCachedContainerMD(id);
  }

  std::shared_ptr<eos::IContainerMD>
  createContainer(eos::IContainerMD::id_t id) override
  {
    return mBackend->createContainer(id);
  }

  void
  updateStore(eos::IContainerMD* obj) override
  {
    mBackend->updateStore(obj);
  }

  void
  removeContainer(eos::IContainerMD* obj) override
  {
    mBackend->removeContainer(obj);
  }

  uint64_t
  getNumContainers() override
  {
    return mBackend->getNumContainers();
  }

  void
  addChangeListener(eos::IContainerMDChangeListener* listener) override
  {
    mBackend->addChangeListener(listener);
  }

  void
  setQuotaStats(eos::IQuotaStats* quota_stats) override
  {
    mBackend->setQuotaStats(quota_stats);
  }

  void
  notifyListeners(eos::IContainerMD* obj,
                  eos::IContainerMDChangeListener::Action a) override
  {
    mBackend->notifyListeners(obj, a);
  }

  std::shared_ptr<eos::IContainerMD>
  getLostFoundContainer(const std::string& name) override
  {
    return mBackend->getLostFoundContainer(name);
  }

  std::shared_ptr<eos::IContainerMD>
  createInParent(const std::string& name, eos::IContainerMD* parent) override
  {
    return mBackend->createInParent(name, parent);
  }

  void
  setFileMDService(eos::IFileMDSvc* file_svc) override
  {
    mBackend->setFileMDService(file_svc);
  }

  void
  setContainerAccounting(eos::IFileMDChangeListener* containerAccounting) override
  {
    mBackend->setContainerAccounting(containerAccounting);
  }

  eos::IContainerMD::id_t
  getFirstFreeId() override
  {
    return mBackend->getFirstFreeId();
  }

  eos::CacheStatistics
  getCacheStatistics() override
  {
    return mBackend->getCacheStatistics();
  }

  void
  blacklistBelow(eos::ContainerIdentifier id) override
  {
    mBackend->blacklistBelow(id);
  }

private:
  void
  WaitIfBlocked(eos::IContainerMD::id_t id)
  {
    if (id != mBlockedId) {
      return;
    }

    std::unique_lock<std::mutex> lock(mMutex);
    mBlocked = true;
    mCv.notify_all();
    mCv.wait(lock, [&]() { return mReleased; });
  }

  eos::IContainerMDSvc* mBackend;
  eos::IContainerMD::id_t mBlockedId;
  std::mutex mMutex;
  std::condition_variable mCv;
  bool mBlocked = false;
  bool mReleased = false;
};

eos::TreeSizeAccountingFenceRequest
MakeFenceRequest(eos::IContainerMD::id_t covered_id, uint64_t validated_through_sequence)
{
  eos::TreeSizeAccountingFenceRequest request;
  request.coveredContainerIds.insert(covered_id);
  request.validatedThroughSequence = validated_through_sequence;
  return request;
}

eos::QuarkContainerAccounting::TreeSizeAccountingUpdate
MakeSequencedUpdate(eos::IContainerMD::id_t target_id, uint64_t sequence)
{
  eos::QuarkContainerAccounting::TreeSizeAccountingUpdate update;
  update.id = target_id;
  update.treeInfos = eos::TreeInfos{10, 1, 0};
  update.hasAccountingMetadata = true;
  update.accountingEvent = {sequence, eos::TreeSizeAccountingEventType::FileDelta,
                            target_id, 99};
  return update;
}

void
SeedPendingUpdate(eos::QuarkContainerAccounting& accounting,
                  const eos::QuarkContainerAccounting::TreeSizeAccountingUpdate& update)
{
  accounting.mBatch[accounting.mAccumulateIndx].mUpdates.push_back(update);
}

} // namespace

TEST_F(TreeSizeAccountingFenceF, WaitsForCoveredInFlightPropagation)
{
  auto child = view()->createContainer("/fence/child", true);
  const auto child_id = child->getId();
  BlockingContainerMDSvc blocking_svc(containerSvc(), child_id);
  eos::QuarkContainerAccounting accounting(&blocking_svc, 0);

  SeedPendingUpdate(accounting, MakeSequencedUpdate(child_id, 5));

  std::thread propagation([&]() { accounting.PropagateUpdates(nullptr); });

  if (!blocking_svc.WaitUntilBlocked(std::chrono::seconds(2))) {
    blocking_svc.Release();
    propagation.join();
    FAIL() << "propagation did not reach the blocking metadata lookup";
  }

  std::promise<void> acquire_started;
  auto acquire_started_future = acquire_started.get_future();
  auto acquire_future = std::async(std::launch::async, [&]() {
    acquire_started.set_value();
    return accounting.AcquireTreeSizeAccountingFence(MakeFenceRequest(child_id, 10));
  });

  ASSERT_EQ(std::future_status::ready,
            acquire_started_future.wait_for(std::chrono::seconds(2)));
  EXPECT_EQ(std::future_status::timeout,
            acquire_future.wait_for(std::chrono::milliseconds(100)));

  blocking_svc.Release();
  propagation.join();

  const auto stats = acquire_future.get();
  EXPECT_TRUE(stats.acquired);
  EXPECT_EQ(1ull, stats.includedInPublishUpdates);
  EXPECT_EQ(0ull, stats.replayAfterPublishUpdates);
  EXPECT_FALSE(stats.inFlightWaitTimeout);

  accounting.ReleaseTreeSizeAccountingFence(
      eos::TreeSizeAccountingFenceReleaseMode::AbortBeforePublish);
  EXPECT_TRUE(accounting.mBatch[accounting.mAccumulateIndx].mUpdates.empty());
}

TEST_F(TreeSizeAccountingFenceF, ReplaysInFlightTailAfterPublishSucceeded)
{
  auto child = view()->createContainer("/fence_tail/child", true);
  const auto child_id = child->getId();
  BlockingContainerMDSvc blocking_svc(containerSvc(), child_id);
  eos::QuarkContainerAccounting accounting(&blocking_svc, 0);

  SeedPendingUpdate(accounting, MakeSequencedUpdate(child_id, 12));

  std::thread propagation([&]() { accounting.PropagateUpdates(nullptr); });

  if (!blocking_svc.WaitUntilBlocked(std::chrono::seconds(2))) {
    blocking_svc.Release();
    propagation.join();
    FAIL() << "propagation did not reach the blocking metadata lookup";
  }

  std::promise<void> acquire_started;
  auto acquire_started_future = acquire_started.get_future();
  auto acquire_future = std::async(std::launch::async, [&]() {
    acquire_started.set_value();
    return accounting.AcquireTreeSizeAccountingFence(MakeFenceRequest(child_id, 10));
  });

  ASSERT_EQ(std::future_status::ready,
            acquire_started_future.wait_for(std::chrono::seconds(2)));
  EXPECT_EQ(std::future_status::timeout,
            acquire_future.wait_for(std::chrono::milliseconds(100)));

  blocking_svc.Release();
  propagation.join();

  const auto stats = acquire_future.get();
  EXPECT_TRUE(stats.acquired);
  EXPECT_EQ(0ull, stats.includedInPublishUpdates);
  EXPECT_EQ(1ull, stats.replayAfterPublishUpdates);
  EXPECT_FALSE(stats.inFlightWaitTimeout);

  accounting.ReleaseTreeSizeAccountingFence(
      eos::TreeSizeAccountingFenceReleaseMode::PublishSucceeded);
  ASSERT_EQ(1ull, accounting.mBatch[accounting.mAccumulateIndx].mUpdates.size());
  EXPECT_EQ(child_id, accounting.mBatch[accounting.mAccumulateIndx].mUpdates[0].id);
}

TEST_F(TreeSizeAccountingFenceF, SubtreePropagationIsSequencedAndJournaled)
{
  auto parent = view()->createContainer("/subtree_fence/parent", true);
  auto moved = view()->createContainer("/subtree_fence/moved", true);
  eos::QuarkContainerAccounting accounting(containerSvc(), 0);
  auto capture_scope = accounting.StartTreeSizeJournalCapture();

  accounting.AddTree(parent.get(), moved->getId(), eos::TreeInfos{100, 2, 3});
  accounting.RemoveTree(parent.get(), moved->getId(), eos::TreeInfos{100, 2, 3});

  const auto snapshot = capture_scope->StopAndSnapshot();
  ASSERT_EQ(2ull, snapshot.entries.size());
  ASSERT_TRUE(snapshot.entries[0].hasAccountingMetadata);
  ASSERT_TRUE(snapshot.entries[1].hasAccountingMetadata);
  EXPECT_EQ(eos::TreeSizeAccountingEventType::SubtreeAttach,
            snapshot.entries[0].accountingEvent.type);
  EXPECT_EQ(eos::TreeSizeAccountingEventType::SubtreeDetach,
            snapshot.entries[1].accountingEvent.type);
  EXPECT_EQ(parent->getId(), snapshot.entries[0].accountingEvent.directParentId);
  EXPECT_EQ(parent->getId(), snapshot.entries[1].accountingEvent.directParentId);
  EXPECT_EQ(moved->getId(), snapshot.entries[0].accountingEvent.objectId);
  EXPECT_EQ(moved->getId(), snapshot.entries[1].accountingEvent.objectId);
  EXPECT_LT(snapshot.entries[0].accountingEvent.sequence,
            snapshot.entries[1].accountingEvent.sequence);
  EXPECT_EQ(100, snapshot.entries[0].treeChange.dsize);
  EXPECT_EQ(-100, snapshot.entries[1].treeChange.dsize);

  const auto stats = accounting.AcquireTreeSizeAccountingFence(
      MakeFenceRequest(parent->getId(), snapshot.latestSequence));

  EXPECT_TRUE(stats.acquired);
  EXPECT_EQ(2ull, stats.includedInPublishUpdates);
  EXPECT_EQ(1ull, stats.includedSubtreeAttachUpdates);
  EXPECT_EQ(1ull, stats.includedSubtreeDetachUpdates);
  EXPECT_EQ(0ull, stats.replayAfterPublishUpdates);
  EXPECT_EQ(0ull, stats.unsequencedCoveredUpdates);

  accounting.ReleaseTreeSizeAccountingFence(
      eos::TreeSizeAccountingFenceReleaseMode::AbortBeforePublish);
}
