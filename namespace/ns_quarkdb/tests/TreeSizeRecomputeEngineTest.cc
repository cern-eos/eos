//------------------------------------------------------------------------------
//! @file TreeSizeRecomputeEngineTest.cc
//! @brief QuarkDB-backed tests for the tree-size recompute workflow
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

#include "namespace/interface/IContainerMD.hh"
#include "namespace/interface/IContainerMDSvc.hh"
#include "namespace/interface/IFileMD.hh"
#include "namespace/interface/IFileMDSvc.hh"
#include "namespace/interface/IView.hh"
#include "namespace/ns_quarkdb/accounting/ContainerAccounting.hh"
#include "namespace/ns_quarkdb/accounting/tree_size/TreeSizeAccountingSequencer.hh"
#include "namespace/ns_quarkdb/accounting/tree_size/TreeSizeRecomputeEngine.hh"
#include "namespace/ns_quarkdb/accounting/tree_size/TreeSizeRecomputePublishDecision.hh"
#include "namespace/ns_quarkdb/tests/TestUtils.hh"
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <future>
#include <gtest/gtest.h>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

class TreeSizeRecomputeEngineF : public eos::ns::testing::NsTestsFixture {};

class ScriptedTreeSizeAccountingService : public eos::ITreeSizeAccountingService {
public:
  std::unique_ptr<eos::TreeSizeJournalCaptureScope>
  StartTreeSizeJournalCapture() override
  {
    return mCaptureController.StartCapture();
  }

  eos::TreeSizeAccountingFenceStats
  AcquireTreeSizeAccountingFence(
      const eos::TreeSizeAccountingFenceRequest& request) override
  {
    ++fenceAcquireCount;
    eos::TreeSizeAccountingFenceStats stats;
    stats.acquired = acquireFence;
    stats.coveredContainerIds = request.coveredContainerIds.size();
    return stats;
  }

  eos::TreeSizeAccountingFenceStats
  ReleaseTreeSizeAccountingFence(eos::TreeSizeAccountingFenceReleaseMode) override
  {
    ++fenceReleaseCount;
    eos::TreeSizeAccountingFenceStats stats;
    stats.acquired = true;
    return stats;
  }

  void
  Capture(const eos::TreeSizeJournalEntry& entry)
  {
    mCaptureController.Capture(entry);
  }

  uint64_t fenceAcquireCount = 0;
  uint64_t fenceReleaseCount = 0;
  bool acquireFence = true;

private:
  eos::TreeSizeJournalCaptureController mCaptureController;
};

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

class FailOnPublishContainerMDSvc : public BlockingContainerMDSvc {
public:
  FailOnPublishContainerMDSvc(eos::IContainerMDSvc* backend,
                              eos::IContainerMD::id_t failed_id)
      : BlockingContainerMDSvc(backend, 0)
      , mFailedId(failed_id)
  {
  }

  void
  Enable()
  {
    mEnabled = true;
  }

  bool
  Failed() const
  {
    return mFailed;
  }

  std::shared_ptr<eos::IContainerMD>
  getContainerMD(eos::IContainerMD::id_t id) override
  {
    ThrowIfNeeded(id);
    return BlockingContainerMDSvc::getContainerMD(id);
  }

  std::shared_ptr<eos::IContainerMD>
  getContainerMD(eos::IContainerMD::id_t id, uint64_t* clock) override
  {
    ThrowIfNeeded(id);
    return BlockingContainerMDSvc::getContainerMD(id, clock);
  }

private:
  void
  ThrowIfNeeded(eos::IContainerMD::id_t id)
  {
    if ((id != mFailedId) || !mEnabled || mFailed) {
      return;
    }

    mFailed = true;
    throw std::runtime_error("injected publish failure");
  }

  eos::IContainerMD::id_t mFailedId = 0;
  bool mEnabled = false;
  bool mFailed = false;
};

struct TreeSizeRecomputeProgress {
  uint64_t discoveredContainers = 0;
  uint64_t snapshotContainers = 0;
  uint64_t snapshotFiles = 0;
  uint64_t publishTargets = 0;
  uint64_t publishAppliedTargets = 0;
};

eos::IFileMDPtr
CreateSizedFile(eos::IView* view, const std::string& path, uint64_t size)
{
  auto file = view->createFile(path);
  file->setSize(size);
  view->updateFileStore(file.get());
  return file;
}

eos::TreeSizeRecomputeResult
RunRecompute(eos::IContainerMDSvc& container_svc, eos::IFileMDSvc& file_svc,
             eos::ITreeSizeAccountingService& accounting_service,
             const eos::TreeSizeRecomputeRequest& request,
             eos::TreeSizeRecomputeDiagnostics& last_diagnostics,
             TreeSizeRecomputeProgress* last_progress = nullptr,
             const std::function<void(const std::string&)>& phase_callback = {},
             const std::function<bool()>& stop_callback = {})
{
  eos::TreeSizeRecomputeEngineCallbacks callbacks;
  callbacks.stopRequested = [&stop_callback]() {
    return stop_callback && stop_callback();
  };
  callbacks.updatePhase = [&phase_callback](const std::string& phase) {
    if (phase_callback) {
      phase_callback(phase);
    }
  };
  callbacks.updateDiagnostics =
      [&last_diagnostics](const eos::TreeSizeRecomputeDiagnostics& diagnostics) {
        last_diagnostics = diagnostics;
      };
  callbacks.updateProgress =
      [last_progress](uint64_t discovered_containers, uint64_t snapshot_containers,
                      uint64_t snapshot_files, uint64_t publish_targets,
                      uint64_t publish_applied_targets) {
        if (!last_progress) {
          return;
        }

        last_progress->discoveredContainers = discovered_containers;
        last_progress->snapshotContainers = snapshot_containers;
        last_progress->snapshotFiles = snapshot_files;
        last_progress->publishTargets = publish_targets;
        last_progress->publishAppliedTargets = publish_applied_targets;
      };

  return eos::TreeSizeRecomputeEngine().Recompute(request, container_svc, file_svc,
                                                  &accounting_service, callbacks);
}

void
ExpectTreeCounters(const eos::IContainerMDPtr& container, uint64_t tree_size,
                   uint64_t tree_files, uint64_t tree_containers)
{
  EXPECT_EQ(tree_size, container->getTreeSize());
  EXPECT_EQ(tree_files, container->getTreeFiles());
  EXPECT_EQ(tree_containers, container->getTreeContainers());
}

eos::TreeSizeJournalEntry
MakeSequencedJournalEntry(eos::TreeSizeAccountingEventType type, uint64_t parent_id,
                          uint64_t object_id, eos::TreeInfos tree_change)
{
  eos::TreeSizeJournalEntry entry;
  entry.accountingEvent = eos::ReserveTreeSizeAccountingEvent(type, parent_id, object_id);
  entry.treeChange = tree_change;
  return entry;
}

} // namespace

TEST_F(TreeSizeRecomputeEngineF, PublishesAbsoluteCountersInQuarkDbNamespace)
{
  auto root = view()->createContainer("/recompute_engine/root", true);
  auto child_a = view()->createContainer("/recompute_engine/root/a", true);
  auto child_b = view()->createContainer("/recompute_engine/root/b", true);
  auto nested = view()->createContainer("/recompute_engine/root/a/nested", true);
  CreateSizedFile(view(), "/recompute_engine/root/a/file-a", 10);
  CreateSizedFile(view(), "/recompute_engine/root/a/nested/file-nested", 20);
  CreateSizedFile(view(), "/recompute_engine/root/b/file-b", 30);

  root->setTreeSize(9999);
  root->setTreeFiles(9999);
  root->setTreeContainers(9999);
  view()->updateContainerStore(root.get());

  eos::QuarkContainerAccounting accounting(containerSvc(), 0);
  eos::TreeSizeRecomputeDiagnostics diagnostics;
  TreeSizeRecomputeProgress progress;
  const auto result = RunRecompute(
      *containerSvc(), *fileSvc(), accounting,
      eos::TreeSizeRecomputeRequest{root->getId(), "/recompute_engine/root/", 0},
      diagnostics, &progress);

  ASSERT_EQ(0, result.retc) << result.error;
  ExpectTreeCounters(containerSvc()->getContainerMD(root->getId()), 60, 3, 3);
  ExpectTreeCounters(containerSvc()->getContainerMD(child_a->getId()), 30, 2, 1);
  ExpectTreeCounters(containerSvc()->getContainerMD(child_b->getId()), 30, 1, 0);
  ExpectTreeCounters(containerSvc()->getContainerMD(nested->getId()), 20, 1, 0);

  EXPECT_TRUE(diagnostics.available);
  EXPECT_TRUE(diagnostics.converged);
  EXPECT_TRUE(diagnostics.publishable);
  EXPECT_FALSE(diagnostics.retryRequired);
  EXPECT_TRUE(diagnostics.retryReason.empty());
  EXPECT_EQ(4ull, diagnostics.discoveredContainers);
  EXPECT_EQ(4ull, diagnostics.snapshotContainers);
  EXPECT_EQ(3ull, diagnostics.snapshotFiles);
  EXPECT_EQ(4ull, diagnostics.publishTargets);
  EXPECT_EQ(4ull, diagnostics.publishAppliedTargets);
  EXPECT_TRUE(diagnostics.fenceAttempted);
  EXPECT_TRUE(diagnostics.fenceAcquired);
  EXPECT_EQ(4ull, progress.publishAppliedTargets);
}

TEST_F(TreeSizeRecomputeEngineF, RecomputeUsesFreshMetadataSnapshot)
{
  auto root = view()->createContainer("/recompute_engine_retry/root", true);
  view()->createContainer("/recompute_engine_retry/root/child", true);
  CreateSizedFile(view(), "/recompute_engine_retry/root/child/file", 10);

  eos::QuarkContainerAccounting accounting(containerSvc(), 0);
  eos::TreeSizeRecomputeDiagnostics first_diagnostics;
  const auto first_result = RunRecompute(
      *containerSvc(), *fileSvc(), accounting,
      eos::TreeSizeRecomputeRequest{root->getId(), "/recompute_engine_retry/root/", 0},
      first_diagnostics);

  ASSERT_EQ(0, first_result.retc) << first_result.error;
  ExpectTreeCounters(containerSvc()->getContainerMD(root->getId()), 10, 1, 1);

  auto file = view()->getFile("/recompute_engine_retry/root/child/file");
  file->setSize(42);
  view()->updateFileStore(file.get());

  eos::TreeSizeRecomputeDiagnostics retry_diagnostics;
  const auto retry_result = RunRecompute(
      *containerSvc(), *fileSvc(), accounting,
      eos::TreeSizeRecomputeRequest{root->getId(), "/recompute_engine_retry/root/", 0},
      retry_diagnostics);

  ASSERT_EQ(0, retry_result.retc) << retry_result.error;
  ExpectTreeCounters(containerSvc()->getContainerMD(root->getId()), 42, 1, 1);
  EXPECT_TRUE(retry_diagnostics.available);
  EXPECT_TRUE(retry_diagnostics.converged);
  EXPECT_EQ(2ull, retry_diagnostics.discoveredContainers);
  EXPECT_EQ(2ull, retry_diagnostics.snapshotContainers);
  EXPECT_EQ(1ull, retry_diagnostics.snapshotFiles);
  EXPECT_EQ(2ull, retry_diagnostics.publishAppliedTargets);
}

TEST_F(TreeSizeRecomputeEngineF, PostDiscoveryTopologyPublishesBestEffort)
{
  auto root = view()->createContainer("/recompute_engine_retryable/root", true);
  view()->createContainer("/recompute_engine_retryable/root/child", true);
  CreateSizedFile(view(), "/recompute_engine_retryable/root/child/file", 10);

  root->setTreeSize(777);
  root->setTreeFiles(8);
  root->setTreeContainers(9);
  view()->updateContainerStore(root.get());

  ScriptedTreeSizeAccountingService accounting;
  eos::TreeSizeRecomputeDiagnostics diagnostics;
  bool injected = false;
  const auto outside_child_id = containerSvc()->getFirstFreeId() + 1000;
  const auto result =
      RunRecompute(*containerSvc(), *fileSvc(), accounting,
                   eos::TreeSizeRecomputeRequest{root->getId(),
                                                 "/recompute_engine_retryable/root/", 0},
                   diagnostics, nullptr, [&](const std::string& phase) {
                     if (injected || (phase != "snapshotting")) {
                       return;
                     }

                     injected = true;
                     accounting.Capture(MakeSequencedJournalEntry(
                         eos::TreeSizeAccountingEventType::ChildAttach, root->getId(),
                         outside_child_id, eos::TreeInfos{0, 0, 1}));
                   });

  EXPECT_TRUE(injected);
  ASSERT_EQ(EAGAIN, result.retc) << result.error;
  EXPECT_TRUE(diagnostics.available);
  EXPECT_FALSE(diagnostics.converged);
  EXPECT_TRUE(diagnostics.publishable);
  EXPECT_TRUE(diagnostics.retryRequired);
  EXPECT_EQ("post_discovery_topology", diagnostics.retryReason);
  EXPECT_EQ(1ull, diagnostics.coveragePostDiscoveryEntries);
  EXPECT_EQ(1ull, diagnostics.coveragePostDiscoveryContainerIds);
  EXPECT_NE(0ull,
            diagnostics.decisionReasonMask &
                eos::TreeSizeRecomputePublishDecisionReasons::PostDiscoveryTopology);
  EXPECT_TRUE(diagnostics.fenceAttempted);
  EXPECT_TRUE(diagnostics.fenceAcquired);
  EXPECT_EQ(3ull, diagnostics.publishAttemptedTargets);
  EXPECT_EQ(2ull, diagnostics.publishAppliedTargets);
  EXPECT_EQ(1ull, diagnostics.publishMissingTargets);
  EXPECT_EQ(0ull, diagnostics.publishWriteFailedTargets);
  EXPECT_EQ(2ull, diagnostics.retryCandidateCount);
  EXPECT_EQ(1ull, accounting.fenceAcquireCount);
  EXPECT_EQ(1ull, accounting.fenceReleaseCount);
  ExpectTreeCounters(containerSvc()->getContainerMD(root->getId()), 10, 1, 2);
}

TEST_F(TreeSizeRecomputeEngineF, FenceNotAcquiredRetainsRetryReason)
{
  auto root = view()->createContainer("/recompute_engine_fence_retry/root", true);
  view()->createContainer("/recompute_engine_fence_retry/root/child", true);
  CreateSizedFile(view(), "/recompute_engine_fence_retry/root/child/file", 10);

  root->setTreeSize(555);
  root->setTreeFiles(6);
  root->setTreeContainers(7);
  view()->updateContainerStore(root.get());

  ScriptedTreeSizeAccountingService accounting;
  accounting.acquireFence = false;
  eos::TreeSizeRecomputeDiagnostics diagnostics;
  const auto result =
      RunRecompute(*containerSvc(), *fileSvc(), accounting,
                   eos::TreeSizeRecomputeRequest{
                       root->getId(), "/recompute_engine_fence_retry/root/", 0},
                   diagnostics);

  ASSERT_EQ(EAGAIN, result.retc) << result.error;
  EXPECT_TRUE(diagnostics.available);
  EXPECT_FALSE(diagnostics.converged);
  EXPECT_FALSE(diagnostics.publishable);
  EXPECT_TRUE(diagnostics.retryRequired);
  EXPECT_EQ("publish_fence_not_acquired", diagnostics.retryReason);
  EXPECT_TRUE(diagnostics.fenceAttempted);
  EXPECT_FALSE(diagnostics.fenceAcquired);
  EXPECT_EQ(0ull, diagnostics.publishAttemptedTargets);
  EXPECT_EQ(0ull, diagnostics.publishAppliedTargets);
  EXPECT_EQ(1ull, accounting.fenceAcquireCount);
  EXPECT_EQ(0ull, accounting.fenceReleaseCount);
  ExpectTreeCounters(containerSvc()->getContainerMD(root->getId()), 555, 6, 7);
}

TEST_F(TreeSizeRecomputeEngineF, NegativeCountersDoNotPublish)
{
  auto root = view()->createContainer("/recompute_engine_negative/root", true);

  root->setTreeSize(555);
  root->setTreeFiles(6);
  root->setTreeContainers(7);
  view()->updateContainerStore(root.get());

  ScriptedTreeSizeAccountingService accounting;
  eos::TreeSizeRecomputeDiagnostics diagnostics;
  bool injected = false;
  const auto result = RunRecompute(
      *containerSvc(), *fileSvc(), accounting,
      eos::TreeSizeRecomputeRequest{root->getId(), "/recompute_engine_negative/root/", 0},
      diagnostics, nullptr, [&](const std::string& phase) {
        if (injected || (phase != "snapshotting")) {
          return;
        }

        injected = true;
        accounting.Capture(
            MakeSequencedJournalEntry(eos::TreeSizeAccountingEventType::FileDelete,
                                      root->getId(), 99, eos::TreeInfos{-10, -1, 0}));
      });

  EXPECT_TRUE(injected);
  ASSERT_EQ(EAGAIN, result.retc) << result.error;
  EXPECT_TRUE(diagnostics.available);
  EXPECT_FALSE(diagnostics.converged);
  EXPECT_FALSE(diagnostics.publishable);
  EXPECT_TRUE(diagnostics.retryRequired);
  EXPECT_NE(std::string::npos, diagnostics.retryReason.find("negative_counters"));
  EXPECT_EQ(1ull, diagnostics.coverageCoveredEntries);
  EXPECT_EQ(1ull, diagnostics.negativeCounters);
  EXPECT_NE(0ull, diagnostics.decisionReasonMask &
                      eos::TreeSizeRecomputePublishDecisionReasons::NegativeCounters);
  EXPECT_FALSE(diagnostics.fenceAttempted);
  EXPECT_EQ(0ull, diagnostics.publishAttemptedTargets);
  EXPECT_EQ(0ull, diagnostics.publishAppliedTargets);
  EXPECT_EQ(0ull, accounting.fenceAcquireCount);
  EXPECT_EQ(0ull, accounting.fenceReleaseCount);
  ExpectTreeCounters(containerSvc()->getContainerMD(root->getId()), 555, 6, 7);
}

TEST_F(TreeSizeRecomputeEngineF, CapturesConcurrentJournalEntryWhileDiscovering)
{
  auto root = view()->createContainer("/recompute_engine_concurrent/root", true);
  auto child = view()->createContainer("/recompute_engine_concurrent/root/child", true);
  auto file = CreateSizedFile(view(), "/recompute_engine_concurrent/root/child/file", 10);

  BlockingContainerMDSvc blocking_svc(containerSvc(), child->getId());
  ScriptedTreeSizeAccountingService accounting;
  eos::TreeSizeRecomputeDiagnostics diagnostics;

  auto recompute = std::async(std::launch::async, [&]() {
    return RunRecompute(blocking_svc, *fileSvc(), accounting,
                        eos::TreeSizeRecomputeRequest{
                            root->getId(), "/recompute_engine_concurrent/root/", 0},
                        diagnostics);
  });

  if (!blocking_svc.WaitUntilBlocked(std::chrono::seconds(2))) {
    blocking_svc.Release();
    const auto result = recompute.get();
    FAIL() << "recompute did not reach the blocking discovery metadata lookup: "
           << result.error;
  }

  accounting.Capture(
      MakeSequencedJournalEntry(eos::TreeSizeAccountingEventType::FileDelta,
                                child->getId(), file->getId(), eos::TreeInfos{32, 0, 0}));
  blocking_svc.Release();

  const auto result = recompute.get();
  ASSERT_EQ(0, result.retc) << result.error;
  ExpectTreeCounters(containerSvc()->getContainerMD(root->getId()), 42, 1, 1);
  ExpectTreeCounters(containerSvc()->getContainerMD(child->getId()), 42, 1, 0);
  EXPECT_TRUE(diagnostics.available);
  EXPECT_TRUE(diagnostics.converged);
  EXPECT_TRUE(diagnostics.publishable);
  EXPECT_FALSE(diagnostics.retryRequired);
  EXPECT_EQ(1ull, diagnostics.journalEntries);
  EXPECT_EQ(1ull, diagnostics.coverageCoveredEntries);
  EXPECT_EQ(2ull, diagnostics.publishAppliedTargets);
  EXPECT_EQ(1ull, accounting.fenceAcquireCount);
  EXPECT_EQ(1ull, accounting.fenceReleaseCount);
}

TEST_F(TreeSizeRecomputeEngineF, ReportsWriteFailureAsRetryCandidate)
{
  auto root = view()->createContainer("/recompute_engine_partial/root", true);
  auto child = view()->createContainer("/recompute_engine_partial/root/child", true);
  CreateSizedFile(view(), "/recompute_engine_partial/root/child/file", 10);

  root->setTreeSize(999);
  root->setTreeFiles(999);
  root->setTreeContainers(999);
  child->setTreeSize(999);
  child->setTreeFiles(999);
  child->setTreeContainers(999);
  view()->updateContainerStore(root.get());
  view()->updateContainerStore(child.get());

  FailOnPublishContainerMDSvc failing_svc(containerSvc(), root->getId());
  eos::QuarkContainerAccounting accounting(containerSvc(), 0);
  eos::TreeSizeRecomputeDiagnostics diagnostics;

  const auto result = RunRecompute(
      failing_svc, *fileSvc(), accounting,
      eos::TreeSizeRecomputeRequest{root->getId(), "/recompute_engine_partial/root/", 0},
      diagnostics, nullptr, [&](const std::string& phase) {
        if (phase == "publishing") {
          failing_svc.Enable();
        }
      });

  ASSERT_EQ(EAGAIN, result.retc) << result.error;
  EXPECT_TRUE(failing_svc.Failed());
  EXPECT_TRUE(diagnostics.available);
  EXPECT_FALSE(diagnostics.converged);
  EXPECT_TRUE(diagnostics.publishable);
  EXPECT_TRUE(diagnostics.retryRequired);
  EXPECT_EQ("write_failed_targets", diagnostics.retryReason);
  EXPECT_TRUE(diagnostics.partialPublish);
  EXPECT_EQ(root->getId(), diagnostics.retryRootContainerId);
  EXPECT_EQ(root->getId(), diagnostics.failedContainerId);
  EXPECT_EQ(1ull, diagnostics.failedContainerCount);
  EXPECT_EQ(2ull, diagnostics.publishAttemptedTargets);
  EXPECT_EQ(1ull, diagnostics.publishAppliedTargets);
  EXPECT_EQ(1ull, diagnostics.publishWriteFailedTargets);
  EXPECT_EQ(1ull, diagnostics.retryCandidateCount);
  ExpectTreeCounters(containerSvc()->getContainerMD(child->getId()), 10, 1, 0);
  ExpectTreeCounters(containerSvc()->getContainerMD(root->getId()), 999, 999, 999);
}

TEST_F(TreeSizeRecomputeEngineF, StopCallbackCancelsWhileDiscoveryIsBlocked)
{
  auto root = view()->createContainer("/recompute_engine_stop/root", true);
  auto child = view()->createContainer("/recompute_engine_stop/root/child", true);
  CreateSizedFile(view(), "/recompute_engine_stop/root/child/file", 10);

  root->setTreeSize(888);
  root->setTreeFiles(9);
  root->setTreeContainers(10);
  view()->updateContainerStore(root.get());

  BlockingContainerMDSvc blocking_svc(containerSvc(), child->getId());
  ScriptedTreeSizeAccountingService accounting;
  eos::TreeSizeRecomputeDiagnostics diagnostics;
  std::atomic<bool> stop_requested{false};

  auto recompute = std::async(std::launch::async, [&]() {
    return RunRecompute(
        blocking_svc, *fileSvc(), accounting,
        eos::TreeSizeRecomputeRequest{root->getId(), "/recompute_engine_stop/root/", 0},
        diagnostics, nullptr, {}, [&]() { return stop_requested.load(); });
  });

  if (!blocking_svc.WaitUntilBlocked(std::chrono::seconds(2))) {
    blocking_svc.Release();
    const auto result = recompute.get();
    FAIL() << "recompute did not reach the blocking discovery metadata lookup: "
           << result.error;
  }

  stop_requested.store(true);
  blocking_svc.Release();

  const auto result = recompute.get();
  ASSERT_EQ(ECANCELED, result.retc) << result.error;
  EXPECT_EQ(0ull, accounting.fenceAcquireCount);
  EXPECT_EQ(0ull, accounting.fenceReleaseCount);
  EXPECT_EQ(0ull, diagnostics.publishAttemptedTargets);
  EXPECT_EQ(0ull, diagnostics.publishAppliedTargets);
  ExpectTreeCounters(containerSvc()->getContainerMD(root->getId()), 888, 9, 10);
}
