//------------------------------------------------------------------------------
//! @file TreeSizeAccountingManager.cc
//! @brief MGM-owned tree-size accounting recompute manager
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

#include "mgm/treesizeaccounting/TreeSizeAccountingManager.hh"
#include <cerrno>
#include <chrono>
#include <deque>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace {

constexpr uint64_t kTreeSizeRecomputeMaxRetryPasses = 3;
constexpr uint64_t kTreeSizeRecomputeMaxPasses = 1 + kTreeSizeRecomputeMaxRetryPasses;
constexpr uint64_t kTreeSizeRecomputeMaxAttemptsPerRoot =
    1 + kTreeSizeRecomputeMaxRetryPasses;
constexpr auto kTreeSizeRecomputeRetryDelay = std::chrono::milliseconds(250);

uint64_t
NowUnixTime()
{
  return std::chrono::duration_cast<std::chrono::seconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

const char*
StateToString(eos::mgm::TreeSizeRecomputeJobState state)
{
  switch (state) {
  case eos::mgm::TreeSizeRecomputeJobState::Idle:
    return "idle";
  case eos::mgm::TreeSizeRecomputeJobState::Queued:
    return "queued";
  case eos::mgm::TreeSizeRecomputeJobState::Running:
    return "running";
  case eos::mgm::TreeSizeRecomputeJobState::Completed:
    return "completed";
  case eos::mgm::TreeSizeRecomputeJobState::Failed:
    return "failed";
  case eos::mgm::TreeSizeRecomputeJobState::Stopped:
    return "stopped";
  }

  return "unknown";
}

bool
IsPartialPublish(const eos::mgm::TreeSizeRecomputeStatus& status)
{
  return status.partialPublish || status.diagnostics.partialPublish;
}

std::string
ResultToString(const eos::mgm::TreeSizeRecomputeStatus& status)
{
  switch (status.state) {
  case eos::mgm::TreeSizeRecomputeJobState::Idle:
    return "none";
  case eos::mgm::TreeSizeRecomputeJobState::Queued:
  case eos::mgm::TreeSizeRecomputeJobState::Running:
    return "active";
  case eos::mgm::TreeSizeRecomputeJobState::Completed:
    if (status.retc != 0) {
      return "failed";
    }

    return status.converged ? "success" : "non_converged";
  case eos::mgm::TreeSizeRecomputeJobState::Failed:
    if (IsPartialPublish(status)) {
      return "partial_publish";
    }

    return status.retc == EAGAIN ? "retry" : "failed";
  case eos::mgm::TreeSizeRecomputeJobState::Stopped:
    return "stopped";
  }

  return "unknown";
}

uint64_t
StatusElapsedSeconds(const eos::mgm::TreeSizeRecomputeStatus& status)
{
  const auto now = NowUnixTime();

  if (status.startedAt != 0) {
    const auto end = status.finishedAt != 0 ? status.finishedAt : now;
    return end > status.startedAt ? end - status.startedAt : 0;
  }

  if (status.submittedAt != 0) {
    return now > status.submittedAt ? now - status.submittedAt : 0;
  }

  return 0;
}

void
AppendDetail(std::ostringstream& oss, const eos::mgm::TreeSizeRecomputeStatus& status)
{
  const auto& details = status.diagnostics;
  oss << "detail available=" << (details.available ? 1u : 0u)
      << " duration=" << StatusElapsedSeconds(status)
      << " converged=" << (details.converged ? 1u : 0u)
      << " publishable=" << (details.publishable ? 1u : 0u)
      << " retry_required=" << (details.retryRequired ? 1u : 0u)
      << " partial_publish=" << (details.partialPublish ? 1u : 0u)
      << " retry_reason=" << details.retryReason
      << " retry_candidates=" << details.retryCandidateCount
      << " missing_targets=" << details.publishMissingTargets
      << " write_failed_targets=" << details.publishWriteFailedTargets
      << " recompute_passes=" << status.recomputePassCount
      << " retry_passes=" << status.retryPassCount
      << " retry_limit_reached=" << (status.retryLimitReached ? 1u : 0u)
      << " retry_root_cid=" << details.retryRootContainerId
      << " failed_containers=" << details.failedContainerCount
      << " failed_cid=" << details.failedContainerId
      << " discovered_containers=" << details.discoveredContainers
      << " snapshot_containers=" << details.snapshotContainers
      << " snapshot_files=" << details.snapshotFiles
      << " decision_reason_mask=" << details.decisionReasonMask << " decision_reasons=\""
      << details.decisionReasons << "\""
      << " discovery_missing_metadata=" << details.discoveryMissingMetadata
      << " discovery_resolved_metadata=" << details.discoveryResolvedMetadata
      << " discovery_unresolved_metadata=" << details.discoveryUnresolvedMetadata
      << " journal_entries=" << details.journalEntries
      << " baseline_sequence=" << details.baselineSequence
      << " journal_latest_sequence=" << details.journalLatestSequence
      << " fence_validated_sequence=" << details.fenceValidatedSequence
      << " journal_missing_metadata=" << details.journalMissingMetadata
      << " non_increasing_sequence=" << details.nonIncreasingSequence
      << " reconcile_missing_metadata=" << details.reconcileMissingMetadata
      << " reconcile_suppressed_entries=" << details.reconcileSuppressedEntries
      << " unknown_parents=" << details.unknownParents
      << " negative_counters=" << details.negativeCounters
      << " unsupported_events=" << details.unsupportedEvents
      << " coverage_missing_metadata=" << details.coverageMissingMetadata
      << " coverage_covered_entries=" << details.coverageCoveredEntries
      << " coverage_outside_entries=" << details.coverageOutsideEntries
      << " coverage_post_discovery_entries=" << details.coveragePostDiscoveryEntries
      << " coverage_post_discovery_container_ids="
      << details.coveragePostDiscoveryContainerIds
      << " compose_missing_direct_counters=" << details.composeMissingDirectCounters
      << " compose_missing_topology=" << details.composeMissingTopology
      << " compose_cycle_edges=" << details.composeCycleEdges
      << " publish_targets=" << details.publishTargets
      << " publish_order_entries=" << details.publishOrderEntries
      << " publish_missing_counters=" << details.publishMissingCounters
      << " publish_negative_counters=" << details.publishNegativeCounters
      << " publish_duplicate_targets=" << details.publishDuplicateTargets
      << " publish_unplanned_counters=" << details.publishUnplannedCounters
      << " publish_apply_status=" << details.publishApplyStatus
      << " publish_attempted_targets=" << details.publishAttemptedTargets
      << " publish_applied_targets=" << details.publishAppliedTargets
      << " publish_skipped_missing_targets=" << details.publishSkippedMissingTargets
      << " publish_missing_targets=" << details.publishMissingTargets
      << " publish_write_failed_targets=" << details.publishWriteFailedTargets
      << " fence_available=" << (details.fenceAvailable ? 1u : 0u)
      << " fence_attempted=" << (details.fenceAttempted ? 1u : 0u)
      << " fence_acquired=" << (details.fenceAcquired ? 1u : 0u)
      << " fence_wait_timeout=" << (details.fenceWaitTimeout ? 1u : 0u)
      << " fence_covered_ids=" << details.fenceCoveredIds
      << " fence_included_updates=" << details.fenceIncludedUpdates
      << " fence_included_subtree_attach_updates="
      << details.fenceIncludedSubtreeAttachUpdates
      << " fence_included_subtree_detach_updates="
      << details.fenceIncludedSubtreeDetachUpdates
      << " fence_replay_after_updates=" << details.fenceReplayAfterUpdates
      << " fence_unsequenced_updates=" << details.fenceUnsequencedUpdates
      << " fence_passed_through_updates=" << details.fencePassedThroughUpdates
      << " fence_drained_raw_updates=" << details.fenceDrainedRawUpdates
      << " fence_drained_batch_updates=" << details.fenceDrainedBatchUpdates
      << " fence_in_flight_covered_updates=" << details.fenceInFlightCoveredUpdates
      << '\n';
}

eos::mgm::TreeSizeRecomputeResult
MakeResult(int retc, std::string error = {})
{
  eos::mgm::TreeSizeRecomputeResult result;
  result.retc = retc;
  result.error = std::move(error);
  return result;
}

std::string
MakeContainerRetrySpecification(eos::IContainerMD::id_t id)
{
  std::ostringstream oss;
  oss << "cid:" << static_cast<unsigned long long>(id);
  return oss.str();
}

} // namespace

EOSMGMNAMESPACE_BEGIN

TreeSizeAccountingManager::~TreeSizeAccountingManager() { StopWorker(); }

void
TreeSizeAccountingManager::StartWorker()
{
  std::lock_guard<std::mutex> lock(mMutex);

  if (mWorkerStarted) {
    return;
  }

  mStopRequested.store(false);
  mWorkerStarted = true;
  mWorker.reset(&TreeSizeAccountingManager::WorkerLoop, this);
}

void
TreeSizeAccountingManager::StopWorker()
{
  {
    std::lock_guard<std::mutex> lock(mMutex);
    mStopRequested.store(true);

    if (mStatus.state == TreeSizeRecomputeJobState::Queued) {
      mStatus.state = TreeSizeRecomputeJobState::Stopped;
      mStatus.phase = "stopped";
      mStatus.retc = ECANCELED;
      mStatus.error = "tree-size recompute stopped before execution";
      mStatus.finishedAt = NowUnixTime();
      mPendingJob.reset();
    }
  }

  mCondition.notify_all();
  mWorker.stop();
  mWorker.join();
}

TreeSizeRecomputeSubmitResult
TreeSizeAccountingManager::SubmitRecompute(
    const TreeSizeRecomputeRequest& request, eos::IContainerMDSvc& container_svc,
    eos::IFileMDSvc& file_svc, eos::ITreeSizeAccountingService* accounting_service)
{
  std::lock_guard<std::mutex> lock(mMutex);
  TreeSizeRecomputeSubmitResult result;

  if (mStopRequested.load()) {
    result.retc = ECANCELED;
    result.message = "error: tree-size recompute manager is stopping";
    return result;
  }

  if (request.rootId == 0) {
    result.retc = ENOENT;
    result.message = "error: container not found";
    return result;
  }

  if (!accounting_service) {
    result.retc = ENOTSUP;
    result.message = "error: tree-size accounting service unavailable";
    return result;
  }

  if ((mStatus.state == TreeSizeRecomputeJobState::Queued) ||
      (mStatus.state == TreeSizeRecomputeJobState::Running) || mPendingJob) {
    result.retc = EAGAIN;
    result.message = "error: tree-size recompute already active";
    return result;
  }

  mPendingJob = WorkerJob{request, &container_svc, &file_svc, accounting_service};
  mStatus = TreeSizeRecomputeStatus{};
  mStatus.state = TreeSizeRecomputeJobState::Queued;
  mStatus.phase = "queued";
  mStatus.rootId = request.rootId;
  mStatus.rootSpecification = request.rootSpecification;
  mStatus.maxDepth = request.maxDepth;
  mStatus.submittedAt = NowUnixTime();
  result.message = "success: tree-size recompute submitted";
  mCondition.notify_one();
  return result;
}

TreeSizeRecomputeStatus
TreeSizeAccountingManager::GetStatus() const
{
  std::lock_guard<std::mutex> lock(mMutex);
  return mStatus;
}

std::string
TreeSizeAccountingManager::FormatStatus(bool detail) const
{
  const auto status = GetStatus();
  std::ostringstream oss;
  oss << "state=" << StateToString(status.state) << " retc=" << status.retc
      << " result=" << ResultToString(status)
      << " cid=" << static_cast<unsigned long long>(status.rootId) << " root=\""
      << status.rootSpecification << "\""
      << " depth=" << status.maxDepth << " phase=" << status.phase
      << " elapsed=" << StatusElapsedSeconds(status)
      << " submitted=" << status.submittedAt << " started=" << status.startedAt
      << " finished=" << status.finishedAt
      << " discovered_containers=" << status.discoveredContainers
      << " snapshot_containers=" << status.snapshotContainers
      << " snapshot_files=" << status.snapshotFiles
      << " publish_targets=" << status.publishTargets
      << " published_targets=" << status.publishAppliedTargets
      << " retry_candidates=" << status.retryCandidateCount
      << " missing_targets=" << status.missingTargetCount
      << " write_failed_targets=" << status.writeFailedTargetCount
      << " recompute_passes=" << status.recomputePassCount
      << " retry_passes=" << status.retryPassCount
      << " retry_limit_reached=" << (status.retryLimitReached ? 1u : 0u)
      << " converged=" << (status.converged ? 1u : 0u)
      << " failed_containers=" << status.failedContainerCount;

  if (IsPartialPublish(status)) {
    oss << " publish_attempted_targets=" << status.publishAttemptedTargets;
  }

  if (!status.retryReason.empty()) {
    oss << " retry_reason=" << status.retryReason;
  }

  if (status.retryRootContainerId != 0) {
    oss << " retry_root_cid=" << status.retryRootContainerId;
  }

  if (status.failedContainerId != 0) {
    oss << " failed_cid=" << status.failedContainerId;
  }

  if (!status.error.empty()) {
    oss << " error=\"" << status.error << "\"";
  }

  oss << '\n';

  if (detail) {
    AppendDetail(oss, status);
  }

  return oss.str();
}

bool
TreeSizeAccountingManager::StopRequested() const
{
  return mStopRequested.load();
}

void
TreeSizeAccountingManager::UpdatePhase(const std::string& phase)
{
  std::lock_guard<std::mutex> lock(mMutex);
  mStatus.phase = phase;
}

void
TreeSizeAccountingManager::UpdateProgress(uint64_t discovered_containers,
                                          uint64_t snapshot_containers,
                                          uint64_t snapshot_files,
                                          uint64_t publish_targets,
                                          uint64_t publish_applied_targets)
{
  std::lock_guard<std::mutex> lock(mMutex);
  mStatus.discoveredContainers = discovered_containers;
  mStatus.snapshotContainers = snapshot_containers;
  mStatus.snapshotFiles = snapshot_files;
  mStatus.publishTargets = publish_targets;
  mStatus.publishAppliedTargets = publish_applied_targets;
}

void
TreeSizeAccountingManager::UpdateRetryProgress(uint64_t recompute_pass_count,
                                               uint64_t retry_pass_count,
                                               bool retry_limit_reached)
{
  std::lock_guard<std::mutex> lock(mMutex);
  mStatus.recomputePassCount = recompute_pass_count;
  mStatus.retryPassCount = retry_pass_count;
  mStatus.retryLimitReached = retry_limit_reached;
}

void
TreeSizeAccountingManager::UpdateDiagnostics(
    const TreeSizeRecomputeDiagnostics& diagnostics)
{
  std::lock_guard<std::mutex> lock(mMutex);
  mStatus.diagnostics = diagnostics;
  mStatus.discoveredContainers = diagnostics.discoveredContainers;
  mStatus.snapshotContainers = diagnostics.snapshotContainers;
  mStatus.snapshotFiles = diagnostics.snapshotFiles;
  mStatus.publishTargets = diagnostics.publishTargets;
  mStatus.publishAttemptedTargets = diagnostics.publishAttemptedTargets;
  mStatus.publishAppliedTargets = diagnostics.publishAppliedTargets;
  mStatus.missingTargetCount = diagnostics.publishMissingTargets;
  mStatus.writeFailedTargetCount = diagnostics.publishWriteFailedTargets;
  mStatus.converged = diagnostics.converged;
  mStatus.partialPublish = diagnostics.partialPublish;
  mStatus.retryReason = diagnostics.retryReason;
  mStatus.retryCandidateCount = diagnostics.retryCandidateCount;
  mStatus.retryRootContainerId = diagnostics.retryRootContainerId;
  mStatus.failedContainerCount = diagnostics.failedContainerCount;
  mStatus.failedContainerId = diagnostics.failedContainerId;
}

void
TreeSizeAccountingManager::WorkerLoop(ThreadAssistant& assistant) noexcept
{
  ThreadAssistant::setSelfThreadName("TreeSizeAcct");
  assistant.registerCallback([this]() { mCondition.notify_all(); });

  while (true) {
    WorkerJob job;
    {
      std::unique_lock<std::mutex> lock(mMutex);
      mCondition.wait(lock, [this, &assistant]() {
        return mStopRequested.load() || assistant.terminationRequested() ||
               mPendingJob.has_value();
      });

      if ((mStopRequested.load() || assistant.terminationRequested()) && !mPendingJob) {
        break;
      }

      if (!mPendingJob) {
        continue;
      }

      job = *mPendingJob;
      mPendingJob.reset();
      mStatus.state = TreeSizeRecomputeJobState::Running;
      mStatus.phase = "starting";
      mStatus.startedAt = NowUnixTime();
    }

    TreeSizeRecomputeResult result;

    if (StopRequested() || assistant.terminationRequested()) {
      result = MakeResult(ECANCELED, "tree-size recompute stopped before execution");
    } else {
      try {
        result = RecomputeTreeSize(job.request, *job.containerSvc, *job.fileSvc,
                                   job.accountingService);
      } catch (const std::exception& e) {
        result = MakeResult(EIO, std::string("error: tree-size recompute failed: ") +
                                     e.what());
      } catch (...) {
        result = MakeResult(EIO, "error: tree-size recompute failed");
      }
    }

    {
      std::lock_guard<std::mutex> lock(mMutex);
      mStatus.retc = result.retc;
      mStatus.error = result.error;
      mStatus.finishedAt = NowUnixTime();

      if (result.retc == 0) {
        mStatus.state = TreeSizeRecomputeJobState::Completed;
        mStatus.phase = "completed";
      } else if ((result.retc == ECANCELED) && mStopRequested.load()) {
        mStatus.state = TreeSizeRecomputeJobState::Stopped;
        mStatus.phase = "stopped";
      } else {
        mStatus.state = TreeSizeRecomputeJobState::Failed;
      }
    }

    if (mStopRequested.load() || assistant.terminationRequested()) {
      break;
    }
  }

  std::lock_guard<std::mutex> lock(mMutex);
  mWorkerStarted = false;
}

TreeSizeRecomputeResult
TreeSizeAccountingManager::RecomputeTreeSize(
    const TreeSizeRecomputeRequest& request, eos::IContainerMDSvc& container_svc,
    eos::IFileMDSvc& file_svc, eos::ITreeSizeAccountingService* accounting_service)
{
  std::deque<TreeSizeRecomputeRequest> pending_requests;
  std::unordered_map<eos::IContainerMD::id_t, uint64_t> attempts_by_root;
  std::unordered_set<eos::IContainerMD::id_t> queued_roots;
  TreeSizeRecomputeResult last_result;
  TreeSizeRecomputeDiagnostics last_diagnostics;
  uint64_t pass_count = 0;
  bool retry_limit_reached = false;
  std::string first_retry_error;

  auto wait_before_retry = [&]() {
    const auto deadline = std::chrono::steady_clock::now() + kTreeSizeRecomputeRetryDelay;

    while (std::chrono::steady_clock::now() < deadline) {
      if (StopRequested()) {
        return false;
      }

      std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }

    return true;
  };

  auto enqueue_retry = [&](eos::IContainerMD::id_t root_id) {
    if (root_id == 0) {
      return;
    }

    if (attempts_by_root[root_id] >= kTreeSizeRecomputeMaxAttemptsPerRoot) {
      retry_limit_reached = true;
      return;
    }

    if (!queued_roots.insert(root_id).second) {
      return;
    }

    TreeSizeRecomputeRequest retry_request;
    retry_request.rootId = root_id;
    retry_request.rootSpecification = root_id == request.rootId
                                          ? request.rootSpecification
                                          : MakeContainerRetrySpecification(root_id);
    retry_request.maxDepth = request.maxDepth;
    pending_requests.push_back(std::move(retry_request));
  };

  enqueue_retry(request.rootId);

  while (!pending_requests.empty()) {
    if (StopRequested()) {
      return MakeResult(ECANCELED, "tree-size recompute stopped");
    }

    if (pass_count >= kTreeSizeRecomputeMaxPasses) {
      retry_limit_reached = true;
      UpdateRetryProgress(pass_count, pass_count == 0 ? 0 : pass_count - 1,
                          retry_limit_reached);
      break;
    }

    auto current_request = pending_requests.front();
    pending_requests.pop_front();
    queued_roots.erase(current_request.rootId);

    auto& attempts = attempts_by_root[current_request.rootId];

    if (attempts >= kTreeSizeRecomputeMaxAttemptsPerRoot) {
      retry_limit_reached = true;
      UpdateRetryProgress(pass_count, pass_count == 0 ? 0 : pass_count - 1,
                          retry_limit_reached);
      continue;
    }

    ++attempts;
    ++pass_count;
    auto pass_result = RecomputeTreeSizeOnce(current_request, container_svc, file_svc,
                                             accounting_service);
    last_result = pass_result.result;
    last_diagnostics = pass_result.diagnostics;
    UpdateRetryProgress(pass_count, pass_count == 0 ? 0 : pass_count - 1,
                        retry_limit_reached);

    if (last_result.retc == ECANCELED) {
      return last_result;
    }

    if ((last_result.retc != 0) && (last_result.retc != EAGAIN)) {
      if ((current_request.rootId == request.rootId) && (pass_count == 1)) {
        return last_result;
      }

      retry_limit_reached = true;
      UpdateRetryProgress(pass_count, pass_count == 0 ? 0 : pass_count - 1,
                          retry_limit_reached);

      if (first_retry_error.empty()) {
        first_retry_error = last_result.error;
      }

      continue;
    }

    if (!last_diagnostics.retryRequired && (last_result.retc == 0)) {
      if ((current_request.rootId == request.rootId) && last_diagnostics.converged) {
        pending_requests.clear();
        queued_roots.clear();
      }

      continue;
    }

    if (first_retry_error.empty()) {
      first_retry_error = last_result.error;
    }

    const auto retry_root_id = last_diagnostics.retryRootContainerId != 0
                                   ? last_diagnostics.retryRootContainerId
                                   : current_request.rootId;
    if (retry_root_id != 0) {
      enqueue_retry(retry_root_id);

      if (!pending_requests.empty() && !wait_before_retry()) {
        return MakeResult(ECANCELED, "tree-size recompute stopped");
      }

      continue;
    }

    for (const auto retry_id : last_diagnostics.retryContainerIds) {
      enqueue_retry(retry_id);
    }

    if (!pending_requests.empty() && !wait_before_retry()) {
      return MakeResult(ECANCELED, "tree-size recompute stopped");
    }
  }

  if (StopRequested()) {
    return MakeResult(ECANCELED, "tree-size recompute stopped");
  }

  if (retry_limit_reached ||
      (last_diagnostics.available && !last_diagnostics.converged)) {
    UpdateRetryProgress(pass_count, pass_count == 0 ? 0 : pass_count - 1,
                        retry_limit_reached);
    return MakeResult(0, first_retry_error.empty()
                             ? "tree-size recompute finished before convergence"
                             : first_retry_error);
  }

  return last_result;
}

//------------------------------------------------------------------------------
// Recompute and publish tree-size counters for one resolved root container.
//------------------------------------------------------------------------------
TreeSizeAccountingManager::RecomputePassResult
TreeSizeAccountingManager::RecomputeTreeSizeOnce(
    const TreeSizeRecomputeRequest& request, eos::IContainerMDSvc& container_svc,
    eos::IFileMDSvc& file_svc, eos::ITreeSizeAccountingService* accounting_service)
{
  RecomputePassResult pass_result;
  eos::TreeSizeRecomputeEngineCallbacks callbacks;
  callbacks.stopRequested = [this]() { return StopRequested(); };
  callbacks.updatePhase = [this](const std::string& phase) { UpdatePhase(phase); };
  callbacks.updateProgress = [this](uint64_t discovered_containers,
                                    uint64_t snapshot_containers, uint64_t snapshot_files,
                                    uint64_t publish_targets,
                                    uint64_t publish_applied_targets) {
    UpdateProgress(discovered_containers, snapshot_containers, snapshot_files,
                   publish_targets, publish_applied_targets);
  };
  callbacks.updateDiagnostics =
      [this, &pass_result](const eos::TreeSizeRecomputeDiagnostics& diagnostics) {
        pass_result.diagnostics = diagnostics;
        UpdateDiagnostics(diagnostics);
      };

  const auto engine_result = eos::TreeSizeRecomputeEngine().Recompute(
      eos::TreeSizeRecomputeRequest{request.rootId, request.rootSpecification,
                                    request.maxDepth},
      container_svc, file_svc, accounting_service, callbacks);
  pass_result.result = MakeResult(engine_result.retc, engine_result.error);
  return pass_result;
}

EOSMGMNAMESPACE_END
