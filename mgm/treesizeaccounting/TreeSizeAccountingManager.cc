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
#include <functional>
#include <sstream>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

namespace {

constexpr uint64_t kTreeSizeRecomputeMaxRetryPasses = 3;
constexpr uint64_t kTreeSizeRecomputeMaxPasses = 1 + kTreeSizeRecomputeMaxRetryPasses;
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

    return status.diagnostics.converged ? "success" : "non_converged";
  case eos::mgm::TreeSizeRecomputeJobState::Failed:
    if (status.diagnostics.partialPublish) {
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

struct TreeSizeRecomputeRetryPass {
  eos::mgm::TreeSizeRecomputeResult result;
  eos::mgm::TreeSizeRecomputeDiagnostics diagnostics;
};

class TreeSizeRecomputeRetryRunner {
public:
  using RunPassCallback = std::function<TreeSizeRecomputeRetryPass(
      const eos::mgm::TreeSizeRecomputeRequest&)>;
  using StopRequestedCallback = std::function<bool()>;
  using UpdateProgressCallback = std::function<void(uint64_t, uint64_t, bool)>;

  TreeSizeRecomputeRetryRunner(const eos::mgm::TreeSizeRecomputeRequest& original_request,
                               RunPassCallback run_pass,
                               StopRequestedCallback stop_requested,
                               UpdateProgressCallback update_progress)
      : mOriginalRequest(original_request)
      , mRunPass(std::move(run_pass))
      , mStopRequested(std::move(stop_requested))
      , mUpdateProgress(std::move(update_progress))
  {
  }

  eos::mgm::TreeSizeRecomputeResult
  Run()
  {
    EnqueueRetry(mOriginalRequest.rootId);

    while (!mPendingRequests.empty()) {
      if (mStopRequested()) {
        return StoppedResult();
      }

      if (mPassCount >= kTreeSizeRecomputeMaxPasses) {
        MarkRetryLimitReached();
        break;
      }

      const auto current_request = PopNextRequest();

      RunAttempt(current_request);

      if (mLastResult.retc == ECANCELED) {
        return mLastResult;
      }

      if (LastPassFailedPermanently()) {
        if (IsInitialRootPass(current_request)) {
          return mLastResult;
        }

        MarkRetryLimitReached();
        RememberFirstRetryError(mLastResult.error);
        continue;
      }

      if (LastPassCompletedWithoutRetry(current_request)) {
        continue;
      }

      RememberFirstRetryError(mLastResult.error);
      EnqueueRetriesFromLastPass(current_request.rootId);

      if (!mPendingRequests.empty()) {
        if (mPassCount >= kTreeSizeRecomputeMaxPasses) {
          MarkRetryLimitReached();
          break;
        }

        if (!WaitBeforeRetry()) {
          return StoppedResult();
        }
      }
    }

    return FinalResult();
  }

private:
  uint64_t
  RetryPassCount() const
  {
    return mPassCount == 0 ? 0 : mPassCount - 1;
  }

  void
  UpdateRetryProgress()
  {
    mUpdateProgress(mPassCount, RetryPassCount(), mRetryLimitReached);
  }

  void
  MarkRetryLimitReached()
  {
    mRetryLimitReached = true;
    UpdateRetryProgress();
  }

  eos::mgm::TreeSizeRecomputeResult
  StoppedResult() const
  {
    return MakeResult(ECANCELED, "tree-size recompute stopped");
  }

  eos::mgm::TreeSizeRecomputeRequest
  MakeRetryRequest(eos::IContainerMD::id_t root_id) const
  {
    eos::mgm::TreeSizeRecomputeRequest retry_request;
    retry_request.rootId = root_id;
    retry_request.rootSpecification = root_id == mOriginalRequest.rootId
                                          ? mOriginalRequest.rootSpecification
                                          : MakeContainerRetrySpecification(root_id);
    retry_request.maxDepth = mOriginalRequest.maxDepth;
    return retry_request;
  }

  void
  EnqueueRetry(eos::IContainerMD::id_t root_id)
  {
    if (root_id == 0) {
      return;
    }

    if (!mQueuedRoots.insert(root_id).second) {
      return;
    }

    mPendingRequests.push_back(MakeRetryRequest(root_id));
  }

  eos::mgm::TreeSizeRecomputeRequest
  PopNextRequest()
  {
    auto current_request = mPendingRequests.front();
    mPendingRequests.pop_front();
    mQueuedRoots.erase(current_request.rootId);
    return current_request;
  }

  void
  RunAttempt(const eos::mgm::TreeSizeRecomputeRequest& request)
  {
    ++mPassCount;
    const auto pass = mRunPass(request);
    mLastResult = pass.result;
    mLastDiagnostics = pass.diagnostics;
    UpdateRetryProgress();
  }

  bool
  LastPassFailedPermanently() const
  {
    return (mLastResult.retc != 0) && (mLastResult.retc != EAGAIN);
  }

  bool
  IsInitialRootPass(const eos::mgm::TreeSizeRecomputeRequest& current_request) const
  {
    return (current_request.rootId == mOriginalRequest.rootId) && (mPassCount == 1);
  }

  void
  RememberFirstRetryError(const std::string& error)
  {
    if (mFirstRetryError.empty()) {
      mFirstRetryError = error;
    }
  }

  bool
  LastPassCompletedWithoutRetry(const eos::mgm::TreeSizeRecomputeRequest& current_request)
  {
    if (mLastDiagnostics.retryRequired || (mLastResult.retc != 0)) {
      return false;
    }

    if ((current_request.rootId == mOriginalRequest.rootId) &&
        mLastDiagnostics.converged) {
      mPendingRequests.clear();
      mQueuedRoots.clear();
    }

    return true;
  }

  void
  EnqueueRetriesFromLastPass(eos::IContainerMD::id_t fallback_root_id)
  {
    const auto retry_root_id = mLastDiagnostics.retryRootContainerId != 0
                                   ? mLastDiagnostics.retryRootContainerId
                                   : fallback_root_id;

    if (retry_root_id != 0) {
      EnqueueRetry(retry_root_id);
      return;
    }

    for (const auto retry_id : mLastDiagnostics.retryContainerIds) {
      EnqueueRetry(retry_id);
    }
  }

  bool
  WaitBeforeRetry() const
  {
    const auto deadline = std::chrono::steady_clock::now() + kTreeSizeRecomputeRetryDelay;

    while (std::chrono::steady_clock::now() < deadline) {
      if (mStopRequested()) {
        return false;
      }

      std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }

    return true;
  }

  eos::mgm::TreeSizeRecomputeResult
  FinalResult()
  {
    if (mStopRequested()) {
      return StoppedResult();
    }

    if (mRetryLimitReached ||
        (mLastDiagnostics.available && !mLastDiagnostics.converged)) {
      UpdateRetryProgress();
      return MakeResult(0, mFirstRetryError.empty()
                               ? "tree-size recompute finished before convergence"
                               : mFirstRetryError);
    }

    return mLastResult;
  }

  const eos::mgm::TreeSizeRecomputeRequest& mOriginalRequest;
  RunPassCallback mRunPass;
  StopRequestedCallback mStopRequested;
  UpdateProgressCallback mUpdateProgress;
  std::deque<eos::mgm::TreeSizeRecomputeRequest> mPendingRequests;
  std::unordered_set<eos::IContainerMD::id_t> mQueuedRoots;
  eos::mgm::TreeSizeRecomputeResult mLastResult;
  eos::mgm::TreeSizeRecomputeDiagnostics mLastDiagnostics;
  uint64_t mPassCount = 0;
  bool mRetryLimitReached = false;
  std::string mFirstRetryError;
};

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
  mStatus.request = request;
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
TreeSizeAccountingManager::FormatStatus() const
{
  const auto status = GetStatus();
  const auto& details = status.diagnostics;
  std::ostringstream oss;
  oss << "state=" << StateToString(status.state) << " retc=" << status.retc
      << " result=" << ResultToString(status)
      << " cid=" << static_cast<unsigned long long>(status.request.rootId) << " root=\""
      << status.request.rootSpecification << "\""
      << " depth=" << status.request.maxDepth
      << " elapsed=" << StatusElapsedSeconds(status)
      << " submitted=" << status.submittedAt << " started=" << status.startedAt
      << " finished=" << status.finishedAt
      << " available=" << (details.available ? 1u : 0u)
      << " discovered_containers=" << details.discoveredContainers
      << " snapshot_containers=" << details.snapshotContainers
      << " snapshot_files=" << details.snapshotFiles
      << " publish_targets=" << details.publishTargets
      << " published_targets=" << details.publishAppliedTargets
      << " publish_attempted_targets=" << details.publishAttemptedTargets
      << " retry_candidates=" << details.retryCandidateCount
      << " missing_targets=" << details.publishMissingTargets
      << " write_failed_targets=" << details.publishWriteFailedTargets
      << " recompute_passes=" << status.recomputePassCount
      << " retry_passes=" << status.retryPassCount
      << " retry_limit_reached=" << (status.retryLimitReached ? 1u : 0u)
      << " converged=" << (details.converged ? 1u : 0u)
      << " publishable=" << (details.publishable ? 1u : 0u)
      << " retry_required=" << (details.retryRequired ? 1u : 0u)
      << " partial_publish=" << (details.partialPublish ? 1u : 0u)
      << " failed_containers=" << details.failedContainerCount
      << " publish_apply_status=" << details.publishApplyStatus
      << " fence_acquired=" << (details.fenceAcquired ? 1u : 0u);

  if (!details.retryReason.empty()) {
    oss << " retry_reason=" << details.retryReason;
  }

  if (details.retryRootContainerId != 0) {
    oss << " retry_root_cid=" << details.retryRootContainerId;
  }

  if (details.failedContainerId != 0) {
    oss << " failed_cid=" << details.failedContainerId;
  }

  if (!status.error.empty()) {
    oss << " error=\"" << status.error << "\"";
  }

  oss << '\n';

  return oss.str();
}

bool
TreeSizeAccountingManager::StopRequested() const
{
  return mStopRequested.load();
}

void
TreeSizeAccountingManager::UpdateProgress(uint64_t discovered_containers,
                                          uint64_t snapshot_containers,
                                          uint64_t snapshot_files,
                                          uint64_t publish_targets,
                                          uint64_t publish_applied_targets)
{
  std::lock_guard<std::mutex> lock(mMutex);
  mStatus.diagnostics.discoveredContainers = discovered_containers;
  mStatus.diagnostics.snapshotContainers = snapshot_containers;
  mStatus.diagnostics.snapshotFiles = snapshot_files;
  mStatus.diagnostics.publishTargets = publish_targets;
  mStatus.diagnostics.publishAppliedTargets = publish_applied_targets;
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
      } else if ((result.retc == ECANCELED) && mStopRequested.load()) {
        mStatus.state = TreeSizeRecomputeJobState::Stopped;
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
  TreeSizeRecomputeRetryRunner runner(
      request,
      [this, &container_svc, &file_svc,
       accounting_service](const TreeSizeRecomputeRequest& pass_request) {
        const auto pass = RecomputeTreeSizeOnce(pass_request, container_svc, file_svc,
                                                accounting_service);
        return TreeSizeRecomputeRetryPass{pass.result, pass.diagnostics};
      },
      [this]() { return StopRequested(); },
      [this](uint64_t recompute_pass_count, uint64_t retry_pass_count,
             bool retry_limit_reached) {
        UpdateRetryProgress(recompute_pass_count, retry_pass_count, retry_limit_reached);
      });

  return runner.Run();
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

  pass_result.result = eos::TreeSizeRecomputeEngine().Recompute(
      request, container_svc, file_svc, accounting_service, callbacks);
  return pass_result;
}

EOSMGMNAMESPACE_END
