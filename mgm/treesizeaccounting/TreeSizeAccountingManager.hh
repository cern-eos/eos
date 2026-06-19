//------------------------------------------------------------------------------
//! @file TreeSizeAccountingManager.hh
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

#pragma once

#include "common/AssistedThread.hh"
#include "mgm/Namespace.hh"
#include "namespace/interface/IContainerMD.hh"
#include "namespace/ns_quarkdb/accounting/tree_size/TreeSizeRecomputeEngine.hh"
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <optional>
#include <string>

namespace eos {
class IContainerMDSvc;
class IFileMDSvc;
class ITreeSizeAccountingService;
} // namespace eos

EOSMGMNAMESPACE_BEGIN

struct TreeSizeRecomputeRequest {
  eos::IContainerMD::id_t rootId = 0;
  std::string rootSpecification;
  uint32_t maxDepth = 0;
};

struct TreeSizeRecomputeResult {
  int retc = 0;
  std::string error;
};

using TreeSizeRecomputeDiagnostics = eos::TreeSizeRecomputeDiagnostics;

enum class TreeSizeRecomputeJobState {
  Idle,
  Queued,
  Running,
  Completed,
  Failed,
  Stopped
};

struct TreeSizeRecomputeStatus {
  TreeSizeRecomputeJobState state = TreeSizeRecomputeJobState::Idle;
  std::string phase = "idle";
  eos::IContainerMD::id_t rootId = 0;
  std::string rootSpecification;
  uint32_t maxDepth = 0;
  int retc = 0;
  std::string error;
  uint64_t submittedAt = 0;
  uint64_t startedAt = 0;
  uint64_t finishedAt = 0;
  uint64_t discoveredContainers = 0;
  uint64_t snapshotContainers = 0;
  uint64_t snapshotFiles = 0;
  uint64_t publishTargets = 0;
  uint64_t publishAttemptedTargets = 0;
  uint64_t publishAppliedTargets = 0;
  uint64_t missingTargetCount = 0;
  uint64_t writeFailedTargetCount = 0;
  uint64_t recomputePassCount = 0;
  uint64_t retryPassCount = 0;
  bool retryLimitReached = false;
  bool converged = false;
  bool partialPublish = false;
  std::string retryReason;
  uint64_t retryCandidateCount = 0;
  uint64_t retryRootContainerId = 0;
  uint64_t failedContainerCount = 0;
  uint64_t failedContainerId = 0;
  TreeSizeRecomputeDiagnostics diagnostics;
};

struct TreeSizeRecomputeSubmitResult {
  int retc = 0;
  std::string message;
};

//------------------------------------------------------------------------------
//! MGM-owned entry point for tree-size recomputation.
//------------------------------------------------------------------------------
class TreeSizeAccountingManager {
public:
  TreeSizeAccountingManager() = default;
  ~TreeSizeAccountingManager();

  TreeSizeAccountingManager(const TreeSizeAccountingManager&) = delete;
  TreeSizeAccountingManager& operator=(const TreeSizeAccountingManager&) = delete;

  void StartWorker();
  void StopWorker();

  TreeSizeRecomputeSubmitResult
  SubmitRecompute(const TreeSizeRecomputeRequest& request,
                  eos::IContainerMDSvc& container_svc, eos::IFileMDSvc& file_svc,
                  eos::ITreeSizeAccountingService* accounting_service);

  TreeSizeRecomputeStatus GetStatus() const;
  std::string FormatStatus(bool detail = false) const;

private:
  struct WorkerJob {
    TreeSizeRecomputeRequest request;
    eos::IContainerMDSvc* containerSvc = nullptr;
    eos::IFileMDSvc* fileSvc = nullptr;
    eos::ITreeSizeAccountingService* accountingService = nullptr;
  };

  struct RecomputePassResult {
    TreeSizeRecomputeResult result;
    TreeSizeRecomputeDiagnostics diagnostics;
  };

  TreeSizeRecomputeResult
  RecomputeTreeSize(const TreeSizeRecomputeRequest& request,
                    eos::IContainerMDSvc& container_svc, eos::IFileMDSvc& file_svc,
                    eos::ITreeSizeAccountingService* accounting_service);
  RecomputePassResult
  RecomputeTreeSizeOnce(const TreeSizeRecomputeRequest& request,
                        eos::IContainerMDSvc& container_svc, eos::IFileMDSvc& file_svc,
                        eos::ITreeSizeAccountingService* accounting_service);

  void WorkerLoop(ThreadAssistant& assistant) noexcept;
  bool StopRequested() const;
  void UpdatePhase(const std::string& phase);
  void UpdateProgress(uint64_t discovered_containers, uint64_t snapshot_containers,
                      uint64_t snapshot_files, uint64_t publish_targets,
                      uint64_t publish_applied_targets);
  void UpdateRetryProgress(uint64_t recompute_pass_count, uint64_t retry_pass_count,
                           bool retry_limit_reached);
  void UpdateDiagnostics(const TreeSizeRecomputeDiagnostics& diagnostics);

  mutable std::mutex mMutex;
  std::condition_variable mCondition;
  AssistedThread mWorker;
  std::optional<WorkerJob> mPendingJob;
  TreeSizeRecomputeStatus mStatus;
  std::atomic<bool> mStopRequested{false};
  bool mWorkerStarted = false;
};

EOSMGMNAMESPACE_END
