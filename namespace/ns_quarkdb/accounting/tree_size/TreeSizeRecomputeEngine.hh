//------------------------------------------------------------------------------
//! @file TreeSizeRecomputeEngine.hh
//! @brief Synchronous tree-size recompute workflow
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

#include "namespace/Namespace.hh"
#include "namespace/interface/IContainerMD.hh"
#include <cstdint>
#include <functional>
#include <string>
#include <vector>

EOSNSNAMESPACE_BEGIN

class IContainerMDSvc;
class IFileMDSvc;
class ITreeSizeAccountingService;

//------------------------------------------------------------------------------
//! Input for one synchronous recompute attempt.
//------------------------------------------------------------------------------
struct TreeSizeRecomputeRequest {
  IContainerMD::id_t rootId = 0;
  std::string rootSpecification;
  uint32_t maxDepth = 0;
};

//------------------------------------------------------------------------------
//! Result of one synchronous recompute attempt.
//------------------------------------------------------------------------------
struct TreeSizeRecomputeResult {
  int retc = 0;
  std::string error;
};

//------------------------------------------------------------------------------
//! Detailed diagnostics for operator status and developer detail output.
//------------------------------------------------------------------------------
struct TreeSizeRecomputeDiagnostics {
  bool available = false;
  bool converged = false;
  bool publishable = false;
  bool retryRequired = false;
  bool partialPublish = false;
  std::string retryReason;
  uint64_t retryCandidateCount = 0;
  uint64_t retryRootContainerId = 0;
  uint64_t failedContainerCount = 0;
  uint64_t failedContainerId = 0;
  uint64_t discoveredContainers = 0;
  uint64_t snapshotFiles = 0;
  uint64_t snapshotContainers = 0;
  uint64_t discoveryMissingMetadata = 0;
  uint64_t discoveryResolvedMetadata = 0;
  uint64_t discoveryUnresolvedMetadata = 0;
  uint64_t journalEntries = 0;
  uint64_t baselineSequence = 0;
  uint64_t journalLatestSequence = 0;
  uint64_t fenceValidatedSequence = 0;
  uint64_t decisionReasonMask = 0;
  std::string decisionReasons;
  uint64_t journalMissingMetadata = 0;
  uint64_t nonIncreasingSequence = 0;
  uint64_t reconcileMissingMetadata = 0;
  uint64_t reconcileSuppressedEntries = 0;
  uint64_t unknownParents = 0;
  uint64_t negativeCounters = 0;
  uint64_t unsupportedEvents = 0;
  uint64_t coverageMissingMetadata = 0;
  uint64_t coverageCoveredEntries = 0;
  uint64_t coverageOutsideEntries = 0;
  uint64_t coveragePostDiscoveryEntries = 0;
  uint64_t coveragePostDiscoveryContainerIds = 0;
  uint64_t composeMissingDirectCounters = 0;
  uint64_t composeMissingTopology = 0;
  uint64_t composeCycleEdges = 0;
  uint64_t publishTargets = 0;
  uint64_t publishOrderEntries = 0;
  uint64_t publishMissingCounters = 0;
  uint64_t publishNegativeCounters = 0;
  uint64_t publishDuplicateTargets = 0;
  uint64_t publishUnplannedCounters = 0;
  uint32_t publishApplyStatus = 0;
  uint64_t publishAttemptedTargets = 0;
  uint64_t publishAppliedTargets = 0;
  uint64_t publishSkippedMissingTargets = 0;
  uint64_t publishMissingTargets = 0;
  uint64_t publishWriteFailedTargets = 0;
  std::vector<IContainerMD::id_t> retryContainerIds;
  std::vector<IContainerMD::id_t> missingContainerIds;
  std::vector<IContainerMD::id_t> writeFailedContainerIds;
  bool fenceAvailable = false;
  bool fenceAttempted = false;
  bool fenceAcquired = false;
  bool fenceWaitTimeout = false;
  uint64_t fenceCoveredIds = 0;
  uint64_t fenceIncludedUpdates = 0;
  uint64_t fenceIncludedSubtreeAttachUpdates = 0;
  uint64_t fenceIncludedSubtreeDetachUpdates = 0;
  uint64_t fenceReplayAfterUpdates = 0;
  uint64_t fenceUnsequencedUpdates = 0;
  uint64_t fencePassedThroughUpdates = 0;
  uint64_t fenceDrainedRawUpdates = 0;
  uint64_t fenceDrainedBatchUpdates = 0;
  uint64_t fenceInFlightCoveredUpdates = 0;
};

//------------------------------------------------------------------------------
//! Optional callbacks used by MGM status tracking and namespace tests.
//------------------------------------------------------------------------------
struct TreeSizeRecomputeEngineCallbacks {
  std::function<bool()> stopRequested;
  std::function<void(const std::string&)> updatePhase;
  std::function<void(uint64_t discovered_containers, uint64_t snapshot_containers,
                     uint64_t snapshot_files, uint64_t publish_targets,
                     uint64_t publish_applied_targets)>
      updateProgress;
  std::function<void(const TreeSizeRecomputeDiagnostics&)> updateDiagnostics;
};

//------------------------------------------------------------------------------
//! Runs one synchronous tree-size recompute attempt over namespace services.
//------------------------------------------------------------------------------
class TreeSizeRecomputeEngine {
public:
  TreeSizeRecomputeResult
  Recompute(const TreeSizeRecomputeRequest& request, IContainerMDSvc& container_svc,
            IFileMDSvc& file_svc, ITreeSizeAccountingService* accounting_service,
            const TreeSizeRecomputeEngineCallbacks& callbacks = {}) const;
};

EOSNSNAMESPACE_END
