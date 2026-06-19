//------------------------------------------------------------------------------
//! @file TreeSizePublisher.hh
//! @brief Build deterministic publish targets for tree-size recomputation
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
#include "namespace/ns_quarkdb/accounting/tree_size/TreeSizeTopologyComposer.hh"
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

EOSNSNAMESPACE_BEGIN

class IContainerMDSvc;

//------------------------------------------------------------------------------
//! One validated container counter update selected for future publication
//------------------------------------------------------------------------------
struct TreeSizePublishTarget {
  uint64_t containerId = 0;
  TreeSizeSubtreeCounters counters;
};

//------------------------------------------------------------------------------
//! Diagnostics collected while building a publish plan
//------------------------------------------------------------------------------
struct TreeSizePublishDiagnostics {
  uint64_t missingCounters = 0;
  uint64_t negativeCounters = 0;
  uint64_t duplicateTargets = 0;
  uint64_t unplannedCounters = 0;
};

//------------------------------------------------------------------------------
//! Deterministic publish targets plus any validation diagnostics
//------------------------------------------------------------------------------
struct TreeSizePublishPlanResult {
  std::vector<TreeSizePublishTarget> targets;
  TreeSizePublishDiagnostics diagnostics;
};

//------------------------------------------------------------------------------
//! Status of applying validated tree-size counters to namespace metadata
//------------------------------------------------------------------------------
enum class TreeSizePublishApplyStatus {
  Success = 0,
  PrePublishFailed,
  PartialPublishFailed
};

//------------------------------------------------------------------------------
//! Result of applying validated tree-size counters to namespace metadata
//------------------------------------------------------------------------------
struct TreeSizePublishApplyResult {
  TreeSizePublishApplyStatus status = TreeSizePublishApplyStatus::Success;
  std::vector<uint64_t> publishedContainerIds;
  std::vector<uint64_t> retryContainerIds;
  std::vector<uint64_t> missingContainerIds;
  std::vector<uint64_t> writeFailedContainerIds;
  uint64_t attemptedTargets = 0;
  uint64_t skippedMissingTargets = 0;
  uint64_t failedContainerId = 0;
  std::string error;
};

//------------------------------------------------------------------------------
//! Status returned by the low-level counter publish sink
//------------------------------------------------------------------------------
enum class TreeSizeCounterPublishStatus { Published = 0, SkippedMissing, Failed };

//------------------------------------------------------------------------------
//! Result returned by the low-level counter publish sink
//------------------------------------------------------------------------------
struct TreeSizeCounterPublishResult {
  TreeSizeCounterPublishStatus status = TreeSizeCounterPublishStatus::Published;
  std::string error;
};

//------------------------------------------------------------------------------
//! Sink that applies absolute tree-size counters
//------------------------------------------------------------------------------
class ITreeSizeCounterPublisher {
public:
  virtual ~ITreeSizeCounterPublisher() = default;

  virtual TreeSizeCounterPublishResult
  PublishTreeSizeCounters(const TreeSizePublishTarget& target) = 0;
};

//------------------------------------------------------------------------------
//! Applies absolute tree-size counters to container metadata
//------------------------------------------------------------------------------
class TreeSizeMetadataPublisher : public ITreeSizeCounterPublisher {
public:
  explicit TreeSizeMetadataPublisher(IContainerMDSvc& container_svc);

  TreeSizeCounterPublishResult
  PublishTreeSizeCounters(const TreeSizePublishTarget& target) override;

private:
  IContainerMDSvc& mContainerSvc;
};

//------------------------------------------------------------------------------
//! Builds a deterministic, non-mutating tree-size publish plan
//------------------------------------------------------------------------------
class TreeSizePublisher {
public:
  TreeSizePublishPlanResult
  Plan(const std::vector<uint64_t>& publish_order,
       const std::unordered_map<uint64_t, TreeSizeSubtreeCounters>& counters) const;

  TreeSizePublishApplyResult Apply(const TreeSizePublishPlanResult& plan,
                                   ITreeSizeCounterPublisher& publisher) const;
};

EOSNSNAMESPACE_END
