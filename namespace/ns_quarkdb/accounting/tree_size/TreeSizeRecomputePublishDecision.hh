//------------------------------------------------------------------------------
//! @file TreeSizeRecomputePublishDecision.hh
//! @brief Publishability decision for one tree-size recompute result
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
#include <cstdint>
#include <string>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Diagnostics used to decide whether one recompute result may publish
//------------------------------------------------------------------------------
struct TreeSizeRecomputePublishDiagnostics {
  uint64_t missingJournalMetadata = 0;
  uint64_t missingReconcileMetadata = 0;
  uint64_t missingCoverageMetadata = 0;
  uint64_t postDiscoveryTopologyEntries = 0;
  uint64_t postDiscoveryContainerIds = 0;
  uint64_t discoveryMissingMetadata = 0;
  uint64_t resolvedDiscoveryMissingMetadata = 0;
  uint64_t unresolvedDiscoveryMissingMetadata = 0;
  uint64_t suppressedJournalEntries = 0;
  uint64_t unknownParents = 0;
  uint64_t negativeCounters = 0;
  uint64_t unsupportedEvents = 0;
  uint64_t missingDirectCounters = 0;
  uint64_t missingTopology = 0;
  uint64_t cycleEdges = 0;
  uint64_t publishMissingCounters = 0;
  uint64_t publishNegativeCounters = 0;
  uint64_t publishDuplicateTargets = 0;
  uint64_t publishUnplannedCounters = 0;
};

//------------------------------------------------------------------------------
//! Compact publishability decision for one recompute result
//------------------------------------------------------------------------------
struct TreeSizeRecomputePublishDecisionResult {
  bool publishable = true;
  bool retryRequired = false;
  uint64_t reasonMask = 0;
};

//------------------------------------------------------------------------------
//! Reason bits explaining why a recompute result must not publish
//------------------------------------------------------------------------------
struct TreeSizeRecomputePublishDecisionReasons {
  static constexpr uint64_t MissingMetadata = 1ull << 0;
  static constexpr uint64_t UnknownParents = 1ull << 1;
  static constexpr uint64_t NegativeCounters = 1ull << 2;
  static constexpr uint64_t UnsupportedEvents = 1ull << 3;
  static constexpr uint64_t MissingDirectCounters = 1ull << 4;
  static constexpr uint64_t MissingTopology = 1ull << 5;
  static constexpr uint64_t CycleEdges = 1ull << 6;
  static constexpr uint64_t DiscoveryMissingMetadata = 1ull << 12;
  static constexpr uint64_t PublishPlanInvalid = 1ull << 13;
  static constexpr uint64_t PostDiscoveryTopology = 1ull << 14;
};

//------------------------------------------------------------------------------
//! Decides whether a recompute result is closed and publishable
//------------------------------------------------------------------------------
class TreeSizeRecomputePublishDecision {
public:
  TreeSizeRecomputePublishDecisionResult
  Evaluate(const TreeSizeRecomputePublishDiagnostics& diagnostics) const;

  static std::string ReasonsToString(uint64_t reason_mask);
};

EOSNSNAMESPACE_END
