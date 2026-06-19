//------------------------------------------------------------------------------
//! @file TreeSizeRecomputePublishDecision.cc
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

#include "namespace/ns_quarkdb/accounting/tree_size/TreeSizeRecomputePublishDecision.hh"
#include <cstddef>
#include <vector>

EOSNSNAMESPACE_BEGIN

namespace {

void
AppendReason(std::vector<std::string>& reasons, uint64_t reason_mask, uint64_t reason,
             const char* reason_name)
{
  if (reason_mask & reason) {
    reasons.emplace_back(reason_name);
  }
}

std::string
JoinReasons(const std::vector<std::string>& reasons)
{
  if (reasons.empty()) {
    return "none";
  }

  std::string result = reasons.front();

  for (std::size_t i = 1; i < reasons.size(); ++i) {
    result += ",";
    result += reasons[i];
  }

  return result;
}

} // namespace

TreeSizeRecomputePublishDecisionResult
TreeSizeRecomputePublishDecision::Evaluate(
    const TreeSizeRecomputePublishDiagnostics& diagnostics) const
{
  using Reasons = TreeSizeRecomputePublishDecisionReasons;

  uint64_t reason_mask = 0;

  if (diagnostics.missingJournalMetadata || diagnostics.missingReconcileMetadata ||
      diagnostics.missingCoverageMetadata) {
    reason_mask |= Reasons::MissingMetadata;
  }

  if (diagnostics.unresolvedDiscoveryMissingMetadata) {
    reason_mask |= Reasons::DiscoveryMissingMetadata;
  }

  if (diagnostics.postDiscoveryTopologyEntries || diagnostics.postDiscoveryContainerIds) {
    reason_mask |= Reasons::PostDiscoveryTopology;
  }

  if (diagnostics.unknownParents) {
    reason_mask |= Reasons::UnknownParents;
  }

  if (diagnostics.negativeCounters) {
    reason_mask |= Reasons::NegativeCounters;
  }

  if (diagnostics.unsupportedEvents) {
    reason_mask |= Reasons::UnsupportedEvents;
  }

  if (diagnostics.missingDirectCounters) {
    reason_mask |= Reasons::MissingDirectCounters;
  }

  if (diagnostics.missingTopology) {
    reason_mask |= Reasons::MissingTopology;
  }

  if (diagnostics.cycleEdges) {
    reason_mask |= Reasons::CycleEdges;
  }

  if (diagnostics.publishMissingCounters || diagnostics.publishNegativeCounters ||
      diagnostics.publishDuplicateTargets || diagnostics.publishUnplannedCounters) {
    reason_mask |= Reasons::PublishPlanInvalid;
  }

  return TreeSizeRecomputePublishDecisionResult{reason_mask == 0, reason_mask != 0,
                                                reason_mask};
}

std::string
TreeSizeRecomputePublishDecision::ReasonsToString(uint64_t reason_mask)
{
  using Reasons = TreeSizeRecomputePublishDecisionReasons;

  std::vector<std::string> reasons;
  reasons.reserve(10);

  AppendReason(reasons, reason_mask, Reasons::MissingMetadata, "missing_metadata");
  AppendReason(reasons, reason_mask, Reasons::DiscoveryMissingMetadata,
               "discovery_missing_metadata");
  AppendReason(reasons, reason_mask, Reasons::PostDiscoveryTopology,
               "post_discovery_topology");
  AppendReason(reasons, reason_mask, Reasons::UnknownParents, "unknown_parents");
  AppendReason(reasons, reason_mask, Reasons::NegativeCounters, "negative_counters");
  AppendReason(reasons, reason_mask, Reasons::UnsupportedEvents, "unsupported_events");
  AppendReason(reasons, reason_mask, Reasons::MissingDirectCounters,
               "missing_direct_counters");
  AppendReason(reasons, reason_mask, Reasons::MissingTopology, "missing_topology");
  AppendReason(reasons, reason_mask, Reasons::CycleEdges, "cycle_edges");
  AppendReason(reasons, reason_mask, Reasons::PublishPlanInvalid, "publish_plan_invalid");

  return JoinReasons(reasons);
}

EOSNSNAMESPACE_END
