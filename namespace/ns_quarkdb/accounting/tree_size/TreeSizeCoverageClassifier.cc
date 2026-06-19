//------------------------------------------------------------------------------
//! @file TreeSizeCoverageClassifier.cc
//! @brief Tree-size journal coverage classification
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

#include "namespace/ns_quarkdb/accounting/tree_size/TreeSizeCoverageClassifier.hh"

EOSNSNAMESPACE_BEGIN

namespace {

enum class TreeSizeCoverageClass {
  MissingMetadata = 0,
  Covered,
  OutsideCoverage,
  PostDiscoveryTopology
};

void
CountCoverage(TreeSizeCoverageClass coverage, TreeSizeCoverageDiagnostics& diagnostics)
{
  switch (coverage) {
  case TreeSizeCoverageClass::MissingMetadata:
    ++diagnostics.missingMetadataEntries;
    break;
  case TreeSizeCoverageClass::Covered:
    ++diagnostics.coveredEntries;
    break;
  case TreeSizeCoverageClass::OutsideCoverage:
    ++diagnostics.outsideCoverageEntries;
    break;
  case TreeSizeCoverageClass::PostDiscoveryTopology:
    ++diagnostics.postDiscoveryTopologyEntries;
    break;
  }
}

} // namespace

TreeSizeCoverageDiagnostics
TreeSizeCoverageClassifier::Classify(
    const std::unordered_set<uint64_t>& covered_container_ids,
    const TreeSizeJournalSnapshot& journal) const
{
  TreeSizeCoverageDiagnostics diagnostics;
  std::unordered_set<uint64_t> active_post_discovery_ids;
  std::unordered_set<uint64_t> observed_post_discovery_ids;

  for (const auto& entry : journal.entries) {
    TreeSizeCoverageClass coverage = TreeSizeCoverageClass::MissingMetadata;

    if (!entry.hasAccountingMetadata) {
      CountCoverage(coverage, diagnostics);
      continue;
    }

    const auto parent_id = entry.accountingEvent.directParentId;
    const auto object_id = entry.accountingEvent.objectId;
    const bool parent_covered = covered_container_ids.count(parent_id) != 0;
    const bool parent_post_discovery = active_post_discovery_ids.count(parent_id) != 0;
    const bool object_covered = covered_container_ids.count(object_id) != 0;
    const bool object_post_discovery = active_post_discovery_ids.count(object_id) != 0;

    if (parent_post_discovery) {
      coverage = TreeSizeCoverageClass::PostDiscoveryTopology;
    } else if (!parent_covered) {
      coverage = TreeSizeCoverageClass::OutsideCoverage;
    } else if ((entry.accountingEvent.type == TreeSizeAccountingEventType::ChildAttach) &&
               !object_covered) {
      coverage = TreeSizeCoverageClass::PostDiscoveryTopology;
    } else if ((entry.accountingEvent.type == TreeSizeAccountingEventType::ChildDetach) &&
               object_post_discovery) {
      coverage = TreeSizeCoverageClass::PostDiscoveryTopology;
    } else {
      coverage = TreeSizeCoverageClass::Covered;
    }

    if ((coverage == TreeSizeCoverageClass::PostDiscoveryTopology) &&
        (entry.accountingEvent.type == TreeSizeAccountingEventType::ChildAttach) &&
        !object_covered) {
      active_post_discovery_ids.insert(object_id);
      observed_post_discovery_ids.insert(object_id);
    }

    if ((entry.accountingEvent.type == TreeSizeAccountingEventType::ChildDetach) &&
        object_post_discovery) {
      active_post_discovery_ids.erase(object_id);
    }

    CountCoverage(coverage, diagnostics);
  }

  diagnostics.postDiscoveryContainerIds = observed_post_discovery_ids.size();
  return diagnostics;
}

EOSNSNAMESPACE_END
