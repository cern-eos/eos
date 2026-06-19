//------------------------------------------------------------------------------
//! @file TreeSizeCoverageClassifier.hh
//! @brief Classify tree-size journal entries against discovered coverage
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
#include "namespace/ns_quarkdb/accounting/tree_size/TreeSizeJournal.hh"
#include <cstdint>
#include <unordered_set>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Diagnostics collected while classifying journal coverage
//------------------------------------------------------------------------------
struct TreeSizeCoverageDiagnostics {
  uint64_t missingMetadataEntries = 0;
  uint64_t coveredEntries = 0;
  uint64_t outsideCoverageEntries = 0;
  uint64_t postDiscoveryTopologyEntries = 0;
  uint64_t postDiscoveryContainerIds = 0;
};

//------------------------------------------------------------------------------
//! Classifies raw journal entries against the discovered container coverage
//------------------------------------------------------------------------------
class TreeSizeCoverageClassifier {
public:
  TreeSizeCoverageDiagnostics
  Classify(const std::unordered_set<uint64_t>& covered_container_ids,
           const TreeSizeJournalSnapshot& journal) const;
};

EOSNSNAMESPACE_END
