//------------------------------------------------------------------------------
//! @file TreeSizeMissingMetadataRepair.hh
//! @brief Repair discovery metadata misses with journaled delete evidence
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
#include "namespace/ns_quarkdb/accounting/tree_size/TreeSizeSnapshotBuilder.hh"
#include <cstdint>
#include <unordered_set>
#include <vector>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Object kind for metadata references that failed to load during discovery
//------------------------------------------------------------------------------
enum class TreeSizeMissingMetadataKind { File = 0, Container };

//------------------------------------------------------------------------------
//! Metadata reference observed in a parent listing but not loadable afterwards
//------------------------------------------------------------------------------
struct TreeSizeMissingMetadataReference {
  TreeSizeMissingMetadataKind kind = TreeSizeMissingMetadataKind::File;
  uint64_t directParentId = 0;
  uint64_t objectId = 0;
};

bool operator==(const TreeSizeMissingMetadataReference& lhs,
                const TreeSizeMissingMetadataReference& rhs);

//------------------------------------------------------------------------------
//! Result of resolving discovery misses against the captured journal
//------------------------------------------------------------------------------
struct TreeSizeMissingMetadataRepairResult {
  uint64_t missingReferences = 0;
  uint64_t resolvedReferences = 0;
  uint64_t unresolvedReferences = 0;
  uint64_t suppressedJournalEntries = 0;
  std::unordered_set<uint64_t> suppressedSequences;
};

//------------------------------------------------------------------------------
//! Resolves discovery metadata misses proven by captured delete/detach events
//------------------------------------------------------------------------------
class TreeSizeMissingMetadataRepair {
public:
  TreeSizeMissingMetadataRepairResult
  Repair(const std::vector<TreeSizeMissingMetadataReference>& missing_references,
         const TreeSizeSnapshot& snapshot, const TreeSizeJournalSnapshot& journal) const;
};

EOSNSNAMESPACE_END
