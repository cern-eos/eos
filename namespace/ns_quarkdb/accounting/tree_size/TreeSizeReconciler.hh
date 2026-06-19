//------------------------------------------------------------------------------
//! @file TreeSizeReconciler.hh
//! @brief Replay captured tree-size accounting journal entries on snapshots
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
#include <unordered_map>
#include <unordered_set>
#include <vector>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Diagnostics collected while replaying a raw journal on a snapshot
//------------------------------------------------------------------------------
struct TreeSizeReconcileDiagnostics {
  uint64_t missingMetadata = 0;
  uint64_t suppressedEntries = 0;
  uint64_t unknownParents = 0;
  uint64_t negativeCounters = 0;
  uint64_t unsupportedEvents = 0;
};

//------------------------------------------------------------------------------
//! Optional controls for journal replay
//------------------------------------------------------------------------------
struct TreeSizeReconcileOptions {
  std::unordered_set<uint64_t> suppressedSequences;
};

//------------------------------------------------------------------------------
//! Result of direct-counter snapshot reconciliation
//------------------------------------------------------------------------------
struct TreeSizeReconcileResult {
  std::unordered_map<uint64_t, TreeSizeDirectCounters> directCounters;
  std::unordered_map<uint64_t, std::vector<uint64_t>> childContainerIds;
  TreeSizeReconcileDiagnostics diagnostics;
};

//------------------------------------------------------------------------------
//! Replays direct tree-size accounting events on a discovered snapshot
//------------------------------------------------------------------------------
class TreeSizeReconciler {
public:
  TreeSizeReconcileResult Reconcile(const TreeSizeSnapshot& snapshot,
                                    const TreeSizeJournalSnapshot& journal) const;
  TreeSizeReconcileResult Reconcile(const TreeSizeSnapshot& snapshot,
                                    const TreeSizeJournalSnapshot& journal,
                                    const TreeSizeReconcileOptions& options) const;
};

EOSNSNAMESPACE_END
