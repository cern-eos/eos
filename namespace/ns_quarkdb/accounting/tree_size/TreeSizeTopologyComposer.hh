//------------------------------------------------------------------------------
//! @file TreeSizeTopologyComposer.hh
//! @brief Compose direct counters into subtree totals using discovered topology
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
#include "namespace/ns_quarkdb/accounting/tree_size/TreeSizeSnapshotBuilder.hh"
#include <cstdint>
#include <unordered_map>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Subtree counters matching the values published by recompute_tree_size
//------------------------------------------------------------------------------
struct TreeSizeSubtreeCounters {
  int64_t treeBytes = 0;
  int64_t treeFiles = 0;
  int64_t treeContainers = 0;
};

//------------------------------------------------------------------------------
//! Diagnostics collected while composing subtree totals
//------------------------------------------------------------------------------
struct TreeSizeTopologyComposeDiagnostics {
  uint64_t missingDirectCounters = 0;
  uint64_t missingTopology = 0;
  uint64_t cycleEdges = 0;
};

//------------------------------------------------------------------------------
//! Result of topology-aware direct-counter composition
//------------------------------------------------------------------------------
struct TreeSizeTopologyComposeResult {
  std::unordered_map<uint64_t, TreeSizeSubtreeCounters> subtreeCounters;
  TreeSizeTopologyComposeDiagnostics diagnostics;
};

//------------------------------------------------------------------------------
//! Computes subtree totals from direct counters and discovered child topology
//------------------------------------------------------------------------------
class TreeSizeTopologyComposer {
public:
  TreeSizeTopologyComposeResult Compose(const TreeSizeSnapshot& snapshot) const;

  TreeSizeTopologyComposeResult Compose(
      const TreeSizeSnapshot& snapshot,
      const std::unordered_map<uint64_t, TreeSizeDirectCounters>& direct_counters) const;
};

EOSNSNAMESPACE_END
