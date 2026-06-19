//------------------------------------------------------------------------------
//! @file TreeSizeTopologyComposer.cc
//! @brief Topology-aware tree-size subtree counter composition
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

#include "namespace/ns_quarkdb/accounting/tree_size/TreeSizeTopologyComposer.hh"
#include <unordered_set>
#include <vector>

EOSNSNAMESPACE_BEGIN

namespace {

struct ComposeFrame {
  uint64_t id = 0;
  bool expanded = false;
};

TreeSizeSubtreeCounters
DirectCountersToSubtreeCounters(const TreeSizeDirectCounters& counters)
{
  TreeSizeSubtreeCounters subtree;
  subtree.treeBytes = counters.fileBytes;
  subtree.treeFiles = counters.fileCount;
  subtree.treeContainers = counters.childContainerCount;
  return subtree;
}

void
AddCounters(TreeSizeSubtreeCounters& target, const TreeSizeSubtreeCounters& source)
{
  target.treeBytes += source.treeBytes;
  target.treeFiles += source.treeFiles;
  target.treeContainers += source.treeContainers;
}

void
ComposeContainer(
    uint64_t root_id,
    const std::unordered_map<uint64_t, TreeSizeDirectCounters>& direct_counters,
    const std::unordered_map<uint64_t, std::vector<uint64_t>>& child_container_ids,
    std::unordered_map<uint64_t, TreeSizeSubtreeCounters>& subtree_counters,
    TreeSizeTopologyComposeDiagnostics& diagnostics)
{
  std::vector<ComposeFrame> stack;
  std::unordered_set<uint64_t> visiting;
  stack.push_back(ComposeFrame{root_id, false});

  while (!stack.empty()) {
    const auto frame = stack.back();
    stack.pop_back();

    if (subtree_counters.count(frame.id) != 0) {
      continue;
    }

    if (frame.expanded) {
      TreeSizeSubtreeCounters totals;
      const auto direct = direct_counters.find(frame.id);

      if (direct == direct_counters.end()) {
        ++diagnostics.missingDirectCounters;
      } else {
        totals = DirectCountersToSubtreeCounters(direct->second);
      }

      const auto children = child_container_ids.find(frame.id);

      if (children == child_container_ids.end()) {
        ++diagnostics.missingTopology;
      } else {
        for (const auto child_id : children->second) {
          const auto child_totals = subtree_counters.find(child_id);

          if (child_totals != subtree_counters.end()) {
            AddCounters(totals, child_totals->second);
          }
        }
      }

      visiting.erase(frame.id);
      subtree_counters[frame.id] = totals;
      continue;
    }

    if (!visiting.insert(frame.id).second) {
      ++diagnostics.cycleEdges;
      continue;
    }

    stack.push_back(ComposeFrame{frame.id, true});
    const auto children = child_container_ids.find(frame.id);

    if (children == child_container_ids.end()) {
      continue;
    }

    for (auto it = children->second.rbegin(); it != children->second.rend(); ++it) {
      if (subtree_counters.count(*it) != 0) {
        continue;
      }

      if (visiting.count(*it) != 0) {
        ++diagnostics.cycleEdges;
        continue;
      }

      stack.push_back(ComposeFrame{*it, false});
    }
  }
}

} // namespace

TreeSizeTopologyComposeResult
TreeSizeTopologyComposer::Compose(const TreeSizeSnapshot& snapshot) const
{
  return Compose(snapshot, snapshot.directCounters);
}

TreeSizeTopologyComposeResult
TreeSizeTopologyComposer::Compose(
    const TreeSizeSnapshot& snapshot,
    const std::unordered_map<uint64_t, TreeSizeDirectCounters>& direct_counters) const
{
  TreeSizeTopologyComposeResult result;

  for (const auto& topology : snapshot.childContainerIds) {
    ComposeContainer(topology.first, direct_counters, snapshot.childContainerIds,
                     result.subtreeCounters, result.diagnostics);
  }

  for (const auto& direct : direct_counters) {
    ComposeContainer(direct.first, direct_counters, snapshot.childContainerIds,
                     result.subtreeCounters, result.diagnostics);
  }

  return result;
}

EOSNSNAMESPACE_END
