//------------------------------------------------------------------------------
//! @file TreeSizeReconciler.cc
//! @brief Tree-size direct-counter journal replay
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

#include "namespace/ns_quarkdb/accounting/tree_size/TreeSizeReconciler.hh"
#include <algorithm>
#include <utility>
#include <vector>

EOSNSNAMESPACE_BEGIN

namespace {

bool
HasNegativeCounter(const TreeSizeDirectCounters& counters)
{
  return counters.fileBytes < 0 || counters.fileCount < 0 ||
         counters.childContainerCount < 0;
}

std::vector<const TreeSizeJournalEntry*>
BuildSequencedReplayEntries(const TreeSizeJournalSnapshot& journal,
                            const TreeSizeReconcileOptions& options,
                            TreeSizeReconcileDiagnostics& diagnostics)
{
  std::vector<const TreeSizeJournalEntry*> entries;
  entries.reserve(journal.entries.size());

  for (const auto& entry : journal.entries) {
    if (!entry.hasAccountingMetadata) {
      ++diagnostics.missingMetadata;
      continue;
    }

    if (options.suppressedSequences.count(entry.accountingEvent.sequence) != 0) {
      ++diagnostics.suppressedEntries;
      continue;
    }

    entries.push_back(&entry);
  }

  std::stable_sort(entries.begin(), entries.end(),
                   [](const TreeSizeJournalEntry* lhs, const TreeSizeJournalEntry* rhs) {
                     return lhs->accountingEvent.sequence < rhs->accountingEvent.sequence;
                   });

  return entries;
}

void
AddChildContainer(
    std::unordered_map<uint64_t, std::vector<uint64_t>>& child_container_ids,
    std::unordered_map<uint64_t, TreeSizeDirectCounters>& direct_counters,
    uint64_t parent_id, uint64_t child_id)
{
  auto& children = child_container_ids[parent_id];

  if (std::find(children.begin(), children.end(), child_id) == children.end()) {
    children.push_back(child_id);
  }

  child_container_ids.emplace(child_id, std::vector<uint64_t>{});
  direct_counters.emplace(child_id, TreeSizeDirectCounters{});
}

void
RemoveChildContainer(
    std::unordered_map<uint64_t, std::vector<uint64_t>>& child_container_ids,
    uint64_t parent_id, uint64_t child_id)
{
  const auto topology = child_container_ids.find(parent_id);

  if (topology == child_container_ids.end()) {
    return;
  }

  auto& children = topology->second;
  children.erase(std::remove(children.begin(), children.end(), child_id), children.end());
}

bool
IsSubtreePropagationMarker(TreeSizeAccountingEventType type)
{
  return (type == TreeSizeAccountingEventType::SubtreeAttach) ||
         (type == TreeSizeAccountingEventType::SubtreeDetach);
}

uint64_t
LookupSnapshotSequence(const std::unordered_map<uint64_t, uint64_t>& sequences,
                       uint64_t id)
{
  const auto sequence = sequences.find(id);
  return sequence == sequences.end() ? 0 : sequence->second;
}

std::pair<bool, uint64_t>
FindFileSizeSnapshotSequence(const TreeSizeSnapshot& snapshot, uint64_t parent_id,
                             uint64_t file_id)
{
  const auto parent = snapshot.fileSizeSnapshotSequences.find(parent_id);

  if (parent == snapshot.fileSizeSnapshotSequences.end()) {
    return std::make_pair(false, 0);
  }

  const auto file = parent->second.find(file_id);

  if (file == parent->second.end()) {
    return std::make_pair(false, 0);
  }

  return std::make_pair(true, file->second);
}

bool
ShouldReplayAgainstSnapshot(const TreeSizeSnapshot& snapshot,
                            const TreeSizeJournalEntry& entry)
{
  const auto sequence = entry.accountingEvent.sequence;
  const auto parent_id = entry.accountingEvent.directParentId;
  const auto object_id = entry.accountingEvent.objectId;

  switch (entry.accountingEvent.type) {
  case TreeSizeAccountingEventType::FileDelta: {
    const auto file_sequence =
        FindFileSizeSnapshotSequence(snapshot, parent_id, object_id);

    if (file_sequence.first) {
      return sequence > file_sequence.second;
    }

    return sequence >
           LookupSnapshotSequence(snapshot.fileMembershipSnapshotSequences, parent_id);
  }
  case TreeSizeAccountingEventType::FileCreate:
  case TreeSizeAccountingEventType::FileDelete:
    return sequence >
           LookupSnapshotSequence(snapshot.fileMembershipSnapshotSequences, parent_id);
  case TreeSizeAccountingEventType::ChildAttach:
  case TreeSizeAccountingEventType::ChildDetach:
    return sequence >
           LookupSnapshotSequence(snapshot.childMembershipSnapshotSequences, parent_id);
  default:
    return true;
  }
}

} // namespace

TreeSizeReconcileResult
TreeSizeReconciler::Reconcile(const TreeSizeSnapshot& snapshot,
                              const TreeSizeJournalSnapshot& journal) const
{
  return Reconcile(snapshot, journal, TreeSizeReconcileOptions{});
}

TreeSizeReconcileResult
TreeSizeReconciler::Reconcile(const TreeSizeSnapshot& snapshot,
                              const TreeSizeJournalSnapshot& journal,
                              const TreeSizeReconcileOptions& options) const
{
  TreeSizeReconcileResult result;
  // Start from the private discovery snapshot. Journal replay then applies only
  // mutations that happened while discovery was in progress.
  result.directCounters = snapshot.directCounters;
  result.childContainerIds = snapshot.childContainerIds;

  const auto replay_entries =
      BuildSequencedReplayEntries(journal, options, result.diagnostics);

  for (const auto* entry : replay_entries) {
    if (IsSubtreePropagationMarker(entry->accountingEvent.type)) {
      continue;
    }

    if (!ShouldReplayAgainstSnapshot(snapshot, *entry)) {
      continue;
    }

    // Direct counters are owned by the container that directly contains the
    // changed file or child container. Subtree propagation is intentionally left
    // to a later topology-aware composition step.
    const auto parent_id = entry->accountingEvent.directParentId;
    auto counters = result.directCounters.find(parent_id);

    // A parent missing from the discovered snapshot means the journal saw a
    // mutation outside, or racing with, the discovered coverage. Keep replaying
    // into a diagnostic bucket so validation accounting stays live and
    // inspectable.
    if (counters == result.directCounters.end()) {
      ++result.diagnostics.unknownParents;
      counters = result.directCounters.emplace(parent_id, TreeSizeDirectCounters{}).first;
    }

    // Each supported event updates only the direct counter dimension it owns.
    // File size changes do not alter file count; child attach/detach does not
    // alter file counters at this direct-counter layer.
    switch (entry->accountingEvent.type) {
    case TreeSizeAccountingEventType::FileCreate:
    case TreeSizeAccountingEventType::FileDelete:
      counters->second.fileBytes += entry->treeChange.dsize;
      counters->second.fileCount += entry->treeChange.dtreefiles;
      break;
    case TreeSizeAccountingEventType::FileDelta:
      counters->second.fileBytes += entry->treeChange.dsize;
      break;
    case TreeSizeAccountingEventType::ChildAttach:
      counters->second.childContainerCount += entry->treeChange.dtreecontainers;
      AddChildContainer(result.childContainerIds, result.directCounters, parent_id,
                        entry->accountingEvent.objectId);
      break;
    case TreeSizeAccountingEventType::ChildDetach:
      counters->second.childContainerCount += entry->treeChange.dtreecontainers;
      RemoveChildContainer(result.childContainerIds, parent_id,
                           entry->accountingEvent.objectId);
      break;
    default:
      ++result.diagnostics.unsupportedEvents;
      break;
    }

    // Negative counters indicate an inconsistent replay window or mutation
    // stream. They are recorded as protocol diagnostics rather than aborting the
    // recompute pipeline immediately.
    const auto updated_counters = result.directCounters.find(parent_id);

    if ((updated_counters != result.directCounters.end()) &&
        HasNegativeCounter(updated_counters->second)) {
      ++result.diagnostics.negativeCounters;
    }
  }

  return result;
}

EOSNSNAMESPACE_END
