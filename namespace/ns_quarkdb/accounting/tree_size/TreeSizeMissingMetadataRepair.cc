//------------------------------------------------------------------------------
//! @file TreeSizeMissingMetadataRepair.cc
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

#include "namespace/ns_quarkdb/accounting/tree_size/TreeSizeMissingMetadataRepair.hh"
#include <algorithm>

EOSNSNAMESPACE_BEGIN

namespace {

bool
ParentMatches(const TreeSizeMissingMetadataReference& reference,
              const TreeSizeJournalEntry& entry)
{
  return (reference.directParentId == 0) ||
         (reference.directParentId == entry.accountingEvent.directParentId);
}

bool
MatchesObject(const TreeSizeMissingMetadataReference& reference,
              const TreeSizeJournalEntry& entry)
{
  return entry.hasAccountingMetadata && ParentMatches(reference, entry) &&
         (reference.objectId == entry.accountingEvent.objectId);
}

bool
IsFileEvent(TreeSizeAccountingEventType type)
{
  return (type == TreeSizeAccountingEventType::FileCreate) ||
         (type == TreeSizeAccountingEventType::FileDelta) ||
         (type == TreeSizeAccountingEventType::FileDelete);
}

bool
IsChildLinkEvent(TreeSizeAccountingEventType type)
{
  return (type == TreeSizeAccountingEventType::ChildAttach) ||
         (type == TreeSizeAccountingEventType::ChildDetach);
}

bool
HasMatchingDelete(const TreeSizeMissingMetadataReference& reference,
                  const TreeSizeJournalSnapshot& journal)
{
  const auto expected_type = (reference.kind == TreeSizeMissingMetadataKind::File)
                                 ? TreeSizeAccountingEventType::FileDelete
                                 : TreeSizeAccountingEventType::ChildDetach;

  return std::any_of(journal.entries.begin(), journal.entries.end(),
                     [&](const TreeSizeJournalEntry& entry) {
                       return MatchesObject(reference, entry) &&
                              (entry.accountingEvent.type == expected_type);
                     });
}

bool
SnapshotContainsChild(const TreeSizeSnapshot& snapshot, uint64_t parent_id,
                      uint64_t child_id)
{
  if (parent_id != 0) {
    const auto parent = snapshot.childContainerIds.find(parent_id);

    if (parent == snapshot.childContainerIds.end()) {
      return false;
    }

    return std::find(parent->second.begin(), parent->second.end(), child_id) !=
           parent->second.end();
  }

  for (const auto& topology : snapshot.childContainerIds) {
    if (std::find(topology.second.begin(), topology.second.end(), child_id) !=
        topology.second.end()) {
      return true;
    }
  }

  return false;
}

std::vector<TreeSizeMissingMetadataReference>
BuildUniqueReferences(
    const std::vector<TreeSizeMissingMetadataReference>& missing_references)
{
  std::vector<TreeSizeMissingMetadataReference> unique_references;
  unique_references.reserve(missing_references.size());

  for (const auto& reference : missing_references) {
    if (std::find(unique_references.begin(), unique_references.end(), reference) ==
        unique_references.end()) {
      unique_references.push_back(reference);
    }
  }

  return unique_references;
}

void
SuppressFileLifecycle(const TreeSizeMissingMetadataReference& reference,
                      const TreeSizeJournalSnapshot& journal,
                      TreeSizeMissingMetadataRepairResult& result)
{
  for (const auto& entry : journal.entries) {
    if (MatchesObject(reference, entry) && IsFileEvent(entry.accountingEvent.type)) {
      result.suppressedSequences.insert(entry.accountingEvent.sequence);
    }
  }
}

void
SuppressChildLinkLifecycle(const TreeSizeMissingMetadataReference& reference,
                           const TreeSizeSnapshot& snapshot,
                           const TreeSizeJournalSnapshot& journal,
                           TreeSizeMissingMetadataRepairResult& result)
{
  const bool snapshot_still_links_child =
      SnapshotContainsChild(snapshot, reference.directParentId, reference.objectId);

  for (const auto& entry : journal.entries) {
    if (!MatchesObject(reference, entry) ||
        !IsChildLinkEvent(entry.accountingEvent.type)) {
      continue;
    }

    if ((entry.accountingEvent.type == TreeSizeAccountingEventType::ChildAttach) ||
        !snapshot_still_links_child) {
      result.suppressedSequences.insert(entry.accountingEvent.sequence);
    }
  }
}

} // namespace

bool
operator==(const TreeSizeMissingMetadataReference& lhs,
           const TreeSizeMissingMetadataReference& rhs)
{
  return (lhs.kind == rhs.kind) && (lhs.directParentId == rhs.directParentId) &&
         (lhs.objectId == rhs.objectId);
}

TreeSizeMissingMetadataRepairResult
TreeSizeMissingMetadataRepair::Repair(
    const std::vector<TreeSizeMissingMetadataReference>& missing_references,
    const TreeSizeSnapshot& snapshot, const TreeSizeJournalSnapshot& journal) const
{
  TreeSizeMissingMetadataRepairResult result;
  const auto unique_references = BuildUniqueReferences(missing_references);
  result.missingReferences = unique_references.size();

  for (const auto& reference : unique_references) {
    if (!HasMatchingDelete(reference, journal)) {
      ++result.unresolvedReferences;
      continue;
    }

    ++result.resolvedReferences;

    if (reference.kind == TreeSizeMissingMetadataKind::File) {
      SuppressFileLifecycle(reference, journal, result);
    } else {
      SuppressChildLinkLifecycle(reference, snapshot, journal, result);
    }
  }

  result.suppressedJournalEntries = result.suppressedSequences.size();
  return result;
}

EOSNSNAMESPACE_END
