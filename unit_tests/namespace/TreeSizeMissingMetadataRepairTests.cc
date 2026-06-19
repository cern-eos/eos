//------------------------------------------------------------------------------
// File: TreeSizeMissingMetadataRepairTests.cc
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
#include <gtest/gtest.h>
#include <vector>

using eos::TreeInfos;
using eos::TreeSizeAccountingEvent;
using eos::TreeSizeAccountingEventType;
using eos::TreeSizeDirectCounters;
using eos::TreeSizeJournalEntry;
using eos::TreeSizeJournalSnapshot;
using eos::TreeSizeMissingMetadataKind;
using eos::TreeSizeMissingMetadataReference;
using eos::TreeSizeMissingMetadataRepair;
using eos::TreeSizeSnapshot;

namespace {

TreeSizeJournalEntry
MakeEntry(uint64_t sequence, TreeSizeAccountingEventType type, uint64_t parent_id,
          uint64_t object_id)
{
  TreeSizeJournalEntry entry;
  entry.accountingEvent = TreeSizeAccountingEvent{sequence, type, parent_id, object_id};
  entry.treeChange = TreeInfos{0, 0, 0};
  return entry;
}

} // namespace

TEST(TreeSizeMissingMetadataRepair, ResolvesMissingFileWithCapturedDelete)
{
  TreeSizeSnapshot snapshot;
  snapshot.directCounters.emplace(7, TreeSizeDirectCounters{0, 0, 0});

  TreeSizeJournalSnapshot journal;
  journal.entries.emplace_back(
      MakeEntry(1, TreeSizeAccountingEventType::FileDelete, 7, 99));

  const auto result = TreeSizeMissingMetadataRepair().Repair(
      {TreeSizeMissingMetadataReference{TreeSizeMissingMetadataKind::File, 7, 99}},
      snapshot, journal);

  EXPECT_EQ(1ull, result.missingReferences);
  EXPECT_EQ(1ull, result.resolvedReferences);
  EXPECT_EQ(0ull, result.unresolvedReferences);
  EXPECT_EQ(1ull, result.suppressedJournalEntries);
  EXPECT_EQ(1u, result.suppressedSequences.count(1));
}

TEST(TreeSizeMissingMetadataRepair, SuppressesFullFileLifecycleAfterCapturedDelete)
{
  TreeSizeSnapshot snapshot;
  snapshot.directCounters.emplace(7, TreeSizeDirectCounters{0, 0, 0});

  TreeSizeJournalSnapshot journal;
  journal.entries.emplace_back(
      MakeEntry(1, TreeSizeAccountingEventType::FileCreate, 7, 99));
  journal.entries.emplace_back(
      MakeEntry(2, TreeSizeAccountingEventType::FileDelta, 7, 99));
  journal.entries.emplace_back(
      MakeEntry(3, TreeSizeAccountingEventType::FileDelete, 7, 99));

  const auto result = TreeSizeMissingMetadataRepair().Repair(
      {TreeSizeMissingMetadataReference{TreeSizeMissingMetadataKind::File, 7, 99}},
      snapshot, journal);

  EXPECT_EQ(1ull, result.resolvedReferences);
  EXPECT_EQ(3ull, result.suppressedJournalEntries);
  EXPECT_EQ(1u, result.suppressedSequences.count(1));
  EXPECT_EQ(1u, result.suppressedSequences.count(2));
  EXPECT_EQ(1u, result.suppressedSequences.count(3));
}

TEST(TreeSizeMissingMetadataRepair, LeavesMissingFileUnresolvedWithoutDelete)
{
  TreeSizeSnapshot snapshot;
  snapshot.directCounters.emplace(7, TreeSizeDirectCounters{0, 0, 0});

  TreeSizeJournalSnapshot journal;
  journal.entries.emplace_back(
      MakeEntry(1, TreeSizeAccountingEventType::FileDelta, 7, 99));

  const auto result = TreeSizeMissingMetadataRepair().Repair(
      {TreeSizeMissingMetadataReference{TreeSizeMissingMetadataKind::File, 7, 99}},
      snapshot, journal);

  EXPECT_EQ(1ull, result.missingReferences);
  EXPECT_EQ(0ull, result.resolvedReferences);
  EXPECT_EQ(1ull, result.unresolvedReferences);
  EXPECT_TRUE(result.suppressedSequences.empty());
}

TEST(TreeSizeMissingMetadataRepair, KeepsContainerDetachWhenSnapshotStillLinksChild)
{
  TreeSizeSnapshot snapshot;
  snapshot.directCounters.emplace(7, TreeSizeDirectCounters{0, 0, 1});
  snapshot.childContainerIds.emplace(7, std::vector<uint64_t>{10});

  TreeSizeJournalSnapshot journal;
  journal.entries.emplace_back(
      MakeEntry(1, TreeSizeAccountingEventType::ChildDetach, 7, 10));

  const auto result = TreeSizeMissingMetadataRepair().Repair(
      {TreeSizeMissingMetadataReference{TreeSizeMissingMetadataKind::Container, 7, 10}},
      snapshot, journal);

  EXPECT_EQ(1ull, result.resolvedReferences);
  EXPECT_EQ(0ull, result.unresolvedReferences);
  EXPECT_TRUE(result.suppressedSequences.empty());
}

TEST(TreeSizeMissingMetadataRepair, SuppressesContainerDetachWhenSnapshotDroppedChild)
{
  TreeSizeSnapshot snapshot;
  snapshot.directCounters.emplace(7, TreeSizeDirectCounters{0, 0, 0});
  snapshot.childContainerIds.emplace(7, std::vector<uint64_t>{});

  TreeSizeJournalSnapshot journal;
  journal.entries.emplace_back(
      MakeEntry(1, TreeSizeAccountingEventType::ChildDetach, 7, 10));

  const auto result = TreeSizeMissingMetadataRepair().Repair(
      {TreeSizeMissingMetadataReference{TreeSizeMissingMetadataKind::Container, 7, 10}},
      snapshot, journal);

  EXPECT_EQ(1ull, result.resolvedReferences);
  EXPECT_EQ(0ull, result.unresolvedReferences);
  EXPECT_EQ(1ull, result.suppressedJournalEntries);
  EXPECT_EQ(1u, result.suppressedSequences.count(1));
}

TEST(TreeSizeMissingMetadataRepair, SuppressesContainerAttachAlreadyInSnapshot)
{
  TreeSizeSnapshot snapshot;
  snapshot.directCounters.emplace(7, TreeSizeDirectCounters{0, 0, 1});
  snapshot.childContainerIds.emplace(7, std::vector<uint64_t>{10});

  TreeSizeJournalSnapshot journal;
  journal.entries.emplace_back(
      MakeEntry(1, TreeSizeAccountingEventType::ChildAttach, 7, 10));
  journal.entries.emplace_back(
      MakeEntry(2, TreeSizeAccountingEventType::ChildDetach, 7, 10));

  const auto result = TreeSizeMissingMetadataRepair().Repair(
      {TreeSizeMissingMetadataReference{TreeSizeMissingMetadataKind::Container, 7, 10}},
      snapshot, journal);

  EXPECT_EQ(1ull, result.resolvedReferences);
  EXPECT_EQ(1ull, result.suppressedJournalEntries);
  EXPECT_EQ(1u, result.suppressedSequences.count(1));
  EXPECT_EQ(0u, result.suppressedSequences.count(2));
}
