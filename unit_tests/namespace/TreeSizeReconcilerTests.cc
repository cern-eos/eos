//------------------------------------------------------------------------------
// File: TreeSizeReconcilerTests.cc
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
#include "namespace/ns_quarkdb/accounting/tree_size/TreeSizeTopologyComposer.hh"
#include <gtest/gtest.h>
#include <vector>

using eos::TreeInfos;
using eos::TreeSizeAccountingEvent;
using eos::TreeSizeAccountingEventType;
using eos::TreeSizeDirectCounters;
using eos::TreeSizeJournalEntry;
using eos::TreeSizeJournalSnapshot;
using eos::TreeSizeReconcileOptions;
using eos::TreeSizeReconciler;
using eos::TreeSizeSnapshot;
using eos::TreeSizeTopologyComposer;

namespace {

TreeSizeJournalEntry
MakeEntry(uint64_t sequence, TreeSizeAccountingEventType type, uint64_t parent_id,
          TreeInfos change, uint64_t object_id = 99)
{
  TreeSizeJournalEntry entry;
  entry.accountingEvent = TreeSizeAccountingEvent{sequence, type, parent_id, object_id};
  entry.treeChange = change;
  return entry;
}

} // namespace

TEST(TreeSizeReconciler, ReplaysFileCreateDeleteAndSizeDelta)
{
  TreeSizeSnapshot snapshot;
  snapshot.directCounters.emplace(7, TreeSizeDirectCounters{100, 2, 0});

  TreeSizeJournalSnapshot journal;
  journal.entries.emplace_back(
      MakeEntry(1, TreeSizeAccountingEventType::FileCreate, 7, TreeInfos{25, 1, 0}));
  journal.entries.emplace_back(
      MakeEntry(2, TreeSizeAccountingEventType::FileDelta, 7, TreeInfos{5, 0, 0}));
  journal.entries.emplace_back(
      MakeEntry(3, TreeSizeAccountingEventType::FileDelete, 7, TreeInfos{-30, -1, 0}));

  const auto result = TreeSizeReconciler().Reconcile(snapshot, journal);
  const auto counters = result.directCounters.at(7);
  EXPECT_EQ(100, counters.fileBytes);
  EXPECT_EQ(2, counters.fileCount);
  EXPECT_EQ(0, counters.childContainerCount);
  EXPECT_EQ(0ull, result.diagnostics.missingMetadata);
  EXPECT_EQ(0ull, result.diagnostics.unknownParents);
}

TEST(TreeSizeReconciler, ReplaysChildAttachAndDetach)
{
  TreeSizeSnapshot snapshot;
  snapshot.directCounters.emplace(7, TreeSizeDirectCounters{0, 0, 2});
  snapshot.childContainerIds.emplace(7, std::vector<uint64_t>{10, 11});
  snapshot.childContainerIds.emplace(10, std::vector<uint64_t>{});
  snapshot.childContainerIds.emplace(11, std::vector<uint64_t>{});

  TreeSizeJournalSnapshot journal;
  journal.entries.emplace_back(
      MakeEntry(1, TreeSizeAccountingEventType::ChildAttach, 7, TreeInfos{0, 0, 1}, 12));
  journal.entries.emplace_back(
      MakeEntry(2, TreeSizeAccountingEventType::ChildDetach, 7, TreeInfos{0, 0, -1}, 10));

  const auto result = TreeSizeReconciler().Reconcile(snapshot, journal);
  const auto counters = result.directCounters.at(7);
  EXPECT_EQ(0, counters.fileBytes);
  EXPECT_EQ(0, counters.fileCount);
  EXPECT_EQ(2, counters.childContainerCount);
  EXPECT_EQ((std::vector<uint64_t>{11, 12}), result.childContainerIds.at(7));
  EXPECT_TRUE(result.childContainerIds.at(12).empty());
  EXPECT_EQ(0, result.directCounters.at(12).fileBytes);
}

TEST(TreeSizeReconciler, SkipsFileDeltaAlreadyIncludedInFileSnapshot)
{
  TreeSizeSnapshot snapshot;
  snapshot.directCounters.emplace(7, TreeSizeDirectCounters{100, 1, 0});
  snapshot.fileSizeSnapshotSequences[7][99] = 5;

  TreeSizeJournalSnapshot journal;
  journal.entries.emplace_back(
      MakeEntry(5, TreeSizeAccountingEventType::FileDelta, 7, TreeInfos{25, 0, 0}, 99));

  const auto result = TreeSizeReconciler().Reconcile(snapshot, journal);
  const auto counters = result.directCounters.at(7);
  EXPECT_EQ(100, counters.fileBytes);
  EXPECT_EQ(1, counters.fileCount);
}

TEST(TreeSizeReconciler, ReplaysFileDeltaNewerThanFileSnapshot)
{
  TreeSizeSnapshot snapshot;
  snapshot.directCounters.emplace(7, TreeSizeDirectCounters{100, 1, 0});
  snapshot.fileSizeSnapshotSequences[7][99] = 5;

  TreeSizeJournalSnapshot journal;
  journal.entries.emplace_back(
      MakeEntry(6, TreeSizeAccountingEventType::FileDelta, 7, TreeInfos{25, 0, 0}, 99));

  const auto result = TreeSizeReconciler().Reconcile(snapshot, journal);
  const auto counters = result.directCounters.at(7);
  EXPECT_EQ(125, counters.fileBytes);
  EXPECT_EQ(1, counters.fileCount);
}

TEST(TreeSizeReconciler, UsesParentMembershipBoundaryForAbsentFileDelta)
{
  TreeSizeSnapshot snapshot;
  snapshot.directCounters.emplace(7, TreeSizeDirectCounters{0, 0, 0});
  snapshot.fileMembershipSnapshotSequences[7] = 5;

  TreeSizeJournalSnapshot journal;
  journal.entries.emplace_back(
      MakeEntry(4, TreeSizeAccountingEventType::FileDelta, 7, TreeInfos{25, 0, 0}, 99));
  journal.entries.emplace_back(
      MakeEntry(6, TreeSizeAccountingEventType::FileDelta, 7, TreeInfos{10, 0, 0}, 100));

  const auto result = TreeSizeReconciler().Reconcile(snapshot, journal);
  const auto counters = result.directCounters.at(7);
  EXPECT_EQ(10, counters.fileBytes);
  EXPECT_EQ(0, counters.fileCount);
}

TEST(TreeSizeReconciler, SkipsFileMembershipEventsAlreadyIncludedInSnapshot)
{
  TreeSizeSnapshot snapshot;
  snapshot.directCounters.emplace(7, TreeSizeDirectCounters{100, 1, 0});
  snapshot.fileMembershipSnapshotSequences[7] = 7;

  TreeSizeJournalSnapshot journal;
  journal.entries.emplace_back(
      MakeEntry(6, TreeSizeAccountingEventType::FileCreate, 7, TreeInfos{25, 1, 0}, 99));
  journal.entries.emplace_back(MakeEntry(7, TreeSizeAccountingEventType::FileDelete, 7,
                                         TreeInfos{-10, -1, 0}, 100));

  const auto result = TreeSizeReconciler().Reconcile(snapshot, journal);
  const auto counters = result.directCounters.at(7);
  EXPECT_EQ(100, counters.fileBytes);
  EXPECT_EQ(1, counters.fileCount);
}

TEST(TreeSizeReconciler, ReplaysFileMembershipEventsNewerThanSnapshot)
{
  TreeSizeSnapshot snapshot;
  snapshot.directCounters.emplace(7, TreeSizeDirectCounters{100, 1, 0});
  snapshot.fileMembershipSnapshotSequences[7] = 7;

  TreeSizeJournalSnapshot journal;
  journal.entries.emplace_back(
      MakeEntry(8, TreeSizeAccountingEventType::FileCreate, 7, TreeInfos{25, 1, 0}, 99));
  journal.entries.emplace_back(MakeEntry(9, TreeSizeAccountingEventType::FileDelete, 7,
                                         TreeInfos{-10, -1, 0}, 100));

  const auto result = TreeSizeReconciler().Reconcile(snapshot, journal);
  const auto counters = result.directCounters.at(7);
  EXPECT_EQ(115, counters.fileBytes);
  EXPECT_EQ(1, counters.fileCount);
}

TEST(TreeSizeReconciler, SkipsChildMembershipEventsAlreadyIncludedInSnapshot)
{
  TreeSizeSnapshot snapshot;
  snapshot.directCounters.emplace(7, TreeSizeDirectCounters{0, 0, 1});
  snapshot.childContainerIds.emplace(7, std::vector<uint64_t>{12});
  snapshot.childContainerIds.emplace(12, std::vector<uint64_t>{});
  snapshot.childMembershipSnapshotSequences[7] = 5;

  TreeSizeJournalSnapshot journal;
  journal.entries.emplace_back(
      MakeEntry(5, TreeSizeAccountingEventType::ChildAttach, 7, TreeInfos{0, 0, 1}, 12));

  const auto result = TreeSizeReconciler().Reconcile(snapshot, journal);
  const auto counters = result.directCounters.at(7);
  EXPECT_EQ(1, counters.childContainerCount);
  EXPECT_EQ((std::vector<uint64_t>{12}), result.childContainerIds.at(7));
}

TEST(TreeSizeReconciler, ReplaysChildMembershipEventsNewerThanSnapshot)
{
  TreeSizeSnapshot snapshot;
  snapshot.directCounters.emplace(7, TreeSizeDirectCounters{0, 0, 0});
  snapshot.childContainerIds.emplace(7, std::vector<uint64_t>{});
  snapshot.childMembershipSnapshotSequences[7] = 5;

  TreeSizeJournalSnapshot journal;
  journal.entries.emplace_back(
      MakeEntry(6, TreeSizeAccountingEventType::ChildAttach, 7, TreeInfos{0, 0, 1}, 12));

  const auto result = TreeSizeReconciler().Reconcile(snapshot, journal);
  const auto counters = result.directCounters.at(7);
  EXPECT_EQ(1, counters.childContainerCount);
  EXPECT_EQ((std::vector<uint64_t>{12}), result.childContainerIds.at(7));
}

TEST(TreeSizeReconciler, SeedsAttachedChildForTopologyComposition)
{
  TreeSizeSnapshot snapshot;
  snapshot.directCounters.emplace(7, TreeSizeDirectCounters{0, 0, 0});
  snapshot.childContainerIds.emplace(7, std::vector<uint64_t>{});

  TreeSizeJournalSnapshot journal;
  journal.entries.emplace_back(
      MakeEntry(1, TreeSizeAccountingEventType::ChildAttach, 7, TreeInfos{0, 0, 1}, 12));
  journal.entries.emplace_back(
      MakeEntry(2, TreeSizeAccountingEventType::FileCreate, 12, TreeInfos{15, 1, 0}, 99));

  const auto reconcile = TreeSizeReconciler().Reconcile(snapshot, journal);
  auto reconciled_snapshot = snapshot;
  reconciled_snapshot.childContainerIds = reconcile.childContainerIds;
  const auto composed =
      TreeSizeTopologyComposer().Compose(reconciled_snapshot, reconcile.directCounters);

  const auto root = composed.subtreeCounters.at(7);
  EXPECT_EQ(15, root.treeBytes);
  EXPECT_EQ(1, root.treeFiles);
  EXPECT_EQ(1, root.treeContainers);
  EXPECT_EQ(0ull, reconcile.diagnostics.unknownParents);
  EXPECT_EQ(0ull, composed.diagnostics.missingDirectCounters);
  EXPECT_EQ(0ull, composed.diagnostics.missingTopology);
}

TEST(TreeSizeReconciler, ReplaysCoveredDirectoryMoveForTopologyComposition)
{
  TreeSizeSnapshot snapshot;
  snapshot.directCounters.emplace(1, TreeSizeDirectCounters{0, 0, 2});
  snapshot.directCounters.emplace(2, TreeSizeDirectCounters{1, 1, 1});
  snapshot.directCounters.emplace(3, TreeSizeDirectCounters{2, 2, 0});
  snapshot.directCounters.emplace(5, TreeSizeDirectCounters{10, 1, 0});
  snapshot.childContainerIds.emplace(1, std::vector<uint64_t>{2, 3});
  snapshot.childContainerIds.emplace(2, std::vector<uint64_t>{5});
  snapshot.childContainerIds.emplace(3, std::vector<uint64_t>{});
  snapshot.childContainerIds.emplace(5, std::vector<uint64_t>{});

  TreeSizeJournalSnapshot journal;
  journal.entries.emplace_back(
      MakeEntry(1, TreeSizeAccountingEventType::ChildDetach, 2, TreeInfos{0, 0, -1}, 5));
  journal.entries.emplace_back(
      MakeEntry(2, TreeSizeAccountingEventType::ChildAttach, 3, TreeInfos{0, 0, 1}, 5));

  const auto reconcile = TreeSizeReconciler().Reconcile(snapshot, journal);
  auto reconciled_snapshot = snapshot;
  reconciled_snapshot.childContainerIds = reconcile.childContainerIds;
  const auto composed =
      TreeSizeTopologyComposer().Compose(reconciled_snapshot, reconcile.directCounters);

  EXPECT_TRUE(reconcile.childContainerIds.at(2).empty());
  EXPECT_EQ((std::vector<uint64_t>{5}), reconcile.childContainerIds.at(3));
  EXPECT_EQ(0, reconcile.directCounters.at(2).childContainerCount);
  EXPECT_EQ(1, reconcile.directCounters.at(3).childContainerCount);

  const auto source = composed.subtreeCounters.at(2);
  EXPECT_EQ(1, source.treeBytes);
  EXPECT_EQ(1, source.treeFiles);
  EXPECT_EQ(0, source.treeContainers);

  const auto target = composed.subtreeCounters.at(3);
  EXPECT_EQ(12, target.treeBytes);
  EXPECT_EQ(3, target.treeFiles);
  EXPECT_EQ(1, target.treeContainers);

  const auto root = composed.subtreeCounters.at(1);
  EXPECT_EQ(13, root.treeBytes);
  EXPECT_EQ(4, root.treeFiles);
  EXPECT_EQ(3, root.treeContainers);
  EXPECT_EQ(0ull, reconcile.diagnostics.unknownParents);
  EXPECT_EQ(0ull, reconcile.diagnostics.negativeCounters);
  EXPECT_EQ(0ull, composed.diagnostics.missingDirectCounters);
  EXPECT_EQ(0ull, composed.diagnostics.missingTopology);
}

TEST(TreeSizeReconciler, IgnoresSubtreePropagationMarkers)
{
  TreeSizeSnapshot snapshot;
  snapshot.directCounters.emplace(7, TreeSizeDirectCounters{10, 1, 2});

  TreeSizeJournalSnapshot journal;
  journal.entries.emplace_back(MakeEntry(1, TreeSizeAccountingEventType::SubtreeAttach,
                                         42, TreeInfos{999, 99, 9}, 5));
  journal.entries.emplace_back(MakeEntry(2, TreeSizeAccountingEventType::SubtreeDetach,
                                         42, TreeInfos{-999, -99, -9}, 5));

  const auto result = TreeSizeReconciler().Reconcile(snapshot, journal);
  const auto counters = result.directCounters.at(7);
  EXPECT_EQ(10, counters.fileBytes);
  EXPECT_EQ(1, counters.fileCount);
  EXPECT_EQ(2, counters.childContainerCount);
  EXPECT_EQ(result.directCounters.end(), result.directCounters.find(42));
  EXPECT_EQ(0ull, result.diagnostics.unknownParents);
  EXPECT_EQ(0ull, result.diagnostics.unsupportedEvents);
}

TEST(TreeSizeReconciler, CountsMissingMetadataWithoutApplyingEntry)
{
  TreeSizeSnapshot snapshot;
  snapshot.directCounters.emplace(7, TreeSizeDirectCounters{10, 1, 0});

  TreeSizeJournalSnapshot journal;
  TreeSizeJournalEntry entry;
  entry.hasAccountingMetadata = false;
  entry.treeChange = TreeInfos{99, 1, 0};
  journal.entries.emplace_back(entry);

  const auto result = TreeSizeReconciler().Reconcile(snapshot, journal);
  const auto counters = result.directCounters.at(7);
  EXPECT_EQ(10, counters.fileBytes);
  EXPECT_EQ(1, counters.fileCount);
  EXPECT_EQ(1ull, result.diagnostics.missingMetadata);
}

TEST(TreeSizeReconciler, ReplaysSequencedEntriesInProtocolOrder)
{
  TreeSizeSnapshot snapshot;
  snapshot.directCounters.emplace(7, TreeSizeDirectCounters{0, 0, 0});

  TreeSizeJournalSnapshot journal;
  journal.entries.emplace_back(
      MakeEntry(2, TreeSizeAccountingEventType::FileDelete, 7, TreeInfos{-10, -1, 0}));
  journal.entries.emplace_back(
      MakeEntry(1, TreeSizeAccountingEventType::FileCreate, 7, TreeInfos{10, 1, 0}));

  const auto result = TreeSizeReconciler().Reconcile(snapshot, journal);
  const auto counters = result.directCounters.at(7);
  EXPECT_EQ(0, counters.fileBytes);
  EXPECT_EQ(0, counters.fileCount);
  EXPECT_EQ(0ull, result.diagnostics.negativeCounters);
}

TEST(TreeSizeReconciler, CountsUnknownParentButKeepsReplay)
{
  TreeSizeSnapshot snapshot;
  TreeSizeJournalSnapshot journal;
  journal.entries.emplace_back(
      MakeEntry(1, TreeSizeAccountingEventType::FileCreate, 42, TreeInfos{10, 1, 0}));

  const auto result = TreeSizeReconciler().Reconcile(snapshot, journal);
  const auto counters = result.directCounters.at(42);
  EXPECT_EQ(10, counters.fileBytes);
  EXPECT_EQ(1, counters.fileCount);
  EXPECT_EQ(1ull, result.diagnostics.unknownParents);
}

TEST(TreeSizeReconciler, CountsNegativeCounters)
{
  TreeSizeSnapshot snapshot;
  snapshot.directCounters.emplace(7, TreeSizeDirectCounters{0, 0, 0});

  TreeSizeJournalSnapshot journal;
  journal.entries.emplace_back(
      MakeEntry(1, TreeSizeAccountingEventType::FileDelete, 7, TreeInfos{-10, -1, 0}));

  const auto result = TreeSizeReconciler().Reconcile(snapshot, journal);
  const auto counters = result.directCounters.at(7);
  EXPECT_EQ(-10, counters.fileBytes);
  EXPECT_EQ(-1, counters.fileCount);
  EXPECT_EQ(1ull, result.diagnostics.negativeCounters);
}

TEST(TreeSizeReconciler, SkipsSuppressedJournalEntries)
{
  TreeSizeSnapshot snapshot;
  snapshot.directCounters.emplace(7, TreeSizeDirectCounters{0, 0, 0});

  TreeSizeJournalSnapshot journal;
  journal.entries.emplace_back(
      MakeEntry(1, TreeSizeAccountingEventType::FileDelete, 7, TreeInfos{-10, -1, 0}));

  TreeSizeReconcileOptions options;
  options.suppressedSequences.insert(1);

  const auto result = TreeSizeReconciler().Reconcile(snapshot, journal, options);
  const auto counters = result.directCounters.at(7);
  EXPECT_EQ(0, counters.fileBytes);
  EXPECT_EQ(0, counters.fileCount);
  EXPECT_EQ(1ull, result.diagnostics.suppressedEntries);
  EXPECT_EQ(0ull, result.diagnostics.negativeCounters);
}
