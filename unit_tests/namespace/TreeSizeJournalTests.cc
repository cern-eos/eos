//------------------------------------------------------------------------------
// File: TreeSizeJournalTests.cc
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

#include "namespace/ns_quarkdb/accounting/tree_size/TreeSizeJournal.hh"
#include <gtest/gtest.h>

using eos::TreeInfos;
using eos::TreeSizeAccountingEvent;
using eos::TreeSizeAccountingEventType;
using eos::TreeSizeJournal;
using eos::TreeSizeJournalEntry;

namespace {

TreeSizeJournalEntry
MakeEntry(uint64_t sequence, TreeSizeAccountingEventType type, int64_t size_delta)
{
  TreeSizeJournalEntry entry;
  entry.accountingEvent = TreeSizeAccountingEvent{sequence, type, 7, 99};
  entry.treeChange = TreeInfos{size_delta, 1, 0};
  return entry;
}

} // namespace

TEST(TreeSizeJournal, AppendsRawEntriesInCaptureOrder)
{
  TreeSizeJournal journal;

  journal.Append(MakeEntry(1, TreeSizeAccountingEventType::FileCreate, 10));
  journal.Append(MakeEntry(2, TreeSizeAccountingEventType::FileDelta, 5));

  const auto snapshot = journal.Snapshot();
  ASSERT_EQ(2u, snapshot.entries.size());
  EXPECT_EQ(1ull, snapshot.entries[0].accountingEvent.sequence);
  EXPECT_EQ(TreeSizeAccountingEventType::FileCreate,
            snapshot.entries[0].accountingEvent.type);
  EXPECT_EQ(10, snapshot.entries[0].treeChange.dsize);
  EXPECT_EQ(2ull, snapshot.entries[1].accountingEvent.sequence);
  EXPECT_EQ(TreeSizeAccountingEventType::FileDelta,
            snapshot.entries[1].accountingEvent.type);
  EXPECT_EQ(5, snapshot.entries[1].treeChange.dsize);
}

TEST(TreeSizeJournal, CountsMissingMetadataWithoutRejectingEntry)
{
  TreeSizeJournal journal;
  TreeSizeJournalEntry entry;
  entry.hasAccountingMetadata = false;
  entry.treeChange = TreeInfos{12, 0, 0};

  journal.Append(entry);

  const auto snapshot = journal.Snapshot();
  ASSERT_EQ(1u, snapshot.entries.size());
  EXPECT_FALSE(snapshot.entries[0].hasAccountingMetadata);
  EXPECT_EQ(1ull, snapshot.diagnostics.missingMetadata);
  EXPECT_EQ(0ull, snapshot.diagnostics.nonIncreasingSequence);
}

TEST(TreeSizeJournal, CountsDuplicateAndLowerSequencesWithoutRejectingEntries)
{
  TreeSizeJournal journal;

  journal.Append(MakeEntry(10, TreeSizeAccountingEventType::FileCreate, 10));
  journal.Append(MakeEntry(10, TreeSizeAccountingEventType::FileDelta, 5));
  journal.Append(MakeEntry(9, TreeSizeAccountingEventType::FileDelete, -15));

  const auto snapshot = journal.Snapshot();
  ASSERT_EQ(3u, snapshot.entries.size());
  EXPECT_EQ(2ull, snapshot.diagnostics.nonIncreasingSequence);
  EXPECT_EQ(10ull, snapshot.latestSequence);
}

TEST(TreeSizeJournal, SnapshotIsAnImmutableCopy)
{
  TreeSizeJournal journal;
  journal.Append(MakeEntry(1, TreeSizeAccountingEventType::FileCreate, 10));

  auto snapshot = journal.Snapshot();
  snapshot.entries.clear();
  snapshot.diagnostics.missingMetadata = 99;

  const auto second_snapshot = journal.Snapshot();
  ASSERT_EQ(1u, second_snapshot.entries.size());
  EXPECT_EQ(0ull, second_snapshot.diagnostics.missingMetadata);
}
