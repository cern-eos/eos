//------------------------------------------------------------------------------
// File: TreeSizeCoverageClassifierTests.cc
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

#include "namespace/ns_quarkdb/accounting/tree_size/TreeSizeCoverageClassifier.hh"
#include <gtest/gtest.h>

using eos::TreeInfos;
using eos::TreeSizeAccountingEvent;
using eos::TreeSizeAccountingEventType;
using eos::TreeSizeCoverageClassifier;
using eos::TreeSizeJournalEntry;
using eos::TreeSizeJournalSnapshot;

namespace {

TreeSizeJournalEntry
MakeEntry(uint64_t sequence, TreeSizeAccountingEventType type, uint64_t parent_id,
          uint64_t object_id)
{
  TreeSizeJournalEntry entry;
  entry.accountingEvent = TreeSizeAccountingEvent{sequence, type, parent_id, object_id};
  entry.treeChange = TreeInfos{1, 1, 1};
  return entry;
}

} // namespace

TEST(TreeSizeCoverageClassifier, ClassifiesCoveredParent)
{
  TreeSizeJournalSnapshot journal;
  journal.entries.emplace_back(
      MakeEntry(1, TreeSizeAccountingEventType::FileCreate, 7, 99));

  const auto result = TreeSizeCoverageClassifier().Classify({7}, journal);

  EXPECT_EQ(1ull, result.coveredEntries);
  EXPECT_EQ(0ull, result.outsideCoverageEntries);
  EXPECT_EQ(0ull, result.postDiscoveryTopologyEntries);
}

TEST(TreeSizeCoverageClassifier, ClassifiesOutsideCoverageParent)
{
  TreeSizeJournalSnapshot journal;
  journal.entries.emplace_back(
      MakeEntry(1, TreeSizeAccountingEventType::FileCreate, 42, 99));

  const auto result = TreeSizeCoverageClassifier().Classify({7}, journal);

  EXPECT_EQ(0ull, result.coveredEntries);
  EXPECT_EQ(1ull, result.outsideCoverageEntries);
}

TEST(TreeSizeCoverageClassifier, TracksPostDiscoveryAttachedChild)
{
  TreeSizeJournalSnapshot journal;
  journal.entries.emplace_back(
      MakeEntry(1, TreeSizeAccountingEventType::ChildAttach, 7, 12));
  journal.entries.emplace_back(
      MakeEntry(2, TreeSizeAccountingEventType::FileCreate, 12, 99));

  const auto result = TreeSizeCoverageClassifier().Classify({7}, journal);

  EXPECT_EQ(0ull, result.coveredEntries);
  EXPECT_EQ(0ull, result.outsideCoverageEntries);
  EXPECT_EQ(2ull, result.postDiscoveryTopologyEntries);
  EXPECT_EQ(1ull, result.postDiscoveryContainerIds);
}

TEST(TreeSizeCoverageClassifier, KeepsAlreadyCoveredChildAttachCovered)
{
  TreeSizeJournalSnapshot journal;
  journal.entries.emplace_back(
      MakeEntry(1, TreeSizeAccountingEventType::ChildAttach, 7, 12));

  const auto result = TreeSizeCoverageClassifier().Classify({7, 12}, journal);

  EXPECT_EQ(1ull, result.coveredEntries);
  EXPECT_EQ(0ull, result.postDiscoveryTopologyEntries);
}

TEST(TreeSizeCoverageClassifier, KeepsCoveredDirectoryMoveCovered)
{
  TreeSizeJournalSnapshot journal;
  journal.entries.emplace_back(
      MakeEntry(1, TreeSizeAccountingEventType::ChildDetach, 2, 5));
  journal.entries.emplace_back(
      MakeEntry(2, TreeSizeAccountingEventType::ChildAttach, 3, 5));

  const auto result = TreeSizeCoverageClassifier().Classify({1, 2, 3, 5}, journal);

  EXPECT_EQ(2ull, result.coveredEntries);
  EXPECT_EQ(0ull, result.outsideCoverageEntries);
  EXPECT_EQ(0ull, result.postDiscoveryTopologyEntries);
  EXPECT_EQ(0ull, result.postDiscoveryContainerIds);
}

TEST(TreeSizeCoverageClassifier, CountsMissingMetadata)
{
  TreeSizeJournalSnapshot journal;
  TreeSizeJournalEntry entry;
  entry.hasAccountingMetadata = false;
  journal.entries.emplace_back(entry);

  const auto result = TreeSizeCoverageClassifier().Classify({7}, journal);

  EXPECT_EQ(1ull, result.missingMetadataEntries);
}
