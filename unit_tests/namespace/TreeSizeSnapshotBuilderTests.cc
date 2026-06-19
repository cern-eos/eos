//------------------------------------------------------------------------------
// File: TreeSizeSnapshotBuilderTests.cc
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

#include "namespace/ns_quarkdb/accounting/tree_size/TreeSizeSnapshotBuilder.hh"
#include <gtest/gtest.h>

using eos::TreeSizeSnapshotBuilder;
using eos::TreeSizeSnapshotContainer;
using eos::TreeSizeSnapshotFile;

TEST(TreeSizeSnapshotBuilder, BuildsDirectCounters)
{
  TreeSizeSnapshotContainer root;
  root.id = 7;
  root.files = {TreeSizeSnapshotFile{1, 10}, TreeSizeSnapshotFile{2, 15}};
  root.childContainerIds = {8, 9};

  TreeSizeSnapshotContainer child;
  child.id = 8;
  child.files = {TreeSizeSnapshotFile{3, 5}};

  const auto snapshot = TreeSizeSnapshotBuilder().Build({root, child});

  ASSERT_EQ(2u, snapshot.directCounters.size());
  ASSERT_EQ(2u, snapshot.childContainerIds.size());

  const auto root_counters = snapshot.directCounters.at(7);
  EXPECT_EQ(25, root_counters.fileBytes);
  EXPECT_EQ(2, root_counters.fileCount);
  EXPECT_EQ(2, root_counters.childContainerCount);
  EXPECT_EQ((std::vector<uint64_t>{8, 9}), snapshot.childContainerIds.at(7));

  const auto child_counters = snapshot.directCounters.at(8);
  EXPECT_EQ(5, child_counters.fileBytes);
  EXPECT_EQ(1, child_counters.fileCount);
  EXPECT_EQ(0, child_counters.childContainerCount);
  EXPECT_TRUE(snapshot.childContainerIds.at(8).empty());
}

TEST(TreeSizeSnapshotBuilder, KeepsEmptyContainers)
{
  TreeSizeSnapshotContainer container;
  container.id = 42;

  const auto snapshot = TreeSizeSnapshotBuilder().Build({container});

  ASSERT_EQ(1u, snapshot.directCounters.size());
  ASSERT_EQ(1u, snapshot.childContainerIds.size());
  const auto counters = snapshot.directCounters.at(42);
  EXPECT_EQ(0, counters.fileBytes);
  EXPECT_EQ(0, counters.fileCount);
  EXPECT_EQ(0, counters.childContainerCount);
  EXPECT_TRUE(snapshot.childContainerIds.at(42).empty());
}

TEST(TreeSizeSnapshotBuilder, CarriesSnapshotSequences)
{
  TreeSizeSnapshotContainer container;
  container.id = 7;
  container.fileMembershipSnapshotSequence = 10;
  container.childMembershipSnapshotSequence = 11;
  container.files = {TreeSizeSnapshotFile{99, 100, 12}};
  container.childContainerIds = {8};

  const auto snapshot = TreeSizeSnapshotBuilder().Build({container});

  EXPECT_EQ(10ull, snapshot.fileMembershipSnapshotSequences.at(7));
  EXPECT_EQ(11ull, snapshot.childMembershipSnapshotSequences.at(7));
  EXPECT_EQ(12ull, snapshot.fileSizeSnapshotSequences.at(7).at(99));
  EXPECT_EQ(12ull, snapshot.latestSnapshotSequence);
}
