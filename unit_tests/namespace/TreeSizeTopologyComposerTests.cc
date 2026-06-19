//------------------------------------------------------------------------------
// File: TreeSizeTopologyComposerTests.cc
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
#include "namespace/ns_quarkdb/accounting/tree_size/TreeSizeTopologyComposer.hh"
#include <cstdint>
#include <gtest/gtest.h>
#include <vector>

using eos::TreeSizeDirectCounters;
using eos::TreeSizeSnapshot;
using eos::TreeSizeSnapshotBuilder;
using eos::TreeSizeSnapshotContainer;
using eos::TreeSizeSnapshotFile;
using eos::TreeSizeTopologyComposer;

TEST(TreeSizeTopologyComposer, ComposesSimpleRootChildTree)
{
  TreeSizeSnapshot snapshot;
  snapshot.directCounters.emplace(1, TreeSizeDirectCounters{10, 1, 1});
  snapshot.directCounters.emplace(2, TreeSizeDirectCounters{20, 2, 0});
  snapshot.childContainerIds.emplace(1, std::vector<uint64_t>{2});
  snapshot.childContainerIds.emplace(2, std::vector<uint64_t>{});

  const auto result = TreeSizeTopologyComposer().Compose(snapshot);

  ASSERT_EQ(2u, result.subtreeCounters.size());
  const auto root = result.subtreeCounters.at(1);
  EXPECT_EQ(30, root.treeBytes);
  EXPECT_EQ(3, root.treeFiles);
  EXPECT_EQ(1, root.treeContainers);

  const auto child = result.subtreeCounters.at(2);
  EXPECT_EQ(20, child.treeBytes);
  EXPECT_EQ(2, child.treeFiles);
  EXPECT_EQ(0, child.treeContainers);
  EXPECT_EQ(0ull, result.diagnostics.missingDirectCounters);
  EXPECT_EQ(0ull, result.diagnostics.missingTopology);
}

TEST(TreeSizeTopologyComposer, ComposesMultiLevelTreeIndependentOfInputOrder)
{
  TreeSizeSnapshotContainer grandchild;
  grandchild.id = 4;
  grandchild.files = {TreeSizeSnapshotFile{40, 40}};

  TreeSizeSnapshotContainer root;
  root.id = 1;
  root.files = {TreeSizeSnapshotFile{10, 10}};
  root.childContainerIds = {2, 3};

  TreeSizeSnapshotContainer sibling;
  sibling.id = 3;
  sibling.files = {TreeSizeSnapshotFile{30, 30}};

  TreeSizeSnapshotContainer child;
  child.id = 2;
  child.files = {TreeSizeSnapshotFile{20, 20}};
  child.childContainerIds = {4};

  const auto snapshot =
      TreeSizeSnapshotBuilder().Build({grandchild, root, sibling, child});

  const auto result = TreeSizeTopologyComposer().Compose(snapshot);

  const auto root_total = result.subtreeCounters.at(1);
  EXPECT_EQ(100, root_total.treeBytes);
  EXPECT_EQ(4, root_total.treeFiles);
  EXPECT_EQ(3, root_total.treeContainers);

  const auto child_total = result.subtreeCounters.at(2);
  EXPECT_EQ(60, child_total.treeBytes);
  EXPECT_EQ(2, child_total.treeFiles);
  EXPECT_EQ(1, child_total.treeContainers);
}

TEST(TreeSizeTopologyComposer, CountsMissingChildCounterAndContinues)
{
  TreeSizeSnapshot snapshot;
  snapshot.directCounters.emplace(1, TreeSizeDirectCounters{10, 1, 1});
  snapshot.childContainerIds.emplace(1, std::vector<uint64_t>{2});
  snapshot.childContainerIds.emplace(2, std::vector<uint64_t>{});

  const auto result = TreeSizeTopologyComposer().Compose(snapshot);

  const auto root = result.subtreeCounters.at(1);
  EXPECT_EQ(10, root.treeBytes);
  EXPECT_EQ(1, root.treeFiles);
  EXPECT_EQ(1, root.treeContainers);
  EXPECT_EQ(1ull, result.diagnostics.missingDirectCounters);
}

TEST(TreeSizeTopologyComposer, UsesReconciledDirectCounters)
{
  TreeSizeSnapshot snapshot;
  snapshot.directCounters.emplace(1, TreeSizeDirectCounters{10, 1, 1});
  snapshot.directCounters.emplace(2, TreeSizeDirectCounters{20, 2, 0});
  snapshot.childContainerIds.emplace(1, std::vector<uint64_t>{2});
  snapshot.childContainerIds.emplace(2, std::vector<uint64_t>{});

  auto reconciled_counters = snapshot.directCounters;
  reconciled_counters[2].fileBytes += 5;
  reconciled_counters[2].fileCount += 1;

  const auto result = TreeSizeTopologyComposer().Compose(snapshot, reconciled_counters);

  const auto root = result.subtreeCounters.at(1);
  EXPECT_EQ(35, root.treeBytes);
  EXPECT_EQ(4, root.treeFiles);
  EXPECT_EQ(1, root.treeContainers);
}

TEST(TreeSizeTopologyComposer, CountsMissingTopologyForCountersOutsideSnapshot)
{
  TreeSizeSnapshot snapshot;
  snapshot.directCounters.emplace(1, TreeSizeDirectCounters{10, 1, 0});
  snapshot.childContainerIds.emplace(1, std::vector<uint64_t>{});

  auto direct_counters = snapshot.directCounters;
  direct_counters.emplace(42, TreeSizeDirectCounters{5, 1, 0});

  const auto result = TreeSizeTopologyComposer().Compose(snapshot, direct_counters);

  EXPECT_EQ(5, result.subtreeCounters.at(42).treeBytes);
  EXPECT_EQ(1, result.subtreeCounters.at(42).treeFiles);
  EXPECT_EQ(1ull, result.diagnostics.missingTopology);
}

TEST(TreeSizeTopologyComposer, HandlesDeepContainerChainsIteratively)
{
  constexpr uint64_t depth = 5000;
  TreeSizeSnapshot snapshot;

  for (uint64_t id = 1; id <= depth; ++id) {
    const int64_t child_count = id == depth ? 0 : 1;
    snapshot.directCounters.emplace(id, TreeSizeDirectCounters{1, 1, child_count});

    if (id == depth) {
      snapshot.childContainerIds.emplace(id, std::vector<uint64_t>{});
    } else {
      snapshot.childContainerIds.emplace(id, std::vector<uint64_t>{id + 1});
    }
  }

  const auto result = TreeSizeTopologyComposer().Compose(snapshot);
  const auto root = result.subtreeCounters.at(1);
  EXPECT_EQ(static_cast<int64_t>(depth), root.treeBytes);
  EXPECT_EQ(static_cast<int64_t>(depth), root.treeFiles);
  EXPECT_EQ(static_cast<int64_t>(depth - 1), root.treeContainers);
  EXPECT_EQ(0ull, result.diagnostics.cycleEdges);
}

TEST(TreeSizeTopologyComposer, CountsCycleEdgesWithoutRecursingForever)
{
  TreeSizeSnapshot snapshot;
  snapshot.directCounters.emplace(1, TreeSizeDirectCounters{10, 1, 1});
  snapshot.directCounters.emplace(2, TreeSizeDirectCounters{20, 2, 1});
  snapshot.childContainerIds.emplace(1, std::vector<uint64_t>{2});
  snapshot.childContainerIds.emplace(2, std::vector<uint64_t>{1});

  const auto result = TreeSizeTopologyComposer().Compose(snapshot);

  EXPECT_EQ(1ull, result.diagnostics.cycleEdges);
  EXPECT_EQ(2u, result.subtreeCounters.size());
}
