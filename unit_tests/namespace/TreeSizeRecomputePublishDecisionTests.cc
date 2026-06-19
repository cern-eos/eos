//------------------------------------------------------------------------------
// File: TreeSizeRecomputePublishDecisionTests.cc
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

#include "namespace/ns_quarkdb/accounting/tree_size/TreeSizeRecomputePublishDecision.hh"
#include <gtest/gtest.h>

using eos::TreeSizeRecomputePublishDecision;
using eos::TreeSizeRecomputePublishDecisionReasons;
using eos::TreeSizeRecomputePublishDiagnostics;

TEST(TreeSizeRecomputePublishDecision, AllowsCleanResult)
{
  const auto result = TreeSizeRecomputePublishDecision().Evaluate({});

  EXPECT_TRUE(result.publishable);
  EXPECT_FALSE(result.retryRequired);
  EXPECT_EQ(0ull, result.reasonMask);
  EXPECT_EQ("none", TreeSizeRecomputePublishDecision::ReasonsToString(result.reasonMask));
}

TEST(TreeSizeRecomputePublishDecision, RetriesOnNegativeCounters)
{
  TreeSizeRecomputePublishDiagnostics diagnostics;
  diagnostics.negativeCounters = 1;

  const auto result = TreeSizeRecomputePublishDecision().Evaluate(diagnostics);

  EXPECT_FALSE(result.publishable);
  EXPECT_TRUE(result.retryRequired);
  EXPECT_NE(0ull, result.reasonMask &
                      TreeSizeRecomputePublishDecisionReasons::NegativeCounters);
  EXPECT_EQ("negative_counters",
            TreeSizeRecomputePublishDecision::ReasonsToString(result.reasonMask));
}

TEST(TreeSizeRecomputePublishDecision, RetriesOnDiscoveryMissingMetadata)
{
  TreeSizeRecomputePublishDiagnostics diagnostics;
  diagnostics.discoveryMissingMetadata = 2;
  diagnostics.resolvedDiscoveryMissingMetadata = 1;
  diagnostics.unresolvedDiscoveryMissingMetadata = 1;

  const auto result = TreeSizeRecomputePublishDecision().Evaluate(diagnostics);

  EXPECT_FALSE(result.publishable);
  EXPECT_TRUE(result.retryRequired);
  EXPECT_NE(0ull, result.reasonMask &
                      TreeSizeRecomputePublishDecisionReasons::DiscoveryMissingMetadata);
  EXPECT_EQ("discovery_missing_metadata",
            TreeSizeRecomputePublishDecision::ReasonsToString(result.reasonMask));
}

TEST(TreeSizeRecomputePublishDecision, AllowsResolvedDiscoveryMissingMetadata)
{
  TreeSizeRecomputePublishDiagnostics diagnostics;
  diagnostics.discoveryMissingMetadata = 1;
  diagnostics.resolvedDiscoveryMissingMetadata = 1;
  diagnostics.suppressedJournalEntries = 1;

  const auto result = TreeSizeRecomputePublishDecision().Evaluate(diagnostics);

  EXPECT_TRUE(result.publishable);
  EXPECT_FALSE(result.retryRequired);
  EXPECT_EQ(0ull, result.reasonMask);
}

TEST(TreeSizeRecomputePublishDecision, RetriesOnPostDiscoveryTopology)
{
  TreeSizeRecomputePublishDiagnostics diagnostics;
  diagnostics.postDiscoveryTopologyEntries = 2;
  diagnostics.postDiscoveryContainerIds = 1;

  const auto result = TreeSizeRecomputePublishDecision().Evaluate(diagnostics);

  EXPECT_FALSE(result.publishable);
  EXPECT_TRUE(result.retryRequired);
  EXPECT_NE(0ull, result.reasonMask &
                      TreeSizeRecomputePublishDecisionReasons::PostDiscoveryTopology);
  EXPECT_EQ("post_discovery_topology",
            TreeSizeRecomputePublishDecision::ReasonsToString(result.reasonMask));
}

TEST(TreeSizeRecomputePublishDecision, RetriesOnUnknownTopology)
{
  TreeSizeRecomputePublishDiagnostics diagnostics;
  diagnostics.unknownParents = 1;
  diagnostics.missingTopology = 2;

  const auto result = TreeSizeRecomputePublishDecision().Evaluate(diagnostics);

  EXPECT_FALSE(result.publishable);
  EXPECT_TRUE(result.retryRequired);
  EXPECT_NE(0ull,
            result.reasonMask & TreeSizeRecomputePublishDecisionReasons::UnknownParents);
  EXPECT_NE(0ull,
            result.reasonMask & TreeSizeRecomputePublishDecisionReasons::MissingTopology);
  EXPECT_EQ("unknown_parents,missing_topology",
            TreeSizeRecomputePublishDecision::ReasonsToString(result.reasonMask));
}

TEST(TreeSizeRecomputePublishDecision, RetriesOnIncompleteComposition)
{
  TreeSizeRecomputePublishDiagnostics diagnostics;
  diagnostics.missingDirectCounters = 1;
  diagnostics.missingTopology = 1;
  diagnostics.cycleEdges = 1;

  const auto result = TreeSizeRecomputePublishDecision().Evaluate(diagnostics);

  EXPECT_FALSE(result.publishable);
  EXPECT_TRUE(result.retryRequired);
  EXPECT_NE(0ull, result.reasonMask &
                      TreeSizeRecomputePublishDecisionReasons::MissingDirectCounters);
  EXPECT_NE(0ull,
            result.reasonMask & TreeSizeRecomputePublishDecisionReasons::MissingTopology);
  EXPECT_NE(0ull,
            result.reasonMask & TreeSizeRecomputePublishDecisionReasons::CycleEdges);
  EXPECT_EQ("missing_direct_counters,missing_topology,cycle_edges",
            TreeSizeRecomputePublishDecision::ReasonsToString(result.reasonMask));
}

TEST(TreeSizeRecomputePublishDecision, RetriesOnMissingMetadata)
{
  TreeSizeRecomputePublishDiagnostics diagnostics;
  diagnostics.missingJournalMetadata = 1;
  diagnostics.missingReconcileMetadata = 1;
  diagnostics.missingCoverageMetadata = 1;

  const auto result = TreeSizeRecomputePublishDecision().Evaluate(diagnostics);

  EXPECT_FALSE(result.publishable);
  EXPECT_TRUE(result.retryRequired);
  EXPECT_NE(0ull,
            result.reasonMask & TreeSizeRecomputePublishDecisionReasons::MissingMetadata);
  EXPECT_EQ("missing_metadata",
            TreeSizeRecomputePublishDecision::ReasonsToString(result.reasonMask));
}

TEST(TreeSizeRecomputePublishDecision, RetriesOnInvalidPublishPlan)
{
  TreeSizeRecomputePublishDiagnostics diagnostics;
  diagnostics.publishMissingCounters = 1;
  diagnostics.publishNegativeCounters = 1;
  diagnostics.publishDuplicateTargets = 1;
  diagnostics.publishUnplannedCounters = 1;

  const auto result = TreeSizeRecomputePublishDecision().Evaluate(diagnostics);

  EXPECT_FALSE(result.publishable);
  EXPECT_TRUE(result.retryRequired);
  EXPECT_NE(0ull, result.reasonMask &
                      TreeSizeRecomputePublishDecisionReasons::PublishPlanInvalid);
  EXPECT_EQ("publish_plan_invalid",
            TreeSizeRecomputePublishDecision::ReasonsToString(result.reasonMask));
}
