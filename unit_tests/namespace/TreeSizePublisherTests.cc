//------------------------------------------------------------------------------
// File: TreeSizePublisherTests.cc
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

#include "namespace/ns_quarkdb/accounting/tree_size/TreeSizePublisher.hh"
#include <gtest/gtest.h>
#include <unordered_map>
#include <unordered_set>
#include <vector>

using eos::ITreeSizeCounterPublisher;
using eos::TreeSizeCounterPublishResult;
using eos::TreeSizeCounterPublishStatus;
using eos::TreeSizePublishApplyStatus;
using eos::TreeSizePublisher;
using eos::TreeSizePublishPlanResult;
using eos::TreeSizePublishTarget;
using eos::TreeSizeSubtreeCounters;

namespace {

class FakeTreeSizeCounterPublisher : public ITreeSizeCounterPublisher {
public:
  TreeSizeCounterPublishResult
  PublishTreeSizeCounters(const TreeSizePublishTarget& target) override
  {
    publishOrder.push_back(target.containerId);

    if (skippedMissingTargets.count(target.containerId) != 0) {
      return TreeSizeCounterPublishResult{TreeSizeCounterPublishStatus::SkippedMissing,
                                          ""};
    }

    if (publishFailures.count(target.containerId) != 0) {
      return TreeSizeCounterPublishResult{TreeSizeCounterPublishStatus::Failed,
                                          "publish failed"};
    }

    publishedCounters[target.containerId] = target.counters;
    return TreeSizeCounterPublishResult{};
  }

  std::unordered_set<uint64_t> skippedMissingTargets;
  std::unordered_set<uint64_t> publishFailures;
  std::vector<uint64_t> publishOrder;
  std::unordered_map<uint64_t, TreeSizeSubtreeCounters> publishedCounters;
};

std::vector<TreeSizePublishTarget>
MakePublishTargets()
{
  return std::vector<TreeSizePublishTarget>{
      TreeSizePublishTarget{7, TreeSizeSubtreeCounters{70, 7, 2}},
      TreeSizePublishTarget{3, TreeSizeSubtreeCounters{30, 3, 1}}};
}

TreeSizePublishPlanResult
MakePublishPlan()
{
  TreeSizePublishPlanResult plan;
  plan.targets = MakePublishTargets();
  return plan;
}

} // namespace

TEST(TreeSizePublisher, PreservesRequestedPublishOrder)
{
  std::unordered_map<uint64_t, TreeSizeSubtreeCounters> counters;
  counters.emplace(7, TreeSizeSubtreeCounters{70, 7, 2});
  counters.emplace(3, TreeSizeSubtreeCounters{30, 3, 1});

  const auto result = TreeSizePublisher().Plan(std::vector<uint64_t>{7, 3}, counters);

  ASSERT_EQ(2u, result.targets.size());
  EXPECT_EQ(7ull, result.targets[0].containerId);
  EXPECT_EQ(70, result.targets[0].counters.treeBytes);
  EXPECT_EQ(3ull, result.targets[1].containerId);
  EXPECT_EQ(30, result.targets[1].counters.treeBytes);
  EXPECT_EQ(0ull, result.diagnostics.missingCounters);
  EXPECT_EQ(0ull, result.diagnostics.negativeCounters);
  EXPECT_EQ(0ull, result.diagnostics.duplicateTargets);
  EXPECT_EQ(0ull, result.diagnostics.unplannedCounters);
}

TEST(TreeSizePublisher, CountsMissingCountersAndOmitsTarget)
{
  std::unordered_map<uint64_t, TreeSizeSubtreeCounters> counters;
  counters.emplace(7, TreeSizeSubtreeCounters{70, 7, 2});

  const auto result = TreeSizePublisher().Plan(std::vector<uint64_t>{7, 3}, counters);

  ASSERT_EQ(1u, result.targets.size());
  EXPECT_EQ(7ull, result.targets[0].containerId);
  EXPECT_EQ(1ull, result.diagnostics.missingCounters);
}

TEST(TreeSizePublisher, CountsNegativeCountersAndOmitsTarget)
{
  std::unordered_map<uint64_t, TreeSizeSubtreeCounters> counters;
  counters.emplace(7, TreeSizeSubtreeCounters{70, 7, 2});
  counters.emplace(3, TreeSizeSubtreeCounters{-1, 3, 1});

  const auto result = TreeSizePublisher().Plan(std::vector<uint64_t>{7, 3}, counters);

  ASSERT_EQ(1u, result.targets.size());
  EXPECT_EQ(7ull, result.targets[0].containerId);
  EXPECT_EQ(1ull, result.diagnostics.negativeCounters);
}

TEST(TreeSizePublisher, CountsDuplicateTargetsAndPublishesOnce)
{
  std::unordered_map<uint64_t, TreeSizeSubtreeCounters> counters;
  counters.emplace(7, TreeSizeSubtreeCounters{70, 7, 2});

  const auto result = TreeSizePublisher().Plan(std::vector<uint64_t>{7, 7}, counters);

  ASSERT_EQ(1u, result.targets.size());
  EXPECT_EQ(7ull, result.targets[0].containerId);
  EXPECT_EQ(1ull, result.diagnostics.duplicateTargets);
}

TEST(TreeSizePublisher, CountsUnplannedCountersAndOmitsTarget)
{
  std::unordered_map<uint64_t, TreeSizeSubtreeCounters> counters;
  counters.emplace(7, TreeSizeSubtreeCounters{70, 7, 2});
  counters.emplace(3, TreeSizeSubtreeCounters{30, 3, 1});

  const auto result = TreeSizePublisher().Plan(std::vector<uint64_t>{7}, counters);

  ASSERT_EQ(1u, result.targets.size());
  EXPECT_EQ(7ull, result.targets[0].containerId);
  EXPECT_EQ(1ull, result.diagnostics.unplannedCounters);
}

TEST(TreeSizePublisher, AppliesAbsoluteCountersInOrder)
{
  FakeTreeSizeCounterPublisher sink;

  const auto result = TreeSizePublisher().Apply(MakePublishPlan(), sink);

  EXPECT_EQ(TreeSizePublishApplyStatus::Success, result.status);
  EXPECT_EQ(2ull, result.attemptedTargets);
  EXPECT_EQ(0ull, result.skippedMissingTargets);
  EXPECT_EQ((std::vector<uint64_t>{7, 3}), sink.publishOrder);
  EXPECT_EQ((std::vector<uint64_t>{7, 3}), result.publishedContainerIds);
  EXPECT_TRUE(result.retryContainerIds.empty());
  EXPECT_TRUE(result.missingContainerIds.empty());
  EXPECT_TRUE(result.writeFailedContainerIds.empty());
  EXPECT_EQ(70, sink.publishedCounters[7].treeBytes);
  EXPECT_EQ(30, sink.publishedCounters[3].treeBytes);
}

TEST(TreeSizePublisher, ContinuesAfterFirstPublishFailure)
{
  FakeTreeSizeCounterPublisher sink;
  sink.publishFailures.insert(7);

  const auto result = TreeSizePublisher().Apply(MakePublishPlan(), sink);

  EXPECT_EQ(TreeSizePublishApplyStatus::PartialPublishFailed, result.status);
  EXPECT_EQ(7ull, result.failedContainerId);
  EXPECT_EQ(2ull, result.attemptedTargets);
  EXPECT_EQ((std::vector<uint64_t>{7, 3}), sink.publishOrder);
  EXPECT_EQ((std::vector<uint64_t>{3}), result.publishedContainerIds);
  EXPECT_EQ((std::vector<uint64_t>{7}), result.retryContainerIds);
  EXPECT_EQ((std::vector<uint64_t>{7}), result.writeFailedContainerIds);
  EXPECT_TRUE(result.missingContainerIds.empty());
  EXPECT_EQ(30, sink.publishedCounters[3].treeBytes);
}

TEST(TreeSizePublisher, ReportsPartialPublishFailureAfterPreviousWrites)
{
  FakeTreeSizeCounterPublisher sink;
  sink.publishFailures.insert(3);

  const auto result = TreeSizePublisher().Apply(MakePublishPlan(), sink);

  EXPECT_EQ(TreeSizePublishApplyStatus::PartialPublishFailed, result.status);
  EXPECT_EQ(3ull, result.failedContainerId);
  EXPECT_EQ(2ull, result.attemptedTargets);
  EXPECT_EQ((std::vector<uint64_t>{7, 3}), sink.publishOrder);
  EXPECT_EQ((std::vector<uint64_t>{7}), result.publishedContainerIds);
  EXPECT_EQ((std::vector<uint64_t>{3}), result.retryContainerIds);
  EXPECT_EQ((std::vector<uint64_t>{3}), result.writeFailedContainerIds);
  EXPECT_TRUE(result.missingContainerIds.empty());
  EXPECT_EQ(70, sink.publishedCounters[7].treeBytes);
  EXPECT_EQ(0u, sink.publishedCounters.count(3));
}

TEST(TreeSizePublisher, ContinuesAfterSkippedMissingTarget)
{
  FakeTreeSizeCounterPublisher sink;
  sink.skippedMissingTargets.insert(7);

  const auto result = TreeSizePublisher().Apply(MakePublishPlan(), sink);

  EXPECT_EQ(TreeSizePublishApplyStatus::Success, result.status);
  EXPECT_EQ(2ull, result.attemptedTargets);
  EXPECT_EQ(1ull, result.skippedMissingTargets);
  EXPECT_EQ((std::vector<uint64_t>{7, 3}), sink.publishOrder);
  EXPECT_EQ((std::vector<uint64_t>{3}), result.publishedContainerIds);
  EXPECT_EQ((std::vector<uint64_t>{7}), result.retryContainerIds);
  EXPECT_EQ((std::vector<uint64_t>{7}), result.missingContainerIds);
  EXPECT_TRUE(result.writeFailedContainerIds.empty());
  EXPECT_EQ(0u, sink.publishedCounters.count(7));
  EXPECT_EQ(30, sink.publishedCounters[3].treeBytes);
}

TEST(TreeSizePublisher, MarksInvalidPlanForRetryAndWritesTargets)
{
  FakeTreeSizeCounterPublisher sink;
  auto plan = MakePublishPlan();
  plan.diagnostics.missingCounters = 1;
  plan.diagnostics.negativeCounters = 1;
  plan.diagnostics.duplicateTargets = 1;
  plan.diagnostics.unplannedCounters = 1;

  const auto result = TreeSizePublisher().Apply(plan, sink);

  EXPECT_EQ(TreeSizePublishApplyStatus::Success, result.status);
  EXPECT_EQ(2ull, result.attemptedTargets);
  EXPECT_EQ(0ull, result.skippedMissingTargets);
  EXPECT_EQ((std::vector<uint64_t>{7, 3}), sink.publishOrder);
  EXPECT_EQ((std::vector<uint64_t>{7, 3}), result.publishedContainerIds);
  EXPECT_EQ((std::vector<uint64_t>{7, 3}), result.retryContainerIds);
  EXPECT_TRUE(result.missingContainerIds.empty());
  EXPECT_TRUE(result.writeFailedContainerIds.empty());
  EXPECT_TRUE(result.error.empty());
}
