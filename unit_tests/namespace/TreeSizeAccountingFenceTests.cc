//------------------------------------------------------------------------------
// File: TreeSizeAccountingFenceTests.cc
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

#include "namespace/ns_quarkdb/accounting/ContainerAccounting.hh"
#include <gtest/gtest.h>

using eos::IFileMDChangeListener;
using eos::QuarkContainerAccounting;
using eos::TreeInfos;
using eos::TreeSizeAccountingEvent;
using eos::TreeSizeAccountingEventType;
using eos::TreeSizeAccountingFenceReleaseMode;
using eos::TreeSizeAccountingFenceRequest;

namespace {

TreeSizeAccountingFenceRequest
MakeRequest(uint64_t covered_id, uint64_t validated_through_sequence)
{
  TreeSizeAccountingFenceRequest request;
  request.coveredContainerIds.insert(covered_id);
  request.validatedThroughSequence = validated_through_sequence;
  return request;
}

void
QueueSequencedUpdate(QuarkContainerAccounting& accounting, uint64_t container_id,
                     uint64_t sequence)
{
  const TreeSizeAccountingEvent accounting_event{
      sequence, TreeSizeAccountingEventType::FileDelta, container_id, 99};
  IFileMDChangeListener::Event event(nullptr, IFileMDChangeListener::SizeChange,
                                     container_id, TreeInfos{10, 1, 0}, accounting_event);
  accounting.fileMDChanged(&event);
}

} // namespace

TEST(TreeSizeAccountingFence, ClassifiesPendingCoveredUpdatesBySequence)
{
  QuarkContainerAccounting accounting(nullptr, 0);
  QueueSequencedUpdate(accounting, 7, 5);
  QueueSequencedUpdate(accounting, 7, 12);

  const auto stats = accounting.AcquireTreeSizeAccountingFence(MakeRequest(7, 10));

  EXPECT_TRUE(stats.acquired);
  EXPECT_EQ(1ull, stats.coveredContainerIds);
  EXPECT_EQ(2ull, stats.drainedRawQueueUpdates);
  EXPECT_EQ(1ull, stats.includedInPublishUpdates);
  EXPECT_EQ(1ull, stats.replayAfterPublishUpdates);
  EXPECT_EQ(0ull, stats.unsequencedCoveredUpdates);
  EXPECT_EQ(0ull, stats.passedThroughUpdates);
  EXPECT_EQ(0ull, stats.inFlightCoveredUpdates);
  EXPECT_FALSE(stats.inFlightWaitTimeout);

  accounting.ReleaseTreeSizeAccountingFence(
      TreeSizeAccountingFenceReleaseMode::AbortBeforePublish);
}

TEST(TreeSizeAccountingFence, ReportsNoInFlightWorkForIdleFence)
{
  QuarkContainerAccounting accounting(nullptr, 0);

  const auto stats = accounting.AcquireTreeSizeAccountingFence(MakeRequest(7, 10));

  EXPECT_TRUE(stats.acquired);
  EXPECT_EQ(0ull, stats.inFlightCoveredUpdates);
  EXPECT_FALSE(stats.inFlightWaitTimeout);

  accounting.ReleaseTreeSizeAccountingFence(
      TreeSizeAccountingFenceReleaseMode::AbortBeforePublish);
}

TEST(TreeSizeAccountingFence, PassesThroughUncoveredUpdates)
{
  QuarkContainerAccounting accounting(nullptr, 0);
  QueueSequencedUpdate(accounting, 8, 5);

  const auto stats = accounting.AcquireTreeSizeAccountingFence(MakeRequest(7, 10));

  EXPECT_TRUE(stats.acquired);
  EXPECT_EQ(1ull, stats.drainedRawQueueUpdates);
  EXPECT_EQ(0ull, stats.drainedBatchUpdates);
  EXPECT_EQ(0ull, stats.includedInPublishUpdates);
  EXPECT_EQ(0ull, stats.replayAfterPublishUpdates);
  EXPECT_EQ(1ull, stats.passedThroughUpdates);
  EXPECT_EQ(0ull, stats.inFlightCoveredUpdates);
  EXPECT_FALSE(stats.inFlightWaitTimeout);

  accounting.ReleaseTreeSizeAccountingFence(
      TreeSizeAccountingFenceReleaseMode::AbortBeforePublish);
}

TEST(TreeSizeAccountingFence, CountsUnsequencedCoveredUpdates)
{
  QuarkContainerAccounting accounting(nullptr, 0);
  accounting.QueueForUpdate(7, TreeInfos{10, 1, 0});

  const auto stats = accounting.AcquireTreeSizeAccountingFence(MakeRequest(7, 10));

  EXPECT_TRUE(stats.acquired);
  EXPECT_EQ(1ull, stats.unsequencedCoveredUpdates);
  EXPECT_EQ(1ull, stats.replayAfterPublishUpdates);
  EXPECT_EQ(0ull, stats.includedInPublishUpdates);

  accounting.ReleaseTreeSizeAccountingFence(
      TreeSizeAccountingFenceReleaseMode::AbortBeforePublish);
}

TEST(TreeSizeAccountingFence, RejectsNestedFence)
{
  QuarkContainerAccounting accounting(nullptr, 0);

  const auto first = accounting.AcquireTreeSizeAccountingFence(MakeRequest(7, 10));
  const auto second = accounting.AcquireTreeSizeAccountingFence(MakeRequest(8, 10));

  EXPECT_TRUE(first.acquired);
  EXPECT_FALSE(second.acquired);

  accounting.ReleaseTreeSizeAccountingFence(
      TreeSizeAccountingFenceReleaseMode::AbortBeforePublish);
}

TEST(TreeSizeAccountingFence, AbortReplaysIncludedAndTailUpdates)
{
  QuarkContainerAccounting accounting(nullptr, 0);
  QueueSequencedUpdate(accounting, 7, 5);
  QueueSequencedUpdate(accounting, 7, 12);

  accounting.AcquireTreeSizeAccountingFence(MakeRequest(7, 10));
  accounting.ReleaseTreeSizeAccountingFence(
      TreeSizeAccountingFenceReleaseMode::AbortBeforePublish);

  const auto reacquire = accounting.AcquireTreeSizeAccountingFence(MakeRequest(7, 10));

  EXPECT_TRUE(reacquire.acquired);
  EXPECT_EQ(2ull, reacquire.drainedBatchUpdates);
  EXPECT_EQ(1ull, reacquire.includedInPublishUpdates);
  EXPECT_EQ(1ull, reacquire.replayAfterPublishUpdates);

  accounting.ReleaseTreeSizeAccountingFence(
      TreeSizeAccountingFenceReleaseMode::AbortBeforePublish);
}

TEST(TreeSizeAccountingFence, PublishSucceededDropsIncludedAndReplaysTail)
{
  QuarkContainerAccounting accounting(nullptr, 0);
  QueueSequencedUpdate(accounting, 7, 5);
  QueueSequencedUpdate(accounting, 7, 12);

  accounting.AcquireTreeSizeAccountingFence(MakeRequest(7, 10));
  accounting.ReleaseTreeSizeAccountingFence(
      TreeSizeAccountingFenceReleaseMode::PublishSucceeded);

  const auto reacquire = accounting.AcquireTreeSizeAccountingFence(MakeRequest(7, 10));

  EXPECT_TRUE(reacquire.acquired);
  EXPECT_EQ(1ull, reacquire.drainedBatchUpdates);
  EXPECT_EQ(0ull, reacquire.includedInPublishUpdates);
  EXPECT_EQ(1ull, reacquire.replayAfterPublishUpdates);

  accounting.ReleaseTreeSizeAccountingFence(
      TreeSizeAccountingFenceReleaseMode::AbortBeforePublish);
}

TEST(TreeSizeAccountingFence, QueuesCoveredUpdatesBehindActiveFence)
{
  QuarkContainerAccounting accounting(nullptr, 0);

  accounting.AcquireTreeSizeAccountingFence(MakeRequest(7, 10));
  QueueSequencedUpdate(accounting, 7, 12);
  const auto release = accounting.ReleaseTreeSizeAccountingFence(
      TreeSizeAccountingFenceReleaseMode::PublishSucceeded);

  EXPECT_TRUE(release.acquired);
  EXPECT_EQ(0ull, release.includedInPublishUpdates);
  EXPECT_EQ(1ull, release.replayAfterPublishUpdates);
}
