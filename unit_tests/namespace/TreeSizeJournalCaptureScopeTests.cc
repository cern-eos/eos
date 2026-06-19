//------------------------------------------------------------------------------
// File: TreeSizeJournalCaptureScopeTests.cc
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
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <gtest/gtest.h>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <type_traits>

// Expose controller internals to seed a deterministic in-flight capture.
#define private public
#include "namespace/ns_quarkdb/accounting/tree_size/TreeSizeJournalCaptureScope.hh"
#undef private

using eos::TreeSizeJournalCaptureController;
using eos::TreeSizeJournalCaptureScope;

namespace {

eos::TreeSizeJournalEntry
MakeJournalEntry(uint64_t sequence)
{
  eos::TreeSizeJournalEntry entry;
  entry.accountingEvent.sequence = sequence;
  entry.accountingEvent.type = eos::TreeSizeAccountingEventType::FileDelta;
  entry.accountingEvent.directParentId = 7;
  entry.accountingEvent.objectId = 42;
  entry.treeChange = eos::TreeInfos{static_cast<int64_t>(sequence), 1, 0};
  return entry;
}

} // namespace

TEST(TreeSizeJournalCaptureScope, CapturesEntriesUntilStopped)
{
  TreeSizeJournalCaptureController controller;
  auto scope = controller.StartCapture();

  controller.Capture(MakeJournalEntry(1));
  controller.Capture(MakeJournalEntry(2));

  const auto snapshot = scope->StopAndSnapshot();
  ASSERT_EQ(2u, snapshot.entries.size());
  EXPECT_EQ(1ull, snapshot.entries[0].accountingEvent.sequence);
  EXPECT_EQ(2ull, snapshot.latestSequence);
  EXPECT_FALSE(controller.IsActive());
}

TEST(TreeSizeJournalCaptureScope, StopAndSnapshotDisablesFutureCaptures)
{
  TreeSizeJournalCaptureController controller;
  auto scope = controller.StartCapture();

  controller.Capture(MakeJournalEntry(1));
  const auto snapshot = scope->StopAndSnapshot();
  controller.Capture(MakeJournalEntry(2));

  ASSERT_EQ(1u, snapshot.entries.size());
  EXPECT_EQ(1ull, snapshot.entries[0].accountingEvent.sequence);

  auto next_scope = controller.StartCapture();
  const auto next_snapshot = next_scope->StopAndSnapshot();
  EXPECT_TRUE(next_snapshot.entries.empty());
}

TEST(TreeSizeJournalCaptureScope, DestructorStopsSession)
{
  TreeSizeJournalCaptureController controller;

  {
    auto scope = controller.StartCapture();
    controller.Capture(MakeJournalEntry(1));
  }

  EXPECT_FALSE(controller.IsActive());
  auto next_scope = controller.StartCapture();
  const auto snapshot = next_scope->StopAndSnapshot();
  EXPECT_TRUE(snapshot.entries.empty());
}

TEST(TreeSizeJournalCaptureScope, RejectsConcurrentSessions)
{
  TreeSizeJournalCaptureController controller;
  auto scope = controller.StartCapture();

  EXPECT_THROW(controller.StartCapture(), std::logic_error);
}

TEST(TreeSizeJournalCaptureScope, StopWaitsForInFlightCapture)
{
  TreeSizeJournalCaptureController controller;
  auto scope = controller.StartCapture();

  {
    std::lock_guard<std::mutex> lock(controller.mMutex);
    ++scope->mState->inFlightCaptures;
  }

  auto stop_future = std::async(std::launch::async, [&scope]() {
    scope->Stop();
    return true;
  });

  EXPECT_EQ(std::future_status::timeout,
            stop_future.wait_for(std::chrono::milliseconds(100)));

  {
    std::lock_guard<std::mutex> lock(controller.mMutex);
    --scope->mState->inFlightCaptures;
  }

  controller.mCv.notify_all();
  EXPECT_EQ(std::future_status::ready, stop_future.wait_for(std::chrono::seconds(2)));
  EXPECT_TRUE(stop_future.get());
  EXPECT_FALSE(controller.IsActive());
}

TEST(TreeSizeJournalCaptureScope, IsNonCopyableAndNonMovable)
{
  EXPECT_FALSE(std::is_copy_constructible<TreeSizeJournalCaptureScope>::value);
  EXPECT_FALSE(std::is_copy_assignable<TreeSizeJournalCaptureScope>::value);
  EXPECT_FALSE(std::is_move_constructible<TreeSizeJournalCaptureScope>::value);
  EXPECT_FALSE(std::is_move_assignable<TreeSizeJournalCaptureScope>::value);
}
