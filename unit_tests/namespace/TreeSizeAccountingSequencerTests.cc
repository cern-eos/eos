//------------------------------------------------------------------------------
// File: TreeSizeAccountingSequencerTests.cc
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

#include "namespace/interface/IFileMDSvc.hh"
#include "namespace/ns_quarkdb/accounting/tree_size/TreeSizeAccountingSequencer.hh"
#include <algorithm>
#include <gtest/gtest.h>
#include <mutex>
#include <thread>
#include <vector>

using eos::TreeSizeAccountingEvent;
using eos::TreeSizeAccountingEventType;
using eos::TreeSizeAccountingSequencer;

TEST(TreeSizeAccountingSequencer, ReservesMonotonicSequences)
{
  TreeSizeAccountingSequencer sequencer;

  EXPECT_EQ(1ull, sequencer.Reserve());
  EXPECT_EQ(2ull, sequencer.Reserve());
  EXPECT_EQ(3ull, sequencer.Reserve());
  EXPECT_EQ(3ull, sequencer.LastReserved());
}

TEST(TreeSizeAccountingSequencer, ReservesUniqueConcurrentSequences)
{
  TreeSizeAccountingSequencer sequencer;
  std::mutex mutex;
  std::vector<uint64_t> sequences;
  std::vector<std::thread> threads;
  constexpr uint32_t thread_count = 8;
  constexpr uint32_t reservations_per_thread = 128;
  sequences.reserve(thread_count * reservations_per_thread);

  for (uint32_t thread = 0; thread < thread_count; ++thread) {
    threads.emplace_back([&]() {
      std::vector<uint64_t> local_sequences;
      local_sequences.reserve(reservations_per_thread);

      for (uint32_t index = 0; index < reservations_per_thread; ++index) {
        local_sequences.emplace_back(sequencer.Reserve());
      }

      std::lock_guard<std::mutex> lock(mutex);
      sequences.insert(sequences.end(), local_sequences.begin(), local_sequences.end());
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  std::sort(sequences.begin(), sequences.end());
  ASSERT_EQ(thread_count * reservations_per_thread, sequences.size());

  for (uint64_t index = 0; index < sequences.size(); ++index) {
    EXPECT_EQ(index + 1, sequences[index]);
  }
}

TEST(TreeSizeAccountingEvent, FileMDChangeEventHasNoAccountingByDefault)
{
  eos::IFileMDChangeListener::Event event(nullptr,
                                          eos::IFileMDChangeListener::SizeChange);

  EXPECT_FALSE(event.treeSizeAccountingEvent.has_value());
}

TEST(TreeSizeAccountingEvent, FileMDChangeEventPreservesAccountingMetadata)
{
  TreeSizeAccountingEvent accounting_event;
  accounting_event.sequence = 42;
  accounting_event.type = TreeSizeAccountingEventType::FileDelta;
  accounting_event.directParentId = 7;
  accounting_event.objectId = 99;

  eos::IFileMDChangeListener::Event event(nullptr, eos::IFileMDChangeListener::SizeChange,
                                          7, {12, 0, 0}, accounting_event);

  ASSERT_TRUE(event.treeSizeAccountingEvent.has_value());
  EXPECT_EQ(42ull, event.treeSizeAccountingEvent->sequence);
  EXPECT_EQ(TreeSizeAccountingEventType::FileDelta, event.treeSizeAccountingEvent->type);
  EXPECT_EQ(7ull, event.treeSizeAccountingEvent->directParentId);
  EXPECT_EQ(99ull, event.treeSizeAccountingEvent->objectId);
}
