//------------------------------------------------------------------------------
//! @file TreeSizeJournal.cc
//! @brief Raw tree-size accounting event journal
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
#include "namespace/ns_quarkdb/accounting/tree_size/TreeSizeJournalCaptureScope.hh"
#include <stdexcept>
#include <utility>

EOSNSNAMESPACE_BEGIN

void
TreeSizeJournal::Append(const TreeSizeJournalEntry& entry)
{
  std::lock_guard<std::mutex> lock(mMutex);

  if (!entry.hasAccountingMetadata) {
    ++mDiagnostics.missingMetadata;
  } else if (entry.accountingEvent.sequence <= mLatestSequence) {
    ++mDiagnostics.nonIncreasingSequence;
  }

  if (entry.hasAccountingMetadata && entry.accountingEvent.sequence > mLatestSequence) {
    mLatestSequence = entry.accountingEvent.sequence;
  }

  mEntries.emplace_back(entry);
}

void
TreeSizeJournal::Capture(const TreeSizeJournalEntry& entry)
{
  Append(entry);
}

TreeSizeJournalSnapshot
TreeSizeJournal::Snapshot() const
{
  std::lock_guard<std::mutex> lock(mMutex);
  TreeSizeJournalSnapshot snapshot;
  snapshot.entries = mEntries;
  snapshot.diagnostics = mDiagnostics;
  snapshot.latestSequence = mLatestSequence;
  return snapshot;
}

TreeSizeJournalCaptureScope::TreeSizeJournalCaptureScope(
    TreeSizeJournalCaptureController* controller,
    std::unique_ptr<TreeSizeJournalCaptureState> state)
    : mController(controller)
    , mState(std::move(state))
{
}

TreeSizeJournalCaptureScope::~TreeSizeJournalCaptureScope() { Stop(); }

void
TreeSizeJournalCaptureScope::Stop() noexcept
{
  if (mController && mState) {
    mController->Stop(mState.get());
    mController = nullptr;
  }
}

TreeSizeJournalSnapshot
TreeSizeJournalCaptureScope::StopAndSnapshot()
{
  Stop();

  if (!mState) {
    return {};
  }

  auto snapshot = mState->journal.Snapshot();
  mState.reset();
  return snapshot;
}

std::unique_ptr<TreeSizeJournalCaptureScope>
TreeSizeJournalCaptureController::StartCapture()
{
  auto state = std::make_unique<TreeSizeJournalCaptureState>();
  std::lock_guard<std::mutex> lock(mMutex);

  if (mActiveState) {
    throw std::logic_error("tree-size journal capture already active");
  }

  mActiveState = state.get();
  mActive.store(true, std::memory_order_release);
  return std::unique_ptr<TreeSizeJournalCaptureScope>(
      new TreeSizeJournalCaptureScope(this, std::move(state)));
}

bool
TreeSizeJournalCaptureController::IsActive() const noexcept
{
  return mActive.load(std::memory_order_acquire);
}

void
TreeSizeJournalCaptureController::Capture(const TreeSizeJournalEntry& entry)
{
  if (!IsActive()) {
    return;
  }

  TreeSizeJournalCaptureState* state = nullptr;

  {
    std::lock_guard<std::mutex> lock(mMutex);
    state = mActiveState;

    if (!state) {
      return;
    }

    ++state->inFlightCaptures;
  }

  try {
    state->journal.Capture(entry);
  } catch (...) {
    FinishCapture(state);
    throw;
  }

  FinishCapture(state);
}

void
TreeSizeJournalCaptureController::Stop(TreeSizeJournalCaptureState* state) noexcept
{
  std::unique_lock<std::mutex> lock(mMutex);

  if (mActiveState == state) {
    mActiveState = nullptr;
    mActive.store(false, std::memory_order_release);
  }

  mCv.wait(lock, [state]() { return state->inFlightCaptures == 0; });
}

void
TreeSizeJournalCaptureController::FinishCapture(
    TreeSizeJournalCaptureState* state) noexcept
{
  {
    std::lock_guard<std::mutex> lock(mMutex);

    if (state->inFlightCaptures != 0) {
      --state->inFlightCaptures;
    }
  }

  mCv.notify_all();
}

EOSNSNAMESPACE_END
