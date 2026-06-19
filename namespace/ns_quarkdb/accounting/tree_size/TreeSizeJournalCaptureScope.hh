//------------------------------------------------------------------------------
//! @file TreeSizeJournalCaptureScope.hh
//! @brief RAII control of tree-size journal capture sessions
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

#pragma once

#include "namespace/Namespace.hh"
#include "namespace/ns_quarkdb/accounting/tree_size/TreeSizeJournal.hh"
#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>

EOSNSNAMESPACE_BEGIN

class TreeSizeJournalCaptureController;

//------------------------------------------------------------------------------
//! Internal state for one active tree-size journal capture session
//------------------------------------------------------------------------------
struct TreeSizeJournalCaptureState {
  TreeSizeJournal journal;
  uint64_t inFlightCaptures = 0;
};

//------------------------------------------------------------------------------
//! RAII session for a tree-size journal capture. Destroying or stopping the
//! session disables future captures and waits for in-flight captures to finish.
//------------------------------------------------------------------------------
class TreeSizeJournalCaptureScope {
public:
  ~TreeSizeJournalCaptureScope();

  TreeSizeJournalCaptureScope(const TreeSizeJournalCaptureScope&) = delete;
  TreeSizeJournalCaptureScope& operator=(const TreeSizeJournalCaptureScope&) = delete;
  TreeSizeJournalCaptureScope(TreeSizeJournalCaptureScope&&) = delete;
  TreeSizeJournalCaptureScope& operator=(TreeSizeJournalCaptureScope&&) = delete;

  void Stop() noexcept;
  TreeSizeJournalSnapshot StopAndSnapshot();

private:
  friend class TreeSizeJournalCaptureController;

  TreeSizeJournalCaptureScope(TreeSizeJournalCaptureController* controller,
                              std::unique_ptr<TreeSizeJournalCaptureState> state);

  TreeSizeJournalCaptureController* mController = nullptr;
  std::unique_ptr<TreeSizeJournalCaptureState> mState;
};

//------------------------------------------------------------------------------
//! Permanent controller for per-recompute tree-size journal capture sessions
//------------------------------------------------------------------------------
class TreeSizeJournalCaptureController {
public:
  std::unique_ptr<TreeSizeJournalCaptureScope> StartCapture();
  bool IsActive() const noexcept;
  void Capture(const TreeSizeJournalEntry& entry);

private:
  friend class TreeSizeJournalCaptureScope;

  void Stop(TreeSizeJournalCaptureState* state) noexcept;
  void FinishCapture(TreeSizeJournalCaptureState* state) noexcept;

  mutable std::mutex mMutex;
  std::condition_variable mCv;
  TreeSizeJournalCaptureState* mActiveState = nullptr;
  std::atomic<bool> mActive{false};
};

//------------------------------------------------------------------------------
//! Interface for objects that can start raw tree-size journal capture
//------------------------------------------------------------------------------
class ITreeSizeJournalCaptureSource {
public:
  virtual ~ITreeSizeJournalCaptureSource() = default;
  virtual std::unique_ptr<TreeSizeJournalCaptureScope> StartTreeSizeJournalCapture() = 0;
};

EOSNSNAMESPACE_END
