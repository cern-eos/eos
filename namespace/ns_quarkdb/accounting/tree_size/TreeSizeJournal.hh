//------------------------------------------------------------------------------
//! @file TreeSizeJournal.hh
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

#pragma once

#include "namespace/Namespace.hh"
#include "namespace/interface/IFileMDSvc.hh"
#include "namespace/interface/TreeSizeAccountingEvent.hh"
#include <cstdint>
#include <mutex>
#include <vector>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Raw tree-size journal entry captured before coverage classification
//------------------------------------------------------------------------------
struct TreeSizeJournalEntry {
  bool hasAccountingMetadata = true;
  TreeSizeAccountingEvent accountingEvent;
  TreeInfos treeChange;
};

//------------------------------------------------------------------------------
//! Raw journal diagnostics recorded without interrupting live accounting
//------------------------------------------------------------------------------
struct TreeSizeJournalDiagnostics {
  uint64_t missingMetadata = 0;
  uint64_t nonIncreasingSequence = 0;
};

//------------------------------------------------------------------------------
//! Copyable snapshot of journal entries and diagnostics
//------------------------------------------------------------------------------
struct TreeSizeJournalSnapshot {
  std::vector<TreeSizeJournalEntry> entries;
  TreeSizeJournalDiagnostics diagnostics;
  uint64_t latestSequence = 0;
};

//------------------------------------------------------------------------------
//! Raw append-only tree-size accounting journal
//------------------------------------------------------------------------------
class TreeSizeJournal {
public:
  void Append(const TreeSizeJournalEntry& entry);
  void Capture(const TreeSizeJournalEntry& entry);
  TreeSizeJournalSnapshot Snapshot() const;

private:
  mutable std::mutex mMutex;
  std::vector<TreeSizeJournalEntry> mEntries;
  TreeSizeJournalDiagnostics mDiagnostics;
  uint64_t mLatestSequence = 0;
};

EOSNSNAMESPACE_END
