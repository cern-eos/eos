//------------------------------------------------------------------------------
//! @brief Container subtree accounting internal types
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
#include <cstdint>
#include <unordered_map>
#include <utility>
#include <vector>

EOSNSNAMESPACE_BEGIN

namespace container_accounting {

using AccountingSequence = uint64_t;

//! Ordered accounting event type
enum class EventType { Delta, Reset, Stop };

//! Ordered accounting event
struct AccountingEvent {
  AccountingEvent() = default;
  AccountingEvent(EventType type, AccountingSequence sequence,
                  AccountingSequence deltaSequence, IContainerMD::id_t id,
                  TreeInfos treeInfos)
      : mType(type)
      , mSequence(sequence)
      , mDeltaSequence(deltaSequence)
      , mId(id)
      , mTreeInfos(treeInfos)
  {
  }

  EventType mType = EventType::Delta;
  AccountingSequence mSequence = 0;
  AccountingSequence mDeltaSequence = 0;
  IContainerMD::id_t mId = 0;
  TreeInfos mTreeInfos;
};

//! Parent topology change record
struct ParentMove {
  AccountingSequence mSequence = 0;
  IContainerMD::id_t mOldParentId = 0;
  IContainerMD::id_t mNewParentId = 0;
};

//! Commit operation type
enum class CommitOperationType { DeltaMap, Reset };

//! Ordered operation applied by the propagation thread
struct CommitOperation {
  CommitOperation() = default;
  CommitOperation(AccountingSequence deltaSequence,
                  std::unordered_map<IContainerMD::id_t, TreeInfos> deltas)
      : mType(CommitOperationType::DeltaMap)
      , mDeltaSequence(deltaSequence)
      , mDeltas(std::move(deltas))
  {
  }

  CommitOperation(AccountingSequence deltaSequence, IContainerMD::id_t id,
                  TreeInfos treeInfos)
      : mType(CommitOperationType::Reset)
      , mDeltaSequence(deltaSequence)
      , mId(id)
      , mTreeInfos(treeInfos)
  {
  }

  CommitOperationType mType = CommitOperationType::DeltaMap;
  AccountingSequence mDeltaSequence = 0;
  std::unordered_map<IContainerMD::id_t, TreeInfos> mDeltas;
  IContainerMD::id_t mId = 0;
  TreeInfos mTreeInfos;
};

//! Aggregated accounting updates waiting to be committed
struct UpdateBatch {
  std::unordered_map<IContainerMD::id_t, TreeInfos> mMap; ///< Map updates
  std::vector<CommitOperation> mOperations;               ///< Ordered commit operations
  AccountingSequence mDeltaSequence = 0;                  ///< Max delta sequence in map
};

} // namespace container_accounting

EOSNSNAMESPACE_END
