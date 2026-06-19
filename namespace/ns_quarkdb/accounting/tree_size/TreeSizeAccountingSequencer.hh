//------------------------------------------------------------------------------
//! @file TreeSizeAccountingSequencer.hh
//! @brief Sequence reservation for tree-size accounting events
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
#include "namespace/interface/TreeSizeAccountingEvent.hh"
#include <atomic>
#include <cstdint>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Thread-safe monotonic sequence generator for accounting protocol events
//------------------------------------------------------------------------------
class TreeSizeAccountingSequencer {
public:
  TreeSizeAccountingSequencer() = default;
  TreeSizeAccountingSequencer(const TreeSizeAccountingSequencer&) = delete;
  TreeSizeAccountingSequencer& operator=(const TreeSizeAccountingSequencer&) = delete;

  uint64_t Reserve();
  uint64_t LastReserved() const;

private:
  std::atomic<uint64_t> mNextSequence{1};
};

//------------------------------------------------------------------------------
//! Return the process-local tree-size accounting sequencer
//------------------------------------------------------------------------------
TreeSizeAccountingSequencer& GetTreeSizeAccountingSequencer();

//------------------------------------------------------------------------------
//! Reserve and describe one sequenced tree-size accounting event
//------------------------------------------------------------------------------
TreeSizeAccountingEvent ReserveTreeSizeAccountingEvent(TreeSizeAccountingEventType type,
                                                       uint64_t direct_parent_id,
                                                       uint64_t object_id);

EOSNSNAMESPACE_END
