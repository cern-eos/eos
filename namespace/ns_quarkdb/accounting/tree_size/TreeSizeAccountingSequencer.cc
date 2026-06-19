//------------------------------------------------------------------------------
//! @file TreeSizeAccountingSequencer.cc
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

#include "namespace/ns_quarkdb/accounting/tree_size/TreeSizeAccountingSequencer.hh"

EOSNSNAMESPACE_BEGIN

uint64_t
TreeSizeAccountingSequencer::Reserve()
{
  return mNextSequence.fetch_add(1, std::memory_order_relaxed);
}

uint64_t
TreeSizeAccountingSequencer::LastReserved() const
{
  return mNextSequence.load(std::memory_order_relaxed) - 1;
}

TreeSizeAccountingSequencer&
GetTreeSizeAccountingSequencer()
{
  static TreeSizeAccountingSequencer sequencer;
  return sequencer;
}

TreeSizeAccountingEvent
ReserveTreeSizeAccountingEvent(TreeSizeAccountingEventType type,
                               uint64_t direct_parent_id, uint64_t object_id)
{
  TreeSizeAccountingEvent event;
  event.sequence = GetTreeSizeAccountingSequencer().Reserve();
  event.type = type;
  event.directParentId = direct_parent_id;
  event.objectId = object_id;
  return event;
}

EOSNSNAMESPACE_END
