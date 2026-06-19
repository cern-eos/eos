//------------------------------------------------------------------------------
//! @file TreeSizeAccountingEvent.hh
//! @brief Metadata carried by tree-size accounting events
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
#include <cstdint>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Tree-size accounting event kinds that can be sequenced and journaled
//------------------------------------------------------------------------------
enum class TreeSizeAccountingEventType {
  FileDelta = 0,
  FileCreate,
  FileDelete,
  ChildAttach,
  ChildDetach,
  SubtreeAttach,
  SubtreeDetach
};

//------------------------------------------------------------------------------
//! Sequence metadata attached to live tree-size accounting events
//------------------------------------------------------------------------------
struct TreeSizeAccountingEvent {
  uint64_t sequence = 0;
  TreeSizeAccountingEventType type = TreeSizeAccountingEventType::FileDelta;
  uint64_t directParentId = 0;
  uint64_t objectId = 0;
};

EOSNSNAMESPACE_END
