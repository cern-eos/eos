//------------------------------------------------------------------------------
//! @file TreeSizeAccountingService.hh
//! @brief Tree-size accounting protocol service interface
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
#include "namespace/ns_quarkdb/accounting/tree_size/TreeSizeAccountingFence.hh"
#include "namespace/ns_quarkdb/accounting/tree_size/TreeSizeJournalCaptureScope.hh"

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Narrow service surface used by tree-size recomputation
//------------------------------------------------------------------------------
class ITreeSizeAccountingService : public ITreeSizeJournalCaptureSource,
                                   public ITreeSizeAccountingFence {
public:
  ~ITreeSizeAccountingService() override = default;
};

EOSNSNAMESPACE_END
