// ----------------------------------------------------------------------
// File: TapeGcStats.hh
// Author: Steven Murray - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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

#ifndef __EOSMGMTGCTAPEGCSTATS_HH__
#define __EOSMGMTGCTAPEGCSTATS_HH__

#include "mgm/Namespace.hh"
#include "mgm/tgc/SpaceStats.hh"

#include <cstdint>

/*----------------------------------------------------------------------------*/
/**
 * @file TapeGcStats.hh
 *
 * @brief Statistics about a tape-aware GC
 *
 */
/*----------------------------------------------------------------------------*/
EOSTGCNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Statistics about a tape-aware GC
//------------------------------------------------------------------------------
struct TapeGcStats {
  //----------------------------------------------------------------------------
  //! Constructor.
  //----------------------------------------------------------------------------
  TapeGcStats():
    nbStagerrms(0),
    lruQueueSize(0),
    queryTimestamp(0) {
  }

  //----------------------------------------------------------------------------
  //! Number of files successfully stagerrm'ed since TapeGc started.
  //! This value is Zero in the case of an error.
  //----------------------------------------------------------------------------
  std::uint64_t nbStagerrms;

  //----------------------------------------------------------------------------
  //! Size of the LRU queue.  This value is Zero in the case of an error.
  //----------------------------------------------------------------------------
  Lru::FidQueue::size_type lruQueueSize;

  //----------------------------------------------------------------------------
  //! Statistics about the EOS space being managed by the tape-aware garbage
  //! collector
  //----------------------------------------------------------------------------
  SpaceStats spaceStats;

  //----------------------------------------------------------------------------
  //! Timestamp at which the EOS space was queried.  This value is zero in the
  //! case of error.
  //----------------------------------------------------------------------------
  time_t queryTimestamp;
};

EOSTGCNAMESPACE_END

#endif
