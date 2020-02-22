// ----------------------------------------------------------------------
// File: SmartSpaceStats.hh
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

#ifndef __EOSMGMSMARTSPACESTATS_HH__
#define __EOSMGMSMARTSPACESTATS_HH__

#include "mgm/tgc/CachedValue.hh"
#include "mgm/tgc/ITapeGcMgm.hh"
#include "mgm/tgc/SpaceStats.hh"

#include <ctime>
#include <mutex>
#include <string>

/*----------------------------------------------------------------------------*/
/**
 * @file SmartSpaceStats.hh
 *
 * @brief Class encapsulating how the tape-aware GC updates its internal
 * statistics about the EOS space it is managing.
 */
/*----------------------------------------------------------------------------*/
EOSTGCNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class encapsulating how the tape-aware GC updates its internal statistics
//! about the EOS space it is managing
//------------------------------------------------------------------------------
class SmartSpaceStats {
public:

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param spaceName Name of the EOS space being managed
  //! @param mgm Interface to the EOS MGM
  //! @param config Configuration of the tape-aware garbage collector
  //----------------------------------------------------------------------------
  SmartSpaceStats(const std::string &spaceName, ITapeGcMgm &mgm, CachedValue<SpaceConfig> &config);

  //----------------------------------------------------------------------------
  //! Notify this object that a file has been queued for deletion
  //!
  //! @param deletedFileSizeBytes The size of the deleted file in bytes
  //----------------------------------------------------------------------------
  void fileQueuedForDeletion(size_t deletedFileSizeBytes);

  //----------------------------------------------------------------------------
  //! @return statistics about the EOS space being managed
  //----------------------------------------------------------------------------
  SpaceStats get();

  //----------------------------------------------------------------------------
  //! @return timestamp at which the last query was made
  //----------------------------------------------------------------------------
  std::time_t getQueryTimestamp();

private:

  //----------------------------------------------------------------------------
  //! Name of the EOS space being managed
  //----------------------------------------------------------------------------
  std::string m_spaceName;

  //----------------------------------------------------------------------------
  //! Interface to the EOS MGM
  //----------------------------------------------------------------------------
  ITapeGcMgm &m_mgm;

  //----------------------------------------------------------------------------
  //! Mutex to protect the member variables of this object
  //----------------------------------------------------------------------------
  mutable std::mutex m_mutex;

  //----------------------------------------------------------------------------
  //! The timestamp at which the last query was made
  //----------------------------------------------------------------------------
  std::time_t m_queryTimestamp;

  //----------------------------------------------------------------------------
  //! Statistics about the EOS space being managed
  //----------------------------------------------------------------------------
  SpaceStats m_stats;

  //----------------------------------------------------------------------------
  //! The configuration of the tape-aware garbage collector
  //----------------------------------------------------------------------------
  CachedValue<SpaceConfig> &m_config;
};

EOSTGCNAMESPACE_END

#endif
