// ----------------------------------------------------------------------
// File: TapeAwareGcFreeSpace.hh
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

#ifndef __EOSMGM_TAPEAWAREGCFREESPACE_HH__
#define __EOSMGM_TAPEAWAREGCFREESPACE_HH__

#include "mgm/Namespace.hh"
#include "mgm/tgc/TapeAwareGcCachedValue.hh"

#include <mutex>
#include <stdexcept>
#include <stdint.h>
#include <string>
#include <time.h>

/*----------------------------------------------------------------------------*/
/**
 * @file TapeAwareGcFreeSpace.hh
 *
 * @brief Templated class for creating a time based cache for a single
 * variable.
 *
 */
/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class for getting the amount of free space in a specific EOS space.  This
//! class respects the constraint of a specified delay between free space
//! queries to the EOS MGM.
//------------------------------------------------------------------------------
class TapeAwareGcFreeSpace {
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //! @param spaceName The name of the space to be queried for free space.
  //! @param defaultSpaceQueryPeriodSecs The default delay in seconds
  //! between free space queries to the EOS MGM.
  //----------------------------------------------------------------------------
  TapeAwareGcFreeSpace(const std::string &spaceName, const time_t defaultSpaceQueryPeriodSecs);

  //----------------------------------------------------------------------------
  //! Notify this object that a file has been queued for deletion so that
  //! the amount of free space can be updated without having to wait for the
  //! next query to the EOS MGM
  //----------------------------------------------------------------------------
  void fileQueuedForDeletion(const size_t deletedFileSize);

  //----------------------------------------------------------------------------
  //! Returns the amount of free space in bytes
  //! @return the amount of free space in bytes
  //! @throw TapeAwareGcSpaceNotFound when the EOS space named m_spaceName
  //! cannot be found
  //----------------------------------------------------------------------------
  uint64_t getFreeBytes();

private:

  /// Mutex
  std::mutex m_mutex;

  /// The name of the space to be queried for free space
  std::string m_spaceName;

  //----------------------------------------------------------------------------
  //! Cached configuration value for the delay in seconds between space queries
  //! to the EOS MGM
  //----------------------------------------------------------------------------
  TapeAwareGcCachedValue<time_t> m_cachedSpaceQueryPeriodSecs;

  /// The current amount of free space in bytes
  uint64_t m_freeSpaceBytes;

  /// The timestamp at which the last free space query was made
  time_t m_freeSpaceQueryTimestamp;

  //----------------------------------------------------------------------------
  //! Queries the EOS MGM for free space
  //! @param spaceName The name of the EOS space to be queried
  //! @return the amount of free space in bytes
  //! @throw TapeAwareGcSpaceNotFound when the EOS space named m_spaceName
  //! cannot be found
  //----------------------------------------------------------------------------
  uint64_t queryMgmForFreeBytes();

  //----------------------------------------------------------------------------
  //! @return The configured delay in seconds between free space queries for the
  //! specified space.  If the configuration value cannot be determined for
  //! whatever reason then the specified default value is returned.
  //!
  //! @param spaceName The name of the space
  //! @param defaultValue The default value
  //----------------------------------------------------------------------------
  static uint64_t getConfSpaceQueryPeriodSecs(const std::string spaceName,
    const uint64_t defaultValue) noexcept;
}; // class TapeAwareGcFreeSpace

EOSMGMNAMESPACE_END

#endif
