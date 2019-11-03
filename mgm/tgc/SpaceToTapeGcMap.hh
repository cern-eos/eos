// ----------------------------------------------------------------------
// File: SpaceToTapeGcMap.hh
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

#ifndef __EOSMGM_SPACETOTAPEAGCMAP_HH__
#define __EOSMGM_SPACETOTAPEAGCMAP_HH__

#include "mgm/tgc/ITapeGcMgm.hh"
#include "mgm/tgc/TapeGc.hh"
#include "mgm/tgc/TapeGcStats.hh"

#include <map>

/*----------------------------------------------------------------------------*/
/**
 * @file SpaceToTapeGcMap.hh
 *
 * @brief Class implementing a thread safe map from EOS space name to tape aware
 * garbage collector
 *
 */
/*----------------------------------------------------------------------------*/
EOSTGCNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class implementing a thread safe map from EOS space name to tape aware
//! garbage collector
//------------------------------------------------------------------------------
class SpaceToTapeGcMap {
public:

  //----------------------------------------------------------------------------
  //! Constructor.
  //!
  //! @param mgm the interface to the EOS MGM
  //----------------------------------------------------------------------------
  SpaceToTapeGcMap(ITapeGcMgm &mgm);

  //----------------------------------------------------------------------------
  //! Deletion of copy constructor.
  //----------------------------------------------------------------------------
  SpaceToTapeGcMap(const SpaceToTapeGcMap &) = delete;

  //----------------------------------------------------------------------------
  //! Deletion of move constructor.
  //----------------------------------------------------------------------------
  SpaceToTapeGcMap(const SpaceToTapeGcMap &&) = delete;

  //----------------------------------------------------------------------------
  //! Exception thrown when a tape aware garbage collector already exists.
  //----------------------------------------------------------------------------
  struct GcAlreadyExists: public std::runtime_error {
    GcAlreadyExists(const std::string &msg): std::runtime_error(msg) {}
  };

  //----------------------------------------------------------------------------
  //! Thread safe method that creates a tape-aware garbage collector for the
  //! specified EOS space.
  //!
  //! @param space The name of the EOS space.
  //! @preturn A reference to the newly created tape-aware garbage collector.
  //! @throw GcAlreadyExists If a tape aware garbage collector already exists
  //! for the specified EOS space.
  //----------------------------------------------------------------------------
  TapeGc &createGc(const std::string &space);

  //----------------------------------------------------------------------------
  //! Exception thrown when an unknown EOS space is encountered.
  //----------------------------------------------------------------------------
  struct UnknownEOSSpace: public std::runtime_error {
    UnknownEOSSpace(const std::string &msg): std::runtime_error(msg) {}
  };

  //----------------------------------------------------------------------------
  //! Thread safe method that returns the garbage collector associated with the
  //! specified EOS space.
  //!
  //! @param space The name of the EOS space.
  //! @return The tape aware garbage collector associated with the specified EOS
  //! space.
  //! @throw UnknownEOSSpace If the specified EOS space is unknown.
  //----------------------------------------------------------------------------
  TapeGc &getGc(const std::string &space) const;

  //----------------------------------------------------------------------------
  //! @return map from EOS space name to tape-aware GC statistics
  //----------------------------------------------------------------------------
  std::map<std::string, TapeGcStats> getStats() const;

  //----------------------------------------------------------------------------
  //! Writes the JSON representation of this object to the specified stream.
  //!
  //! @param os Input/Output parameter specifying the stream to write to.
  //! @param maxLen The maximum length the stream should be.  A value of 0 means
  //! unlimited.  This method can go over the maxLen limit but it MUST throw
  //! a MaxLenExceeded exception if it does.
  //!
  //! @throw MaxLenExceeded if the length of the JSON string has exceeded maxLen
  //----------------------------------------------------------------------------
  void toJson(std::ostringstream &os, std::uint64_t maxLen = 0) const;

private:

  //--------------------------------------------------------------------------
  //! The interface to the EOS MGM
  //--------------------------------------------------------------------------
  ITapeGcMgm &m_mgm;

  //--------------------------------------------------------------------------
  //! Mutex protecting the map
  //--------------------------------------------------------------------------
  mutable std::mutex m_mutex;

  //----------------------------------------------------------------------------
  //! Map from space name to tape aware garbage collector
  //----------------------------------------------------------------------------
  std::map<std::string, std::unique_ptr<TapeGc> > m_gcs;
};

EOSTGCNAMESPACE_END

#endif
