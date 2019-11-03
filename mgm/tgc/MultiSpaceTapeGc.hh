// ----------------------------------------------------------------------
// File: MultiSpaceTapeGc.hh
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

#ifndef __EOSMGM_MULTISPACETAPEGC_HH__
#define __EOSMGM_MULTISPACETAPEGC_HH__

#include "mgm/Namespace.hh"
#include "mgm/tgc/ITapeGcMgm.hh"
#include "mgm/tgc/Lru.hh"
#include "mgm/tgc/SpaceToTapeGcMap.hh"
#include "mgm/tgc/TapeGcStats.hh"

#include <atomic>
#include <map>
#include <string>
#include <XrdSfs/XrdSfsInterface.hh>

/*----------------------------------------------------------------------------*/
/**
 * @file MultiSpaceTapeGc.hh
 *
 * @brief Class implementing a tape aware garbage collector that can work over
 * multiple EOS spaces
 *
 */
/*----------------------------------------------------------------------------*/
EOSTGCNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! A tape aware garbage collector that can work over multiple EOS spaces
//------------------------------------------------------------------------------
class MultiSpaceTapeGc
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param mgm the interface to the EOS MGM
  //----------------------------------------------------------------------------
  explicit MultiSpaceTapeGc(ITapeGcMgm &mgm);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~MultiSpaceTapeGc() = default;

  //----------------------------------------------------------------------------
  //! Delete copy constructor
  //----------------------------------------------------------------------------
  MultiSpaceTapeGc(const MultiSpaceTapeGc &) = delete;

  //----------------------------------------------------------------------------
  //! Delete assignment operator
  //----------------------------------------------------------------------------
  MultiSpaceTapeGc &operator=(const MultiSpaceTapeGc &) = delete;

  //----------------------------------------------------------------------------
  //! Enable garbage collection for the specified EOS space
  //!
  //! @param space The name of the EOs space
  //----------------------------------------------------------------------------
  void enable(const std::string &space) noexcept;

  //----------------------------------------------------------------------------
  //! Notify GC the specified file has been opened
  //! @note This method does nothing and returns immediately if the GC has not
  //! been enabled
  //!
  //! @param space the name of the EOS space where the file resides
  //! @param path file path
  //! @param fid file identifier
  //----------------------------------------------------------------------------
  void fileOpened(const std::string &space, const std::string &path,
    const IFileMD::id_t fid) noexcept;

  //----------------------------------------------------------------------------
  //! @return map from EOS space name to tape-aware GC statistics
  //----------------------------------------------------------------------------
  std::map<std::string, TapeGcStats> getStats() const;

  //----------------------------------------------------------------------------
  //! Handles a cmd=SFS_FSCTL_PLUGIO arg1=tgc request
  //----------------------------------------------------------------------------
  int handleFSCTL_PLUGIO_tgc(XrdOucErrInfo& error, eos::common::VirtualIdentity& vid, const XrdSecEntity* client);

private:

  //----------------------------------------------------------------------------
  // ! True if tape support is enabled
  //----------------------------------------------------------------------------
  std::atomic<bool> m_tapeEnabled;

  //----------------------------------------------------------------------------
  //! Thread safe map from EOS space name to tape aware garbage collector
  //----------------------------------------------------------------------------
  SpaceToTapeGcMap m_gcs;
};

EOSTGCNAMESPACE_END

#endif
