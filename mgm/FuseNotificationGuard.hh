// ----------------------------------------------------------------------
// File: FuseNotificationGuard.hh
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#include "mgm/Namespace.hh"
#include "namespace/interface/Identifiers.hh"
#include <set>

class XrdMgmOfs;

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class FuseNotificationGuard - calls FuseXCast and friends on destruction
//------------------------------------------------------------------------------
class FuseNotificationGuard {
public:

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  FuseNotificationGuard(XrdMgmOfs *ofs);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~FuseNotificationGuard();

  //----------------------------------------------------------------------------
  //! Schedule a call to FuseXCastFile during this object's destruction
  //----------------------------------------------------------------------------
  void castFile(eos::FileIdentifier id);

  //----------------------------------------------------------------------------
  //! Schedule a call to FuseXCastContainer during this object's destruction
  //----------------------------------------------------------------------------
  void castContainer(eos::ContainerIdentifier id);

  //----------------------------------------------------------------------------
  //! Schedule a call to FuseXCastRefresh during this object's destruction
  //----------------------------------------------------------------------------
  void castRefresh(eos::ContainerIdentifier id, eos::ContainerIdentifier pid);

  //----------------------------------------------------------------------------
  //! Schedule a call to FuseXCastRefresh during this object's destruction
  //----------------------------------------------------------------------------
  void castRefresh(eos::FileIdentifier id, eos::ContainerIdentifier pid);

  //----------------------------------------------------------------------------
  //! Schedule a call to FuseXCastDeletion during this object's destruction
  //----------------------------------------------------------------------------
  void castDeletion(eos::ContainerIdentifier id, const std::string &name);

  //----------------------------------------------------------------------------
  //! Instead of casting during destruction, you can also call this function
  //! manually.
  //!
  //! Note: perform() calls clear() afterwards - any previously scheduled
  //! operations are cleared out.
  //----------------------------------------------------------------------------
  void perform();

  //----------------------------------------------------------------------------
  //! Clear - cancel any scheduled operations
  //----------------------------------------------------------------------------
  void clear();

private:
  XrdMgmOfs *mOfs = nullptr;

  //----------------------------------------------------------------------------
  //! Set of scheduled files to cast
  //----------------------------------------------------------------------------
  std::set<eos::FileIdentifier> mScheduledFiles;

  //----------------------------------------------------------------------------
  //! Set of scheduled containers to cast
  //----------------------------------------------------------------------------
  std::set<eos::ContainerIdentifier> mScheduledContainers;

  //----------------------------------------------------------------------------
  //! Set of scheduled "cast refresh" pairs
  //----------------------------------------------------------------------------
  std::set<std::pair<eos::ContainerIdentifier, eos::ContainerIdentifier>> mScheduledContainersRefresh;

  std::set<std::pair<eos::FileIdentifier, eos::ContainerIdentifier>> mScheduledFilesRefresh;

  //----------------------------------------------------------------------------
  //! Set of scheduled deletions
  //----------------------------------------------------------------------------
  std::set<std::pair<eos::ContainerIdentifier, std::string>> mScheduledDeletions;
};

EOSMGMNAMESPACE_END
