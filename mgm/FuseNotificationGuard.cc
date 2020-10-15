//------------------------------------------------------------------------------
// File: FuseNotificationGuard.cc
// Author: Georgios Bitzes - CERN
//------------------------------------------------------------------------------

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

#include "mgm/FuseNotificationGuard.hh"
#include "mgm/XrdMgmOfs.hh"

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FuseNotificationGuard::FuseNotificationGuard(XrdMgmOfs *ofs) : mOfs(ofs) {}

//----------------------------------------------------------------------------
// Destructor
//----------------------------------------------------------------------------
FuseNotificationGuard::~FuseNotificationGuard() {
  perform();
}

//------------------------------------------------------------------------------
// Schedule a call to FuseXCastFile during this object's destruction
//------------------------------------------------------------------------------
void FuseNotificationGuard::castFile(eos::FileIdentifier id) {
  mScheduledFiles.insert(id);
}

//------------------------------------------------------------------------------
//! Schedule a call to FuseXCastContainer during this object's destruction
//------------------------------------------------------------------------------
void FuseNotificationGuard::castContainer(eos::ContainerIdentifier id) {
  mScheduledContainers.insert(id);
}

//----------------------------------------------------------------------------
//! Schedule a call to FuseXCastRefresh during this object's destruction
//----------------------------------------------------------------------------
void FuseNotificationGuard::castRefresh(eos::ContainerIdentifier id, eos::ContainerIdentifier pid) {
  mScheduledContainersRefresh.emplace(id, pid);
}

void FuseNotificationGuard::castRefresh(eos::FileIdentifier id, eos::ContainerIdentifier pid) {
  mScheduledFilesRefresh.emplace(id, pid);
}

//------------------------------------------------------------------------------
// Schedule a call to FuseXCastDeletion during this object's destruction
//------------------------------------------------------------------------------
void FuseNotificationGuard::castDeletion(eos::ContainerIdentifier id, const std::string &name) {
  mScheduledDeletions.emplace(id, name);
}

//------------------------------------------------------------------------------
// Instead of casting during destruction, you can also do it manually
//------------------------------------------------------------------------------
void FuseNotificationGuard::perform() {
  for(auto it = mScheduledDeletions.begin(); it != mScheduledDeletions.end(); it++) {
    mOfs->FuseXCastDeletion(it->first, it->second);
  }

  for(auto it = mScheduledContainersRefresh.begin(); it != mScheduledContainersRefresh.end(); it++) {
    mOfs->FuseXCastRefresh(it->first, it->second);
  }

  for(auto it = mScheduledFilesRefresh.begin(); it != mScheduledFilesRefresh.end(); it++) {
    mOfs->FuseXCastRefresh(it->first, it->second);
  }

  for(auto it = mScheduledFiles.begin(); it != mScheduledFiles.end(); it++) {
    mOfs->FuseXCastFile(*it);
  }

  for(auto it = mScheduledContainers.begin(); it != mScheduledContainers.end(); it++) {
    mOfs->FuseXCastContainer(*it);
  }

  clear();
}

//------------------------------------------------------------------------------
//! Clear - cancel any scheduled operations
//------------------------------------------------------------------------------
void FuseNotificationGuard::clear() {
  mScheduledFiles.clear();
  mScheduledContainers.clear();
  mScheduledContainersRefresh.clear();
  mScheduledFilesRefresh.clear();
  mScheduledDeletions.clear();
}

EOSMGMNAMESPACE_END
