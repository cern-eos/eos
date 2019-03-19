// ----------------------------------------------------------------------
// File: FilesystemUuidMapper.cc
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

#include "mgm/utils/FilesystemUuidMapper.hh"

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FilesystemUuidMapper::FilesystemUuidMapper() {}

//------------------------------------------------------------------------------
// Inject mapping. If the given id and / or uuid are already occupied, we
// refuse and return false.
//
// Otherwise, we return true.
//------------------------------------------------------------------------------
bool FilesystemUuidMapper::injectMapping(eos::common::FileSystem::fsid_t id,
  const std::string uuid) {

  //----------------------------------------------------------------------------
  // Valid id?
  //----------------------------------------------------------------------------
  if(id <= 0) {
    return false;
  }

  //----------------------------------------------------------------------------
  // Valid uuid?
  //----------------------------------------------------------------------------
  if(uuid.empty()) {
    return false;
  }

  std::unique_lock<std::shared_timed_mutex> lock(mutex);

  //----------------------------------------------------------------------------
  // Do we have an entry with the given uuid already?
  //----------------------------------------------------------------------------
  auto it1 = uuid2fs.find(uuid);
  if(it1 != uuid2fs.end()) {
    if(it1->second != id) {
      //------------------------------------------------------------------------
      // We already have an entry for the given uuid, and it's assigned to
      // a different id.. reject.
      //------------------------------------------------------------------------
      return false;
    }
  }

  //----------------------------------------------------------------------------
  // Do we have an entry with the given fsid already?
  //----------------------------------------------------------------------------
  auto it2 = fs2uuid.find(id);
  if(it2 != fs2uuid.end()) {
    if(it2->second != uuid) {
      //------------------------------------------------------------------------
      // We already have an entry for the given fsid, and it's assigned to
      // a different uuid.. reject.
      //------------------------------------------------------------------------
      return false;
    }
  }

  //----------------------------------------------------------------------------
  // No conflicts, insert.
  //----------------------------------------------------------------------------
  uuid2fs[uuid] = id;
  fs2uuid[id] = uuid;
  return true;
}

//------------------------------------------------------------------------------
// Retrieve size of the map
//------------------------------------------------------------------------------
size_t FilesystemUuidMapper::size() const {
  std::shared_lock<std::shared_timed_mutex> lock(mutex);
  return uuid2fs.size();
}

EOSMGMNAMESPACE_END
