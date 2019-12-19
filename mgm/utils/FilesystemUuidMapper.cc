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
#include "common/Assert.hh"

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
  const std::string &uuid) {

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
  eos_assert(uuid2fs.size() == fs2uuid.size());
  return uuid2fs.size();
}

//------------------------------------------------------------------------------
// Clear contents
//------------------------------------------------------------------------------
void FilesystemUuidMapper::clear() {
  std::unique_lock<std::shared_timed_mutex> lock(mutex);

  uuid2fs.clear();
  fs2uuid.clear();
}

//------------------------------------------------------------------------------
// Is there any entry with the given fsid?
//------------------------------------------------------------------------------
bool FilesystemUuidMapper::hasFsid(eos::common::FileSystem::fsid_t id) const {
  std::shared_lock<std::shared_timed_mutex> lock(mutex);
  return fs2uuid.find(id) != fs2uuid.end();
}

//------------------------------------------------------------------------------
// Is there any entry with the given uuid?
//------------------------------------------------------------------------------
bool FilesystemUuidMapper::hasUuid(const std::string &uuid) const {
  std::shared_lock<std::shared_timed_mutex> lock(mutex);
  return uuid2fs.find(uuid) != uuid2fs.end();
}

//------------------------------------------------------------------------------
// Retrieve the fsid that corresponds to the given uuid. Return 0 if none
// exists.
//------------------------------------------------------------------------------
eos::common::FileSystem::fsid_t FilesystemUuidMapper::lookup(
  const std::string &uuid) const {

  std::shared_lock<std::shared_timed_mutex> lock(mutex);
  auto it = uuid2fs.find(uuid);
  if(it == uuid2fs.end()) {
    return 0;
  }

  return it->second;
}

//------------------------------------------------------------------------------
//! Retrieve the uuid that corresponds to the given fsid. Return "" if none
//! exists.
//------------------------------------------------------------------------------
std::string FilesystemUuidMapper::lookup(
  eos::common::FileSystem::fsid_t id) const {

  std::shared_lock<std::shared_timed_mutex> lock(mutex);
  auto it = fs2uuid.find(id);
  if(it == fs2uuid.end()) {
    return "";
  }

  return it->second;
}

//------------------------------------------------------------------------------
//! Remove a mapping, given the fsid. Returns true if the element was found
//! and removed, and false if not found.
//------------------------------------------------------------------------------
bool FilesystemUuidMapper::remove(eos::common::FileSystem::fsid_t id) {
  std::unique_lock<std::shared_timed_mutex> lock(mutex);

  auto it = fs2uuid.find(id);
  if(it == fs2uuid.end()) {
    return false;
  }

  // Find the reverse relationship, which _must_ exist
  auto it2 = uuid2fs.find(it->second);
  eos_assert(it2 != uuid2fs.end());

  // Drop both
  fs2uuid.erase(it);
  uuid2fs.erase(it2);
  return true;
}

//------------------------------------------------------------------------------
//! Remove a mapping, given the uuid. Returns true if the element was found
//! and removed, and false if not found.
//------------------------------------------------------------------------------
bool FilesystemUuidMapper::remove(const std::string &uuid) {
  std::unique_lock<std::shared_timed_mutex> lock(mutex);

  auto it = uuid2fs.find(uuid);
  if(it == uuid2fs.end()) {
    return false;
  }

  // Find the reverse relationship, which _must_ exist
  auto it2 = fs2uuid.find(it->second);
  eos_assert(it2 != fs2uuid.end());

  // Drop both
  uuid2fs.erase(it);
  fs2uuid.erase(it2);
  return true;
}

//------------------------------------------------------------------------------
// Allocate a new fsid for the given uuid.
// - If the given uuid is registered already, simply map to the existing
//   one, don't modify anything.
// - If not, allocate a brand new, currently-unused fsid.
// - This map cannot hold more than 64k filesystems - legacy limitation from
//   original implementation in FsView, not sure if we can remove it.
//------------------------------------------------------------------------------
eos::common::FileSystem::fsid_t FilesystemUuidMapper::allocate(
  const std::string &uuid) {

  std::unique_lock<std::shared_timed_mutex> lock(mutex);

  // Given uuid exists already?
  auto it = uuid2fs.find(uuid);
  if(it != uuid2fs.end()) {
    return it->second; // nothing more to do
  }

  // Does not exist, need to allocate..
  if(uuid2fs.empty()) {
    // Entire structure is empty, start from 1.
    eos::common::FileSystem::fsid_t id = 1;
    uuid2fs[uuid] = id;
    fs2uuid[id] = uuid;
    return id;
  }

  // Find largest fsid currently in use
  eos::common::FileSystem::fsid_t maxInUse = fs2uuid.rbegin()->first;

  if(maxInUse < 64000) {
    // Allocate maxInUse+1
    eos::common::FileSystem::fsid_t id = maxInUse+1;
    uuid2fs[uuid] = id;
    fs2uuid[id] = uuid;
    return id;
  }

  // We don't allow values larger than 64k.. linear search from 1 to 64k
  // to find an open spot.
  for(eos::common::FileSystem::fsid_t id = 1; id < 64000; id++) {
    if(fs2uuid.count(id) == 0) {
      // Found an empty spot
      uuid2fs[uuid] = id;
      fs2uuid[id] = uuid;
      return id;
    }
  }

  // Unable to allocate, something is wrong, abort
  eos_static_crit("all filesystem id's exhausted (64.000) - aborting the program");
  exit(-1);
}

EOSMGMNAMESPACE_END
