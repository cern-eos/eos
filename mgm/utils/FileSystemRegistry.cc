//------------------------------------------------------------------------------
// File: FileSystemRegistry.cc
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

#include "mgm/utils/FileSystemRegistry.hh"
#include "common/Logging.hh"
#include "common/Assert.hh"

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FileSystemRegistry::FileSystemRegistry() {

}

//------------------------------------------------------------------------------
// Lookup a FileSystem object by ID - return nullptr if none exists.
//------------------------------------------------------------------------------
FileSystem* FileSystemRegistry::lookupByID(eos::common::FileSystem::fsid_t id) const {
  std::shared_lock<std::shared_timed_mutex> lock(mMutex);

  auto it = mById.find(id);
  if(it == mById.end()) {
    return nullptr;
  }

  return it->second;
}

//------------------------------------------------------------------------------
// Lookup a FileSystem id by FileSystem pointer - return 0 if none exists
//------------------------------------------------------------------------------
eos::common::FileSystem::fsid_t FileSystemRegistry::lookupByPtr(mgm::FileSystem* fs) const {
  std::shared_lock<std::shared_timed_mutex> lock(mMutex);

  auto it = mByFsPtr.find(fs);
  if(it == mByFsPtr.end()) {
    return 0;
  }

  return it->second;
}

//----------------------------------------------------------------------------
// Register new FileSystem with the given ID.
//
// Refuse if either the FileSystem pointer already exists, or another
// FileSystem has the same ID.
//----------------------------------------------------------------------------
bool FileSystemRegistry::registerFileSystem(eos::common::FileSystem::fsid_t fsid, FileSystem *fs) {
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);

  //----------------------------------------------------------------------------
  // fsid collision?
  //----------------------------------------------------------------------------
  if(mById.count(fsid) > 0) {
    eos_static_crit("Could not insert fsid=%llu to FileSystemRegistry - fsid already exists!", fsid);
    return false;
  }

  //----------------------------------------------------------------------------
  // fs pointer collision?
  //----------------------------------------------------------------------------
  if(mByFsPtr.count(fs) > 0) {
    eos_static_crit("Could not insert fsid=%llu to FileSystemRegistry - fs pointer %x already exists!", fsid, fs);
    return false;
  }

  //----------------------------------------------------------------------------
  // Attempting to insert fsid=0?
  //----------------------------------------------------------------------------
  if(fsid == 0) {
    eos_static_crit("Attempted to insert fsid=0 into FileSystemRegistry");
    return false;
  }

  //----------------------------------------------------------------------------
  // Attempting to insert fs=nullptr?
  //----------------------------------------------------------------------------
  if(fs == nullptr) {
    eos_static_crit("Attempted to insert fs=nullptr into FileSystemRegistry");
    return false;
  }


  //----------------------------------------------------------------------------
  // All good, insert.
  //----------------------------------------------------------------------------
  mById.emplace(fsid, fs);
  mByFsPtr.emplace(fs, fsid);

  eos_assert(mById.size() == mByFsPtr.size());
  return true;
}

//------------------------------------------------------------------------------
// Erase by fsid - return true if found and erased, false otherwise
//------------------------------------------------------------------------------
bool FileSystemRegistry::eraseById(eos::common::FileSystem::fsid_t id) {
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);

  //----------------------------------------------------------------------------
  // id exists?
  //----------------------------------------------------------------------------
  auto it = mById.find(id);
  if(it == mById.end()) {
    return false;
  }

  //----------------------------------------------------------------------------
  // Lookup corresponding mByFsPtr entry, which MUST exist
  //----------------------------------------------------------------------------
  auto it2 = mByFsPtr.find(it->second);
  eos_assert(it2 != mByFsPtr.end());

  mById.erase(it);
  mByFsPtr.erase(it2);

  eos_assert(mById.size() == mByFsPtr.size());
  return true;
}

//------------------------------------------------------------------------------
// Erase by ptr - return true if found and erased, false otherwise
//------------------------- ----------------------------------------------------
bool FileSystemRegistry::eraseByPtr(mgm::FileSystem *fs) {
  std::unique_lock<std::shared_timed_mutex> lock(mMutex);

  //----------------------------------------------------------------------------
  // ptr exists?
  //----------------------------------------------------------------------------
  auto it = mByFsPtr.find(fs);
  if(it == mByFsPtr.end()) {
    return false;
  }

  //----------------------------------------------------------------------------
  // Lookup corresponding mById entry, which MUST exist
  //----------------------------------------------------------------------------
  auto it2 = mById.find(it->second);
  eos_assert(it2 != mById.end());

  mByFsPtr.erase(it);
  mById.erase(it2);

  eos_assert(mById.size() == mByFsPtr.size());
  return true;
}

//------------------------------------------------------------------------------
// Does a FileSystem with the given id exist?
//------------------------------------------------------------------------------
bool FileSystemRegistry::exists(eos::common::FileSystem::fsid_t id) const {
  std::shared_lock<std::shared_timed_mutex> lock(mMutex);
  return mById.count(id) > 0;
}

//------------------------------------------------------------------------------
// Return number of registered filesystems
//------------------------------------------------------------------------------
size_t FileSystemRegistry::size() const {
  std::shared_lock<std::shared_timed_mutex> lock(mMutex);
  return mById.size();
}

EOSMGMNAMESPACE_END
