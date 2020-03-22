//------------------------------------------------------------------------------
// File: FileSystemRegistry.hh
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

#pragma once

#include "mgm/Namespace.hh"
#include "mgm/FileSystem.hh"
#include "common/RWMutex.hh"
#include <map>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class to keep track of currently registered filesystems. For compatibility
//! purposes with what existed before, initially this class will behave exactly
//! like an id -> FileSystem* map.
//!
//! The API (together with users of this class) will be improved incrementally.
//------------------------------------------------------------------------------

class FileSystemRegistry
{
public:
  //----------------------------------------------------------------------------
  //! Types
  //----------------------------------------------------------------------------
  using const_iterator =
    std::map<eos::common::FileSystem::fsid_t, mgm::FileSystem*>::const_iterator;

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  FileSystemRegistry();

  //----------------------------------------------------------------------------
  //! Map compatibility: begin()
  //----------------------------------------------------------------------------
  const_iterator begin() const
  {
    return mById.cbegin();
  }

  //----------------------------------------------------------------------------
  //! Map compatibility: cbegin()
  //----------------------------------------------------------------------------
  const_iterator cbegin() const
  {
    return mById.cbegin();
  }

  //----------------------------------------------------------------------------
  //! Map compatibility: end()
  //----------------------------------------------------------------------------
  const_iterator end() const
  {
    return mById.cend();
  }

  //----------------------------------------------------------------------------
  //! Map compatibility: cend()
  //----------------------------------------------------------------------------
  const_iterator cend() const
  {
    return mById.cend();
  }

  //----------------------------------------------------------------------------
  //! Lookup a FileSystem object by ID - return nullptr if none exists.
  //----------------------------------------------------------------------------
  mgm::FileSystem* lookupByID(eos::common::FileSystem::fsid_t id) const;

  //----------------------------------------------------------------------------
  //! Lookup a FileSystem id by FileSystem pointer - return 0 if none exists
  //----------------------------------------------------------------------------
  eos::common::FileSystem::fsid_t lookupByPtr(mgm::FileSystem* fs) const;

  //----------------------------------------------------------------------------
  //! Lookup a FileSystem object by queuepath - return nullptr if none exists
  //----------------------------------------------------------------------------
  mgm::FileSystem* lookupByQueuePath(const std::string& queuepath) const;

  //----------------------------------------------------------------------------
  //! Does a FileSystem with the given id exist?
  //----------------------------------------------------------------------------
  bool exists(eos::common::FileSystem::fsid_t id) const;

  //----------------------------------------------------------------------------
  //! Register new FileSystem with the given ID.
  //!
  //! Refuse if either the FileSystem pointer already exists, or another
  //! FileSystem has the same ID.
  //----------------------------------------------------------------------------
  bool registerFileSystem(const common::FileSystemLocator& locator,
                          common::FileSystem::fsid_t fsid, mgm::FileSystem* fs);

  //----------------------------------------------------------------------------
  //! Return number of registered filesystems
  //----------------------------------------------------------------------------
  size_t size() const;

  //----------------------------------------------------------------------------
  //! Erase by fsid - return true if found and erased, false otherwise
  //----------------------------------------------------------------------------
  bool eraseById(eos::common::FileSystem::fsid_t id);

  //----------------------------------------------------------------------------
  //! Erase by ptr - return true if found and erased, false otherwise
  //------------------------- ---------------------------------------------------
  bool eraseByPtr(mgm::FileSystem* fs);

  //------------------------------------------------------------------------------
  //! Entirely clear registry contents
  //------------------------------------------------------------------------------
  void clear();

private:
  struct IdAndQueuePath {
    eos::common::FileSystem::fsid_t mId;
    std::string mQueuePath;

    IdAndQueuePath(eos::common::FileSystem::fsid_t id,
                   const std::string& queuepath) : mId(id), mQueuePath(queuepath) {}
  };

  mutable eos::common::RWMutex mMutex;
  std::map<eos::common::FileSystem::fsid_t, mgm::FileSystem*> mById;
  std::map<mgm::FileSystem*, IdAndQueuePath> mByFsPtr;
  std::map<std::string, mgm::FileSystem*> mByQueuePath;
};

EOSMGMNAMESPACE_END
