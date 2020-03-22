//------------------------------------------------------------------------------
// File: FilesystemUuidMapper.hh
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

#ifndef EOS_MGM_UTILS_FILESYSTEM_UUID_MAPPER_HH
#define EOS_MGM_UTILS_FILESYSTEM_UUID_MAPPER_HH

#include "mgm/Namespace.hh"
#include "common/FileSystem.hh"
#include "common/RWMutex.hh"

//------------------------------------------------------------------------------
//! @file  FilesystemUuidMapper.hh
//! @brief Utility class for uuid <-> fs id mapping of filesystems,
//!        and vice-versa
//------------------------------------------------------------------------------
EOSMGMNAMESPACE_BEGIN

class FilesystemUuidMapper
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  FilesystemUuidMapper();

  //----------------------------------------------------------------------------
  //! Inject mapping. If the given id and / or uuid are already occupied, we
  //! refuse and return false.
  //!
  //! Otherwise, we return true.
  //----------------------------------------------------------------------------
  bool injectMapping(eos::common::FileSystem::fsid_t id,
                     const std::string& uuid);

  //----------------------------------------------------------------------------
  //! Is there any entry with the given fsid?
  //----------------------------------------------------------------------------
  bool hasFsid(eos::common::FileSystem::fsid_t id) const;

  //----------------------------------------------------------------------------
  //! Is there any entry with the given uuid?
  //----------------------------------------------------------------------------
  bool hasUuid(const std::string& uuid) const;

  //----------------------------------------------------------------------------
  //! Retrieve size of the map
  //----------------------------------------------------------------------------
  size_t size() const;

  //----------------------------------------------------------------------------
  //! Retrieve the fsid that corresponds to the given uuid. Return 0 if none
  //! exists.
  //----------------------------------------------------------------------------
  eos::common::FileSystem::fsid_t lookup(const std::string& uuid) const;

  //----------------------------------------------------------------------------
  //! Retrieve the uuid that corresponds to the given fsid. Return "" if none
  //! exists.
  //----------------------------------------------------------------------------
  std::string lookup(eos::common::FileSystem::fsid_t id) const;

  //----------------------------------------------------------------------------
  //! Remove a mapping, given the fsid. Returns true if the element was found
  //! and removed, and false if not found.
  //----------------------------------------------------------------------------
  bool remove(eos::common::FileSystem::fsid_t id);

  //----------------------------------------------------------------------------
  //! Remove a mapping, given the uuid. Returns true if the element was found
  //! and removed, and false if not found.
  //----------------------------------------------------------------------------
  bool remove(const std::string& uuid);

  //----------------------------------------------------------------------------
  //! Clear contents
  //----------------------------------------------------------------------------
  void clear();

  //----------------------------------------------------------------------------
  //! Allocate a new fsid for the given uuid.
  //! - If the given uuid is registered already, simply map to the existing
  //!   one, don't modify anything.
  //! - If not, allocate a brand new, currently-unused fsid.
  //! - This map cannot hold more than 64k filesystems - legacy limitation from
  //!   original implementation in FsView, not sure if we can remove it.
  //----------------------------------------------------------------------------
  eos::common::FileSystem::fsid_t allocate(const std::string& uuid);

private:
  mutable eos::common::RWMutex mMutex;
  //! Map translating a file system ID to a unique ID
  std::map<eos::common::FileSystem::fsid_t, std::string> fs2uuid;
  //! Map translating a unique ID to a filesystem ID
  std::map<std::string, eos::common::FileSystem::fsid_t> uuid2fs;
};

EOSMGMNAMESPACE_END

#endif
