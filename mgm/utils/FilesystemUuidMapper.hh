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
#include <shared_mutex>

//------------------------------------------------------------------------------
//! @file  FilesystemUuidMapper.hh
//! @brief Utility class for uuid <-> fs id mapping of filesystems,
//!        and vice-versa
//------------------------------------------------------------------------------
EOSMGMNAMESPACE_BEGIN

class FilesystemUuidMapper {
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
    const std::string uuid);

  //----------------------------------------------------------------------------
  //! Retrieve size of the map
  //----------------------------------------------------------------------------
  size_t size() const;


private:
  mutable std::shared_timed_mutex mutex;

  //! Map translating a file system ID to a unique ID
  std::map<eos::common::FileSystem::fsid_t, std::string> fs2uuid;
  //! Map translating a unique ID to a filesystem ID
  std::map<std::string, eos::common::FileSystem::fsid_t> uuid2fs;
};

EOSMGMNAMESPACE_END

#endif
