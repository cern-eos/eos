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
  auto it = mById.find(id);
  if(it == mById.end()) {
    return nullptr;
  }

  return it->second;
}

//------------------------------------------------------------------------------
//! Does a FileSystem with the given id exist?
//------------------------------------------------------------------------------
bool FileSystemRegistry::exists(eos::common::FileSystem::fsid_t id) const {
  return mById.count(id) > 0;
}

EOSMGMNAMESPACE_END
