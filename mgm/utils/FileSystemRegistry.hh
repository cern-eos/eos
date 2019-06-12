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
#include <map>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class to keep track of currently registered filesystems. For compatibility
//! purposes with what existed before, initially this class will behave exactly
//! like an id -> FileSystem* map.
//!
//! The API (together with users of this class) will be improved incrementally.
//------------------------------------------------------------------------------

class FileSystemRegistry {
public:
  //----------------------------------------------------------------------------
  //! Types
  //----------------------------------------------------------------------------
  using iterator = std::map<eos::common::FileSystem::fsid_t, mgm::FileSystem*>::iterator;
  using const_iterator = std::map<eos::common::FileSystem::fsid_t, mgm::FileSystem*>::const_iterator;

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  FileSystemRegistry();

  //----------------------------------------------------------------------------
  //! Map compatibility: size()
  //----------------------------------------------------------------------------
  size_t size() const {
    return mById.size();
  }

  //----------------------------------------------------------------------------
  //! Map compatibility: count()
  //----------------------------------------------------------------------------
  size_t count(eos::common::FileSystem::fsid_t id) const {
    return mById.count(id);
  }

  //----------------------------------------------------------------------------
  //! Map compatibility: operator[]
  //! Yes, reference to pointer is awful, but we try to follow exactly the
  //! std::map API..
  //----------------------------------------------------------------------------
  mgm::FileSystem*& operator[](const eos::common::FileSystem::fsid_t id) {
    return mById[id];
  }

  //----------------------------------------------------------------------------
  //! Map compatibility: begin()
  //----------------------------------------------------------------------------
  iterator begin() {
    return mById.begin();
  }

  //----------------------------------------------------------------------------
  //! Map compatibility: cbegin()
  //----------------------------------------------------------------------------
  const_iterator cbegin() {
    return mById.cbegin();
  }

  //----------------------------------------------------------------------------
  //! Map compatibility: end()
  //----------------------------------------------------------------------------
  iterator end() {
    return mById.end();
  }

  //----------------------------------------------------------------------------
  //! Map compatibility: cend()
  //----------------------------------------------------------------------------
  const_iterator cend() {
    return mById.cend();
  }

  //----------------------------------------------------------------------------
  //! Map compatibility: erase()
  //----------------------------------------------------------------------------
  size_t erase(eos::common::FileSystem::fsid_t id) {
    return mById.erase(id);
  }

  //----------------------------------------------------------------------------
  //! Map compatibility: find()
  //----------------------------------------------------------------------------
  iterator find(eos::common::FileSystem::fsid_t id) {
    return mById.find(id);
  }

  //----------------------------------------------------------------------------
  //! Map compatibility: clear()
  //----------------------------------------------------------------------------
  void clear() {
    return mById.clear();
  }



private:
  std::map<eos::common::FileSystem::fsid_t, mgm::FileSystem*> mById;
};

EOSMGMNAMESPACE_END

