//------------------------------------------------------------------------------
// File: InodeTranslator.hh
// Author: Georgios Bitzes <georgios.bitzes@cern.ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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
#include "common/Namespace.hh"
#include "common/FileId.hh"

EOSCOMMONNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Translate inodes while respecting the initial encoding scheme.
//! You MUST call InodeToFid BEFORE FidToInode - otherwise, we don't know
//! which encoding scheme to apply in FidToInode.
//------------------------------------------------------------------------------
class InodeTranslator {
public:
  InodeTranslator() {}

  unsigned long long InodeToFid(unsigned long long inode) {
    if(FileId::NewIsFileInode(inode)) {
      oldEncodingScheme = false;
    }
    else {
      oldEncodingScheme = true;
    }

    return FileId::InodeToFid(inode);
  }

  unsigned long long FidToInode(unsigned long long fid) {
    if(oldEncodingScheme) {
      return FileId::LegacyFidToInode(fid);
    }

    return FileId::NewFidToInode(fid);
  }

private:
  bool oldEncodingScheme = true;
};

EOSCOMMONNAMESPACE_END
