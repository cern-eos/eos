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
#include "common/Logging.hh"
#include <sstream>

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
    if(encodingScheme == EncodingScheme::kUninitialized) {
      initialize(inode);
    }

    if(encodingScheme == EncodingScheme::kLegacy && !FileId::LegacyIsFileInode(inode)) {
      std::string err = SSTR("Configured to use legacy encoding scheme, but encountered inode which is not recognized as legacy: " << inode);
      eos_static_crit(err.c_str());
      std::cerr << err << std::endl;
      std::abort();
    }

    if(encodingScheme == EncodingScheme::kLegacy && FileId::NewIsFileInode(inode)) {
      std::string err = SSTR("Configured to use legacy encoding scheme, but encountered inode which is recognized as new: " << inode);
      eos_static_crit(err.c_str());
      std::cerr << err << std::endl;
      std::abort();
    }

    if(encodingScheme == EncodingScheme::kNew && !FileId::NewIsFileInode(inode)) {
      std::string err = SSTR("Configured to use new encoding scheme, but encountered inode which is not recognized as new: " << inode);
      eos_static_crit(err.c_str());
      std::cerr << err << std::endl;
      std::abort();
    }

    return FileId::InodeToFid(inode);
  }

  unsigned long long FidToInode(unsigned long long fid) {
    if(encodingScheme == EncodingScheme::kUninitialized) {
      std::string err = SSTR("Attempted to convert from file ID (" << fid << ") to inode before discovering the inode encoding scheme.");
      eos_static_crit(err.c_str());
      std::cerr << err << std::endl;
      std::abort();
    }

    if(encodingScheme == EncodingScheme::kLegacy) {
      return FileId::LegacyFidToInode(fid);
    }

    return FileId::NewFidToInode(fid);
  }

private:
  void initialize(unsigned long long inode) {
    if(FileId::NewIsFileInode(inode)) {
      encodingScheme = EncodingScheme::kNew;
    }
    else {
      encodingScheme = EncodingScheme::kLegacy;
    }
  }

  enum class EncodingScheme {
    kLegacy,
    kNew,
    kUninitialized
  };

  EncodingScheme encodingScheme = EncodingScheme::kUninitialized;
};

EOSCOMMONNAMESPACE_END
