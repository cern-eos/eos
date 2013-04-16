// ----------------------------------------------------------------------
// File: Fmd.hh
// Author: Geoffray Adde - CERN
// ----------------------------------------------------------------------

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

/**
 * @file   Fmd.hh
 * 
 * @brief  Structure holding the File Meta Data
 * 
 * 
 */

#ifndef __EOSFST_FMD_HH__
#define __EOSFST_FMD_HH__

EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
#include "common/SymKeys.hh"
#include "common/FileId.hh"
#include "common/FileSystem.hh"
#include "common/LayoutId.hh"
/*----------------------------------------------------------------------------*/

// ---------------------------------------------------------------------------
//! In-memory entry struct
// ---------------------------------------------------------------------------

struct Fmd
{
  eos::common::FileId::fileid_t fid; //< fileid
  eos::common::FileId::fileid_t cid; //< container id (e.g. directory id)
  eos::common::FileSystem::fsid_t fsid; //< filesystem id
  unsigned long ctime; //< creation time
  unsigned long ctime_ns; //< ns of creation time
  unsigned long mtime; //< modification time | deletion time
  unsigned long mtime_ns; //< ns of modification time
  unsigned long atime; //< access time
  unsigned long atime_ns; //< ns of access time
  unsigned long checktime; //< time of last checksum scan
  unsigned long long size; //< size              - 0xfffffff1ULL means it is still undefined
  unsigned long long disksize; //< size on disk      - 0xfffffff1ULL means it is still undefined
  unsigned long long mgmsize; //< size on the MGM   - 0xfffffff1ULL means it is still undefined
  std::string checksum; //< checksum in hex representation
  std::string diskchecksum; //< checksum in hex representation
  std::string mgmchecksum; //< checksum in hex representation
  eos::common::LayoutId::layoutid_t lid; //< layout id
  uid_t uid; //< user  id
  gid_t gid; //< roup id
  int filecxerror; //< indicator for file checksum error
  int blockcxerror; //< indicator for block checksum error
  int layouterror; //< indicator for resync errors e.g. the mgm layout information is inconsistent e.g. only 1 of 2 replicas exist
  std::string locations; //< fsid list with locations e.g. 1,2,3,4,10

  // Copy assignment operator

  Fmd &operator = (const Fmd & fmd)
  {
    fid = fmd.fid;
    fsid = fmd.fsid;
    cid = fmd.cid;
    ctime = fmd.ctime;
    ctime_ns = fmd.ctime_ns;
    mtime = fmd.mtime;
    mtime_ns = fmd.mtime_ns;
    atime = fmd.atime;
    atime_ns = fmd.atime_ns;
    checktime = fmd.checktime;
    size = fmd.size;
    disksize = fmd.disksize;
    mgmsize = fmd.mgmsize;
    checksum = fmd.checksum;
    diskchecksum = fmd.diskchecksum;
    mgmchecksum = fmd.mgmchecksum;
    lid = fmd.lid;
    uid = fmd.uid;
    gid = fmd.gid;
    filecxerror = fmd.filecxerror;
    blockcxerror = fmd.blockcxerror;
    layouterror = fmd.layouterror;
    locations = fmd.locations;
    return *this;
  }
};

EOSFSTNAMESPACE_END

#endif
