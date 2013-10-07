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


/*----------------------------------------------------------------------------*/
#include "common/SymKeys.hh"
#include "common/FileId.hh"
#include "common/FileSystem.hh"
#include "common/LayoutId.hh"

#include "FmdBase.pb.h"

/*----------------------------------------------------------------------------*/
EOSFSTNAMESPACE_BEGIN

// ---------------------------------------------------------------------------
//! In-memory entry struct
// ---------------------------------------------------------------------------

class Fmd : public FmdBase
{
//  eos::common::FileId::fileid_t fid; //< fileid
//  eos::common::FileId::fileid_t cid; //< container id (e.g. directory id)
//  eos::common::FileSystem::fsid_t fsid; //< filesystem id
//  unsigned long ctime; //< creation time
//  unsigned long ctime_ns; //< ns of creation time
//  unsigned long mtime; //< modification time | deletion time
//  unsigned long mtime_ns; //< ns of modification time
//  unsigned long atime; //< access time
//  unsigned long atime_ns; //< ns of access time
//  unsigned long checktime; //< time of last checksum scan
//  unsigned long long size; //< size              - 0xfffffff1ULL means it is still undefined
//  unsigned long long disksize; //< size on disk      - 0xfffffff1ULL means it is still undefined
//  unsigned long long mgmsize; //< size on the MGM   - 0xfffffff1ULL means it is still undefined
//  std::string checksum; //< checksum in hex representation
//  std::string diskchecksum; //< checksum in hex representation
//  std::string mgmchecksum; //< checksum in hex representation
//  eos::common::LayoutId::layoutid_t lid; //< layout id
//  uid_t uid; //< user  id
//  gid_t gid; //< roup id
//  int filecxerror; //< indicator for file checksum error
//  int blockcxerror; //< indicator for block checksum error
//  int layouterror; //< indicator for resync errors e.g. the mgm layout information is inconsistent e.g. only 1 of 2 replicas exist
//  std::string locations; //< fsid list with locations e.g. 1,2,3,4,10

  // Copy assignment operator
//public:
//  Fmd &operator = (const Fmd & fmd)
//  {
//    fid = fmd.fid;
//    fsid = fmd.fsid;
//    cid = fmd.cid;
//    ctime = fmd.ctime;
//    ctime_ns = fmd.ctime_ns;
//    mtime = fmd.mtime;
//    mtime_ns = fmd.mtime_ns;
//    atime = fmd.atime;
//    atime_ns = fmd.atime_ns;
//    checktime = fmd.checktime;
//    size = fmd.size;
//    disksize = fmd.disksize;
//    mgmsize = fmd.mgmsize;
//    checksum = fmd.checksum;
//    diskchecksum = fmd.diskchecksum;
//    mgmchecksum = fmd.mgmchecksum;
//    lid = fmd.lid;
//    uid = fmd.uid;
//    gid = fmd.gid;
//    filecxerror = fmd.filecxerror;
//    blockcxerror = fmd.blockcxerror;
//    layouterror = fmd.layouterror;
//    locations = fmd.locations;
//    return *this;
//  }
public:
	virtual ~Fmd() {}
};

// ---------------------------------------------------------------------------
//! Class implementing file meta data
// ---------------------------------------------------------------------------
class FmdHelper : public eos::common::LogId
{
public:
  // ---------------------------------------------------------------------------
  //! File Metadata object
  Fmd fMd;
  // ---------------------------------------------------------------------------

  // ---------------------------------------------------------------------------
  //! File meta data object replication function (copy constructor)
  // ---------------------------------------------------------------------------

  void
  Replicate (Fmd &fmd)
  {
    fMd = fmd;
  }

  // ---------------------------------------------------------------------------
  //! Compute layout error
  // ---------------------------------------------------------------------------

  static int
  LayoutError (eos::common::FileSystem::fsid_t fsid, eos::common::LayoutId::layoutid_t lid, std::string locations)
  {
    if (lid == 0)
    {
      // an orphans has not lid at the MGM e.g. lid=0
      return eos::common::LayoutId::kOrphan;
    }

    int lerror = 0;
    std::vector<std::string> location_vector;
    std::set<eos::common::FileSystem::fsid_t> location_set;
    eos::common::StringConversion::Tokenize(locations, location_vector, ",");
    size_t validreplicas = 0;
    for (size_t i = 0; i < location_vector.size(); i++)
    {
      if (location_vector[i].length())
      {
        // unlinked locates have a '!' infront of the fsid
        if (location_vector[i][0] == '!')
        {
          location_set.insert(strtoul(location_vector[i].c_str() + 1, 0, 10));
        }
        else
        {
          location_set.insert(strtoul(location_vector[i].c_str(), 0, 10));
          validreplicas++;
        }
      }
    }
    size_t nstripes = eos::common::LayoutId::GetStripeNumber(lid) + 1;
    if (nstripes != validreplicas)
    {
      lerror |= eos::common::LayoutId::kReplicaWrong;
    }
    if (!location_set.count(fsid))
    {
      lerror |= eos::common::LayoutId::kUnregistered;
    }
    return lerror;
  }

  // ---------------------------------------------------------------------------
  //! File meta data object reset function
  // ---------------------------------------------------------------------------

  static void
  Reset (Fmd &fmd)
  {
    fmd.set_fid(0);
    fmd.set_cid(0);
    fmd.set_ctime(0);
    fmd.set_ctime_ns(0);
    fmd.set_mtime(0);
    fmd.set_mtime_ns(0);
    fmd.set_atime(0);
    fmd.set_atime_ns(0);
    fmd.set_checktime(0);
    fmd.set_size(0xfffffff1ULL);
    fmd.set_disksize(0xfffffff1ULL);
    fmd.set_mgmsize(0xfffffff1ULL);
    fmd.set_checksum("");
    fmd.set_diskchecksum("");
    fmd.set_mgmchecksum("");
    fmd.set_lid(0);
    fmd.set_uid(0);
    fmd.set_gid(0);
    fmd.set_filecxerror(0);
    fmd.set_blockcxerror(0);
    fmd.set_layouterror(0);
    fmd.set_locations("");
  }

  // ---------------------------------------------------------------------------
  //! Dump Fmd
  // ---------------------------------------------------------------------------
  static void Dump (Fmd* fmd);

  // ---------------------------------------------------------------------------
  //! Convert Fmd into env representation
  // ---------------------------------------------------------------------------
  XrdOucEnv* FmdToEnv ();

  // ---------------------------------------------------------------------------
  //! Constructor
  // ---------------------------------------------------------------------------

  FmdHelper (int fid = 0, int fsid = 0)
  {
    Reset(fMd);
    LogId();
    fMd.set_fid(fid);
    fMd.set_fsid(fsid);
  };

  // ---------------------------------------------------------------------------
  //! Destructor
  // ---------------------------------------------------------------------------

  ~FmdHelper () { };
};


EOSFSTNAMESPACE_END

#endif
