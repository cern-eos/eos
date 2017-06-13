/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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

#include "fst/Namespace.hh"
#include "proto/FmdBase.pb.h"
#include "common/FileSystem.hh"
#include "common/LayoutId.hh"
#include "common/StringConversion.hh"
#include "common/Logging.hh"

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Structure holding file metadata
//------------------------------------------------------------------------------
struct Fmd : public FmdBase {
public:
  virtual ~Fmd() {}
};

//------------------------------------------------------------------------------
//! Class implementing file meta data
//------------------------------------------------------------------------------
class FmdHelper : public eos::common::LogId
{
public:
  //---------------------------------------------------------------------------
  //! Compute layout error
  //!
  //! @param fmd protobuf file meta data
  //! @param fsid file system id to check against
  //!
  //! @return 0 if there are no errors, otherwise encoded type of layout error
  //!        stored in the int.
  //---------------------------------------------------------------------------
  static int LayoutError(const Fmd& fmd, eos::common::FileSystem::fsid_t fsid);

  //---------------------------------------------------------------------------
  //! Reset file meta data object
  //!
  //! @param fmd protobuf file meta data
  //---------------------------------------------------------------------------
  static void Reset(Fmd& fmd);

  //---------------------------------------------------------------------------
  //! Get set of locations for the given fmd
  //!
  //! @param fmd file metadata object
  //! @param valid_replicas number of valid replicas <= size of the returned
  //!        set i.e. replicas which are not unlinked
  //!
  //! @return set of file system ids representing the locations
  //---------------------------------------------------------------------------
  static std::set<eos::common::FileSystem::fsid_t>
  GetLocations(const Fmd& fmd, size_t& valid_replicas);

  //---------------------------------------------------------------------------
  //! Constructor
  //---------------------------------------------------------------------------
  FmdHelper(int fid = 0, int fsid = 0): LogId()
  {
    Reset(mProtoFmd);
    mProtoFmd.set_fid(fid);
    mProtoFmd.set_fsid(fsid);
  }

  //---------------------------------------------------------------------------
  //! Destructor
  //---------------------------------------------------------------------------
  virtual ~FmdHelper() = default;

  //---------------------------------------------------------------------------
  //! Convert fmd object to env representation
  //!
  //! @return XrdOucEnv holding information about current object
  //---------------------------------------------------------------------------
  std::unique_ptr<XrdOucEnv> FmdToEnv();

  //---------------------------------------------------------------------------
  //! File meta data object replication function (copy constructor)
  //---------------------------------------------------------------------------
  void
  Replicate(Fmd& fmd)
  {
    mProtoFmd = fmd;
  }

  Fmd mProtoFmd; ///< Protobuf file metadata info
};

EOSFSTNAMESPACE_END
