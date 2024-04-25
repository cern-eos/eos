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
#include "common/Namespace.hh"
#include "proto/FmdBase.pb.h"
#include "common/FileSystem.hh"
#include "common/Logging.hh"
#include "common/FileId.hh"
#include <XrdOuc/XrdOucEnv.hh>

EOSCOMMONNAMESPACE_BEGIN

//! Map of fsck errors to map of file system identifiers and file ids
using FsckErrsPerFsMap = std::map<std::string,
      std::map<eos::common::FileSystem::fsid_t,
      std::set<eos::common::FileId::fileid_t>>>;

//------------------------------------------------------------------------------
//! Class modelling the file metadata stored on the FST. It wrapps an FmdBase
//! class generated from the ProtoBuffer specification and adds some extra
//! functionality for conversion to/from string/env representation.
//------------------------------------------------------------------------------
class FmdHelper : public eos::common::LogId
{
public:
  static constexpr uint64_t UNDEF = 0xfffffffffff1ULL;

  //---------------------------------------------------------------------------
  //! Constructor
  //---------------------------------------------------------------------------
  FmdHelper(eos::common::FileId::fileid_t fid = 0, int fsid = 0)
  {
    Reset();
    mProtoFmd.set_fid(fid);
    mProtoFmd.set_fsid(fsid);
  }

  //---------------------------------------------------------------------------
  //! Constructor with protobuf object: Move only variant
  //---------------------------------------------------------------------------
  FmdHelper(eos::fst::FmdBase&& _mProtoFmd): mProtoFmd(std::move(_mProtoFmd)) {}

  //---------------------------------------------------------------------------
  //! Constructor with protobuf object
  //---------------------------------------------------------------------------
  FmdHelper(const eos::fst::FmdBase& _mProtoFmd): mProtoFmd(_mProtoFmd) {}

  //---------------------------------------------------------------------------
  //! Destructor
  //---------------------------------------------------------------------------
  virtual ~FmdHelper() = default;

  //---------------------------------------------------------------------------
  //! Compute layout error
  //!
  //! @param fsid file system id to check against
  //!
  //! @return 0 if there are no errors, otherwise encoded type of layout error
  //!        stored in the int.
  //---------------------------------------------------------------------------
  int LayoutError(eos::common::FileSystem::fsid_t fsid);

  //---------------------------------------------------------------------------
  //! Reset file meta data object
  //!
  //! @param fmd protobuf file meta data
  //---------------------------------------------------------------------------
  void Reset();

  //---------------------------------------------------------------------------
  //! Get set of valid locations (not unlinked) for the given fmd
  //!
  //! @return set of file system ids representing the locations
  //---------------------------------------------------------------------------
  std::set<eos::common::FileSystem::fsid_t> GetLocations() const;

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
  Replicate(FmdHelper& fmd)
  {
    mProtoFmd = fmd.mProtoFmd;
  }

  eos::fst::FmdBase mProtoFmd; ///< Protobuf file metadata info
};

//------------------------------------------------------------------------------
//! Fsck constants used throughout the code
//------------------------------------------------------------------------------
static constexpr auto FSCK_M_CX_DIFF     = "m_cx_diff";
static constexpr auto FSCK_M_MEM_SZ_DIFF = "m_mem_sz_diff";
static constexpr auto FSCK_D_CX_DIFF     = "d_cx_diff";
static constexpr auto FSCK_D_MEM_SZ_DIFF = "d_mem_sz_diff";
static constexpr auto FSCK_UNREG_N       = "unreg_n";
static constexpr auto FSCK_REP_DIFF_N    = "rep_diff_n";
static constexpr auto FSCK_REP_MISSING_N = "rep_missing_n";
static constexpr auto FSCK_BLOCKXS_ERR   = "blockxs_err";
static constexpr auto FSCK_ORPHANS_N     = "orphans_n";
static constexpr auto FSCK_STRIPE_ERR    = "stripe_err";

//------------------------------------------------------------------------------
//! FsckErr types
//------------------------------------------------------------------------------
enum class FsckErr {
  None = 0x00,
  MgmXsDiff  = 0x01,
  FstXsDiff  = 0x02,
  MgmSzDiff  = 0x03,
  FstSzDiff  = 0x04,
  UnregRepl  = 0x05,
  DiffRepl   = 0x06,
  MissRepl   = 0x07,
  BlockxsErr = 0x08,
  Orphans    = 0x09,
  StripeErr  = 0x0A
};

//------------------------------------------------------------------------------
//! Get set of known fsck error strings
//------------------------------------------------------------------------------
std::set<std::string> GetKnownFsckErrs();

//------------------------------------------------------------------------------
//! Convert string to FsckErr type
//!
//! @param serr string error type
//!
//! @return FsckErr type
//------------------------------------------------------------------------------
FsckErr ConvertToFsckErr(const std::string& serr);

//------------------------------------------------------------------------------
//! Convert to FsckErr type to string
//!
//! @param fsck_err FsckErr type
//!
//! @return string fsck error
//------------------------------------------------------------------------------
std::string FsckErrToString(const FsckErr& err);

//------------------------------------------------------------------------------
//! Convert an FST env representation to an Fmd struct
//!
//! @param env env representation
//! @param fmd reference to Fmd struct
//!
//! @return true if successful, otherwise false
//------------------------------------------------------------------------------
bool EnvToFstFmd(XrdOucEnv& env, FmdHelper& fmd);

//------------------------------------------------------------------------------
//! Populate data structures with any inconsistencies detected while inspecting
//! the FmdHelper object
//!
//! @param fmd file info object
//! @param fsid file system id
//! @param map of errors to filesystem id and file identifiers
//------------------------------------------------------------------------------
void
CollectInconsistencies(const FmdHelper& fmd,
                       const eos::common::FileSystem::fsid_t fsid,
                       FsckErrsPerFsMap& errs_map);

EOSCOMMONNAMESPACE_END
