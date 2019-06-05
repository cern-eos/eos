//------------------------------------------------------------------------------
//! @file FsckEntry.hh
//! @author Elvin Sindrilaru - CERN
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
#include "common/Logging.hh"
#include "common/FileSystem.hh"
#include "mgm/drain/DrainTransferJob.hh"
#include "namespace/interface/IFileMD.hh"
#include "namespace/ns_quarkdb/qclient/include/qclient/QClient.hh"
#include "fst/Fmd.hh"
#include <functional>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! FsckErr types
//------------------------------------------------------------------------------
enum class FsckErr {
  None = 0x00,
  MgmXsDiff = 0x01,
  FstXsDiff = 0x02
};


//------------------------------------------------------------------------------
//! Types of errors that come up on the FST side
//------------------------------------------------------------------------------
enum class FstErr {
  None = 0x00,
  NoContact = 0x01,
  NotOnDisk = 0x02,
  NoFmdInfo = 0x03,
  NoXattrInfo = 0x04
};

//------------------------------------------------------------------------------
//! Convert string to FsckErr type
//!
//! @param serr string error type
//!
//! @return FsckErr type
//------------------------------------------------------------------------------
FsckErr ConvertToFsckErr(const std::string& serr);

//------------------------------------------------------------------------------
//! FstFileInfoT holds file metadata info retrieved from an FST
//------------------------------------------------------------------------------
struct FstFileInfoT {
public:
  std::string mLocalPath;
  uint64_t mDiskSize;
  std::string mScanXs;
  eos::fst::Fmd mFstFmd;
  FstErr mFstErr;

  //------------------------------------------------------------------------------
  //! Constructor
  //------------------------------------------------------------------------------
  FstFileInfoT(const::string& local_path, FstErr err):
    mLocalPath(local_path), mFstErr(err)
  {}
};

//! Forward declaration and aliases
class FsckEntry;
using RepairFnT = std::function<bool(FsckEntry&)>;
using FsckRepairJob = eos::mgm::DrainTransferJob;

//------------------------------------------------------------------------------
//! Class FsckEntry
//------------------------------------------------------------------------------
class FsckEntry: public eos::common::LogId
{
public:


  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param fid file identifier
  //! @param fsid_err file system which reported an error
  //! @param expected_err expected type of error reported by the scanner on the
  //!        FST
  //----------------------------------------------------------------------------
  FsckEntry(eos::IFileMD::id_t fid, eos::common::FileSystem::fsid_t fsid_err,
            const std::string& expected_err):
    mFid(fid), mFsidErr(fsid_err),
    mReportedErr(ConvertToFsckErr(expected_err))
  {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~FsckEntry() = default;

  //----------------------------------------------------------------------------
  //! Collect MGM file metadata information
  //!
  //! @param qcl QClient object used for retrieved file md without polluting
  //!        the MGM cache
  //----------------------------------------------------------------------------
  void CollectMgmInfo(qclient::QClient& qcl);

  //----------------------------------------------------------------------------
  //! Collect FST file metadata information from all replicas
  //----------------------------------------------------------------------------
  void CollectAllFstInfo();

  //----------------------------------------------------------------------------
  //! Collect FST file metadata information
  //!
  //! @param fsid file system identifier
  //----------------------------------------------------------------------------
  void CollectFstInfo(eos::common::FileSystem::fsid_t fsid);

  //----------------------------------------------------------------------------
  //! Generate repair workflow for the current entry
  //!
  //! @return list of repair function to be applied to the current entry
  //----------------------------------------------------------------------------
  std::list<RepairFnT>
  GenerateRepairWokflow();

  //----------------------------------------------------------------------------
  //! Method to repair an mgm checksum difference error
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool RepairMgmXsDiff();

  //----------------------------------------------------------------------------
  //! Method to repair an FST checksum difference error
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool RepairFstXsDiff();

private:

  //----------------------------------------------------------------------------
  //! Get file metadata info stored at the FST
  //!
  //! @param finfo object holding file info to be populated
  //! @param fs file system object used for queries
  //! @param fsid file system identifier
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool GetFstFmd(std::unique_ptr<FstFileInfoT>& finfo, XrdCl::FileSystem& fs,
                 eos::common::FileSystem::fsid_t fsid);

  //----------------------------------------------------------------------------
  //! Get extended attribute concerning the current file entry
  //!
  //! @param finfo object holding file info to be populated
  //! @param fs file system object used for doing the queries
  //! @param fsid file system id
  //! @param key xattr key to retrieve
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool GetXattr(std::unique_ptr<FstFileInfoT>& finfo, XrdCl::FileSystem& fs,
                eos::common::FileSystem::fsid_t fsid, const std::string& key);


  eos::IFileMD::id_t mFid; ///< File id
  eos::common::FileSystem::fsid_t mFsidErr; ///< File system id with expected err
  FsckErr mReportedErr; ///< Reported error type
  eos::ns::FileMdProto mMgmFmd; ///< MGM file metadata protobuf object
  //! Map of file system id to file metadata held at the corresponding fs
  std::map<eos::common::FileSystem::fsid_t,
      std::unique_ptr<FstFileInfoT>> mFstFileInfo;
};

EOSMGMNAMESPACE_END
