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
#include "common/Fmd.hh"
#include "XrdCl/XrdClFileSystem.hh"
#include <functional>

EOSMGMNAMESPACE_BEGIN

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
  BlockxsErr = 0x08
};

//------------------------------------------------------------------------------
//! Types of errors that come up on the FST side
//------------------------------------------------------------------------------
enum class FstErr {
  None = 0x00,
  NoContact = 0x01,
  NotOnDisk = 0x02,
  NoFmdInfo = 0x03
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
  eos::common::FmdHelper mFstFmd;
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
using FsckRepairJob = eos::mgm::DrainTransferJob;
using RepairFnT = std::function<bool(FsckEntry*)>;
using RepairFactoryFnT =
  std::function<std::shared_ptr<FsckRepairJob>
  (eos::common::FileId::fileid_t fid,
   eos::common::FileSystem::fsid_t fsid_src,
   eos::common::FileSystem::fsid_t fsid_trg ,
   std::set<eos::common::FileSystem::fsid_t> exclude_srcs,
   std::set<eos::common::FileSystem::fsid_t> exclude_dsts,
   bool drop_src,
   const std::string& app_tag)>;

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
  //! @param expected_err expected type of error reported by the scanner
  //! @param qcl QClient object for getting metadata information
  //----------------------------------------------------------------------------
  FsckEntry(eos::IFileMD::id_t fid,
            eos::common::FileSystem::fsid_t fsid_err,
            const std::string& expected_err,
            std::shared_ptr<qclient::QClient> qcl);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~FsckEntry() = default;

  //----------------------------------------------------------------------------
  //! Repair current entry
  //!
  //! @return true if successful repair and/or no errors, otherwise false
  //----------------------------------------------------------------------------
  bool Repair();

private:
#ifdef IN_TEST_HARNESS
public:
#endif
  //----------------------------------------------------------------------------
  //! Method to repair an mgm checksum and/or size difference error
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool RepairMgmXsSzDiff();

  //----------------------------------------------------------------------------
  //! Method to repair an FST checksum and/or size difference error
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool RepairFstXsSzDiff();

  //----------------------------------------------------------------------------
  //! Method to repair inconsistencies e.g. unregistered replicas,
  //! under/over replication, missing replicas
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool RepairInconsistencies();

  //----------------------------------------------------------------------------
  //! Method to repair inconsistencies for replica files e.g. unregistered
  //! replicas, under/over replication, missing replicas
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool RepairReplicaInconsistencies();

  //----------------------------------------------------------------------------
  //! Method to repair inconsistencies for RAIN files e.g. unregistered
  //! stripes, under/over replication, missing stripes
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool RepairRainInconsistencies();

  //----------------------------------------------------------------------------
  //! Collect MGM file metadata information
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool CollectMgmInfo();

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
  //! Drop replica form FST and also update the namespace view for the given
  //! file system id
  //!
  //! @param fsid file system id from where to drop the replica
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool DropReplica(eos::common::FileSystem::fsid_t fsid) const;

  //----------------------------------------------------------------------------
  //! Update MGM stats depending on the final outcome
  //!
  //! @param success true if repair successful, otherwise false
  //----------------------------------------------------------------------------
  void UpdateMgmStats(bool success) const;

  //----------------------------------------------------------------------------
  //! Resync local FST metadata with the MGM info.
  //!
  //! @param refresh_mgm_md if true then the MGM metadata corresponding to the
  //!        current file is retrieved again, otherwise we use what we have
  //!        already. This needs tobe set whenever there is an FsckRepairJob
  //!        done before.
  //----------------------------------------------------------------------------
  void ResyncFstMd(bool refresh_mgm_md = false);

  eos::IFileMD::id_t mFid; ///< File id
  eos::common::FileSystem::fsid_t mFsidErr; ///< File system id with expected err
  FsckErr mReportedErr; ///< Reported error type
  eos::ns::FileMdProto mMgmFmd; ///< MGM file metadata protobuf object
  //! Map of file system id to file metadata held at the corresponding fs
  std::map<eos::common::FileSystem::fsid_t,
      std::unique_ptr<FstFileInfoT>> mFstFileInfo;
  //! Map of fsck error to list of repair operations
  std::map<FsckErr, RepairFnT> mMapRepairOps;
  //! Factory callable creating fsck repair jobs
  RepairFactoryFnT mRepairFactory;
  std::shared_ptr<qclient::QClient> mQcl; ///< QClient object for metadata
};

EOSMGMNAMESPACE_END
