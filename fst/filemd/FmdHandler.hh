//------------------------------------------------------------------------------
//! @file FmdHandler.hh
//! @author Abhishek Lekshmanan - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2021 CERN/Switzerland                                  *
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
#include "common/Fmd.hh"
#include "common/FileId.hh"
#include "common/LayoutId.hh"
#include "namespace/interface/IFileMD.hh"
#include "namespace/ns_quarkdb/FileMD.hh"
#include <set>

//! Forward declaration
namespace eos
{
  class QdbContactDetails;
}

EOSFSTNAMESPACE_BEGIN

enum class fmd_handler_t {
  DB,
  ATTR,
  UNDEF
};

class FmdHandler: public eos::common::LogId
{
public:
  FmdHandler() = default;
  virtual ~FmdHandler() = default;

  virtual fmd_handler_t get_type() = 0;
  //----------------------------------------------------------------------------
  //! Check if entry has a file checksum error
  //!
  //! @param lpath file local path
  //! @param fsid file system identifier
  //!
  //! @return true if file has checksum error, otherwise false
  //----------------------------------------------------------------------------
  virtual bool FileHasXsError(const std::string& lpath,
                              eos::common::FileSystem::fsid_t fsid);

  // Meta data handling functions

  //----------------------------------------------------------------------------
  //! Return/create an Fmd struct for the given file/filesystem from the local
  //! database
  //!
  //! @param fid file id
  //! @param fsid filesystem id
  //! @param force_retrieve get object even in the presence of inconsistencies
  //! @param do_create if true create a non-existing Fmd if needed
  //! @param uid user id of the caller
  //! @param gid group id of the caller
  //! @param layoutid layout id used to store during creation
  //!
  //! @return pointer to Fmd struct if successful, otherwise nullptr
  //----------------------------------------------------------------------------
  virtual std::unique_ptr<eos::common::FmdHelper>
  LocalGetFmd(eos::common::FileId::fileid_t fid,
              eos::common::FileSystem::fsid_t fsid,
              bool force_retrieve = false, bool do_create = false,
              uid_t uid = 0, gid_t gid = 0,
              eos::common::LayoutId::layoutid_t layoutid = 0) = 0;

  //----------------------------------------------------------------------------
  //! Delete a record associated with fid and filesystem fsid
  //!
  //! @param fid file id
  //! @param fsid filesystem id
  //----------------------------------------------------------------------------
  virtual void LocalDeleteFmd(eos::common::FileId::fileid_t fid,
                              eos::common::FileSystem::fsid_t fsid) = 0;

  //----------------------------------------------------------------------------
  //! Commit modified Fmd record to the local database
  //!
  //! @param fmd pointer to Fmd
  //!
  //! @return true if record was committed, otherwise false
  //----------------------------------------------------------------------------
  virtual bool Commit(eos::common::FmdHelper* fmd, bool lockit = true) = 0;

  //----------------------------------------------------------------------------
  //! Update local fmd with info from the disk i.e. physical file extended
  //! attributes
  //!
  //! @param fsid file system id
  //! @param fid  file id to update
  //! @param disk_size size of the file on disk
  //! @param disk_xs checksum of the file on disk
  //! @param check_ts_sec time of the last check of that file
  //! @param filexs_err indicator for file checksum error
  //! @param blockxs_err inidicator for block checksum error
  //! @param layout_err indication for layout error
  //!
  //! @return true if record has been committed
  //----------------------------------------------------------------------------
  virtual bool UpdateWithDiskInfo(eos::common::FileSystem::fsid_t fsid,
                                  eos::common::FileId::fileid_t fid,
                                  unsigned long long disk_size,
                                  const std::string& disk_xs,
                                  unsigned long check_ts_sec, bool filexs_err,
                                  bool blockxs_err, bool layout_err);

  //----------------------------------------------------------------------------
  //! Update local fmd with info from the MGM
  //!
  //! @param fsid file system id
  //! @param fid  file id to update
  //! @param cid  container id
  //! @param lid  layout id
  //! @param mgmsize size of the file in the mgm namespace
  //! @param mgmchecksum checksum of the file in the mgm namespace
  //!
  //! @return true if record has been committed
  //----------------------------------------------------------------------------
  virtual bool UpdateWithMgmInfo(eos::common::FileSystem::fsid_t fsid,
                                 eos::common::FileId::fileid_t fid,
                                 eos::common::FileId::fileid_t cid,
                                 eos::common::LayoutId::layoutid_t lid,
                                 unsigned long long mgmsize,
                                 std::string mgmchecksum,
                                 uid_t uid, gid_t gid,
                                 unsigned long long ctime,
                                 unsigned long long ctime_ns,
                                 unsigned long long mtime,
                                 unsigned long long mtime_ns,
                                 int layouterror, std::string locations);

  //----------------------------------------------------------------------------
  //! Update local fmd with info from the scanner
  //!
  //! @param fid file identifier
  //! @param fsid file system id
  //! @param fpath local file path
  //! @param scan_sz size of the file computed by the scanner
  //! @param scan_xs_hex hex checksum of the file computed by the scanner
  //! @param qcl QClient used to communicate to QDB backend
  //!
  //! @note: the qclient should favor followers as we're doing only read
  //!        operations and this should reduce the load on the master QDB
  //----------------------------------------------------------------------------
  virtual void UpdateWithScanInfo(eos::common::FileId::fileid_t fid,
                                  eos::common::FileSystem::fsid_t fsid,
                                  const std::string& fpath,
                                  uint64_t scan_sz, const std::string& scan_xs_hex,
                                  std::shared_ptr<qclient::QClient> qcl);

  //----------------------------------------------------------------------------
  //! Resync a single entry from disk
  //!
  //! @param fstpath file system location
  //! @param fsid filesystem id
  //! @param flaglayouterror indicates a layout error
  //! @param scan_sz size of file computed by the scanner
  //! @param scan_xs_hex hex checksum of the file computed by the scanner
  //!
  //! @return 0 if successful, otherwise errno
  //----------------------------------------------------------------------------
  virtual int ResyncDisk(const char* fstpath,
                         eos::common::FileSystem::fsid_t fsid,
                         bool flaglayouterror, uint64_t scan_sz = 0ull,
                         const std::string& scan_xs_hex = "");


  //----------------------------------------------------------------------------
  //! Resync files under path into local database
  //!
  //! @param path path to scan
  //! @param fsid file system id
  //! @param flaglayouterror flag to indicate a layout error
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  virtual bool ResyncAllDisk(const char* path,
                             eos::common::FileSystem::fsid_t fsid,
                             bool flaglayouterror);

  //----------------------------------------------------------------------------
  //! Resync file meta data from MGM into local database
  //!
  //! @param fsid filesystem id
  //! @param fid file id
  //! @param manager manager hostname
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  virtual bool ResyncMgm(eos::common::FileSystem::fsid_t fsid,
                         eos::common::FileId::fileid_t fid, const char* manager);

  //----------------------------------------------------------------------------
  //! Resync all meta data from MGM into local database
  //!
  //! @param fsid filesystem id
  //! param manager manger hostname
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  virtual bool ResyncAllMgm(eos::common::FileSystem::fsid_t fsid,
                            const char* manager);

  //------------------------------------------------------------------------------
  //! Resync file meta data from QuarkDB into local database
  //!
  //! @param fid file identifier
  //! @param fsid file system identifier
  //! @param fpath local file path
  //! @param qcl QClient object used to connect to QuarkDB (this should have a
  //!        preference to connect to followers as it's doing only read ops.)
  //!
  //! @return 0 if successful, otherwise errno
  //------------------------------------------------------------------------------
  virtual int ResyncFileFromQdb(eos::common::FileId::fileid_t fid,
                                eos::common::FileSystem::fsid_t fsid,
                                const std::string& fpath,
                                std::shared_ptr<qclient::QClient> qcl);

  //----------------------------------------------------------------------------
  //! Resync all meta data from QuarkdDB
  //!
  //! @param contact_details QDB contact details
  //! @param fsid filesystem id
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  virtual bool ResyncAllFromQdb(const QdbContactDetails& contact_details,
                                eos::common::FileSystem::fsid_t fsid);

  //----------------------------------------------------------------------------
  //! Move given file to orphans directory and also set its extended attribute
  //! to reflect the original path to the file.
  //!
  //! @param fpath file to move
  //----------------------------------------------------------------------------
  static void MoveToOrphans(const std::string& fpath);

  // A shutdown/cleanup routine for the specific handler, meant to be overriden
  // if the class handles some objects needing cleanup
  virtual void Shutdown() {}

  static std::unique_ptr<eos::common::FmdHelper>
  make_fmd_helper(common::FileId::fileid_t fid,
                  common::FileSystem::fsid_t fsid,
                  uid_t uid,
                  gid_t gid,
                  common::LayoutId::layoutid_t layoutid);

  //----------------------------------------------------------------------------
  //! Get inconsistency statistics
  //!
  //! @param fsid file system id
  //! @param statistics map of inconsistency type to counter
  //! @param fidset map of fid to set of inconsitent file ids
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  virtual bool GetInconsistencyStatistics(eos::common::FileSystem::fsid_t fsid,
                                          std::map<std::string, size_t>& statistics,
                                          std::map<std::string,
                                              std::set <eos::common::FileId::fileid_t>>& fidset) = 0;

private:

  // Virtual private methods are overrideable at derived classes, this allows
  // for the interface to remain the same while the specific implementation is
  // done in the derived class
  virtual bool LocalPutFmd(eos::common::FileId::fileid_t fid,
                           eos::common::FileSystem::fsid_t fsid,
                           const eos::common::FmdHelper& fmd) = 0;

  virtual std::pair<bool,eos::common::FmdHelper>
  LocalRetrieveFmd(eos::common::FileId::fileid_t fid,
                   eos::common::FileSystem::fsid_t fsid) = 0;


  virtual bool ResetDiskInformation(eos::common::FileSystem::fsid_t fsid) = 0;
  virtual bool ResetMgmInformation(eos::common::FileSystem::fsid_t fsid) = 0;

  // TODO: Technically we could hold move the mIsSyncing map & mutex to this class. Do
  // this if AttrHandler also needs a syncing lock vs noop
  virtual void SetSyncStatus(eos::common::FileSystem::fsid_t fsid, bool is_syncing) = 0;
};

void UpdateInconsistencyStats(const eos::common::FmdHelper& fmd,
                             std::map<std::string, size_t>& statistics,
                             std::map<std::string,
                                      std::set<eos::common::FileId::fileid_t>>& fidset);

EOSFSTNAMESPACE_END
