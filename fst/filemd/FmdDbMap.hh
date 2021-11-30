//------------------------------------------------------------------------------
//! @file FmdDbMap.hh
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
#include "fst/Namespace.hh"
#include "common/Fmd.hh"
#include "common/DbMap.hh"
#include "common/FileId.hh"
#include "common/LayoutId.hh"
#include "namespace/interface/IFileMD.hh"
#include "namespace/ns_quarkdb/FileMD.hh"
#include "fst/filemd/FmdHandler.hh"

#ifdef __APPLE__
#define ECOMM 70
#endif


EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class handling many Fmd changelog files at a time
//------------------------------------------------------------------------------
class FmdDbMapHandler final : public FmdHandler
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  FmdDbMapHandler();

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~FmdDbMapHandler()
  {
    Shutdown();

    for (auto it = mFsMtxMap.begin(); it != mFsMtxMap.end(); ++it) {
      delete it->second;
    }

    mFsMtxMap.clear();
  }

  fmd_handler_t get_type() override {
    return fmd_handler_t::DB;
  }


  //----------------------------------------------------------------------------
  //! Set a new DB file for a filesystem id
  //!
  //! @param meta_dir meta data directory where to place the files
  //! @param fsid filesystem id identified by this file
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool SetDBFile(const char* dbfile, int fsid);

  //----------------------------------------------------------------------------
  //! Shutdown an open DB file
  //!
  //! @param fsid filesystem id
  //! @param do_lock if true then lock the mMapMutex
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool ShutdownDB(eos::common::FileSystem::fsid_t fsid, bool do_lock = false);

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
  std::unique_ptr<eos::common::FmdHelper>
  LocalGetFmd(eos::common::FileId::fileid_t fid,
              eos::common::FileSystem::fsid_t fsid,
              bool force_retrieve = false, bool do_create = false,
              uid_t uid = 0, gid_t gid = 0,
              eos::common::LayoutId::layoutid_t layoutid = 0) override;
  //----------------------------------------------------------------------------
  //! Delete a record associated with fid and filesystem fsid
  //!
  //! @param fid file id
  //! @param fsid filesystem id
  //----------------------------------------------------------------------------
  void LocalDeleteFmd(eos::common::FileId::fileid_t fid,
                      eos::common::FileSystem::fsid_t fsid) override;

  //----------------------------------------------------------------------------
  //! Commit modified Fmd record to the local database
  //!
  //! @param fmd pointer to Fmd
  //!
  //! @return true if record was committed, otherwise false
  //----------------------------------------------------------------------------
  bool Commit(eos::common::FmdHelper* fmd, bool lockit = true) override;

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
  bool UpdateWithDiskInfo(eos::common::FileSystem::fsid_t fsid,
                          eos::common::FileId::fileid_t fid,
                          unsigned long long disk_size,
                          const std::string& disk_xs,
                          unsigned long check_ts_sec, bool filexs_err,
                          bool blockxs_err, bool layout_err) override;

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
  bool UpdateWithMgmInfo(eos::common::FileSystem::fsid_t fsid,
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
                         int layouterror, std::string locations) override;

  //----------------------------------------------------------------------------
  //! Remove ghost entries - entries which are neither on disk nor at the MGM
  //!
  //! @param path mount prefix of the filesystem
  //! @param fsid filesystem id
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool RemoveGhostEntries(const char* prefix,
                          eos::common::FileSystem::fsid_t fsid);

  //----------------------------------------------------------------------------
  //! Get inconsistency statistics
  //!
  //! @param fsid file system id
  //! @param statistics map of inconsistency type to counter
  //! @param fidset map of fid to set of inconsitent file ids
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool GetInconsistencyStatistics(eos::common::FileSystem::fsid_t fsid,
                                  std::map<std::string, size_t>& statistics,
                                  std::map<std::string,
                                  std::set < eos::common::FileId::fileid_t>
                                  >& fidset);

  //----------------------------------------------------------------------------
  //! Reset(clear) the contents of the DB
  //!
  //! @param fsid filesystem id
  //!
  //! @return true if deleted, false if it does not exist
  //----------------------------------------------------------------------------
  bool ResetDB(eos::common::FileSystem::fsid_t fsid);

  //----------------------------------------------------------------------------
  //! Trim all DB files
  //!
  //! @return true if successful, othewise false
  //----------------------------------------------------------------------------
  bool TrimDB();

  inline void FsLockRead(const eos::common::FileSystem::fsid_t& fsid)
  {
    _FsLock(fsid, false);
  }

  inline void FsLockWrite(const eos::common::FileSystem::fsid_t& fsid)
  {
    _FsLock(fsid, true);
  }

  inline void FsUnlockRead(const eos::common::FileSystem::fsid_t& fsid)
  {
    _FsUnlock(fsid, false);
  }

  inline void FsUnlockWrite(const eos::common::FileSystem::fsid_t& fsid)
  {
    _FsUnlock(fsid, true);
  }

  //----------------------------------------------------------------------------
  //! Shutdown
  //----------------------------------------------------------------------------
  void
  Shutdown() override
  {
    while (!mDbMap.empty()) {
      ShutdownDB(mDbMap.begin()->first, true);
    }

    eos::common::RWMutexWriteLock lock(mMapMutex);
    mDbMap.clear();
  }

  //----------------------------------------------------------------------------
  //! Get number of files on file system
  //!
  //! @param fsid file system id
  //!
  //! @return number of files
  //----------------------------------------------------------------------------
  long long GetNumFiles(eos::common::FileSystem::fsid_t fsid);

  //----------------------------------------------------------------------------
  //! Get number of file systems
  //!
  //! @return number of file systems on the current machine
  //----------------------------------------------------------------------------
  uint32_t GetNumFileSystems() const;

  //----------------------------------------------------------------------------
  //! Check if given file system is currently syncing
  //!
  //! @param fsid file system id
  //!
  //! @return true if syncing, otherwise false
  //----------------------------------------------------------------------------
  bool IsSyncing(eos::common::FileSystem::fsid_t fsid) const;

private:
  std::map<eos::common::FileSystem::fsid_t, eos::common::DbMap*> mDbMap;
  mutable eos::common::RWMutex mMapMutex; ///< Mutex protecting the Fmd handler
  eos::common::LvDbDbMapInterface::Option lvdboption;
  //! Mutex protecting the mIsSyncing map
  mutable eos::common::RWMutex mSyncMapMutex;
  std::map<eos::common::FileSystem::fsid_t, bool> mIsSyncing;
  ///! Map containing mutexes for each file system id
  google::dense_hash_map<eos::common::FileSystem::fsid_t, eos::common::RWMutex*>
  mFsMtxMap;
  eos::common::RWMutex mFsMtxMapMutex; ///< Mutex protecting the previous map

  //----------------------------------------------------------------------------
  //! Set syncing status of the given file system
  //!
  //! @param fsid file system id
  //! @param is_syncing true if syncing, otherwise false
  //!
  //! @return true if syncing, otherwise false
  //----------------------------------------------------------------------------
  void SetSyncStatus(eos::common::FileSystem::fsid_t fsid, bool is_syncing);

  //----------------------------------------------------------------------------
  //! Lock mutex corresponding to the given file systemd id
  //!
  //! @param fsid file system id
  //! @param write if true lock for write, otherwise lock for read
  //----------------------------------------------------------------------------
  inline void
  _FsLock(const eos::common::FileSystem::fsid_t& fsid, bool write)
  {
    mFsMtxMapMutex.LockRead();

    if (mFsMtxMap.count(fsid)) {
      if (write) {
        mFsMtxMap[fsid]->LockWrite();
      } else {
        mFsMtxMap[fsid]->LockRead();
      }

      mFsMtxMapMutex.UnLockRead();
    } else {
      mFsMtxMapMutex.UnLockRead();
      eos::common::RWMutexWriteLock wr_lock(mFsMtxMapMutex);
      auto it = mFsMtxMap.find(fsid);

      if (it == mFsMtxMap.end()) {
        auto pair = mFsMtxMap.insert(std::make_pair(fsid,
                                     new eos::common::RWMutex()));
        it = pair.first;
        it->second->SetBlocking(true);
      }

      if (write) {
        it->second->LockWrite();
      } else {
        it->second->LockRead();
      }
    }
  }

  //----------------------------------------------------------------------------
  //! Unlock mutex corresponding to the given file systemd id
  //!
  //! @param fsid file system id
  //! @param write if true lock for write, otherwise lock for read
  //----------------------------------------------------------------------------
  inline void
  _FsUnlock(const eos::common::FileSystem::fsid_t& fsid, bool write)
  {
    eos::common::RWMutexReadLock rd_lock(mFsMtxMapMutex);
    auto it = mFsMtxMap.find(fsid);

    if (it != mFsMtxMap.end()) {
      if (write) {
        it->second->UnLockWrite();
      } else {
        it->second->UnLockRead();
      }
    } else {
      // This should NEVER happen as the entry should be in the map because
      // mutex #i should have been locked at some point.
      std::abort();
    }
  }

  //----------------------------------------------------------------------------
  //! Get Fmd struct from local database for a file we know for sure it exists
  //!
  //! @param fid file id
  //! @param fsid filesystem id
  //! @param fmd structure populated in case fid found
  //!
  //! @return true if object found and retrieved, otherwise false
  //! @note this function must be called with the mMapMutex locked and also the
  //! mutex corresponding to the filesystem locked
  //----------------------------------------------------------------------------
  std::pair<bool,eos::common::FmdHelper>
  LocalRetrieveFmd(eos::common::FileId::fileid_t fid,
                   eos::common::FileSystem::fsid_t fsid) override;

  //----------------------------------------------------------------------------
  //! Store Fmd structure in the local database
  //!
  //! @param fid file id
  //! @param fsid filesystem id
  //! @param fmd Fmd structure to be saved
  //!
  //! @return true if successful, otherwise false
  //! @note this function must be called with the mMapMutex locked and also the
  //! mutex corresponding to the filesystem locked
  //----------------------------------------------------------------------------
  bool LocalPutFmd(eos::common::FileId::fileid_t fid,
                   eos::common::FileSystem::fsid_t fsid,
                   const eos::common::FmdHelper& fmd) override
  {
    std::string sval;
    fmd.mProtoFmd.SerializePartialToString(&sval);
    auto it_db = mDbMap.find(fsid);

    if (it_db != mDbMap.end()) {
      return it_db->second->set(eos::common::Slice((const char*)&fid, sizeof(fid)),
                                sval, "") == 0;
    } else {
      return false;
    }
  }

  //----------------------------------------------------------------------------
  //! Reset mgm information for all files stored on a particular file system
  //!
  //! @param fsid file system id
  //!
  //! return true if information has been reset successfully
  //----------------------------------------------------------------------------
  bool ResetMgmInformation(eos::common::FileSystem::fsid_t fsid) final;

  //----------------------------------------------------------------------------
  //! Reset disk information for all files stored on a particular file system
  //!
  //! @param fsid file system id
  //!
  //! @return true if information has been reset successfully
  //----------------------------------------------------------------------------
  bool ResetDiskInformation(eos::common::FileSystem::fsid_t fsid);
};

extern FmdDbMapHandler gFmdDbMapHandler;

//------------------------------------------------------------------------------
//! DB read lock per fsid
//------------------------------------------------------------------------------
class FsReadLock
{
private:
  eos::common::FileSystem::fsid_t mFsId;

public:
  FsReadLock(const eos::common::FileSystem::fsid_t& fsid) :
    mFsId(fsid)
  {
    gFmdDbMapHandler.FsLockRead(mFsId);
  }

  ~FsReadLock()
  {
    gFmdDbMapHandler.FsUnlockRead(mFsId);
  }
};

//------------------------------------------------------------------------------
//! DB write lock per fsid
//------------------------------------------------------------------------------
class FsWriteLock
{
private:
  eos::common::FileSystem::fsid_t mFsId;

public:
  FsWriteLock(const eos::common::FileSystem::fsid_t& fsid) : mFsId(fsid)
  {
    gFmdDbMapHandler.FsLockWrite(mFsId);
  }

  ~FsWriteLock()
  {
    gFmdDbMapHandler.FsUnlockWrite(mFsId);
  }
};

EOSFSTNAMESPACE_END
