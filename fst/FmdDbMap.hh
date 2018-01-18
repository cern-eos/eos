//------------------------------------------------------------------------------
//! @file FmdDbMap.hh
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
#include "fst/Namespace.hh"
#include "fst/Fmd.hh"
#include "common/DbMap.hh"
#include "common/FileId.hh"
#include "common/LayoutId.hh"
#include "XrdSys/XrdSysPthread.hh"
#include "namespace/interface/IFileMD.hh"
#include "namespace/ns_quarkdb/FileMD.hh"

#ifdef __APPLE__
#define ECOMM 70
#endif

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class handling many Fmd changelog files at a time
//------------------------------------------------------------------------------
class FmdDbMapHandler : public eos::common::LogId
{
public:
  //----------------------------------------------------------------------------
  //! Convert an FST env representation to an Fmd struct
  //!
  //! @param env env representation
  //! @param fmd reference to Fmd struct
  //!
  //! @return true if successful otherwise false
  //----------------------------------------------------------------------------
  static bool EnvMgmToFmd(XrdOucEnv& env, struct Fmd& fmd);

  //----------------------------------------------------------------------------
  //! Convert namespace file metadata to an Fmd struct
  //!
  //! @param file namespace file metadata object
  //! @param fmd reference to Fmd struct
  //!
  //! @return true if successful otherwise false
  //----------------------------------------------------------------------------
  bool NsFileMDToFmd(eos::IFileMD* file, struct Fmd& fmd);

  //----------------------------------------------------------------------------
  //! Return Fmd from an mgm
  //!
  //! @param manager host:port of the mgm to contact
  //! @param fid file id
  //! @param fmd reference to the Fmd struct to store Fmd
  //!
  //! @return
  //----------------------------------------------------------------------------
  static int GetMgmFmd(const char* manager, eos::common::FileId::fileid_t fid,
                       struct Fmd& fmd);

  //----------------------------------------------------------------------------
  //! Call the 'auto repair' function e.g. 'file convert --rewrite'
  //!
  //! @param manager host:port of the server to contact
  //! @param fid file id to auto-repair
  //!
  //! @return
  //----------------------------------------------------------------------------
  static int CallAutoRepair(const char* manager,
                            eos::common::FileId::fileid_t fid);

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  FmdDbMapHandler()
  {
    SetLogId("CommonFmdDbMapHandler");
    lvdboption.CacheSizeMb = 0;
    lvdboption.BloomFilterNbits = 0;
    FsMutexMap.set_deleted_key(
      std::numeric_limits<eos::common::FileSystem::fsid_t>::max() - 2);
    FsMutexMap.set_empty_key(
      std::numeric_limits<eos::common::FileSystem::fsid_t>::max() - 1);
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~FmdDbMapHandler()
  {
    Shutdown();
  }

  //----------------------------------------------------------------------------
  //! Create a new changelog filename in 'dir' (the fsid suffix is not added!)
  //----------------------------------------------------------------------------
  virtual const char*
  CreateDBFileName(const char* cldir, std::string& clname)
  {
    clname = cldir;
    clname += "/";
    clname += "fmd";
    return clname.c_str();
  }

  //----------------------------------------------------------------------------
  //! Set a new DB file for a filesystem id
  //!
  //! @param dbfilename path to the sqlite db file
  //! @param fsid filesystem id identified by this file
  //!
  //! @return true if successfull false if failed
  //----------------------------------------------------------------------------
  virtual bool SetDBFile(const char* dbfile, int fsid);

  //----------------------------------------------------------------------------
  //! Shutdown an open DB file
  //!
  //! @param fsid filesystem id identifier
  //!
  //! @return true if successfull false if failed
  //----------------------------------------------------------------------------
  virtual bool ShutdownDB(eos::common::FileSystem::fsid_t fsid);

  //----------------------------------------------------------------------------
  //! Mark as clean the DB corresponding to given the file system id
  //!
  //! @param fsid file system id
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool MarkCleanDB(eos::common::FileSystem::fsid_t fsid);

  // the meta data handling functions

  //----------------------------------------------------------------------------
  //! Return/create an Fmd struct for the given file/filesystem id for user
  //! uid/gid and layout layoutid
  //!
  //! @param fid file id
  //! @param fsid filesystem id
  //! @param uid user id of the caller
  //! @param gid group id of the caller
  //! @param layoutid layout id used to store during creation
  //! @param isRW indicates if we create a non-existing Fmd
  //!
  //! @return pointer to Fmd struct if successfull, otherwise nullpt
  //----------------------------------------------------------------------------
  virtual FmdHelper* GetFmd(eos::common::FileId::fileid_t fid,
                            eos::common::FileSystem::fsid_t fsid,
                            uid_t uid, gid_t gid,
                            eos::common::LayoutId::layoutid_t layoutid,
                            bool isRW = false, bool force = false);

  //----------------------------------------------------------------------------
  //! Delete a record associated with fid and filesystem fsid
  //!
  //! @param fid file id
  //! @param fsid filesystem id
  //!
  //! @return true if deleted, false if it does not exist
  //----------------------------------------------------------------------------
  virtual bool DeleteFmd(eos::common::FileId::fileid_t fid,
                         eos::common::FileSystem::fsid_t fsid);

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  inline bool ExistFmd(eos::common::FileId::fileid_t fid,
                       eos::common::FileSystem::fsid_t fsid)
  {
    eos::common::DbMap::Tval val;

    if (!mDbMap.count(fsid)) {
      return false;
    }

    bool retval = mDbMap[fsid]->get(eos::common::Slice((const char*)&fid,
                                    sizeof(fid)), &val);
    return retval;
  }

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  inline Fmd RetrieveFmd(eos::common::FileId::fileid_t fid,
                         eos::common::FileSystem::fsid_t fsid)
  {
    eos::common::DbMap::Tval val;
    mDbMap[fsid]->get(eos::common::Slice((const char*)&fid, sizeof(fid)), &val);
    Fmd retval;
    retval.ParseFromString(val.value);
    return retval;
  }

  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  inline bool PutFmd(eos::common::FileId::fileid_t fid,
                     eos::common::FileSystem::fsid_t fsid, const Fmd& fmd)
  {
    std::string sval;
    fmd.SerializePartialToString(&sval);
    return mDbMap[fsid]->set(eos::common::Slice((const char*)&fid, sizeof(fid)),
                             sval, "") == 0;
  }

  //----------------------------------------------------------------------------
  //! Commit modified Fmd record to the DB file
  //!
  //! @param fmd pointer to Fmd
  //!
  //! @return true if record has been commited
  //----------------------------------------------------------------------------
  virtual bool Commit(FmdHelper* fmd, bool lockit = true);

  //----------------------------------------------------------------------------
  //! Reset disk information
  //!
  //! @param fsid file system id
  //!
  //! @return true if information has been reset successfully
  //----------------------------------------------------------------------------
  virtual bool ResetDiskInformation(eos::common::FileSystem::fsid_t fsid);

  //----------------------------------------------------------------------------
  //! Reset mgm information
  //!
  //! @param fsid file system id
  //!
  //! return true if information has been reset successfully
  //----------------------------------------------------------------------------
  virtual bool ResetMgmInformation(eos::common::FileSystem::fsid_t fsid);

  //----------------------------------------------------------------------------
  //! Update fmd from disk i.e. physical file extended attributes
  //!
  //! @param fsid file system id
  //! @param fid  file id to update
  //! @param disksize size of the file on disk
  //! @param diskchecksum checksum of the file on disk
  //! @param checktime time of the last check of that file
  //! @param filecxerror indicator for file checksum error
  //! @param blockcxerror inidicator for block checksum error
  //! @param flaglayouterror indication for layout error
  //!
  //! @return true if record has been commited
  //----------------------------------------------------------------------------
  virtual bool UpdateFromDisk(eos::common::FileSystem::fsid_t fsid,
                              eos::common::FileId::fileid_t fid,
                              unsigned long long disksize,
                              std::string diskchecksum,
                              unsigned long checktime, bool filecxerror,
                              bool blockcxerror, bool flaglayouterror);

  //----------------------------------------------------------------------------
  //! Update fmd from MGM metadata
  //!
  //! @param fsid file system id
  //! @param fid  file id to update
  //! @param cid  container id
  //! @param lid  layout id
  //! @param mgmsize size of the file in the mgm namespace
  //! @param mgmchecksum checksum of the file in the mgm namespace
  //!
  //! @return true if record has been commited
  //----------------------------------------------------------------------------
  virtual bool UpdateFromMgm(eos::common::FileSystem::fsid_t fsid,
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
  //! Resync files under path into DB
  //!
  //! @param path path to scan
  //! @param fsid file system id
  //! @param flaglayouterror flag to indicate a layout error
  //!
  //! @return true if successfull, otherwise false
  //----------------------------------------------------------------------------
  virtual bool ResyncAllDisk(const char* path,
                             eos::common::FileSystem::fsid_t fsid,
                             bool flaglayouterror);

  //----------------------------------------------------------------------------
  //! Resync a single entry from disk
  //!
  //! @param fstpath file system location
  //! @param fsid filesystem id
  //! @param flaglayouterror indicates a layout error
  //! @param callautorepair flag to call auto-repair
  //!
  //! @return true if successfull, otherwise false
  //----------------------------------------------------------------------------
  virtual bool ResyncDisk(const char* fstpath,
                          eos::common::FileSystem::fsid_t fsid,
                          bool flaglayouterror,
                          bool callautorepair = false);

  //----------------------------------------------------------------------------
  //! Resync file meta data from MGM into DB
  //!
  //! @param fsid filesystem id
  //! @param fid file id
  //! @param manager manager hostname
  //!
  //! @return true if successfull, otherwise false
  //----------------------------------------------------------------------------
  virtual bool ResyncMgm(eos::common::FileSystem::fsid_t fsid,
                         eos::common::FileId::fileid_t fid, const char* manager);

  //----------------------------------------------------------------------------
  //! Resync all meta data from MGM into DB
  //!
  //! @param fsid filesystem id
  //! param manager manger hostname
  //!
  //! @return true if successfull, otherwise false
  //----------------------------------------------------------------------------
  virtual bool ResyncAllMgm(eos::common::FileSystem::fsid_t fsid,
                            const char* manager);

  //----------------------------------------------------------------------------
  //! Resync all meta data from QuarkdDB
  //!
  //! @param qcl qclient object
  //! @param fsid filesystem id
  //!
  //! @return true if successfull, otherwise false
  //----------------------------------------------------------------------------
  virtual bool ResyncAllFromQdb(qclient::QClient* qcl,
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
  virtual bool
  GetInconsistencyStatistics(eos::common::FileSystem::fsid_t fsid,
                             std::map<std::string, size_t>& statistics,
                             std::map<std::string,
                             std::set < eos::common::FileId::fileid_t>
                             >& fidset);

  //----------------------------------------------------------------------------
  //! Remove ghost entries - entries which are neither on disk nor ath the MGM
  //!
  //! @param path mount prefix of the filesystem
  //! @param fsid filesystem id
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool RemoveGhostEntries(const char* prefix,
                          eos::common::FileSystem::fsid_t fsid);

  //----------------------------------------------------------------------------
  //! Reset(clear) the contents of the DB
  //!
  //! @param fsid filesystem id
  //!
  //! @return true if deleted, false if it does not exist
  //----------------------------------------------------------------------------
  virtual bool ResetDB(eos::common::FileSystem::fsid_t fsid);

  //----------------------------------------------------------------------------
  //! Trim all DB files
  //!
  //! @return true if successful, othewise false
  //----------------------------------------------------------------------------
  virtual bool TrimDB();

  //----------------------------------------------------------------------------
  //! Return's the syncing flag (if we sync, all files on disk are flagge as
  //! orphans until the MGM meta data has been verified and when this flag is
  //! set, we don't report orphans!
  //----------------------------------------------------------------------------
  virtual bool
  IsSyncing(eos::common::FileSystem::fsid_t fsid)
  {
    return isSyncing[fsid];
  }

  //----------------------------------------------------------------------------
  //! Return's the dirty flag indicating a non-clean shutdown
  //----------------------------------------------------------------------------
  virtual bool
  IsDirty(eos::common::FileSystem::fsid_t fsid)
  {
    return isDirty[fsid];
  }

  //----------------------------------------------------------------------------
  //! Set the stay dirty flag indicating a non completed bootup
  //----------------------------------------------------------------------------
  virtual void
  StayDirty(eos::common::FileSystem::fsid_t fsid, bool dirty)
  {
    stayDirty[fsid] = dirty;
  }

  // That is all we need for meta data handling

  //----------------------------------------------------------------------------
  //! Hash map protecting each filesystem map in FmdSqliteMap
  //----------------------------------------------------------------------------
  google::dense_hash_map<eos::common::FileSystem::fsid_t, eos::common::RWMutex>
  FsMutexMap;

  //----------------------------------------------------------------------------
  //! Mutex protecting the previous map
  //----------------------------------------------------------------------------
  eos::common::RWMutex FsMutexMapMutex;

  inline void
  _FsLock(const eos::common::FileSystem::fsid_t& fsid, bool write)
  {
    FsMutexMapMutex.LockRead();

    if (FsMutexMap.count(fsid)) {
      if (write) {
        FsMutexMap[fsid].LockWrite();
      } else {
        FsMutexMap[fsid].LockRead();
      }

      FsMutexMapMutex.UnLockRead();
    } else {
      FsMutexMapMutex.UnLockRead();
      FsMutexMapMutex.LockWrite();

      if (write) {
        FsMutexMap[fsid].LockWrite();
      } else {
        FsMutexMap[fsid].LockRead();
      }

      FsMutexMapMutex.UnLockWrite();
    }
  }

  inline void
  _FsUnlock(const eos::common::FileSystem::fsid_t& fsid, bool write)
  {
    FsMutexMapMutex.LockRead();

    if (FsMutexMap.count(fsid)) {
      if (write) {
        FsMutexMap[fsid].UnLockWrite();
      } else {
        FsMutexMap[fsid].UnLockRead();
      }

      FsMutexMapMutex.UnLockRead();
    } else {
      // This should NEVER happen as the entry should be in the map because
      // mutex #i should have been locked at some point.
      FsMutexMapMutex.UnLockRead();
      FsMutexMapMutex.LockWrite();

      if (write) {
        FsMutexMap[fsid].UnLockWrite();
      } else {
        FsMutexMap[fsid].UnLockRead();
      }

      FsMutexMapMutex.UnLockWrite();
    }
  }

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
  Shutdown()
  {
    for (auto it = mDbMap.begin(); it != mDbMap.end(); it++) {
      ShutdownDB(it->first);
    }

    eos::common::RWMutexWriteLock lock(Mutex);
    mDbMap.clear();
  }

  std::map<eos::common::FileSystem::fsid_t, eos::common::DbMap*> mDbMap;
  eos::common::RWMutex Mutex;//< Mutex protecting the Fmd handler

private:
  eos::common::LvDbDbMapInterface::Option lvdboption;
  std::map<eos::common::FileSystem::fsid_t, std::string> DBfilename;
  std::map<eos::common::FileSystem::fsid_t, bool> isDirty;
  std::map<eos::common::FileSystem::fsid_t, bool> stayDirty;
  std::map<eos::common::FileSystem::fsid_t, bool> isSyncing;

  //----------------------------------------------------------------------------
  //! Get file metadata info from QuarkDB
  //!
  //! @param qcl qclient object
  //! @param fid file id
  //!
  //! @return file metadata object
  //----------------------------------------------------------------------------
  std::unique_ptr<eos::FileMD>
  GetFmdFromQdb(qclient::QClient* qcl, eos::IFileMD::id_t id) const;
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
