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

#include <dirent.h>

#ifdef __APPLE__
#define ECOMM 70
#endif

//! Forward declaration
namespace eos
{
namespace ns
{
class FileMdProto;
}
}

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
  //! Convert namespace file proto md to an Fmd struct
  //!
  //! @param file namespace file proto object
  //! @param fmd reference to Fmd struct
  //!
  //! @return true if successful otherwise false
  //----------------------------------------------------------------------------
  static bool NsFileProtoToFmd(eos::ns::FileMdProto&& filemd, struct Fmd& fmd);

  //----------------------------------------------------------------------------
  //! Return Fmd from MGM doing getfmd command
  //!
  //! @param manager host:port of the mgm to contact
  //! @param fid file id
  //! @param fmd reference to the Fmd struct to store Fmd
  //!
  //! @return 0 if successful, otherwise errno
  //----------------------------------------------------------------------------
  static int GetMgmFmd(const char* manager, eos::common::FileId::fileid_t fid,
                       struct Fmd& fmd);

  //----------------------------------------------------------------------------
  //! Call the 'auto repair' function e.g. 'file convert --rewrite'
  //!
  //! @param manager host:port of the server to contact
  //! @param fid file id to auto-repair
  //!
  //! @return 0 if successful, otherwise errno
  //----------------------------------------------------------------------------
  static int CallAutoRepair(const char* manager,
                            eos::common::FileId::fileid_t fid);

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
  //!
  //! @return true if successfull, otherwise false
  //----------------------------------------------------------------------------
  bool ShutdownDB(eos::common::FileSystem::fsid_t fsid);

  //----------------------------------------------------------------------------
  //! Mark as clean the DB corresponding to given the file system id
  //!
  //! @param fsid file system id
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool MarkCleanDB(eos::common::FileSystem::fsid_t fsid);

  // Meta data handling functions

  //----------------------------------------------------------------------------
  //! Return/create an Fmd struct for the given file/filesystem from the local
  //! database
  //!
  //! @param fid file id
  //! @param fsid filesystem id
  //! @param uid user id of the caller
  //! @param gid group id of the caller
  //! @param layoutid layout id used to store during creation
  //! @param isRW indicates if we create a non-existing Fmd
  //!
  //! @return pointer to Fmd struct if successfull, otherwise nullptr
  //----------------------------------------------------------------------------
  FmdHelper* LocalGetFmd(eos::common::FileId::fileid_t fid,
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
  bool LocalDeleteFmd(eos::common::FileId::fileid_t fid,
                      eos::common::FileSystem::fsid_t fsid);

  static std::vector<int> GetFsidInMetaDir(const char* path)
  {
    std::unique_ptr<DIR, decltype(&closedir)> dir(opendir(path), &closedir);
    if(!dir) {
      return std::vector<int>();
    }

    struct dirent *entry = readdir(dir.get());

    std::vector<int> fsidList;
    while (entry != nullptr)
    {
      if (entry->d_type == DT_DIR && std::string(entry->d_name).find_first_of( "0123456789" ) != std::string::npos) {
        fsidList.emplace_back(std::stoi(first_numberstring(entry->d_name)));
      }

      entry = readdir(dir.get());
    }

    return fsidList;
  }

  inline std::list<Fmd> RetrieveAllFmd()
  {
    std::list<Fmd> fmdList;
    const eos::common::DbMap::Tval* val;
    const eos::common::DbMap::Tkey* key;
    Fmd fmd;
    for (const auto& map : mDbMap) {
      map.second->beginIter();
      while(map.second->iterate(&key, &val)) {
        fmd.Clear();
        fmd.ParseFromString(val->value);
        fmdList.emplace_back(fmd);
      }
    }

    return fmdList;
  }

  //----------------------------------------------------------------------------
  //! Commit modified Fmd record to the local database
  //!
  //! @param fmd pointer to Fmd
  //!
  //! @return true if record was commited, otherwise false
  //----------------------------------------------------------------------------
  bool Commit(FmdHelper* fmd, bool lockit = true);

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
  bool UpdateFromDisk(eos::common::FileSystem::fsid_t fsid,
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
  bool UpdateFromMgm(eos::common::FileSystem::fsid_t fsid,
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
  //! Reset disk information for all files stored on a particular file system
  //!
  //! @param fsid file system id
  //!
  //! @return true if information has been reset successfully
  //----------------------------------------------------------------------------
  bool ResetDiskInformation(eos::common::FileSystem::fsid_t fsid);

  //----------------------------------------------------------------------------
  //! Reset mgm information for all files stored on a particular file system
  //!
  //! @param fsid file system id
  //!
  //! return true if information has been reset successfully
  //----------------------------------------------------------------------------
  bool ResetMgmInformation(eos::common::FileSystem::fsid_t fsid);

  //----------------------------------------------------------------------------
  //! Resync a single entry from disk
  //!
  //! @param fstpath file system location
  //! @param fsid filesystem id
  //! @param flaglayouterror indicates a layout error
  //!
  //! @return true if successfull, otherwise false
  //----------------------------------------------------------------------------
  bool ResyncDisk(const char* fstpath,
                  eos::common::FileSystem::fsid_t fsid,
                  bool flaglayouterror);

  //----------------------------------------------------------------------------
  //! Resync files under path into local database
  //!
  //! @param path path to scan
  //! @param fsid file system id
  //! @param flaglayouterror flag to indicate a layout error
  //!
  //! @return true if successfull, otherwise false
  //----------------------------------------------------------------------------
  bool ResyncAllDisk(const char* path,
                     eos::common::FileSystem::fsid_t fsid,
                     bool flaglayouterror);

  //----------------------------------------------------------------------------
  //! Resync file meta data from MGM into local database
  //!
  //! @param fsid filesystem id
  //! @param fid file id
  //! @param manager manager hostname
  //!
  //! @return true if successfull, otherwise false
  //----------------------------------------------------------------------------
  bool ResyncMgm(eos::common::FileSystem::fsid_t fsid,
                 eos::common::FileId::fileid_t fid, const char* manager);

  //----------------------------------------------------------------------------
  //! Resync all meta data from MGM into local database
  //!
  //! @param fsid filesystem id
  //! param manager manger hostname
  //!
  //! @return true if successfull, otherwise false
  //----------------------------------------------------------------------------
  bool ResyncAllMgm(eos::common::FileSystem::fsid_t fsid,
                    const char* manager);

  //----------------------------------------------------------------------------
  //! Resync all meta data from QuarkdDB
  //!
  //! @param qdb_members members of the QDB cluster
  //! @param fsid filesystem id
  //!
  //! @return true if successfull, otherwise false
  //----------------------------------------------------------------------------
  bool ResyncAllFromQdb(const qclient::Members& qdb_members,
                        eos::common::FileSystem::fsid_t fsid);

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

  //----------------------------------------------------------------------------
  //! Return's the syncing flag (if we sync, all files on disk are flagge as
  //! orphans until the MGM meta data has been verified and when this flag is
  //! set, we don't report orphans!
  //----------------------------------------------------------------------------
  inline bool IsSyncing(eos::common::FileSystem::fsid_t fsid)
  {
    return mIsSyncing[fsid];
  }

  //----------------------------------------------------------------------------
  //! Return's the dirty flag indicating a non-clean shutdown
  //----------------------------------------------------------------------------
  inline bool IsDirty(eos::common::FileSystem::fsid_t fsid)
  {
    return mIsDirty[fsid];
  }

  //----------------------------------------------------------------------------
  //! Set the stay dirty flag indicating a non completed bootup
  //----------------------------------------------------------------------------
  void StayDirty(eos::common::FileSystem::fsid_t fsid, bool dirty)
  {
    mStayDirty[fsid] = dirty;
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

private:
  std::map<eos::common::FileSystem::fsid_t, eos::common::DbMap*> mDbMap;
  mutable eos::common::RWMutex mMapMutex;//< Mutex protecting the Fmd handler
  eos::common::LvDbDbMapInterface::Option lvdboption;
  std::map<eos::common::FileSystem::fsid_t, std::string> DBfilename;
  std::map<eos::common::FileSystem::fsid_t, bool> mIsDirty;
  std::map<eos::common::FileSystem::fsid_t, bool> mStayDirty;
  std::map<eos::common::FileSystem::fsid_t, bool> mIsSyncing;
  ///! Map containing mutexes for each file system id
  google::dense_hash_map<eos::common::FileSystem::fsid_t, eos::common::RWMutex>
  mFsMtxMap;
  eos::common::RWMutex mFsMtxMapMutex; ///< Mutex protecting the previous map

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
        mFsMtxMap[fsid].LockWrite();
      } else {
        mFsMtxMap[fsid].LockRead();
      }

      mFsMtxMapMutex.UnLockRead();
    } else {
      mFsMtxMapMutex.UnLockRead();
      mFsMtxMapMutex.LockWrite();

      if (write) {
        mFsMtxMap[fsid].LockWrite();
      } else {
        mFsMtxMap[fsid].LockRead();
      }

      mFsMtxMapMutex.UnLockWrite();
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
    mFsMtxMapMutex.LockRead();

    if (mFsMtxMap.count(fsid)) {
      if (write) {
        mFsMtxMap[fsid].UnLockWrite();
      } else {
        mFsMtxMap[fsid].UnLockRead();
      }

      mFsMtxMapMutex.UnLockRead();
    } else {
      // This should NEVER happen as the entry should be in the map because
      // mutex #i should have been locked at some point.
      mFsMtxMapMutex.UnLockRead();
      mFsMtxMapMutex.LockWrite();

      if (write) {
        mFsMtxMap[fsid].UnLockWrite();
      } else {
        mFsMtxMap[fsid].UnLockRead();
      }

      mFsMtxMapMutex.UnLockWrite();
    }
  }

  //----------------------------------------------------------------------------
  //! Check if file record exists in the local database
  //!
  //! @param fid file id
  //! @param fsid filesystem id
  //!
  //! @return true if it exits, otherwise false
  //! @note this function must be called with the mMapMutex locked and also the
  //! mutex corresponding to the filesystem locked
  //----------------------------------------------------------------------------
  bool LocalExistFmd(eos::common::FileId::fileid_t fid,
                     eos::common::FileSystem::fsid_t fsid)
  {
    if (!mDbMap.count(fsid)) {
      return false;
    }

    eos::common::DbMap::Tval val;
    bool retval = mDbMap[fsid]->get(eos::common::Slice((const char*)&fid,
                                    sizeof(fid)), &val);
    return retval;
  }

  //----------------------------------------------------------------------------
  //! Get Fmd struct from local database for a file we know for sure it exists
  //!
  //! @param fid file id
  //! @param fsid filesystem id
  //!
  //! @return Fmd structure
  //! @note this function must be called with the mMapMutex locked and also the
  //! mutex corresponding to the filesystem locked
  //----------------------------------------------------------------------------
  Fmd LocalRetrieveFmd(eos::common::FileId::fileid_t fid,
                       eos::common::FileSystem::fsid_t fsid)
  {
    eos::common::DbMap::Tval val;
    mDbMap[fsid]->get(eos::common::Slice((const char*)&fid, sizeof(fid)), &val);
    Fmd retval;
    retval.ParseFromString(val.value);
    return retval;
  }

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
                   eos::common::FileSystem::fsid_t fsid, const Fmd& fmd)
  {
    std::string sval;
    fmd.SerializePartialToString(&sval);
    return mDbMap[fsid]->set(eos::common::Slice((const char*)&fid, sizeof(fid)),
                             sval, "") == 0;
  }

  //----------------------------------------------------------------------------
  //! Execute "fs dumpmd" on the MGM node
  //!
  //! @param mgm_host MGM hostname
  //! @param fsid filesystem id
  //! @param fn_output file name where output is written
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  static bool ExecuteDumpmd(const std::string& mgm_hosst,
                            eos::common::FileSystem::fsid_t fsid,
                            std::string& fn_output);

  static inline std::string first_numberstring(const std::string & str)
  {
    std::size_t const n = str.find_first_of("0123456789");
    if (n != std::string::npos)
    {
      std::size_t const m = str.find_first_not_of("0123456789", n);
      return str.substr(n, m != std::string::npos ? m-n : m);
    }
    return std::string();
  }
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
