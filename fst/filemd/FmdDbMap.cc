//------------------------------------------------------------------------------
// File: FmdDbMap.cc
// Author: Geoffray Adde - CERN
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
#include "fst/filemd/FmdDbMap.hh"
#include "fst/filemd/FmdMgm.hh"

EOSFSTNAMESPACE_BEGIN

using eos::common::LayoutId;

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FmdDbMapHandler::FmdDbMapHandler()
{
  using eos::common::FileSystem;
  SetLogId("CommonFmdDbMapHandler");
  lvdboption.CacheSizeMb = 0;
  lvdboption.BloomFilterNbits = 0;
  mFsMtxMap.set_deleted_key(std::numeric_limits<FileSystem::fsid_t>::max() - 2);
  mFsMtxMap.set_empty_key(std::numeric_limits<FileSystem::fsid_t>::max() - 1);
  mSyncMapMutex.SetBlocking(true);
}

//------------------------------------------------------------------------------
// Get number of file systems
//------------------------------------------------------------------------------
uint32_t
FmdDbMapHandler::GetNumFileSystems() const
{
  eos::common::RWMutexReadLock rd_lock(mMapMutex);
  return mDbMap.size();
}

//------------------------------------------------------------------------------
// Set a new DB file for a filesystem id
//------------------------------------------------------------------------------
bool
FmdDbMapHandler::SetDBFile(const char* meta_dir, int fsid)
{
  bool is_attached = false;
  {
    // First check if DB is already open - in this case we first do a shutdown
    eos::common::RWMutexWriteLock wr_lock(mMapMutex);

    if (mDbMap.count(fsid)) {
      is_attached = true;
    }
  }

  if (is_attached) {
    if (ShutdownDB(fsid, true)) {
      is_attached = false;
    }
  }

  char fsDBFileName[1024];
  sprintf(fsDBFileName, "%s/fmd.%04d.%s", meta_dir, fsid,
          eos::common::DbMap::getDbType().c_str());
  eos_info("%s DB is now %s", eos::common::DbMap::getDbType().c_str(),
           fsDBFileName);
  eos::common::RWMutexWriteLock wr_lock(mMapMutex);
  FsWriteLock wlock(this, fsid);

  if (!is_attached) {
    auto result = mDbMap.insert(std::make_pair(fsid, new eos::common::DbMap()));

    if (result.second == false) {
      eos_err("msg=\"failed to insert new db in map, fsid=%lli", fsid);
      return false;
    }
  }

  // Create / or attach the db (try to repair if needed)
  eos::common::LvDbDbMapInterface::Option* dbopt = &lvdboption;

  // If we have not set the leveldb option, use the default (currently, bloom
  // filter 10 bits and 100MB cache)
  if (lvdboption.BloomFilterNbits == 0) {
    dbopt = NULL;
  }

  if (!mDbMap[fsid]->attachDb(fsDBFileName, true, 0, dbopt)) {
    eos_static_err("failed to attach %s database file %s",
                   eos::common::DbMap::getDbType().c_str(), fsDBFileName);
    return false;
  } else {
    if (getenv("EOS_FST_CACHE_LEVELDB")) {
      mDbMap[fsid]->outOfCore(false);
    } else {
      mDbMap[fsid]->outOfCore(true);
    }
  }

  return true;
}

//------------------------------------------------------------------------------
// Shutdown an open DB file
//------------------------------------------------------------------------------
bool
FmdDbMapHandler::ShutdownDB(eos::common::FileSystem::fsid_t fsid, bool do_lock)
{
  eos_info("msg=\"DB shutdown\" dbpath=%s fsid=%lu",
           eos::common::DbMap::getDbType().c_str(), fsid);
  eos::common::RWMutexWriteLock wr_lock;

  if (do_lock) {
    wr_lock.Grab(mMapMutex);
  }

  if (mDbMap.count(fsid)) {
    if (mDbMap[fsid]->detachDb()) {
      delete mDbMap[fsid];
      mDbMap.erase(fsid);
      return true;
    }
  }

  return false;
}

//------------------------------------------------------------------------------
// Return/create an Fmd struct for the given file/filesystem id for user
// uid/gid and layout layoutid
//------------------------------------------------------------------------------
std::unique_ptr<eos::common::FmdHelper>
FmdDbMapHandler::LocalGetFmd(eos::common::FileId::fileid_t fid,
                             eos::common::FileSystem::fsid_t fsid,
                             bool force_retrieve, bool do_create,
                             uid_t uid, gid_t gid,
                             eos::common::LayoutId::layoutid_t layoutid)
{
  if (fid == 0ull) {
    eos_warning("msg=\"no such fmd in db\" fxid=0 fsid=%lu", fsid);
    return nullptr;
  }

  eos::common::RWMutexReadLock lock(mMapMutex);

  if (mDbMap.count(fsid)) {
    eos::common::FmdHelper valfmd;
    {
      FsReadLock fs_rd_lock(this, fsid);

      if (auto [status, valfmd] = LocalRetrieveFmd(fid, fsid);
          status) {
        std::unique_ptr<eos::common::FmdHelper> fmd {
          new eos::common::FmdHelper()};

        if (!fmd) {
          return nullptr;
        }

        // Make a copy of the current record
        fmd->Replicate(valfmd);

        if ((fmd->mProtoFmd.fid() != fid) ||
            (fmd->mProtoFmd.fsid() != fsid)) {
          eos_crit("msg=\"mismatch between requested fid/fsid and retrieved ones\" "
                   "fid=%08llx retrieved_fid=%08llx fsid=%lu retrieved_fsid=%lu",
                   fid, fmd->mProtoFmd.fid(), fsid, fmd->mProtoFmd.fsid());
          return nullptr;
        }

        // Force flag allows to retrieve 'any' value ignoring inconsistencies
        if (force_retrieve) {
          return fmd;
        }

        // Handle only replica and plain files
        if (!eos::common::LayoutId::IsRain(fmd->mProtoFmd.lid())) {
          // Don't return a record if there is a size mismatch
          if ((!do_create) &&
              ((fmd->mProtoFmd.disksize() &&
                (fmd->mProtoFmd.disksize() != eos::common::FmdHelper::UNDEF) &&
                (fmd->mProtoFmd.disksize() != fmd->mProtoFmd.size())) ||
               (fmd->mProtoFmd.mgmsize() &&
                (fmd->mProtoFmd.mgmsize() != eos::common::FmdHelper::UNDEF) &&
                (fmd->mProtoFmd.mgmsize() != fmd->mProtoFmd.size())))) {
            eos_crit("msg=\"size mismatch disk/mgm vs memory\" fxid=%08llx "
                     "fsid=%lu size=%llu disksize=%llu mgmsize=%llu",
                     fid, (unsigned long) fsid, fmd->mProtoFmd.size(),
                     fmd->mProtoFmd.disksize(), fmd->mProtoFmd.mgmsize());
            return nullptr;
          }

          // Don't return a record, if there is a checksum error flagged
          if ((!do_create) &&
              ((fmd->mProtoFmd.filecxerror() == 1) ||
               (fmd->mProtoFmd.mgmchecksum().length() &&
                (fmd->mProtoFmd.mgmchecksum() != fmd->mProtoFmd.checksum())))) {
            eos_crit("msg=\"checksum error flagged/detected\" fxid=%08llx "
                     "fsid=%lu checksum=%s diskchecksum=%s mgmchecksum=%s "
                     "filecxerror=%d blockcxerror=%d", fid,
                     (unsigned long) fsid, fmd->mProtoFmd.checksum().c_str(),
                     fmd->mProtoFmd.diskchecksum().c_str(),
                     fmd->mProtoFmd.mgmchecksum().c_str(),
                     fmd->mProtoFmd.filecxerror(),
                     fmd->mProtoFmd.blockcxerror());
            return nullptr;
          }
        } else {
          // Don't return a record if there is a blockxs error
          if (!do_create && (fmd->mProtoFmd.blockcxerror() == 1)) {
            eos_crit("msg=\"blockxs error detected\" fxid=%08llx fsid=%lu",
                     fid, fsid);
            return nullptr;
          }
        }

        return fmd;
      }
    }

    if (do_create) {
      // Create a new record
      struct timeval tv;
      struct timezone tz;
      gettimeofday(&tv, &tz);
      valfmd.Reset();
      FsWriteLock fs_wr_lock(this, fsid); // --> (return)
      valfmd.mProtoFmd.set_uid(uid);
      valfmd.mProtoFmd.set_gid(gid);
      valfmd.mProtoFmd.set_lid(layoutid);
      valfmd.mProtoFmd.set_fsid(fsid);
      valfmd.mProtoFmd.set_fid(fid);
      valfmd.mProtoFmd.set_ctime(tv.tv_sec);
      valfmd.mProtoFmd.set_mtime(tv.tv_sec);
      valfmd.mProtoFmd.set_atime(tv.tv_sec);
      valfmd.mProtoFmd.set_ctime_ns(tv.tv_usec * 1000);
      valfmd.mProtoFmd.set_mtime_ns(tv.tv_usec * 1000);
      valfmd.mProtoFmd.set_atime_ns(tv.tv_usec * 1000);
      std::unique_ptr<eos::common::FmdHelper> fmd {
        new eos::common::FmdHelper(fid, fsid)};

      if (!fmd) {
        return nullptr;
      }

      fmd->Replicate(valfmd);

      if (Commit(fmd.get(), false)) {
        eos_debug("msg=\"return fmd object\" fid=%08llx fsid=%lu", fid, fsid);
        return fmd;
      } else {
        eos_crit("msg=\"failed to commit fmd to db\" fid=%08llx fsid=%lu",
                 fid, fsid);
        return nullptr;
      }
    } else {
      eos_warning("msg=\"no fmd record found\" fid=%08llx fsid=%lu", fid, fsid);
      return nullptr;
    }
  } else {
    eos_crit("msg=\"no db object available\" fid=%08llx fid=%lu", fid, fsid);
    return nullptr;
  }
}

//------------------------------------------------------------------------------
// Get Fmd struct from local database for a file we know for sure it exists
//------------------------------------------------------------------------------
std::pair<bool, eos::common::FmdHelper>
FmdDbMapHandler::LocalRetrieveFmd(eos::common::FileId::fileid_t fid,
                                  eos::common::FileSystem::fsid_t fsid,
                                  std::string* path, bool lock)
{
  bool found {false};
  eos::common::FmdHelper fmd;
  eos::common::DbMap* ptr_db {nullptr};
  eos::common::RWMutexReadLock map_lock;

  if (lock) {
    map_lock.Grab(mMapMutex);
  }

  auto it = mDbMap.find(fsid);

  if (it == mDbMap.end()) {
    eos_crit("msg=\"db not open\" dbpath=%s fsid=%lu",
             eos::common::DbMap::getDbType().c_str(), fsid);
    return {found, std::move(fmd)};
  }

  // We found the entry we were looking for
  ptr_db = it->second;
  map_lock.Release();
  eos::common::DbMap::Tval val;

  if (ptr_db->get(eos::common::Slice((const char*)&fid, sizeof(fid)), &val)) {
    fmd.mProtoFmd.ParseFromString(val.value);
    found = true;
  }

  // In this particular case we need the move construction only because this
  // will be directly passed on to the destructuring bind at call sites, if we
  // were only returning fmd for eg. the copy ellision would've been guaranteed
  return {found, std::move(fmd)};
}

//------------------------------------------------------------------------------
// Delete a record associated with fid and filesystem fsid
//------------------------------------------------------------------------------
void
FmdDbMapHandler::LocalDeleteFmd(eos::common::FileId::fileid_t fid,
                                eos::common::FileSystem::fsid_t fsid,
                                bool drop_file)
{
  eos::common::RWMutexReadLock map_rd_lock(mMapMutex);
  FsWriteLock fs_wr_lock(this, fsid);
  auto it = mDbMap.find(fsid);

  if (it != mDbMap.end()) {
    (void) mDbMap[fsid]->remove(eos::common::Slice((const char*)&fid, sizeof(fid)));
  }
}

//------------------------------------------------------------------------------
// Commit modified Fmd record to the DB file
//------------------------------------------------------------------------------
bool
FmdDbMapHandler::Commit(eos::common::FmdHelper* fmd,
                        bool lockit, std::string* /*path*/)
{
  if (!fmd) {
    return false;
  }

  uint32_t fsid = fmd->mProtoFmd.fsid();
  uint64_t fid = fmd->mProtoFmd.fid();
  struct timeval tv;
  struct timezone tz;
  gettimeofday(&tv, &tz);
  fmd->mProtoFmd.set_mtime(tv.tv_sec);
  fmd->mProtoFmd.set_atime(tv.tv_sec);
  fmd->mProtoFmd.set_mtime_ns(tv.tv_usec * 1000);
  fmd->mProtoFmd.set_atime_ns(tv.tv_usec * 1000);

  if (lockit) {
    mMapMutex.LockRead();
    FsLockWrite(fsid);
  }

  if (mDbMap.count(fsid)) {
    bool res = LocalPutFmd(fid, fsid, *fmd);

    // Updateed in-memory
    if (lockit) {
      FsUnlockWrite(fsid);
      mMapMutex.UnLockRead();
    }

    return res;
  } else {
    eos_crit("msg=\"DB not open\" dbpath=%s fsid=%lu",
             eos::common::DbMap::getDbType().c_str(), fsid);

    if (lockit) {
      FsUnlockWrite(fsid);
      mMapMutex.UnLockRead();
    }
  }

  return false;
}

//------------------------------------------------------------------------------
// Update fmd with disk info i.e. physical file extended attributes
//------------------------------------------------------------------------------
bool
FmdDbMapHandler::UpdateWithDiskInfo(eos::common::FileSystem::fsid_t fsid,
                                    eos::common::FileId::fileid_t fid,
                                    unsigned long long disk_size,
                                    const std::string& disk_xs,
                                    unsigned long check_ts_sec,
                                    bool filexs_err, bool blockxs_err,
                                    bool layout_err)
{
  if (!fid) {
    eos_err("%s", "msg=\"skipping insert of file with fid=0\"");
    return false;
  }

  eos::common::RWMutexReadLock map_rd_lock(mMapMutex);
  FsWriteLock fs_wr_lock(this, fsid);
  return FmdHandler::UpdateWithDiskInfo(fsid, fid, disk_size, disk_xs,
                                        check_ts_sec,
                                        filexs_err, blockxs_err, layout_err);
}

//------------------------------------------------------------------------------
// Update fmd from MGM metadata
//------------------------------------------------------------------------------
bool
FmdDbMapHandler::UpdateWithMgmInfo(eos::common::FileSystem::fsid_t fsid,
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
                                   int layouterror, std::string locations)
{
  if (!fid) {
    eos_err("%s", "msg=\"skipping insert of file with fid=0\"");
    return false;
  }

  eos::common::RWMutexReadLock map_rd_lock(mMapMutex);
  FsWriteLock fs_wr_lock(this, fsid);
  return FmdHandler::UpdateWithMgmInfo(fsid, fid, cid, lid, mgmsize,
                                       mgmchecksum, uid, gid,
                                       ctime, ctime_ns, mtime, mtime_ns,
                                       layouterror, std::move(locations));
}

//------------------------------------------------------------------------------
// Reset disk information
//------------------------------------------------------------------------------
bool
FmdDbMapHandler::ResetDiskInformation(eos::common::FileSystem::fsid_t fsid)
{
  return ResetAllFmd(fsid, &FmdHandler::ResetFmdDiskInfo);
}

//------------------------------------------------------------------------------
// Reset mgm information
//------------------------------------------------------------------------------
bool
FmdDbMapHandler::ResetMgmInformation(eos::common::FileSystem::fsid_t fsid)
{
  return ResetAllFmd(fsid, &FmdHandler::ResetFmdMgmInfo);
}

//------------------------------------------------------------------------------
// Remove ghost entries - entries which are neither on disk nor at the MGM
//------------------------------------------------------------------------------
bool
FmdDbMapHandler::RemoveGhostEntries(const char* fs_root,
                                    eos::common::FileSystem::fsid_t fsid)
{
  using eos::common::FileId;
  eos_static_info("fsid=%lu", fsid);
  std::vector<eos::common::FileId::fileid_t> to_delete;

  if (!IsSyncing(fsid)) {
    {
      eos::common::RWMutexReadLock rd_lock(mMapMutex);
      FsReadLock fs_rd_lock(this, fsid);

      if (!mDbMap.count(fsid)) {
        return true;
      }

      const eos::common::DbMapTypes::Tkey* k;
      const eos::common::DbMapTypes::Tval* v;
      eos::common::DbMap* db_map = mDbMap.find(fsid)->second;
      eos_static_info("msg=\"verifying %d entries on fsid=%lu\"",
                      db_map->size(), fsid);

      // Report values only when we are not in the sync phase from disk/mgm
      for (db_map->beginIter(false); db_map->iterate(&k, &v, false);) {
        eos::common::FmdHelper f;
        eos::common::FileId::fileid_t fid {0ul};
        f.mProtoFmd.ParseFromString(v->value);
        (void)memcpy(&fid, (void*)k->data(), k->size());

        if (f.mProtoFmd.layouterror()) {
          struct stat buf;
          const std::string hex_fid = FileId::Fid2Hex(fid);
          std::string fpath = FileId::FidPrefix2FullPath(hex_fid.c_str(), fs_root);
          errno = 0;

          if (stat(fpath.c_str(), &buf)) {
            if ((errno == ENOENT) || (errno == ENOTDIR)) {
              if ((f.mProtoFmd.layouterror() & LayoutId::kOrphan) ||
                  (f.mProtoFmd.layouterror() & LayoutId::kUnregistered)) {
                eos_static_info("msg=\"push back for deletion\" fxid=%08llx", fid);
                to_delete.push_back(fid);
              }
            }
          }
        }
      }
    }

    // Delete ghost entries from local database
    for (const auto& id : to_delete) {
      LocalDeleteFmd(id, fsid);
      eos_static_info("msg=\"removed FMD ghost entry\" fxid=%08llx fsid=%d",
                      id, fsid);
    }

    return true;
  } else {
    return false;
  }
}

//------------------------------------------------------------------------------
// Get inconsitency statistics
//------------------------------------------------------------------------------
bool
FmdDbMapHandler::GetInconsistencyStatistics(
  eos::common::FileSystem::fsid_t fsid,
  std::map<std::string, size_t>& statistics,
  std::map<std::string, std::set<eos::common::FileId::fileid_t>>& fidset)
{
  using namespace eos::common;
  eos::common::RWMutexReadLock map_rd_lock(mMapMutex);

  if (!mDbMap.count(fsid)) {
    return false;
  }

  fidset.clear();
  statistics  = {
    {"mem_n",            0}, // no. of files in db
    {"d_sync_n",         0}, // no. of synced files from disk
    {"m_sync_n",         0}, // no. of synced files from MGM
    {FSCK_D_MEM_SZ_DIFF, 0}, // no. files with disk and reference size mismatch
    {FSCK_M_MEM_SZ_DIFF, 0}, // no. files with MGM and reference size mismatch
    {FSCK_D_CX_DIFF,     0}, // no. files with disk and reference checksum mismatch
    {FSCK_M_CX_DIFF,     0}, // no. files with MGM and reference checksum mismatch
    {FSCK_UNREG_N,       0}, // no. of unregistered replicas
    {FSCK_REP_DIFF_N,    0}, // no. of files with replicas number mismatch
    {FSCK_REP_MISSING_N, 0}, // no. of files with replicas missing on disk
    {FSCK_BLOCKXS_ERR,   0}, // no. of replicas with blockxs error
    {FSCK_ORPHANS_N,     0}  // no. of orphaned replicas
  };

  if (!IsSyncing(fsid)) {
    const eos::common::DbMapTypes::Tkey* k;
    const eos::common::DbMapTypes::Tval* v;
    eos::common::DbMapTypes::Tval val;
    FsReadLock fs_rd_lock(this, fsid);

    // We report values only when we are not in the sync phase from disk/mgm
    for (mDbMap[fsid]->beginIter(false);
         mDbMap[fsid]->iterate(&k, &v, false);) {
      eos::common::FmdHelper f;
      auto& proto_fmd = f.mProtoFmd;
      proto_fmd.ParseFromString(v->value);
      CollectInconsistencies(f, statistics, fidset);
    }
  }

  return true;
}

//------------------------------------------------------------------------------
// Reset (clear) the contents of the DB
//------------------------------------------------------------------------------
bool
FmdDbMapHandler::ResetDB(eos::common::FileSystem::fsid_t fsid)
{
  bool rc = true;
  eos::common::RWMutexWriteLock lock(mMapMutex);

  // Erase the hash entry
  if (mDbMap.count(fsid)) {
    FsWriteLock fs_wr_lock(this, fsid);

    // Delete in the in-memory hash
    if (!mDbMap[fsid]->clear()) {
      eos_err("unable to delete all from fst table");
      rc = false;
    } else {
      rc = true;
    }
  } else {
    rc = false;
  }

  return rc;
}

//------------------------------------------------------------------------------
// Trim DB
//------------------------------------------------------------------------------
bool
FmdDbMapHandler::TrimDB()
{
  for (auto it = mDbMap.begin(); it != mDbMap.end(); ++it) {
    eos_static_info("Trimming fsid=%llu ", it->first);

    if (!it->second->trimDb()) {
      eos_static_err("Cannot trim the DB file for fsid=%llu ", it->first);
      return false;
    } else {
      eos_static_info("Trimmed %s DB file for fsid=%llu ",
                      it->second->getDbType().c_str(), it->first);
    }
  }

  return true;
}

//------------------------------------------------------------------------------
// Get number of flies on file system
//------------------------------------------------------------------------------
long long
FmdDbMapHandler::GetNumFiles(eos::common::FileSystem::fsid_t fsid)
{
  eos::common::RWMutexReadLock lock(mMapMutex);
  FsReadLock fs_rd_lock(this, fsid);

  if (mDbMap.count(fsid)) {
    return mDbMap[fsid]->size();
  } else {
    return 0ll;
  }
}


//------------------------------------------------------------------------------
// Check if given file system is currently syncing
//------------------------------------------------------------------------------
bool
FmdDbMapHandler::IsSyncing(eos::common::FileSystem::fsid_t fsid) const
{
  eos::common::RWMutexReadLock rd_lock(mSyncMapMutex);
  const auto it = mIsSyncing.find(fsid);

  if (it == mIsSyncing.end()) {
    return false;
  }

  return it->second;
}

//------------------------------------------------------------------------------
// Set syncing status of the given file system
//------------------------------------------------------------------------------
void
FmdDbMapHandler::SetSyncStatus(eos::common::FileSystem::fsid_t fsid,
                               bool is_syncing)
{
  eos::common::RWMutexWriteLock wr_lock(mSyncMapMutex);
  mIsSyncing[fsid] = is_syncing;
}

bool
FmdDbMapHandler::LocalPutFmd(eos::common::FileId::fileid_t fid,
                             eos::common::FileSystem::fsid_t fsid,
                             const common::FmdHelper& fmd)
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

void
FmdDbMapHandler::ConvertAllFmd(eos::common::FileSystem::fsid_t fsid,
                               FmdHandler* const target_fmd_handler)
{
  const eos::common::DbMapTypes::Tkey* k;
  const eos::common::DbMapTypes::Tval* v;
  //eos::common::DbMapTypes::Tval val;
  FsReadLock fs_rd_lock(this, fsid);

  for (mDbMap[fsid]->beginIter(false);
       mDbMap[fsid]->iterate(&k, &v, false);) {
    eos::common::FmdHelper f;
    auto& proto_fmd = f.mProtoFmd;
    proto_fmd.ParseFromString(v->value);
    auto [status, _] = target_fmd_handler->LocalRetrieveFmd(fsid, proto_fmd.fid());

    if (status) {
      continue;
    }

    target_fmd_handler->Commit(&f);
  }
}

EOSFSTNAMESPACE_END
