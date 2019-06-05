//------------- ----------------------------------------------------------------
// File: FmdDbMap.cc
// Author: Geoffray Adde - CERN
//------------- ----------------------------------------------------------------

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

#include "fst/FmdDbMap.hh"
#include "common/Path.hh"
#include "common/ShellCmd.hh"
#include "proto/Fs.pb.h"
#include "proto/ConsoleRequest.pb.h"
#include "fst/checksum/ChecksumPlugins.hh"
#include "fst/io/FileIoPluginCommon.hh"
#include "fst/Config.hh"
#include "XrdCl/XrdClFileSystem.hh"
#include "namespace/ns_quarkdb/persistency/FileMDSvc.hh"
#include "namespace/ns_quarkdb/persistency/MetadataFetcher.hh"
#include "namespace/ns_quarkdb/persistency/RequestBuilder.hh"
#include "namespace/ns_quarkdb/QdbContactDetails.hh"
#include "qclient/structures/QSet.hh"
#include <stdio.h>
#include <sys/mman.h>
#include <fts.h>
#include <iostream>
#include <fstream>
#include <algorithm>

EOSFSTNAMESPACE_BEGIN

// Global objects
FmdDbMapHandler gFmdDbMapHandler;

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
}

//------------------------------------------------------------------------------
// Convert an MGM env representation to an Fmd struct
//------------------------------------------------------------------------------
bool
FmdDbMapHandler::EnvMgmToFmd(XrdOucEnv& env, struct Fmd& fmd)
{
  // Check that all tags are present
  if (!env.Get("id") ||
      !env.Get("cid") ||
      !env.Get("ctime") ||
      !env.Get("ctime_ns") ||
      !env.Get("mtime") ||
      !env.Get("mtime_ns") ||
      !env.Get("size") ||
      !env.Get("checksum") ||
      !env.Get("lid") ||
      !env.Get("uid") ||
      !env.Get("gid")) {
    return false;
  }

  fmd.set_fid(strtoull(env.Get("id"), 0, 10));
  fmd.set_cid(strtoull(env.Get("cid"), 0, 10));
  fmd.set_ctime(strtoul(env.Get("ctime"), 0, 10));
  fmd.set_ctime_ns(strtoul(env.Get("ctime_ns"), 0, 10));
  fmd.set_mtime(strtoul(env.Get("mtime"), 0, 10));
  fmd.set_mtime_ns(strtoul(env.Get("mtime_ns"), 0, 10));
  fmd.set_mgmsize(strtoull(env.Get("size"), 0, 10));
  fmd.set_lid(strtoul(env.Get("lid"), 0, 10));
  fmd.set_uid((uid_t) strtoul(env.Get("uid"), 0, 10));
  fmd.set_gid((gid_t) strtoul(env.Get("gid"), 0, 10));
  fmd.set_mgmchecksum(env.Get("checksum"));
  fmd.set_locations(env.Get("location") ? env.Get("location") : "");
  size_t cslen = eos::common::LayoutId::GetChecksumLen(fmd.lid()) * 2;
  fmd.set_mgmchecksum(std::string(fmd.mgmchecksum()).erase(std::min(
                        fmd.mgmchecksum().length(), cslen)));
  return true;
}

//----------------------------------------------------------------------------
// Convert namespace file proto object to an Fmd struct
//----------------------------------------------------------------------------
bool
FmdDbMapHandler::NsFileProtoToFmd(eos::ns::FileMdProto&& filemd,
                                  struct Fmd& fmd)
{
  fmd.set_fid(filemd.id());
  fmd.set_cid(filemd.cont_id());
  eos::IFileMD::ctime_t ctime;
  (void) memcpy(&ctime, filemd.ctime().data(), sizeof(ctime));
  eos::IFileMD::ctime_t mtime;
  (void) memcpy(&mtime, filemd.mtime().data(), sizeof(mtime));
  fmd.set_ctime(ctime.tv_sec);
  fmd.set_ctime_ns(ctime.tv_nsec);
  fmd.set_mtime(mtime.tv_sec);
  fmd.set_mtime_ns(mtime.tv_nsec);
  fmd.set_mgmsize(filemd.size());
  fmd.set_lid(filemd.layout_id());
  fmd.set_uid(filemd.uid());
  fmd.set_gid(filemd.gid());
  std::string str_xs;
  uint8_t size = filemd.checksum().size();

  for (uint8_t i = 0; i < size; i++) {
    char hx[3];
    hx[0] = 0;
    snprintf(static_cast<char*>(hx), sizeof(hx), "%02x",
             *(unsigned char*)(filemd.checksum().data() + i));
    str_xs += static_cast<char*>(hx);
  }

  size_t cslen = eos::common::LayoutId::GetChecksumLen(filemd.layout_id()) * 2;
  // Truncate the checksum to the right string length
  str_xs.erase(std::min(str_xs.length(), cslen));
  fmd.set_mgmchecksum(str_xs);
  std::string slocations;

  for (const auto& loc : filemd.locations()) {
    slocations += std::to_string(loc);
    slocations += ",";
  }

  if (!slocations.empty()) {
    slocations.pop_back();
  }

  fmd.set_locations(slocations);
  return true;
}

//------------------------------------------------------------------------------
// Return Fmd from MGM doing getfmd command
//------------------------------------------------------------------------------
int
FmdDbMapHandler::GetMgmFmd(const std::string& manager,
                           eos::common::FileId::fileid_t fid, struct Fmd& fmd)
{
  if (!fid) {
    return EINVAL;
  }

  int rc = 0;
  std::string mgr;
  XrdCl::Buffer arg;
  std::string query = SSTR("/?mgm.pcmd=getfmd&mgm.getfmd.fid=" << fid).c_str();
  std::unique_ptr<XrdCl::Buffer> response;
  XrdCl::Buffer* resp_raw = nullptr;
  XrdCl::XRootDStatus status;

  do {
    mgr = manager;

    if (mgr.empty()) {
      mgr = eos::fst::Config::gConfig.GetManager();

      if (mgr.empty()) {
        eos_static_err("msg=\"no manager info available\"");
        return EINVAL;
      }
    }

    std::string address = SSTR("root://" << mgr <<
                               "//dummy?xrd.wantprot=sss").c_str();
    XrdCl::URL url(address.c_str());

    if (!url.IsValid()) {
      eos_static_err("msg=\"invalid URL=%s\"", address.c_str());
      return EINVAL;
    }

    std::unique_ptr<XrdCl::FileSystem> fs {new XrdCl::FileSystem(url)};

    if (!fs) {
      eos_static_err("%s", "msg=\"failed to allocate FS object\"");
      return EINVAL;
    }

    arg.FromString(query.c_str());
    uint16_t timeout = 10;
    status = fs->Query(XrdCl::QueryCode::OpaqueFile, arg, resp_raw, timeout);
    response.reset(resp_raw);
    resp_raw = nullptr;

    if (status.IsOK()) {
      rc = 0;
      eos_static_debug("msg=\"got metadata from mgm\" manager=%s fid=%08llx",
                       mgr.c_str(), fid);
    } else {
      eos_static_err("msg=\"query error\" fid=%08llx status=%d code=%d", fid,
                     status.status, status.code);

      if ((status.code >= 100) &&
          (status.code <= 300)) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        eos_static_info("msg=\"retry query\" fid=%08llx query=\"%s\"", fid,
                        query.c_str());
      } else {
        eos_static_err("msg=\"failed to retrieve metadata from mgm\" manager=%s "
                       "fid=%08llx", mgr.c_str(), fid);
        rc = ECOMM;
      }
    }
  } while ((status.code >= 100) && (status.code <= 300));

  if (rc) {
    return EIO;
  }

  // Check if response contains any data
  if (!response->GetBuffer()) {
    eos_static_err("msg=\"empty response buffer\" manager=%s fxid=%08llx",
                   mgr.c_str(), fid);
    return ENODATA;
  }

  std::string sresult = response->GetBuffer();
  std::string search_tag = "getfmd: retc=0 ";

  if ((sresult.find(search_tag)) == std::string::npos) {
    eos_static_info("msg=\"no metadata info at the mgm\" manager=%s fxid=%08llx "
                    " resp_buff=\"%s\"", mgr.c_str(), fid, response->GetBuffer());
    return ENODATA;
  } else {
    sresult.erase(0, search_tag.length());
  }

  // Get the remote file meta data into an env hash
  XrdOucEnv fmd_env(sresult.c_str());

  if (!EnvMgmToFmd(fmd_env, fmd)) {
    int envlen;
    eos_static_err("msg=\"failed to parse metadata info\" data=\"%s\" fxid=%08llx",
                   fmd_env.Env(envlen), fid);
    return EIO;
  }

  if (fmd.fid() != fid) {
    eos_static_err("msg=\"received wrong meta data from mgm\" fid=%08llx "
                   "recv_fid=%08llx", fmd.fid(), fid);
    return EIO;
  }

  return 0;
}

//------------------------------------------------------------------------------
// Call the 'auto repair' function e.g. 'file convert --rewrite'
//------------------------------------------------------------------------------
int
FmdDbMapHandler::CallAutoRepair(const char* manager,
                                eos::common::FileId::fileid_t fid)
{
  if (!fid) {
    return EINVAL;
  }

  int rc = 0;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;
  XrdCl::XRootDStatus status;
  XrdOucString fmdquery = "/?mgm.pcmd=rewrite&mgm.fid=";
  const std::string hex_fid = eos::common::FileId::Fid2Hex(fid);
  fmdquery += hex_fid.c_str();
  // @todo(esindril) Legacy, remove once fsctl/Rewrite.cc no longer expects 'fxid'
  fmdquery += "&mgm.fxid=";
  fmdquery += hex_fid.c_str(); // legacy
  XrdOucString address = "root://";
  std::string current_mgr;

  if (!manager) {
    // Use the broadcasted manager name
    current_mgr = eos::fst::Config::gConfig.GetManager();
  } else {
    current_mgr = manager;
  }

  address += current_mgr.c_str();
  address += "//dummy?xrd.wantprot=sss";
  XrdCl::URL url(address.c_str());

  if (!url.IsValid()) {
    eos_static_err("error=URL is not valid: %s", address.c_str());
    return EINVAL;
  }

  std::unique_ptr<XrdCl::FileSystem> fs(new XrdCl::FileSystem(url));

  if (!fs) {
    eos_static_err("error=failed to get new FS object");
    return EINVAL;
  }

  arg.FromString(fmdquery.c_str());
  status = fs->Query(XrdCl::QueryCode::OpaqueFile, arg, response);

  if (status.IsOK()) {
    eos_static_debug("msg=\"scheduled repair\" mgm=%s fxid=%s",
                     current_mgr.c_str(), hex_fid.c_str());
    rc = 0;
  } else {
    eos_static_err("msg=\"failed to schedule repair\" mgm=%s fxid=%s "
                   "err_msg=\"%s\"", current_mgr.c_str(), hex_fid.c_str(),
                   status.ToString().c_str());
    rc = ECOMM;
  }

  delete response;
  return rc;
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
  FsWriteLock wlock(fsid);

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
    mDbMap[fsid]->outOfCore(true);
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
std::unique_ptr<FmdHelper>
FmdDbMapHandler::LocalGetFmd(eos::common::FileId::fileid_t fid,
                             eos::common::FileSystem::fsid_t fsid,
                             uid_t uid, gid_t gid,
                             eos::common::LayoutId::layoutid_t layoutid,
                             bool do_create, bool force_retrieve)
{
  if (fid == 0ull) {
    eos_warning("msg=\"no such fmd in db\" fxid=0 fsid=%lu", fsid);
    return nullptr;
  }

  eos::common::RWMutexReadLock lock(mMapMutex);

  if (mDbMap.count(fsid)) {
    Fmd valfmd;
    {
      FsReadLock fs_rd_lock(fsid);

      if (LocalRetrieveFmd(fid, fsid, valfmd)) {
        std::unique_ptr<FmdHelper> fmd {new FmdHelper()};

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
                (fmd->mProtoFmd.disksize() != Fmd::UNDEF) &&
                (fmd->mProtoFmd.disksize() != fmd->mProtoFmd.size())) ||
               (fmd->mProtoFmd.mgmsize() &&
                (fmd->mProtoFmd.mgmsize() != Fmd::UNDEF) &&
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
            eos_crit("msg=\"checksum error flagged/detected fxid=%08llx "
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
          // @todo(esindril) decide what flags to set for rain layouts
        }

        return fmd;
      }
    }

    if (do_create) {
      // Create a new record
      struct timeval tv;
      struct timezone tz;
      gettimeofday(&tv, &tz);
      FmdHelper::Reset(valfmd);
      FsWriteLock fs_wr_lock(fsid); // --> (return)
      valfmd.set_uid(uid);
      valfmd.set_gid(gid);
      valfmd.set_lid(layoutid);
      valfmd.set_fsid(fsid);
      valfmd.set_fid(fid);
      valfmd.set_ctime(tv.tv_sec);
      valfmd.set_mtime(tv.tv_sec);
      valfmd.set_atime(tv.tv_sec);
      valfmd.set_ctime_ns(tv.tv_usec * 1000);
      valfmd.set_mtime_ns(tv.tv_usec * 1000);
      valfmd.set_atime_ns(tv.tv_usec * 1000);
      std::unique_ptr<FmdHelper> fmd {new FmdHelper(fid, fsid)};

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
// Delete a record associated with fid and filesystem fsid
//------------------------------------------------------------------------------
void
FmdDbMapHandler::LocalDeleteFmd(eos::common::FileId::fileid_t fid,
                                eos::common::FileSystem::fsid_t fsid)
{
  eos::common::RWMutexReadLock map_rd_lock(mMapMutex);
  FsWriteLock fs_wr_lock(fsid);
  auto it = mDbMap.find(fsid);

  if (it != mDbMap.end()) {
    (void) mDbMap[fsid]->remove(eos::common::Slice((const char*)&fid, sizeof(fid)));
  }
}

//------------------------------------------------------------------------------
// Commit modified Fmd record to the DB file
//------------------------------------------------------------------------------
bool
FmdDbMapHandler::Commit(FmdHelper* fmd, bool lockit)
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
    bool res = LocalPutFmd(fid, fsid, fmd->mProtoFmd);

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
                                    std::string disk_xs, unsigned long check_ts_sec,
                                    bool filexs_err, bool blockxs_err,
                                    bool layout_err)
{
  if (!fid) {
    eos_err("%s", "msg=\"skipping insert of file with fid=0\"");
    return false;
  }

  eos_debug("fsid=%lu fxid=%08llx disksize=%llu diskchecksum=%s checktime=%llu "
            "fcxerror=%d bcxerror=%d flaglayouterror=%d",
            fsid, fid, disk_size, disk_xs.c_str(), check_ts_sec,
            filexs_err, blockxs_err, layout_err);
  Fmd valfmd;
  eos::common::RWMutexReadLock map_rd_lock(mMapMutex);
  FsWriteLock fs_wr_lock(fsid);
  (void)LocalRetrieveFmd(fid, fsid, valfmd);
  valfmd.set_disksize(disk_size);
  valfmd.set_size(disk_size);
  valfmd.set_fid(fid);
  valfmd.set_fsid(fsid);
  valfmd.set_diskchecksum(disk_xs);
  valfmd.set_checktime(check_ts_sec);
  valfmd.set_filecxerror(filexs_err);
  valfmd.set_blockcxerror(blockxs_err);

  // Update the reference checksum only if empty
  if (valfmd.checksum().empty()) {
    valfmd.set_checksum(disk_xs);
  }

  if (layout_err) {
    // If the mgm sync is run afterwards, every disk file is by construction an
    // orphan, until it is synced from the mgm
    valfmd.set_layouterror(LayoutId::kOrphan);
  }

  return LocalPutFmd(fid, fsid, valfmd);
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
    eos_err("msg=\"skip inserting file with fid=0\"");
    return false;
  }

  eos_debug("fsid=%lu fxid=%08llx cid=%llu lid=%lx mgmsize=%llu mgmchecksum=%s",
            (unsigned long) fsid, fid, cid, lid, mgmsize, mgmchecksum.c_str());
  Fmd valfmd;
  eos::common::RWMutexReadLock map_rd_lock(mMapMutex);
  FsWriteLock fs_wr_lock(fsid);
  (void)LocalRetrieveFmd(fid, fsid, valfmd);
  valfmd.set_mgmsize(mgmsize);
  valfmd.set_size(mgmsize);
  //valfmd.set_checksum(mgmchecksum);
  valfmd.set_mgmchecksum(mgmchecksum);
  valfmd.set_cid(cid);
  valfmd.set_lid(lid);
  valfmd.set_uid(uid);
  valfmd.set_gid(gid);
  valfmd.set_ctime(ctime);
  valfmd.set_ctime_ns(ctime_ns);
  valfmd.set_mtime(mtime);
  valfmd.set_mtime_ns(mtime_ns);
  valfmd.set_layouterror(layouterror);
  valfmd.set_locations(locations);
  // Truncate the checksum to the right length
  size_t cslen = LayoutId::GetChecksumLen(lid) * 2;
  valfmd.set_mgmchecksum(std::string(valfmd.mgmchecksum()).erase
                         (std::min(valfmd.mgmchecksum().length(), cslen)));
  // valfmd.set_checksum(std::string(valfmd.checksum()).erase
  //                     (std::min(valfmd.checksum().length(), cslen)));
  return LocalPutFmd(fid, fsid, valfmd);
}

//------------------------------------------------------------------------------
// Update local fmd with info from the scanner
//------------------------------------------------------------------------------
void
FmdDbMapHandler::UpdateWithScanInfo(eos::common::FileSystem::fsid_t fsid,
                                    const std::string& fs_root,
                                    const std::string& fpath,
                                    bool filexs_err, bool blockxs_err)
{
  eos::common::Path cPath(fpath.c_str());
  eos::common::FileId::fileid_t fid {0ull};

  try {
    fid = std::stoull(cPath.GetName(), 0, 16);
  } catch (...) {
    eos_err("msg=\"failed to extract fid\" path=%s", fpath.c_str());
    return;
  }

  std::string manager = eos::fst::Config::gConfig.GetManager();

  if (manager.empty()) {
    eos_err("msg=\"no manager hostname info available\"");
    return;
  }

  // Check if we have this file in the local DB, if not, we resync first
  // the disk and then the MGM meta data
  bool orphaned = false;
  auto fmd = LocalGetFmd(fid, fsid, 0, 0, false, true);

  if (fmd) {
    // Real orphans and unregistered replicas get rechecked
    if ((fmd->mProtoFmd.layouterror() & eos::common::LayoutId::kOrphan) ||
        (fmd->mProtoFmd.layouterror() & eos::common::LayoutId::kUnregistered)) {
      orphaned = true;
    }
  }

  if ((fmd == nullptr) || filexs_err || blockxs_err || orphaned) {
    eos_notice("msg=\"resyncing from disk\" fsid=%d fid=%08llx", fsid, fid);
    ResyncDisk(fpath.c_str(), fsid, true);
    eos_notice("msg=\"resyncing from mgm\" fsid=%d fid=%08llx", fsid, fid);
    bool resynced = ResyncMgm(fsid, fid, manager.c_str());
    fmd = LocalGetFmd(fid, fsid, 0, 0, 0, false, true);

    if (resynced && fmd) {
      if ((fmd->mProtoFmd.layouterror() &  eos::common::LayoutId::kOrphan) ||
          ((fmd->mProtoFmd.layouterror() & eos::common::LayoutId::kUnregistered) &&
           (!(fmd->mProtoFmd.layouterror() & eos::common::LayoutId::kReplicaWrong)))) {
        char oname[4096];
        snprintf(oname, sizeof(oname), "%s/.eosorphans/%08llx",
                 fs_root.c_str(), (unsigned long long) fid);
        // Store the original path name as an extended attribute in case ...
        std::unique_ptr<FileIo> io(FileIoPluginHelper::GetIoObject(fpath));
        io->attrSet("user.eos.orphaned", fpath.c_str());

        // If orphan move it into the orphaned directory
        if (!rename(fpath.c_str(), oname)) {
          eos_warning("msg=\"orphaned/unregistered quarantined\" "
                      "fst-path=%s orphan-path=%s", fpath.c_str(), oname);
        } else {
          eos_err("msg=\"failed to quarantine orphaned/unregistered\" "
                  "fst-path=%s orphan-path=%s", fpath.c_str(), oname);
        }

        gFmdDbMapHandler.LocalDeleteFmd(fid, fsid);
        return;
      }

      // Call the autorepair method on the MGM - but not for orphaned or
      // unregistered files. If MGM autorepair is disabled then it doesn't do
      // anything
      if ((orphaned == false) &&
          ((fmd->mProtoFmd.layouterror() & eos::common::LayoutId::kUnregistered)
           == false)) {
        CallAutoRepair(manager.c_str(), fid);
      }
    }
  }
}

//------------------------------------------------------------------------------
// Reset disk information
//------------------------------------------------------------------------------
bool
FmdDbMapHandler::ResetDiskInformation(eos::common::FileSystem::fsid_t fsid)
{
  eos::common::RWMutexReadLock lock(mMapMutex);
  FsWriteLock wlock(fsid);

  if (mDbMap.count(fsid)) {
    const eos::common::DbMapTypes::Tkey* k;
    const eos::common::DbMapTypes::Tval* v;
    eos::common::DbMapTypes::Tval val;
    mDbMap[fsid]->beginSetSequence();
    unsigned long cpt = 0;

    for (mDbMap[fsid]->beginIter(false); mDbMap[fsid]->iterate(&k, &v, false);) {
      Fmd f;
      f.ParseFromString(v->value);
      f.set_disksize(Fmd::UNDEF);
      f.set_diskchecksum("");
      f.set_checktime(0);
      f.set_filecxerror(-1);
      f.set_blockcxerror(-1);
      val = *v;
      f.SerializeToString(&val.value);
      mDbMap[fsid]->set(*k, val);
      cpt++;
    }

    // The endSetSequence makes it impossible to know which key is faulty
    if (mDbMap[fsid]->endSetSequence() != cpt) {
      eos_err("unable to update fsid=%lu", fsid);
      return false;
    }
  } else {
    eos_crit("no %s DB open for fsid=%llu", eos::common::DbMap::getDbType().c_str(),
             (unsigned long) fsid);
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Reset mgm information
//------------------------------------------------------------------------------
bool
FmdDbMapHandler::ResetMgmInformation(eos::common::FileSystem::fsid_t fsid)
{
  eos::common::RWMutexReadLock lock(mMapMutex);
  FsWriteLock vlock(fsid);

  if (mDbMap.count(fsid)) {
    const eos::common::DbMapTypes::Tkey* k;
    const eos::common::DbMapTypes::Tval* v;
    eos::common::DbMapTypes::Tval val;
    mDbMap[fsid]->beginSetSequence();
    unsigned long cpt = 0;

    for (mDbMap[fsid]->beginIter(false); mDbMap[fsid]->iterate(&k, &v, false);) {
      Fmd f;
      f.ParseFromString(v->value);
      f.set_mgmsize(Fmd::UNDEF);
      f.set_mgmchecksum("");
      f.set_locations("");
      val = *v;
      f.SerializeToString(&val.value);
      mDbMap[fsid]->set(*k, val);
      cpt++;
    }

    // The endSetSequence makes it impossible to know which key is faulty
    if (mDbMap[fsid]->endSetSequence() != cpt) {
      eos_err("unable to update fsid=%lu", fsid);
      return false;
    }
  } else {
    eos_crit("no leveldb DB open for fsid=%llu", (unsigned long) fsid);
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Resync a single entry from disk
//------------------------------------------------------------------------------
bool
FmdDbMapHandler::ResyncDisk(const char* path,
                            eos::common::FileSystem::fsid_t fsid,
                            bool flaglayouterror)
{
  off_t disk_size = 0;
  eos::common::Path cPath(path);
  eos::common::FileId::fileid_t fid =
    eos::common::FileId::Hex2Fid(cPath.GetName());

  if (fid == 0) {
    eos_err("%s", "msg=\"unable to sync fid=0\"");
    return false;
  }

  std::unique_ptr<eos::fst::FileIo>
  io(eos::fst::FileIoPluginHelper::GetIoObject(path));

  if (io == nullptr) {
    eos_err("msg=\"failed to get IO object\" paht=%s", path);
    return false;
  }

  struct stat buf;

  if ((!io->fileStat(&buf)) && S_ISREG(buf.st_mode)) {
    std::string sxs_type, scheck_stamp, filexs_err, blockxs_err;
    char xs_val[SHA_DIGEST_LENGTH];
    size_t xs_len = 0;
    disk_size = buf.st_size;
    memset(xs_val, 0, sizeof(xs_val));
    xs_len = SHA_DIGEST_LENGTH;

    // @todo(esindril) update the diskchecksum from the localdb with the
    // scan checksum if available

    if (io->attrGet("user.eos.checksum", xs_val, xs_len)) {
      xs_len = 0;
    }

    io->attrGet("user.eos.checksumtype", sxs_type);
    io->attrGet("user.eos.filecxerror", filexs_err);
    io->attrGet("user.eos.blockcxerror", blockxs_err);
    io->attrGet("user.eos.timestamp", scheck_stamp);

    // Handle the old format in microseconds, truncate to seconds
    if (scheck_stamp.length() > 10) {
      scheck_stamp.erase(10);
    }

    unsigned long check_ts_sec = std::stoul(scheck_stamp);
    std::string disk_xs {""};

    if (xs_len) {
      int xs_type = LayoutId::GetChecksumFromString(sxs_type);
      LayoutId::layoutid_t layoutid = LayoutId::GetId(LayoutId::kPlain, xs_type);
      std::unique_ptr<CheckSum> checksum =
        eos::fst::ChecksumPlugins::GetChecksumObjectPtr(layoutid, false);

      if (checksum) {
        if (checksum->SetBinChecksum(xs_val, xs_len)) {
          disk_xs = checksum->GetHexChecksum();
        }
      }
    }

    // Update the DB
    if (!UpdateWithDiskInfo(fsid, fid, disk_size, disk_xs, check_ts_sec,
                            (filexs_err == "1"), (blockxs_err == "1"),
                            flaglayouterror)) {
      eos_err("msg=\"failed to update DB\" dbpath=%s fsid=%lu fxid=%08llx",
              eos::common::DbMap::getDbType().c_str(), (unsigned long) fsid, fid);
      return false;
    }
  } else {
    eos_err("msg=\"failed stat or entry is not a file\" path=%s", path);
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Resync files under path into DB
//------------------------------------------------------------------------------
bool
FmdDbMapHandler::ResyncAllDisk(const char* path,
                               eos::common::FileSystem::fsid_t fsid,
                               bool flaglayouterror)
{
  char** paths = (char**) calloc(2, sizeof(char*));

  if (!paths) {
    eos_err("error: failed to allocate memory");
    return false;
  }

  paths[0] = (char*) path;
  paths[1] = 0;

  if (flaglayouterror) {
    mIsSyncing[fsid] = true;
  }

  if (!ResetDiskInformation(fsid)) {
    eos_err("failed to reset the disk information before resyncing fsid=%lu",
            fsid);
    free(paths);
    return false;
  }

  // Scan all the files
  FTS* tree = fts_open(paths, FTS_NOCHDIR, 0);

  if (!tree) {
    eos_err("fts_open failed");
    free(paths);
    return false;
  }

  FTSENT* node;
  unsigned long long cnt = 0;

  while ((node = fts_read(tree))) {
    if (node->fts_level > 0 && node->fts_name[0] == '.') {
      fts_set(tree, node, FTS_SKIP);
    } else {
      if (node->fts_info == FTS_F) {
        XrdOucString filePath = node->fts_accpath;

        if (!filePath.matches("*.xsmap")) {
          cnt++;
          eos_debug("file=%s", filePath.c_str());
          ResyncDisk(filePath.c_str(), fsid, flaglayouterror);

          if (!(cnt % 10000)) {
            eos_info("msg=\"synced files so far\" nfiles=%llu fsid=%lu", cnt,
                     (unsigned long) fsid);
          }
        }
      }
    }
  }

  if (fts_close(tree)) {
    eos_err("fts_close failed");
    free(paths);
    return false;
  }

  free(paths);
  return true;
}

//------------------------------------------------------------------------------
// Resync file meta data from MGM into local database
//------------------------------------------------------------------------------
bool
FmdDbMapHandler::ResyncMgm(eos::common::FileSystem::fsid_t fsid,
                           eos::common::FileId::fileid_t fid,
                           const char* manager)
{
  Fmd fMd;
  FmdHelper::Reset(fMd);
  int rc = GetMgmFmd(manager, fid, fMd);

  if ((rc == 0) || (rc == ENODATA)) {
    if (rc == ENODATA) {
      eos_warning("no such file on MGM for fxid=%08llx", fid);
      fMd.set_fid(fid);

      if (fid == 0) {
        eos_warning("msg=\"removing fxid=0 entry\"");
        LocalDeleteFmd(fMd.fid(), fsid);
        return true;
      }
    }

    // Define layouterrors
    fMd.set_layouterror(FmdHelper::LayoutError(fMd, fsid));
    // Get an existing record without creating the record !!!
    std::unique_ptr<FmdHelper> fmd {
      LocalGetFmd(fMd.fid(), fsid, fMd.uid(), fMd.gid(), fMd.lid(), false, true)};

    if (fmd) {
      // Check if exists on disk
      if (fmd->mProtoFmd.disksize() == Fmd::UNDEF) {
        if (fMd.layouterror() & LayoutId::kUnregistered) {
          // There is no replica supposed to be here and there is nothing on
          // disk, so remove it from the database
          eos_warning("msg=\"removing ghost fmd from db\" fsid=%lu fxid=%08llx",
                      fsid, fid);
          LocalDeleteFmd(fMd.fid(), fsid);
          return true;
        }
      }
    } else {
      // No file locally and also not registered with the MGM
      if ((fMd.layouterror() & LayoutId::kUnregistered) ||
          (fMd.layouterror() & LayoutId::kOrphan)) {
        return true;
      }
    }

    // Get/create a record
    fmd = LocalGetFmd(fMd.fid(), fsid, fMd.uid(), fMd.gid(), fMd.lid(), true, true);

    if (fmd) {
      if (!UpdateWithMgmInfo(fsid, fMd.fid(), fMd.cid(), fMd.lid(), fMd.mgmsize(),
                             fMd.mgmchecksum(), fMd.uid(), fMd.gid(), fMd.ctime(),
                             fMd.ctime_ns(), fMd.mtime(), fMd.mtime_ns(),
                             fMd.layouterror(), fMd.locations())) {
        eos_err("msg=\"failed to update fmd with mgm info\" fxid=%08llx", fid);
        return false;
      }

      // Check if it exists on disk
      if (fmd->mProtoFmd.disksize() == Fmd::UNDEF) {
        fMd.set_layouterror(fMd.layouterror() | LayoutId::kMissing);
        eos_warning("msg=\"found missing replica\" fxid=%08llx on fsid=%lu",
                    fid, fsid);
      }

      // Check if it exists on disk and at the mgm
      if ((fmd->mProtoFmd.disksize() == Fmd::UNDEF) &&
          (fmd->mProtoFmd.mgmsize() == Fmd::UNDEF)) {
        // There is no replica supposed to be here and there is nothing on
        // disk, so remove it from the database
        eos_warning("removing <ghost> entry for fxid=%08llx on fsid=%lu", fid,
                    (unsigned long) fsid);
        LocalDeleteFmd(fMd.fid(), fsid);
        return true;
      }
    } else {
      eos_err("failed to create fmd for fxid=%08llx", fid);
      return false;
    }
  } else {
    eos_err("failed to retrieve MGM fmd for fxid=%08llx", fid);
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Resync all meta data from MGM into local database
//------------------------------------------------------------------------------
bool
FmdDbMapHandler::ResyncAllMgm(eos::common::FileSystem::fsid_t fsid,
                              const char* manager)
{
  if (!ResetMgmInformation(fsid)) {
    eos_err("failed to reset the mgm information before resyncing");
    return false;
  }

  std::string tmpfile;

  if (!ExecuteDumpmd(manager, fsid, tmpfile)) {
    return false;
  }

  // Parse the result and unlink temporary file
  std::ifstream inFile(tmpfile);
  std::string dumpentry;
  unlink(tmpfile.c_str());
  unsigned long long cnt = 0;

  while (std::getline(inFile, dumpentry)) {
    cnt++;
    eos_debug("line=%s", dumpentry.c_str());
    std::unique_ptr<XrdOucEnv> env(new XrdOucEnv(dumpentry.c_str()));

    if (env) {
      Fmd fMd;
      FmdHelper::Reset(fMd);

      if (EnvMgmToFmd(*env, fMd)) {
        // get/create one
        auto fmd = LocalGetFmd(fMd.fid(), fsid, fMd.uid(), fMd.gid(),
                               fMd.lid(), true, true);
        fMd.set_layouterror(FmdHelper::LayoutError(fMd, fsid));

        if (fmd) {
          // Check if it exists on disk
          if (fmd->mProtoFmd.disksize() == Fmd::UNDEF) {
            fMd.set_layouterror(fMd.layouterror() | LayoutId::kMissing);
            eos_warning("found missing replica for fxid=%08llx on fsid=%lu", fMd.fid(),
                        (unsigned long) fsid);
          }

          if (!UpdateWithMgmInfo(fsid, fMd.fid(), fMd.cid(), fMd.lid(),
                                 fMd.mgmsize(), fMd.mgmchecksum(), fMd.uid(),
                                 fMd.gid(), fMd.ctime(), fMd.ctime_ns(),
                                 fMd.mtime(), fMd.mtime_ns(),
                                 fMd.layouterror(), fMd.locations())) {
            eos_err("msg=\"failed to update fmd\" entry=\"%s\"",
                    dumpentry.c_str());
          }
        } else {
          eos_err("msg=\"failed to get/create fmd\" enrty=\"%s\"",
                  dumpentry.c_str());
        }
      } else {
        eos_err("msg=\"failed to convert\" entry=\"%s\"", dumpentry.c_str());
      }
    }

    if (!(cnt % 10000)) {
      eos_info("msg=\"synced files so far\" nfiles=%llu fsid=%lu", cnt,
               (unsigned long) fsid);
    }
  }

  mIsSyncing[fsid] = false;
  return true;
}

//------------------------------------------------------------------------------
// Resync all meta data from QuarkdDB
//------------------------------------------------------------------------------
bool
FmdDbMapHandler::ResyncAllFromQdb(const QdbContactDetails& contactDetails,
                                  eos::common::FileSystem::fsid_t fsid)
{
  using namespace std::chrono;

  if (!ResetMgmInformation(fsid)) {
    eos_err("failed to reset the mgm information before resyncing");
    return false;
  }

  // Collect all file ids on the desired file system
  std::string cursor = "0";
  long long count = 250000;
  std::pair<std::string, std::vector<std::string>> reply;
  std::unique_ptr<qclient::QClient>
  qcl(new qclient::QClient(contactDetails.members,
                           contactDetails.constructOptions()));
  qclient::QSet qset(*qcl.get(),  eos::RequestBuilder::keyFilesystemFiles(fsid));
  std::unordered_set<eos::IFileMD::id_t> file_ids;

  try {
    do {
      reply = qset.sscan(cursor, count);
      cursor = reply.first;

      for (const auto& elem : reply.second) {
        file_ids.insert(std::stoull(elem));
      }
    } while (cursor != "0");
  } catch (const std::runtime_error& e) {
    // It means there are no records for the current file system
  }

  auto start = steady_clock::now();
  uint64_t total = file_ids.size();
  eos_info("resyncing %llu files for file_system %u", total, fsid);
  uint64_t num_files = 0;
  auto it = file_ids.begin();
  std::list<folly::Future<eos::ns::FileMdProto>> files;

  // Pre-fetch the first 1000 files
  while ((it != file_ids.end()) && (num_files < 1000)) {
    ++num_files;
    files.emplace_back(MetadataFetcher::getFileFromId(*qcl.get(),
                       FileIdentifier(*it)));
    ++it;
  }

  while (!files.empty()) {
    Fmd ns_fmd;
    FmdHelper::Reset(ns_fmd);

    try {
      NsFileProtoToFmd(files.front().get(), ns_fmd);
    } catch (const eos::MDException& e) {
      eos_err("msg=\"failed to get metadata from QuarkDB: %s\"", e.what());
      files.pop_front();
      continue;
    }

    files.pop_front();
    auto local_fmd = LocalGetFmd(ns_fmd.fid(), fsid, ns_fmd.uid(),
                                 ns_fmd.gid(), ns_fmd.lid(), true, true);
    ns_fmd.set_layouterror(FmdHelper::LayoutError(ns_fmd, fsid));

    if (local_fmd) {
      // Check if it exists on disk
      if (local_fmd->mProtoFmd.disksize() == Fmd::UNDEF) {
        ns_fmd.set_layouterror(ns_fmd.layouterror() | LayoutId::kMissing);
        eos_warning("found missing replica for fxid=%08llx on fsid=%lu",
                    ns_fmd.fid(), (unsigned long) fsid);
      }

      if (!UpdateWithMgmInfo(fsid, ns_fmd.fid(), ns_fmd.cid(), ns_fmd.lid(),
                             ns_fmd.mgmsize(), ns_fmd.mgmchecksum(), ns_fmd.uid(),
                             ns_fmd.gid(), ns_fmd.ctime(), ns_fmd.ctime_ns(),
                             ns_fmd.mtime(), ns_fmd.mtime_ns(),
                             ns_fmd.layouterror(), ns_fmd.locations())) {
        eos_err("failed to update fid %llu", ns_fmd.fid());
      }
    } else {
      eos_err("failed to get/create local fid %llu", ns_fmd.fid());
    }

    if (it != file_ids.end()) {
      ++num_files;
      files.emplace_back(MetadataFetcher::getFileFromId(*qcl.get(),
                         FileIdentifier(*it)));
      ++it;
    }

    if (num_files % 10000 == 0) {
      double rate = 0;
      auto duration = steady_clock::now() - start;
      auto ms = duration_cast<milliseconds>(duration);

      if (ms.count()) {
        rate = (num_files * 1000.0) / (double)ms.count();
      }

      eos_info("fsid=%u resynced %llu/%llu files at a rate of %.2f Hz",
               fsid, num_files, total, rate);
    }
  }

  double rate = 0;
  auto duration = steady_clock::now() - start;
  auto ms = duration_cast<milliseconds>(duration);

  if (ms.count()) {
    rate = (num_files * 1000.0) / (double)ms.count();
  }

  eos_info("fsid=%u resynced %llu/%llu files at a rate of %.2f Hz",
           fsid, num_files, total, rate);
  return true;
}

//------------------------------------------------------------------------------
// Remove ghost entries - entries which are neither on disk nor at the MGM
//------------------------------------------------------------------------------
bool
FmdDbMapHandler::RemoveGhostEntries(const char* fs_root,
                                    eos::common::FileSystem::fsid_t fsid)
{
  eos_static_info("fsid=%lu", fsid);
  std::vector<eos::common::FileId::fileid_t> to_delete;

  if (!IsSyncing(fsid)) {
    {
      eos::common::RWMutexReadLock rd_lock(mMapMutex);
      FsReadLock fs_rd_lock(fsid);

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
        Fmd f;
        eos::common::FileId::fileid_t fid {0ul};
        f.ParseFromString(v->value);
        (void)memcpy(&fid, (void*)k->data(), k->size());

        if (f.layouterror()) {
          struct stat buf;
          XrdOucString fpath;
          const std::string hex_fid = eos::common::FileId::Fid2Hex(fid);
          eos::common::FileId::FidPrefix2FullPath(hex_fid.c_str(), fs_root, fpath);
          errno = 0;

          if (stat(fpath.c_str(), &buf)) {
            if ((errno == ENOENT) || (errno == ENOTDIR)) {
              if ((f.layouterror() & LayoutId::kOrphan) ||
                  (f.layouterror() & LayoutId::kUnregistered)) {
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
FmdDbMapHandler::GetInconsistencyStatistics(eos::common::FileSystem::fsid_t
    fsid,
    std::map<std::string, size_t>& statistics,
    std::map<std::string, std::set < eos::common::FileId::fileid_t> >& fidset)
{
  eos::common::RWMutexReadLock map_rd_lock(mMapMutex);

  if (!mDbMap.count(fsid)) {
    return false;
  }

  // query in-memory
  statistics["mem_n"] = 0; // number of files in DB
  statistics["d_sync_n"] = 0; // number of synced files from disk
  statistics["m_sync_n"] = 0; // number of synced files from MGM server
  // number of files with disk and reference size mismatch
  statistics["d_mem_sz_diff"] = 0;
  // number of files with MGM and reference size mismatch
  statistics["m_mem_sz_diff"] = 0;
  // number of files with disk and reference checksum mismatch
  statistics["d_cx_diff"] = 0;
  // number of files with MGM and reference checksum mismatch
  statistics["m_cx_diff"] = 0;
  statistics["orphans_n"] = 0; // number of orphaned replicas
  statistics["unreg_n"] = 0; // number of unregistered replicas
  statistics["rep_diff_n"] = 0; // number of files with replica number mismatch
  statistics["rep_missing_n"] = 0; // number of files which are missing on disk
  fidset["m_mem_sz_diff"].clear();
  fidset["d_mem_sz_diff"].clear();
  fidset["m_cx_diff"].clear();
  fidset["d_cx_diff"].clear();
  fidset["orphans_n"].clear();
  fidset["unreg_n"].clear();
  fidset["rep_diff_n"].clear();
  fidset["rep_missing_n"].clear();

  if (!IsSyncing(fsid)) {
    const eos::common::DbMapTypes::Tkey* k;
    const eos::common::DbMapTypes::Tval* v;
    eos::common::DbMapTypes::Tval val;
    FsReadLock fs_rd_lock(fsid);

    // We report values only when we are not in the sync phase from disk/mgm
    for (mDbMap[fsid]->beginIter(false); mDbMap[fsid]->iterate(&k, &v, false);) {
      Fmd f;
      f.ParseFromString(v->value);
      statistics["mem_n"]++;

      if (f.layouterror()) {
        if (f.layouterror() & LayoutId::kOrphan) {
          statistics["orphans_n"]++;
          fidset["orphans_n"].insert(f.fid());
        }

        if (f.layouterror() & LayoutId::kUnregistered) {
          statistics["unreg_n"]++;
          fidset["unreg_n"].insert(f.fid());
        }

        if (f.layouterror() & LayoutId::kReplicaWrong) {
          statistics["rep_diff_n"]++;
          fidset["rep_diff_n"].insert(f.fid());
        }

        if (f.layouterror() & LayoutId::kMissing) {
          statistics["rep_missing_n"]++;
          fidset["rep_missing_n"].insert(f.fid());
        }
      }

      if (f.mgmsize() != Fmd::UNDEF) {
        statistics["m_sync_n"]++;

        if (f.size() != Fmd::UNDEF) {
          if (f.size() != f.mgmsize()) {
            statistics["m_mem_sz_diff"]++;
            fidset["m_mem_sz_diff"].insert(f.fid());
          }
        }
      }

      if (!f.layouterror()) {
        if (f.size() && f.diskchecksum().length() &&
            (f.diskchecksum() != f.checksum())) {
          statistics["d_cx_diff"]++;
          fidset["d_cx_diff"].insert(f.fid());
        }

        if (f.size() && f.mgmchecksum().length() &&
            (f.mgmchecksum() != f.checksum())) {
          statistics["m_cx_diff"]++;
          fidset["m_cx_diff"].insert(f.fid());
        }
      }

      if (f.disksize() != Fmd::UNDEF) {
        statistics["d_sync_n"]++;

        if (f.size() != Fmd::UNDEF) {
          // Report missmatch only for replica layout files
          if ((f.size() != f.disksize()) &&
              (eos::common::LayoutId::GetLayoutType(f.lid())
               == eos::common::LayoutId::kReplica)) {
            statistics["d_mem_sz_diff"]++;
            fidset["d_mem_sz_diff"].insert(f.fid());
          }
        }
      }
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
    FsWriteLock fs_wr_lock(fsid);

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
  FsReadLock fs_rd_lock(fsid);

  if (mDbMap.count(fsid)) {
    return mDbMap[fsid]->size();
  } else {
    return 0ll;
  }
}

//------------------------------------------------------------------------------
// Execute "fs dumpmd" on the MGM node
//------------------------------------------------------------------------------
bool
FmdDbMapHandler::ExecuteDumpmd(const std::string& mgm_host,
                               eos::common::FileSystem::fsid_t fsid,
                               std::string& fn_output)
{
  // Create temporary file used as output for the command
  char tmpfile[] = "/tmp/efstd.XXXXXX";
  int tmp_fd = mkstemp(tmpfile);

  if (tmp_fd == -1) {
    eos_static_err("failed to create a temporary file");
    return false;
  }

  (void) close(tmp_fd);
  fn_output = tmpfile;
  std::ostringstream cmd;
  // First try to do the dumpmd using protobuf requests
  using eos::console::FsProto_DumpMdProto;
  eos::console::RequestProto request;
  eos::console::FsProto* fs = request.mutable_fs();
  FsProto_DumpMdProto* dumpmd = fs->mutable_dumpmd();
  dumpmd->set_fsid(fsid);
  dumpmd->set_display(eos::console::FsProto::DumpMdProto::MONITOR);
  request.set_format(eos::console::RequestProto::FUSE);
  std::string b64buff;

  if (eos::common::SymKey::ProtobufBase64Encode(&request, b64buff)) {
    // Increase the request timeout to 4 hours
    cmd << "env XrdSecPROTOCOL=sss XRD_REQUESTTIMEOUT=14400 "
        << "xrdcp -f -s \"root://" << mgm_host.c_str() << "/"
        << "/proc/admin/?mgm.cmd.proto=" << b64buff << "\" "
        << tmpfile;
    eos::common::ShellCmd bootcmd(cmd.str().c_str());
    eos::common::cmd_status rc = bootcmd.wait(1800);

    if (rc.exit_code) {
      eos_static_err("%s returned %d", cmd.str().c_str(), rc.exit_code);
    } else {
      eos_static_debug("%s executed successfully", cmd.str().c_str());
      return true;
    }
  } else {
    eos_static_err("msg=\"failed to serialize protobuf request for dumpmd\"");
  }

  eos_static_info("msg=\"falling back to classic dumpmd command\"");
  cmd.str("");
  cmd.clear();
  cmd << "env XrdSecPROTOCOL=sss XRD_STREAMTIMEOUT=600 xrdcp -f -s \""
      << "root://" << mgm_host.c_str() << "/"
      << "/proc/admin/?&mgm.format=fuse&mgm.cmd=fs&mgm.subcmd=dumpmd&"
      << "mgm.dumpmd.option=m&mgm.fsid=" << fsid << "\" "
      << tmpfile;
  eos::common::ShellCmd bootcmd(cmd.str().c_str());
  eos::common::cmd_status rc = bootcmd.wait(1800);

  if (rc.exit_code) {
    eos_static_err("%s returned %d", cmd.str().c_str(), rc.exit_code);
    return false;
  } else {
    eos_static_debug("%s executed successfully", cmd.str().c_str());
    return true;
  }
}

EOSFSTNAMESPACE_END
