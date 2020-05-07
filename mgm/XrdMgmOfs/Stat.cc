// ----------------------------------------------------------------------
// File: Stat.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

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


// -----------------------------------------------------------------------
// This file is included source code in XrdMgmOfs.cc to make the code more
// transparent without slowing down the compilation time.
// -----------------------------------------------------------------------

/*----------------------------------------------------------------------------*/

int
XrdMgmOfs::stat(const char* inpath,
                struct stat* buf,
                XrdOucErrInfo& error,
                const XrdSecEntity* client,
                const char* ininfo
               )
{
  return stat(inpath, buf, error, 0, client, ininfo, false);
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::stat(const char* inpath,
                struct stat* buf,
                XrdOucErrInfo& error,
                std::string* etag,
                const XrdSecEntity* client,
                const char* ininfo,
                bool follow,
                std::string* uri)
/*----------------------------------------------------------------------------*/
/*
 * @brief return stat information for a given path
 *
 * @param inpath path to stat
 * @param buf stat buffer where to store the stat information
 * @param error error object
 * @param client XRootD authentication object
 * @param ininfo CGI
 * @param etag string to return the ETag for that object
 * @param follow to indicate to follow symbolic links on leave nodes
 * @return SFS_OK on success otherwise SFS_ERROR
 *
 * See the internal implemtation _stat for details.
 */
/*----------------------------------------------------------------------------*/

{
  static const char* epname = "stat";
  const char* tident = error.getErrUser();
  // use a thread private vid
  eos::common::VirtualIdentity vid = eos::common::VirtualIdentity::Nobody();
  NAMESPACEMAP;
  BOUNCE_ILLEGAL_NAMES;
  XrdOucEnv Open_Env(ininfo);
  AUTHORIZE(client, &Open_Env, AOP_Stat, "stat", inpath, error);
  EXEC_TIMING_BEGIN("IdMap");
  eos::common::Mapping::IdMap(client, ininfo, tident, vid, false);
  EXEC_TIMING_END("IdMap");
  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);
  BOUNCE_NOT_ALLOWED;
  ACCESSMODE_R;
  MAYSTALL;
  eos::common::Path cPath(path);

  // Never redirect stat's for the master mode
  if (cPath.GetFullPath() != gOFS->MgmProcMasterPath) {
    MAYREDIRECT;
  }

  errno = 0;
  int rc = _stat(path, buf, error, vid, ininfo, etag, follow, uri);

  if (rc) {
    if (errno == ENOENT) {
      MAYREDIRECT_ENOENT;
      MAYSTALL_ENOENT;
    }
  } else {
    _stat_set_flags(buf);
  }

  return rc;
}

/*----------------------------------------------------------------------------*/
void
XrdMgmOfs::_stat_set_flags(struct stat* buf)
/*----------------------------------------------------------------------------*/
/*
 * @brief set XRDSFS_OFFLINE and XRDSFS_HASBKUP flags
 *
 * @param[in,out] buf    Stat structure
 *
 * XRDSFS_HASBKUP is set iff there is a tape copy for the file
 * XRDSFS_OFFLINE is set iff there is no disk copy for the file
 *                (i.e. only a tape copy exists)
 */
/*----------------------------------------------------------------------------*/
{
  // If EOS_TAPE_MODE_T is set, there is a copy on tape
  if(buf->st_mode & EOS_TAPE_MODE_T) {
    buf->st_rdev |= XRDSFS_HASBKUP;
  } else {
    buf->st_rdev &= ~XRDSFS_HASBKUP;
  }

  // Number of disk copies = total number of copies - 1 if there is a tape copy
  auto numDiskCopies = buf->st_nlink - (buf->st_mode & EOS_TAPE_MODE_T ? 1 : 0);
  if (numDiskCopies > 0) {
    buf->st_rdev &= ~XRDSFS_OFFLINE;
  } else {
    buf->st_rdev |= XRDSFS_OFFLINE;
  }
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_stat(const char* path,
                 struct stat* buf,
                 XrdOucErrInfo& error,
                 eos::common::VirtualIdentity& vid,
                 const char* ininfo,
                 std::string* etag,
                 bool follow,
                 std::string* uri)
/*----------------------------------------------------------------------------*/
/*
 * @brief return stat information for a given path
 *
 * @param inpath path to stat
 * @param buf stat buffer where to store the stat information
 * @param error error object
 * @param vid virtual identity of the client
 * @param ininfo CGI
 * @param follow to indicate to follow symbolic links on leave nodes
 * @return SFS_OK on success otherwise SFS_ERROR
 *
 * We don't apply any access control on stat calls for performance reasons.
 * Modification times of directories are only emulated and returned from an
 * in-memory map.
 */
/*----------------------------------------------------------------------------*/
{
  static const char* epname = "_stat";
  EXEC_TIMING_BEGIN("Stat");
  gOFS->MgmStats.Add("Stat", vid.uid, vid.gid, 1);
  // ---------------------------------------------------------------------------
  // try if that is a file
  errno = 0;
  std::shared_ptr<eos::IFileMD> fmd;
  eos::common::Path cPath(path);

  // Stat on the master proc entry succeeds only if this MGM is in RW master mode
  if (cPath.GetFullPath() == gOFS->MgmProcMasterPath) {
    if (!gOFS->mMaster->IsMaster()) {
      return Emsg(epname, error, ENODEV, "stat", cPath.GetPath());
    }
  }

  // public access level restriction
  if (!gOFS->allow_public_access(path,vid)) {
    eos_static_err("vid.uid=%d\n", vid.uid);
    errno = EACCES;
    return Emsg(epname, error, EACCES, "access - public access level restriction", path);
  }

  // Prefetch path
  eos::Prefetcher::prefetchItemAndWait(gOFS->eosView, cPath.GetPath(), follow);
  eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);

  try {
    fmd = gOFS->eosView->getFile(cPath.GetPath(), follow);

    // if a stat comes with file/ return an error
    if ( std::string(path).back() == '/' ) {
      errno = EISDIR;
      return Emsg(epname, error, errno, "stat", cPath.GetPath());
    }

    if (uri) {
      *uri = gOFS->eosView->getUri(fmd.get());
    }
  } catch (eos::MDException& e) {
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"", e.getErrno(),
              e.getMessage().str().c_str());

    if (errno == ELOOP) {
      return Emsg(epname, error, errno, "stat", cPath.GetPath());
    }
  }

  if (fmd) {
    memset(buf, 0, sizeof(struct stat));
    buf->st_dev = 0xcaff;
    buf->st_ino = eos::common::FileId::FidToInode(fmd->getId());

    if (fmd->isLink()) {
      buf->st_nlink = 1;
    } else {
      buf->st_nlink = eos::common::LayoutId::GetRedundancy(fmd->getLayoutId(), fmd->getNumLocation());
    }

    buf->st_size = fmd->getSize();
    buf->st_mode = eos::modeFromMetadataEntry(fmd);
    buf->st_uid = fmd->getCUid();
    buf->st_gid = fmd->getCGid();
    buf->st_rdev = 0; /* device type (if inode device) */
    buf->st_blksize = 512;
    buf->st_blocks = (Quota::MapSizeCB(fmd.get())+512) / 512; // including layout factor
    eos::IFileMD::ctime_t atime;
    // adding also nanosecond to stat struct
    fmd->getCTime(atime);
#ifdef __APPLE__
    buf->st_ctimespec.tv_sec = atime.tv_sec;
    buf->st_ctimespec.tv_nsec = atime.tv_nsec;
#else
    buf->st_ctime = atime.tv_sec;
    buf->st_ctim.tv_sec = atime.tv_sec;
    buf->st_ctim.tv_nsec = atime.tv_nsec;
#endif
    fmd->getMTime(atime);
#ifdef __APPLE__
    buf->st_mtimespec.tv_sec = atime.tv_sec;
    buf->st_mtimespec.tv_nsec = atime.tv_nsec;
    buf->st_atimespec.tv_sec = atime.tv_sec;
    buf->st_atimespec.tv_nsec = atime.tv_nsec;
#else
    buf->st_mtime = atime.tv_sec;
    buf->st_mtim.tv_sec = atime.tv_sec;
    buf->st_mtim.tv_nsec = atime.tv_nsec;
    buf->st_atime = atime.tv_sec;
    buf->st_atim.tv_sec = atime.tv_sec;
    buf->st_atim.tv_nsec = atime.tv_nsec;
#endif

    if (etag) {
      eos::calculateEtag(fmd.get(), *etag);
      if (fmd->hasAttribute("sys.eos.mdino")) {
	*etag = "hardlink";
      }
    }

    EXEC_TIMING_END("Stat");
    return SFS_OK;
  }

  // Check if it's a directory
  std::shared_ptr<eos::IContainerMD> cmd;
  errno = 0;

  // ---------------------------------------------------------------------------
  try {
    cmd = gOFS->eosView->getContainer(cPath.GetPath(), follow);

    if (uri) {
      *uri = gOFS->eosView->getUri(cmd.get());
    }

    memset(buf, 0, sizeof(struct stat));
    buf->st_dev = 0xcaff;
    buf->st_ino = cmd->getId();
    buf->st_mode = eos::modeFromMetadataEntry(cmd);
    buf->st_nlink = 1;
    buf->st_uid = cmd->getCUid();
    buf->st_gid = cmd->getCGid();
    buf->st_rdev = 0; /* device type (if inode device) */
    buf->st_size = cmd->getTreeSize();
    buf->st_blksize = cmd->getNumContainers() + cmd->getNumFiles();
    buf->st_blocks = 0;
    eos::IContainerMD::ctime_t ctime;
    eos::IContainerMD::ctime_t mtime;
    eos::IContainerMD::ctime_t tmtime;
    cmd->getCTime(ctime);
    cmd->getMTime(mtime);

    if (gOFS->eosSyncTimeAccounting) {
      cmd->getTMTime(tmtime);
    } else
      // if there is no sync time accounting we just use the normal modification time
    {
      tmtime = mtime;
    }

#ifdef __APPLE__
    buf->st_atimespec.tv_sec = tmtime.tv_sec;
    buf->st_mtimespec.tv_sec = mtime.tv_sec;
    buf->st_ctimespec.tv_sec = ctime.tv_sec;
    buf->st_atimespec.tv_nsec = tmtime.tv_nsec;
    buf->st_mtimespec.tv_nsec = mtime.tv_nsec;
    buf->st_ctimespec.tv_nsec = ctime.tv_nsec;
#else
    buf->st_atime = tmtime.tv_sec;
    buf->st_mtime = mtime.tv_sec;
    buf->st_ctime = ctime.tv_sec;
    buf->st_atim.tv_sec = tmtime.tv_sec;
    buf->st_mtim.tv_sec = mtime.tv_sec;
    buf->st_ctim.tv_sec = ctime.tv_sec;
    buf->st_atim.tv_nsec = tmtime.tv_nsec;
    buf->st_mtim.tv_nsec = mtime.tv_nsec;
    buf->st_ctim.tv_nsec = ctime.tv_nsec;
#endif

    if (etag) {
      eos::calculateEtag(cmd.get(), *etag);
    }

    return SFS_OK;
  } catch (eos::MDException& e) {
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"", e.getErrno(),
              e.getMessage().str().c_str());
    return Emsg(epname, error, errno, "stat", cPath.GetPath());
  }
}

// ---------------------------------------------------------------------------
//  get the checksum info of a file
// ---------------------------------------------------------------------------
int
XrdMgmOfs::_getchecksum(const char* Name,
                        XrdOucErrInfo& error,
                        std::string* xstype,
                        std::string* xs,
                        const XrdSecEntity* client,
                        const char* opaque,
                        bool follow)
{
  // ---------------------------------------------------------------------------
  errno = 0;
  std::shared_ptr<eos::IFileMD> fmd;
  eos::common::Path cPath(Name);
  eos::Prefetcher::prefetchFileMDAndWait(gOFS->eosView, cPath.GetPath(), follow);
  eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);

  try {
    fmd = gOFS->eosView->getFile(cPath.GetPath(), follow);
  } catch (eos::MDException& e) {
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"", e.getErrno(),
              e.getMessage().str().c_str());
    return errno;
  }

  if (fmd) {
    size_t cxlen = eos::common::LayoutId::GetChecksumLen(fmd->getLayoutId());

    if (cxlen) {
      *xstype = eos::common::LayoutId::GetChecksumStringReal(fmd->getLayoutId());
      eos::appendChecksumOnStringAsHex(fmd.get(), *xs);
    }
  }

  return 0;
}
//------------------------------------------------------------------------------
// Stat following links (not existing in EOS - behaves like stat)
//------------------------------------------------------------------------------
int
XrdMgmOfs::lstat(const char* path,
                 struct stat* buf,
                 XrdOucErrInfo& error,
                 const XrdSecEntity* client,
                 const char* info)

{
  return stat(path, buf, error, client, info);
}
