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
XrdMgmOfs::stat (const char *inpath,
                 struct stat *buf,
                 XrdOucErrInfo &error,
                 const XrdSecEntity *client,
                 const char *ininfo)
{

  return stat(inpath, buf, error, 0, client, ininfo);
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::stat (const char *inpath,
                 struct stat *buf,
                 XrdOucErrInfo &error,
                 std::string *etag,
                 const XrdSecEntity *client,
                 const char *ininfo)
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
 * @return SFS_OK on success otherwise SFS_ERROR
 *
 * See the internal implemtation _stat for details.
 */
/*----------------------------------------------------------------------------*/

{
  static const char *epname = "stat";
  const char *tident = error.getErrUser();


  // use a thread private vid
  eos::common::Mapping::VirtualIdentity vid;

  XrdSecEntity mappedclient;

  NAMESPACEMAP;
  BOUNCE_ILLEGAL_NAMES;

  XrdOucEnv Open_Env(info);

  AUTHORIZE(client, &Open_Env, AOP_Stat, "stat", inpath, error);

  EXEC_TIMING_BEGIN("IdMap");
  eos::common::Mapping::IdMap(client, info, tident, vid, false);
  EXEC_TIMING_END("IdMap");

  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);

  BOUNCE_NOT_ALLOWED;
  ACCESSMODE_R;
  MAYSTALL;
  MAYREDIRECT;

  errno = 0;
  int rc = _stat(path, buf, error, vid, info, etag);
  if (rc && (errno == ENOENT))
  {

    MAYREDIRECT_ENOENT;
    MAYSTALL_ENOENT;
  }
  return rc;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_stat (const char *path,
                  struct stat *buf,
                  XrdOucErrInfo &error,
                  eos::common::Mapping::VirtualIdentity &vid,
                  const char *ininfo,
                  std::string* etag)
/*----------------------------------------------------------------------------*/
/*
 * @brief return stat information for a given path
 *
 * @param inpath path to stat
 * @param buf stat buffer where to store the stat information
 * @param error error object
 * @param vid virtual identity of the client
 * @param ininfo CGI
 * @return SFS_OK on success otherwise SFS_ERROR
 *
 * We don't apply any access control on stat calls for performance reasons.
 * Modification times of directories are only emulated and returned from an
 * in-memory map.
 */
/*----------------------------------------------------------------------------*/
{
  static const char *epname = "_stat";

  EXEC_TIMING_BEGIN("Stat");

  gOFS->MgmStats.Add("Stat", vid.uid, vid.gid, 1);

  // ---------------------------------------------------------------------------
  // try if that is a file
  errno = 0;
  eos::FileMD* fmd = 0;
  eos::common::Path cPath(path);

  // ---------------------------------------------------------------------------
  // a stat on the master proc entry succeeds
  // only, if this MGM is in RW master mode
  // ---------------------------------------------------------------------------

  if (cPath.GetFullPath() == gOFS->MgmProcMasterPath)
  {
    if (!gOFS->MgmMaster.IsMaster())
    {
      return Emsg(epname, error, ENODEV, "stat", cPath.GetPath());
    }
  }
  // ---------------------------------------------------------------------------
  eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);

  try
  {
    fmd = gOFS->eosView->getFile(cPath.GetPath());
  }
  catch (eos::MDException &e)
  {
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
              e.getErrno(), e.getMessage().str().c_str());
  }

  // ---------------------------------------------------------------------------
  if (fmd)
  {
    eos::FileMD fmdCopy(*fmd);
    fmd = &fmdCopy;
    memset(buf, 0, sizeof (struct stat));

    buf->st_dev = 0xcaff;
    buf->st_ino = eos::common::FileId::FidToInode(fmd->getId());
    buf->st_mode = S_IFREG;
    uint16_t flags = fmd->getFlags();
    if (!flags)
      buf->st_mode |= (S_IRUSR | S_IRGRP | S_IROTH | S_IWUSR);
    else
      buf->st_mode |= flags;
    buf->st_nlink = fmd->getNumLocation();
    buf->st_uid = fmd->getCUid();
    buf->st_gid = fmd->getCGid();
    buf->st_rdev = 0; /* device type (if inode device) */
    buf->st_size = fmd->getSize();
    buf->st_blksize = 512;
    buf->st_blocks = Quota::MapSizeCB(fmd) / 512; // including layout factor
    eos::FileMD::ctime_t atime;

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

    if (etag)
    {
      // if there is a checksum we use the checksum, otherwise we return inode+mtime
      size_t cxlen = eos::common::LayoutId::GetChecksumLen(fmd->getLayoutId());
      if (cxlen)
      {
        // use inode + checksum
        char setag[256];
        snprintf(setag, sizeof (setag) - 1, "\"%llu:", (unsigned long long) buf->st_ino);
        // if MD5 checksums are used we omit the inode number in the ETag (S3 wants that)
        if (eos::common::LayoutId::GetChecksum(fmd->getLayoutId()) != eos::common::LayoutId::kMD5)
          *etag = setag;
        else
          *etag = "";

        for (unsigned int i = 0; i < cxlen; i++)
        {
          char hb[3];
          sprintf(hb, "%02x", (i < cxlen) ? (unsigned char) (fmd->getChecksum().getDataPadded(i)) : 0);
          *etag += hb;
        }
        *etag += "\"";
      }
      else
      {
        // use inode + mtime
        char setag[256];
        snprintf(setag, sizeof (setag) - 1, "\"%llu:%llu\"", (unsigned long long) buf->st_ino, (unsigned long long) buf->st_mtime);
        *etag = setag;
      }
    }
    EXEC_TIMING_END("Stat");
    return SFS_OK;
  }

  // try if that is directory
  eos::ContainerMD* cmd = 0;
  errno = 0;

  // ---------------------------------------------------------------------------
  try
  {
    cmd = gOFS->eosView->getContainer(cPath.GetPath());

    memset(buf, 0, sizeof (struct stat));

    buf->st_dev = 0xcaff;
    buf->st_ino = cmd->getId();
    buf->st_mode = cmd->getMode();
    if (cmd->attributesBegin() != cmd->attributesEnd())
    {
      buf->st_mode |= S_ISVTX;
    }
    buf->st_nlink = 1;
    buf->st_uid = cmd->getCUid();
    buf->st_gid = cmd->getCGid();
    buf->st_rdev = 0; /* device type (if inode device) */
    buf->st_size = cmd->getNumContainers();
    buf->st_blksize = 0;
    buf->st_blocks = 0;
    eos::ContainerMD::ctime_t atime;
    cmd->getCTime(atime);

#ifdef __APPLE__
    buf->st_atimespec.tv_sec = atime.tv_sec;
    buf->st_mtimespec.tv_sec = atime.tv_sec;
    buf->st_ctimespec.tv_sec = atime.tv_sec;
    buf->st_atimespec.tv_nsec = atime.tv_nsec;
    buf->st_mtimespec.tv_nsec = atime.tv_nsec;
    buf->st_ctimespec.tv_nsec = atime.tv_nsec;
#else
    buf->st_atime = atime.tv_sec;
    buf->st_mtime = atime.tv_sec;
    buf->st_ctime = atime.tv_sec;

    buf->st_atim.tv_sec = atime.tv_sec;
    buf->st_mtim.tv_sec = atime.tv_sec;
    buf->st_ctim.tv_sec = atime.tv_sec;
    buf->st_atim.tv_nsec = atime.tv_nsec;
    buf->st_mtim.tv_nsec = atime.tv_nsec;
    buf->st_ctim.tv_nsec = atime.tv_nsec;
#endif

    // if we have a cached modification time, return that one
    // -->
    gOFS->MgmDirectoryModificationTimeMutex.Lock();
    if (gOFS->MgmDirectoryModificationTime.count(buf->st_ino))
    {
#ifdef __APPLE__
      buf->st_mtimespec.tv_sec = gOFS->MgmDirectoryModificationTime[buf->st_ino].tv_sec;
      buf->st_mtimespec.tv_nsec = gOFS->MgmDirectoryModificationTime[buf->st_ino].tv_nsec;
#else
      buf->st_mtime = gOFS->MgmDirectoryModificationTime[buf->st_ino].tv_sec;
      buf->st_mtim.tv_sec = buf->st_mtime;
      buf->st_mtim.tv_nsec = gOFS->MgmDirectoryModificationTime[buf->st_ino].tv_nsec;
#endif
    }
    gOFS->MgmDirectoryModificationTimeMutex.UnLock();

    if (etag)
    {
      // use inode + mtime
      char setag[256];
      snprintf(setag, sizeof (setag) - 1, "\"%llu:%llu\"", (unsigned long long) buf->st_ino, (unsigned long long) buf->st_mtime);
      *etag = setag;
    }
    // --|
    return SFS_OK;
  }
  catch (eos::MDException &e)
  {

    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(), e.getMessage().str().c_str());
    return Emsg(epname, error, errno, "stat", cPath.GetPath());
  }
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::lstat (const char *path,
                  struct stat *buf,
                  XrdOucErrInfo &error,
                  const XrdSecEntity *client,
                  const char *info)
/*----------------------------------------------------------------------------*/
/*
 * @brief stat following links (not existing in EOS - behaves like stat)
 */
/*----------------------------------------------------------------------------*/
{

  return stat(path, buf, error, client, info);
}

