// ----------------------------------------------------------------------
// File: Mkdir.cc
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

//------------------------------------------------------------------------------
// This file is included source code in XrdMgmOfs.cc to make the code more
// transparent without slowing down the compilation time.
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
/*
 * @brief create a directory with the given mode
 *
 * @param inpath directory path to create
 * @param Mode mode to set
 * @param error error object
 * @param client XRootD authentication object
 * @param ininfo CGI
 * @param outino return inode number
 * @return SFS_OK if success otherwise SFS_ERROR
 *
 * If mode contains SFS_O_MKPTH the full path is (possibly) created.
 */
//------------------------------------------------------------------------------
int
XrdMgmOfs::mkdir(const char* inpath,
                 XrdSfsMode Mode,
                 XrdOucErrInfo& error,
                 const XrdSecEntity* client,
                 const char* ininfo,
                 ino_t* outino)
{
  static const char* epname = "mkdir";
  const char* tident = error.getErrUser();
  // use a thread private vid
  eos::common::VirtualIdentity vid;
  EXEC_TIMING_BEGIN("IdMap");
  eos::common::Mapping::IdMap(client, ininfo, tident, vid);
  EXEC_TIMING_END("IdMap");
  NAMESPACEMAP;
  TOKEN_SCOPE;
  BOUNCE_ILLEGAL_NAMES;
  XrdOucEnv mkdir_Env(ininfo);
  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);
  eos_info("path=%s ininfo=%s", path, ininfo);
  BOUNCE_NOT_ALLOWED;
  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;
  return _mkdir(path, Mode, error, vid, ininfo, outino);
}

//------------------------------------------------------------------------------
/*
 * @brief create a directory with the given mode
 *
 * @param inpath directory path to create
 * @param Mode mode to set
 * @param error error object
 * @param client XRootD authentication object
 * @param ininfo CGI
 * @param outino return inode number
 * @return SFS_OK on success otherwise SFS_ERROR
 *
 * If mode contains SFS_O_MKPTH the full path is (possibly) created.
 *
 */
//------------------------------------------------------------------------------
int
XrdMgmOfs::_mkdir(const char* path,
                  XrdSfsMode Mode,
                  XrdOucErrInfo& error,
                  eos::common::VirtualIdentity& vid,
                  const char* ininfo,
                  ino_t* outino)
{
  static const char* epname = "_mkdir";
  //mode_t acc_mode = (Mode & S_IAMB) | S_IFDIR;
  errno = 0;
  EXEC_TIMING_BEGIN("Mkdir");
  gOFS->MgmStats.Add("Mkdir", vid.uid, vid.gid, 1);
  XrdOucString spath = path;
  eos_info("path=%s", spath.c_str());

  if (!spath.beginswith("/")) {
    errno = EINVAL;
    return Emsg(epname, error, EINVAL, "create directory - you have to specify"
                " an absolute pathname", path);
  }

  bool recurse = false;
  eos::common::Path cPath(path);
  bool noParent = false;
  std::shared_ptr<eos::IContainerMD> dir;
  eos::IContainerMD::XAttrMap attrmap;
  {
    eos::Prefetcher::prefetchContainerMDAndWait(gOFS->eosView,
        cPath.GetParentPath());
    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__,
                                      __FILE__);

    // Check for the parent directory
    if (spath != "/") {
      try {
        dir = eosView->getContainer(cPath.GetParentPath());
      } catch (eos::MDException& e) {
        eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                  e.getErrno(), e.getMessage().str().c_str());
        dir.reset();
        noParent = true;
      }
    }

    // Check permission
    if (dir) {
      uid_t d_uid = dir->getCUid();
      gid_t d_gid = dir->getCGid();
      // ACL and permission check
      Acl acl(cPath.GetParentPath(), error, vid, attrmap, false);
      eos_info("path=%s acl=%d r=%d w=%d wo=%d egroup=%d mutable=%d",
               cPath.GetParentPath(),
               acl.HasAcl(), acl.CanRead(), acl.CanWrite(), acl.CanWriteOnce(),
               acl.HasEgroup(), acl.IsMutable());

      // Immutable directory
      if (vid.uid && !acl.IsMutable()) {
        errno = EPERM;
        return Emsg(epname, error, EPERM, "create directory - immutable",
                    cPath.GetParentPath());
      }

      bool sticky_owner = false;

      // Check for sys.owner.auth entries, which let people operate as the owner of the directory
      if (attrmap.count("sys.owner.auth")) {
        if (attrmap["sys.owner.auth"] == "*") {
          sticky_owner = true;
        } else {
          attrmap["sys.owner.auth"] += ",";
          std::string ownerkey = vid.prot.c_str();
          ownerkey += ":";

          if (vid.prot == "gsi") {
            ownerkey += vid.dn.c_str();
          } else {
            ownerkey += vid.uid_string.c_str();
          }

          if ((attrmap["sys.owner.auth"].find(ownerkey)) != std::string::npos) {
            eos_info("msg=\"client authenticated as directory owner\" path=\"%s\"uid=\"%u=>%u\" gid=\"%u=>%u\"",
                     path, vid.uid, vid.gid, d_uid, d_gid);
            // The client can operate as the owner, we rewrite the virtual identity
            // to the directory uid/gid pair
            vid.uid = d_uid;
            vid.gid = d_gid;
          }
        }
      }

      bool stdpermcheck = false;

      if (acl.HasAcl()) {
        if ((!acl.CanWrite()) && (!acl.CanWriteOnce())) {
          // we have to check the standard permissions
          stdpermcheck = true;
        }
      } else {
        stdpermcheck = true;
      }

      // Admin can always create a directory
      if (stdpermcheck && (!dir->access(vid.uid, vid.gid, X_OK | W_OK))) {
        errno = EPERM;
        return Emsg(epname, error, EPERM, "access(XW) parent directory",
                    cPath.GetParentPath());
      }

      if (sticky_owner) {
        eos_info("msg=\"client acting as directory owner\" path=\"%s\"uid=\"%u=>%u\" gid=\"%u=>%u\"",
                 path, vid.uid, vid.gid, d_uid, d_gid);
        // The client can operate as the owner, we rewrite the virtual identity
        // to the directory uid/gid pair
        vid.uid = d_uid;
        vid.gid = d_gid;
      }
    }
  }

  // Check if the path exists anyway
  if (Mode & SFS_O_MKPTH) {
    recurse = true;
    eos_debug("SFS_O_MKPATH set", path);

    if (dir) {
      std::shared_ptr<eos::IContainerMD> fulldir;
      eos::Prefetcher::prefetchContainerMDAndWait(gOFS->eosView, path);
      eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__,
                                        __FILE__);

      // Only if the parent exists, can the full path exist!
      try {
        fulldir = eosView->getContainer(path);
      } catch (eos::MDException& e) {
        fulldir.reset();
        eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),
                  e.getMessage().str().c_str());
      }

      if (fulldir) {
        EXEC_TIMING_END("Exists");
        return SFS_OK;
      }
    }
  }

  eos_debug("mkdir path=%s deepness=%d dirname=%s basename=%s", path,
            cPath.GetSubPathSize(), cPath.GetParentPath(), cPath.GetName());
  std::shared_ptr<eos::IContainerMD> newdir;

  if (noParent) {
    if (recurse) {
      int i, j;
      uid_t d_uid = 99;
      gid_t d_gid = 99;
      std::string existingdir;

      // Walk up the paths until one exists
      for (i = cPath.GetSubPathSize() - 1; i >= 0; i--) {
        eos_debug("testing path %s", cPath.GetSubPath(i));
        errno = 0;
        eos::Prefetcher::prefetchContainerMDAndWait(gOFS->eosView, cPath.GetSubPath(i));
        eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__,
                                          __FILE__);
        attrmap.clear();

        try {
          dir = eosView->getContainer(cPath.GetSubPath(i));
          existingdir = cPath.GetSubPath(i);
          d_uid = dir->getCUid();
          d_gid = dir->getCGid();
        } catch (eos::MDException& e) {
          dir.reset();
        }

        if (dir) {
          break;
        }
      }

      // This is really a serious problem!
      if (!dir) {
        eos_crit("didn't find any parent path traversing the namespace");
        errno = ENODATA;
        return Emsg(epname, error, ENODATA, "create directory", cPath.GetSubPath(i));
      }

      // ACL and permission check
      Acl acl(existingdir.c_str(), error, vid, attrmap, true);
      eos_info("acl=%d r=%d w=%d wo=%d egroup=%d mutable=%d",
               acl.HasAcl(), acl.CanRead(), acl.CanWrite(), acl.CanWriteOnce(),
               acl.HasEgroup(), acl.IsMutable());

      // Check for sys.owner.auth entries, which let people operate as the owner of the directory
      if (attrmap.count("sys.owner.auth")) {
        if (attrmap["sys.owner.auth"] == "*") {
          eos_info("msg=\"client acting as directory owner\" path=\"%s\"uid=\"%u=>%u\" gid=\"%u=>%u\"",
                   existingdir.c_str(), vid.uid, vid.gid, d_uid, d_gid);
          // The client can operate as the owner, we rewrite the virtual identity
          // to the directory uid/gid pair
          vid.uid = d_uid;
          vid.gid = d_gid;
        } else {
          attrmap["sys.owner.auth"] += ",";
          std::string ownerkey = vid.prot.c_str();
          ownerkey += ":";

          if (vid.prot == "gsi") {
            ownerkey += vid.dn.c_str();
          } else {
            ownerkey += vid.uid_string.c_str();
          }

          if ((attrmap["sys.owner.auth"].find(ownerkey)) != std::string::npos) {
            eos_info("msg=\"client authenticated as directory owner\" path=\"%s\"uid=\"%u=>%u\" gid=\"%u=>%u\"",
                     path, vid.uid, vid.gid, d_uid, d_gid);
            // The client can operate as the owner, we rewrite the virtual identity
            // to the directory uid/gid pair
            vid.uid = d_uid;
            vid.gid = d_gid;
          }
        }
      }

      if (vid.uid && !acl.IsMutable()) {
        errno = EPERM;
        return Emsg(epname, error, EPERM, "create parent directory - immutable",
                    cPath.GetParentPath());
      }

      bool stdpermcheck = false;

      if (acl.HasAcl()) {
        if (!acl.CanWrite() && !acl.CanWriteOnce()) {
          // We have to check the standard permissions
          stdpermcheck = true;
        }
      } else {
        stdpermcheck = true;
      }

      if (stdpermcheck && (!dir->access(vid.uid, vid.gid, X_OK | W_OK))) {
        errno = EPERM;
        return Emsg(epname, error, EPERM, "create parent directory",
                    cPath.GetParentPath());
      }

      eos::common::Path tmp_path("");

      for (j = i + 1; j < (int) cPath.GetSubPathSize(); ++j) {
        eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__,
                                           __FILE__);

        try {
          errno = 0;
          eos_debug("creating path %s", cPath.GetSubPath(j));
          tmp_path.Init(cPath.GetSubPath(j));
          dir = eosView->getContainer(tmp_path.GetParentPath());
          newdir = eosView->createContainer(cPath.GetSubPath(j), recurse);
          newdir->setCUid(vid.uid);
          newdir->setCGid(vid.gid);
          newdir->setMode(dir->getMode());
          // Inherit the attributes
          eos::IFileMD::XAttrMap xattrs = dir->getAttributes();

          for (const auto& elem : xattrs) {
            newdir->setAttribute(elem.first, elem.second);
          }

          // Store the in-memory modification time into the parent
          eos::IContainerMD::ctime_t ctime;
          newdir->getCTime(ctime);
          newdir->setMTime(ctime);
          // Store the birth time
          char btime[256];
          snprintf(btime, sizeof(btime), "%lu.%lu", ctime.tv_sec, ctime.tv_nsec);
          newdir->setAttribute("sys.eos.btime", btime);
          dir->setMTime(ctime);
          dir->notifyMTimeChange(gOFS->eosDirectoryService);
          // commit
          eosView->updateContainerStore(newdir.get());
          eosView->updateContainerStore(dir.get());
          dir->notifyMTimeChange(gOFS->eosDirectoryService);
          newdir->notifyMTimeChange(gOFS->eosDirectoryService);
          eos::ContainerIdentifier nd_id = newdir->getIdentifier();
          eos::ContainerIdentifier d_id = dir->getIdentifier();
          eos::ContainerIdentifier d_pid = dir->getParentIdentifier();
          lock.Release();
          gOFS->FuseXCastContainer(nd_id);
          gOFS->FuseXCastContainer(d_id);
          gOFS->FuseXCastRefresh(d_id, d_pid);
        } catch (eos::MDException& e) {
          errno = e.getErrno();
          eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                    e.getErrno(), e.getMessage().str().c_str());
          return Emsg(epname, error, errno, "mkdir", path);
        }

        if (!newdir && (errno != EEXIST)) {
          return Emsg(epname, error, errno, "mkdir - newdir is 0", path);
        }

        dir.swap(newdir);
      }
    } else {
      errno = ENOENT;
      return Emsg(epname, error, errno, "mkdir", path);
    }
  }

  // This might not be needed, but it is detected by coverty
  if (!dir) {
    return Emsg(epname, error, errno, "mkdir", path);
  }

  eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__,
                                     __FILE__);

  try {
    errno = 0;
    dir = eosView->getContainer(cPath.GetParentPath());
    newdir = eosView->createContainer(path);
    newdir->setCUid(vid.uid);
    newdir->setCGid(vid.gid);
    // @note: we always inherit the mode of the parent directory. So far nobody
    // complained so we'll keep it as it is until someone does.
    //newdir->setMode(acc_mode);
    newdir->setMode(dir->getMode());
    // Store the in-memory modification time
    eos::IContainerMD::ctime_t ctime;
    newdir->getCTime(ctime);
    newdir->setMTime(ctime);
    // Store the birth time
    char btime[256];
    snprintf(btime, sizeof(btime), "%lu.%lu", ctime.tv_sec, ctime.tv_nsec);
    newdir->setAttribute("sys.eos.btime", btime);
    dir->setMTime(ctime);

    // If not version directory, then inherit attributes
    if (cPath.GetFullPath().find(EOS_COMMON_PATH_VERSION_PREFIX) == STR_NPOS) {
      eos::IFileMD::XAttrMap xattrs = dir->getAttributes();

      for (const auto& elem : xattrs) {
        newdir->setAttribute(elem.first, elem.second);
      }
    }

    if (outino) {
      *outino = newdir->getId();
    }

    // Commit to backend
    eosView->updateContainerStore(newdir.get());
    eosView->updateContainerStore(dir.get());
    // Notify after attribute inheritance
    newdir->notifyMTimeChange(gOFS->eosDirectoryService);
    dir->notifyMTimeChange(gOFS->eosDirectoryService);
    eos::ContainerIdentifier nd_id = newdir->getIdentifier();
    eos::ContainerIdentifier d_id = dir->getIdentifier();
    eos::ContainerIdentifier d_pid = dir->getParentIdentifier();
    lock.Release();
    gOFS->FuseXCastContainer(nd_id);
    gOFS->FuseXCastContainer(d_id);
    gOFS->FuseXCastRefresh(d_id, d_pid);
  } catch (eos::MDException& e) {
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"",
              e.getErrno(), e.getMessage().str().c_str());
  }

  if (!newdir && (errno != EEXIST)) {
    return Emsg(epname, error, errno, "mkdir", path);
  }

  EXEC_TIMING_END("Mkdir");
  return SFS_OK;
}
