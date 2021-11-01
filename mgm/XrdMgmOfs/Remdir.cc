// ----------------------------------------------------------------------
// File: Remdir.cc
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

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::remdir(const char* inpath,
                  XrdOucErrInfo& error,
                  const XrdSecEntity* client,
                  const char* ininfo)
/*----------------------------------------------------------------------------*/
/*
 * @brief delete a directory from the namespace
 *
 * @param inpath directory to delete
 * @param error error object
 * @param client XRootD authentication object
 * @param ininfo CGI
 * @return SFS_OK on success otherwise SFS_ERROR
 *
 */
/*----------------------------------------------------------------------------*/
{
  static const char* epname = "remdir";
  const char* tident = error.getErrUser();
  eos::common::VirtualIdentity vid;
  EXEC_TIMING_BEGIN("IdMap");
  eos::common::Mapping::IdMap(client, ininfo, tident, vid);
  EXEC_TIMING_END("IdMap");
  NAMESPACEMAP;
  NAMESPACE_NO_TRAILING_SLASH;
  BOUNCE_ILLEGAL_NAMES;
  TOKEN_SCOPE;
  XrdOucEnv remdir_Env(ininfo);
  AUTHORIZE(client, &remdir_Env, AOP_Delete, "remove", inpath, error);
  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);
  BOUNCE_NOT_ALLOWED;
  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;
  return _remdir(path, error, vid, ininfo);
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_remdir(const char* path,
                   XrdOucErrInfo& error,
                   eos::common::VirtualIdentity& vid,
                   const char* ininfo,
                   bool simulate)
/*----------------------------------------------------------------------------*/
/*
 * @brief delete a directory from the namespace
 *
 * @param inpath directory to delete
 * @param error error object
 * @param vid virtual identity of the client
 * @param ininfo CGI
 * @return SFS_OK on success otherwise SFS_ERROR
 *
 * We support a special ACL to forbid deletion if it would be allowed by the
 * normal POSIX settings (ACL !d flag).
 */
/*----------------------------------------------------------------------------*/

{
  static const char* epname = "remdir";
  errno = 0;
  eos_info("path=%s", path);
  EXEC_TIMING_BEGIN("RmDir");
  gOFS->MgmStats.Add("RmDir", vid.uid, vid.gid, 1);
  std::shared_ptr<eos::IContainerMD> dhpar;
  std::shared_ptr<eos::IContainerMD> dh;
  eos::common::Path cPath(path);
  eos::IContainerMD::XAttrMap attrmap;
  // Make sure this is not a quota node
  std::string qpath = path;

  if (Quota::Exists(qpath)) {
    errno = EBUSY;
    return Emsg(epname, error, errno, "rmdir - this is a quota node",
                qpath.c_str());
  }

  eos::common::RWMutexWriteLock viewLock(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__, __FILE__);
  std::string aclpath;

  try {
    dh = gOFS->eosView->getContainer(path);
    eos::common::Path pPath(gOFS->eosView->getUri(dh.get()).c_str());
    dhpar = gOFS->eosView->getContainer(pPath.GetParentPath());
    aclpath = pPath.GetParentPath();
  } catch (eos::MDException& e) {
    dh.reset();
    dhpar.reset();
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
              e.getErrno(), e.getMessage().str().c_str());
  }

  eos_info("path='%s' scope='%s' aclpath='%s'\n", path, vid.scope.c_str(), aclpath.c_str());
  // check existence
  if (!dh) {
    errno = ENOENT;
    return Emsg(epname, error, errno, "rmdir", path);
  }

  // ACL and permission check
  Acl acl(aclpath.c_str(), error, vid, attrmap, false);

  if (vid.uid && !acl.IsMutable()) {
    errno = EPERM;
    return Emsg(epname, error, EPERM, "rmdir - immutable", path);
  }

  if (!gOFS->allow_public_access(aclpath.c_str(), vid)) {
    errno = EACCES;
    return Emsg(epname, error, EACCES, "access - public access level restriction",
                aclpath.c_str());
  }

  if (ininfo) {
    XrdOucEnv env_info(ininfo);

    if (env_info.Get("mgm.option")) {
      XrdOucString option = env_info.Get("mgm.option");

      if (option == "r") {
        // Recursive delete - need to unlock before calling the proc function
        viewLock.Release();
        ProcCommand cmd;
        XrdOucString info = "mgm.cmd=rm&mgm.option=r&mgm.path=";
        info += path;
        cmd.open("/proc/user", info.c_str(), vid, &error);
        cmd.close();
        int rc = cmd.GetRetc();

        if (rc) {
          return Emsg(epname, error, rc, "rmdir", path);
        }

        return SFS_OK;
      }
    }
  }

  bool stdpermcheck = false;
  bool aclok = false;

  if (vid.avatar) {
    vid.uid = dh->getCUid();
    vid.gid = dh->getCGid();
  }


  if (acl.HasAcl()) {
    if ((dh->getCUid() != vid.uid) &&
        (vid.uid) && // not the root user
        (vid.uid != 3) && // not the admin user
        (vid.gid != 4) && // not the admin group
        (acl.CanNotDelete())) {
      // deletion is explicitly forbidden
      errno = EPERM;
      return Emsg(epname, error, EPERM, "rmdir by ACL", path);
    }

    if ((!acl.CanWrite())) {
      // we have to check the standard permissions
      stdpermcheck = true;
    } else {
      aclok = true;
    }
  } else {
    stdpermcheck = true;
  }

  // Check permissions
  bool permok = stdpermcheck ? (dhpar ? (dhpar->access(vid.uid, vid.gid,
                                         X_OK | W_OK)) : false) : aclok;

  if (!permok) {
    errno = EPERM;
    return Emsg(epname, error, errno, "rmdir", path);
  }

  if ((dh->getFlags() && eos::QUOTA_NODE_FLAG) && (vid.uid)) {
    errno = EADDRINUSE;
    eos_err("%s is a quota node - deletion canceled", path);
    return Emsg(epname, error, errno, "rmdir - this is a quota node", path);
  }

  if (!simulate) {
    try {
      eos::ContainerIdentifier dhpar_id;
      eos::ContainerIdentifier dhpar_pid;
      std::string dh_name;

      // update the in-memory modification time of the parent directory
      if (dhpar) {
        dhpar->setMTimeNow();
        dhpar->notifyMTimeChange(gOFS->eosDirectoryService);
        eosView->updateContainerStore(dhpar.get());
        dhpar_id = dhpar->getIdentifier();
        dh_name = dh->getName();
      }

      eosView->removeContainer(path);
      viewLock.Release();

      if (dhpar) {
        gOFS->FuseXCastContainer(dhpar_id);
        gOFS->FuseXCastDeletion(dhpar_id, dh_name);
        gOFS->FuseXCastRefresh(dhpar_id, dhpar_pid);
      }
    } catch (eos::MDException& e) {
      errno = e.getErrno();
      eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                e.getErrno(), e.getMessage().str().c_str());
    }
  }

  viewLock.Release();
  EXEC_TIMING_END("RmDir");

  if (errno) {
    if (errno == ENOTEMPTY) {
      return Emsg(epname, error, errno, "rmdir - Directory not empty", path);
    } else {
      return Emsg(epname, error, errno, "rmdir", path);
    }
  } else {
    return SFS_OK;
  }
}
