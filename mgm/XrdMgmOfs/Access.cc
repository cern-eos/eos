// ----------------------------------------------------------------------
// File: Access.cc
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
XrdMgmOfs::access(const char* inpath,
                  int mode,
                  XrdOucErrInfo& error,
                  const XrdSecEntity* client,
                  const char* ininfo)
/*----------------------------------------------------------------------------*/
/*
 * @brief check access permissions for file/directories
 *
 * @param inpath path to access
 * @param mode access mode can be R_OK |& W_OK |& X_OK or F_OK
 * @param client XRootD authentication object
 * @param ininfo CGI
 * @return SFS_OK if possible otherwise SFS_ERROR
 *
 * See the internal implementation _access for details
 */
/*----------------------------------------------------------------------------*/
{
  static const char* epname = "access";
  const char* tident = error.getErrUser();
  NAMESPACEMAP;
  BOUNCE_ILLEGAL_NAMES;
  XrdOucEnv access_Env(ininfo);
  AUTHORIZE(client, &access_Env, AOP_Stat, "access", inpath, error);
  // use a thread private vid
  eos::common::VirtualIdentity vid;
  EXEC_TIMING_BEGIN("IdMap");
  eos::common::Mapping::IdMap(client, ininfo, tident, vid);
  EXEC_TIMING_END("IdMap");
  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);
  BOUNCE_NOT_ALLOWED;
  ACCESSMODE_R;
  MAYSTALL;
  MAYREDIRECT;
  return _access(path, mode, error, vid, ininfo);
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_access(const char* path,
                   int mode,
                   XrdOucErrInfo& error,
                   eos::common::VirtualIdentity& vid,
                   const char* info,
                   bool lock)
/*----------------------------------------------------------------------------*/
/*
 * @brief check access permissions for file/directories
 *
 * @param inpath path to access
 * @param mode access mode can be R_OK |& W_OK |& X_OK |& F_OK or P_OK
 * @param client XRootD authentication object
 * @param ininfo CGI
 * @return SFS_OK if possible otherwise SFS_ERROR
 *
 * If F_OK is specified we just check for the existence of the path, which can
 * be a file or directory. We don't support X_OK since it cannot be mapped
 * in case of files (we don't have explicit execution permissions).
 */
/*----------------------------------------------------------------------------*/
{
  static const char* epname = "_access";
  eos_debug("path=%s mode=%x uid=%u gid=%u", path, mode, vid.uid, vid.gid);
  gOFS->MgmStats.Add("Access", vid.uid, vid.gid, 1);
  eos::common::Path cPath(path);
  std::shared_ptr<eos::IContainerMD> dh;
  std::shared_ptr<eos::IFileMD> fh;
  bool permok = false;
  std::string attr_path = cPath.GetPath();
  // ---------------------------------------------------------------------------
  eos::Prefetcher::prefetchItemAndWait(gOFS->eosView, cPath.GetPath());
  eos::common::RWMutexReadLock viewReadLock;

  if (lock) {
    viewReadLock.Grab(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__, __FILE__);
  }

  // check for existing file
  try {
    fh = gOFS->eosView->getFile(cPath.GetPath());
  } catch (eos::MDException& e) {
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"", e.getErrno(),
              e.getMessage().str().c_str());
  }

  try {
    dh = gOFS->eosView->getContainer(cPath.GetPath());
  } catch (eos::MDException& e) {
    eos_debug("trying as container: msg=\"exception\" ec=%d emsg=\"%s\"", e.getErrno(),
              e.getMessage().str().c_str());
  }

  errno = 0;
  int cmode;

  try {
    eos::IContainerMD::XAttrMap attrmap;

    if (fh || (!dh)) {
      std::string uri;

      // if this is a file or a not existing directory we check the access on the parent directory
      if (fh) {
        uri = gOFS->eosView->getUri(fh.get());
        cmode = X_OK;
        /* P_OK checked on dir only and requires W_OK on dir instead of file */
        if (mode & P_OK) cmode |= mode & (P_OK | W_OK);
      } else {
        cmode = mode;
        mode &= ~(P_OK);                    /* don't require this for files, it's dir-only */
        uri = cPath.GetPath();
      }

      eos::common::Path pPath(uri.c_str());
      dh = gOFS->eosView->getContainer(pPath.GetParentPath());
      attr_path = pPath.GetParentPath();
    }

    // ACL and permission check
    Acl acl(attr_path.c_str(), error, vid, attrmap, false);
    eos_info("acl=%d r=%d w=%d wo=%d x=%d egroup=%d mutable=%d",
             acl.HasAcl(), acl.CanRead(), acl.CanWrite(), acl.CanWriteOnce(),
             acl.CanStat(), acl.HasEgroup(), acl.IsMutable());

    if (!AccessChecker::checkContainer(dh.get(), acl, cmode, vid)) {
      errno = EPERM;
      return Emsg(epname, error, EPERM, "access", path);
    }

    if (fh && !AccessChecker::checkFile(fh.get(), mode, vid)) {
      errno = EPERM;
      return Emsg(epname, error, EPERM, "access", path);
    }

    permok = true;
  } catch (eos::MDException& e) {
    dh.reset();
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
              e.getErrno(), e.getMessage().str().c_str());
  }


  // Check permissions
  if (!dh) {
    eos_debug("msg=\"access\" errno=ENOENT");
    errno = ENOENT;
    return Emsg(epname, error, ENOENT, "access", path);
  }

  // root/daemon can always access, daemon only for reading!
  if ((vid.uid == 0) || ((vid.uid == DAEMONUID) && (!(mode & W_OK)))) {
    permok = true;
  }

  if (dh) {
    eos_debug("msg=\"access\" uid=%d gid=%d retc=%d mode=#%o dirmode=%#0",
              vid.uid, vid.gid, permok, mode, dh->getMode());
  }

  if (dh && (mode & F_OK)) {
    return SFS_OK;
  }

  if (dh && permok) {
    return SFS_OK;
  }

  if (dh && (!permok)) {
    errno = EACCES;
    return Emsg(epname, error, EACCES, "access", path);
  }

  // check publicaccess level
  if (!allow_public_access(path, vid)) {
    errno = EACCES;
    return Emsg(epname, error, EACCES, "access - public access level restriction",
                path);
  }

  errno = EOPNOTSUPP;
  return Emsg(epname, error, EOPNOTSUPP, "access", path);
}

//------------------------------------------------------------------------------
// Define access permissions by vid for a file/directory
//------------------------------------------------------------------------------
int
XrdMgmOfs::acc_access(const char* path,
                      XrdOucErrInfo& error,
                      eos::common::VirtualIdentity& vid,
                      std::string& accperm)
{
  eos_debug("path=%s mode=%x uid=%u gid=%u", path, vid.uid, vid.gid);
  gOFS->MgmStats.Add("Access", vid.uid, vid.gid, 1);
  eos::common::Path cPath(path);
  std::shared_ptr<eos::IContainerMD> dh;
  std::shared_ptr<eos::IFileMD> fh;
  std::string attr_path = cPath.GetPath();
  bool r_ok = false;
  bool w_ok = false;
  bool x_ok = false;
  bool d_ok = false;
  // ---------------------------------------------------------------------------
  eos::Prefetcher::prefetchItemAndWait(gOFS->eosView, cPath.GetPath());
  eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__, __FILE__);

  // check for existing file
  try {
    fh = gOFS->eosView->getFile(cPath.GetPath());
  } catch (eos::MDException& e) {
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),
              e.getMessage().str().c_str());
  }

  try {
    dh = gOFS->eosView->getContainer(cPath.GetPath());
  } catch (eos::MDException& e) {
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),
              e.getMessage().str().c_str());
  }

  try {
    eos::IContainerMD::XAttrMap attrmap;

    if (fh || (!dh)) {
      std::string uri;

      // if this is a file or a not existing directory we check the access on the parent directory
      if (fh) {
        uri = gOFS->eosView->getUri(fh.get());
      } else {
        uri = cPath.GetPath();
      }

      eos::common::Path pPath(uri.c_str());
      dh = gOFS->eosView->getContainer(pPath.GetParentPath());
      attr_path = pPath.GetParentPath();
    }

    if (dh->access(vid.uid, vid.gid, R_OK)) {
      r_ok = true;
    }

    if (dh->access(vid.uid, vid.gid, W_OK)) {
      w_ok = true;
      d_ok = true;
    }

    if (dh->access(vid.uid, vid.gid, X_OK)) {
      x_ok = true;
    }

    // ACL and permission check
    Acl acl(attr_path.c_str(), error, vid, attrmap, false);
    eos_info("acl=%d r=%d w=%d wo=%d x=%d egroup=%d mutable=%d",
             acl.HasAcl(), acl.CanRead(), acl.CanWrite(), acl.CanWriteOnce(),
             acl.CanStat(), acl.HasEgroup(), acl.IsMutable());

    // browse permission by ACL
    if (acl.HasAcl()) {
      if (acl.CanWrite()) {
        w_ok = true;
        d_ok = true;
      }

      // write-once or write is fine for OC write permission
      if (!(acl.CanWrite() || acl.CanWriteOnce())) {
        w_ok = false;
      }

      // deletion might be overwritten/forbidden
      if (acl.CanNotDelete()) {
        d_ok = false;
      }

      // the r/x are added to the posix permissions already set
      if (acl.CanRead()) {
        r_ok |= true;
      }

      if (acl.CanStat()) {
        x_ok |= true;
      }

      if (!acl.IsMutable()) {
        w_ok = d_ok = false;
      }
    }
  } catch (eos::MDException& e) {
    dh = std::shared_ptr<eos::IContainerMD>((eos::IContainerMD*)0);
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
              e.getErrno(), e.getMessage().str().c_str());
  }

  // check permissions
  if (!dh) {
    accperm = "";
    return SFS_ERROR;
  }

  // return the OC string;
  if (r_ok) {
    accperm += "R";
  }

  if (w_ok) {
    accperm += "WCKNV";
  }

  if (d_ok) {
    accperm += "D";
  }

  return SFS_OK;
}

//------------------------------------------------------------------------------
// Test if public access is allowed for a given path
//------------------------------------------------------------------------------
bool
XrdMgmOfs::allow_public_access(const char* path,
                               eos::common::VirtualIdentity& vid)
{
  // check only for anonymous access
  if (vid.uid != 99) {
    return true;
  }

  // check publicaccess level
  int level = eos::common::Mapping::GetPublicAccessLevel();

  if (level >= 1024) {
    // short cut
    return true;
  }

  eos::common::Path cPath(path);

  if ((int)cPath.GetSubPathSize() >= level) {
    // forbid everything to nobody in that case
    errno = EACCES;
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Get the allowed XrdAccPrivs i.e. allowed operations on the given path
// for the client in the XrdSecEntity
//------------------------------------------------------------------------------
XrdAccPrivs
XrdMgmOfs::GetXrdAccPrivs(const std::string& path, const XrdSecEntity* client,
                          XrdOucEnv* env)
{
  std::string eos_path;
  eos::common::VirtualIdentity vid;
  auto basic_checks = [&, this]() -> int {
    const char* epname = "access";
    char* ininfo = nullptr;
    XrdOucErrInfo error;
    const char* inpath = path.c_str();
    const char* tident = client->tident;
    NAMESPACEMAP;
    BOUNCE_ILLEGAL_NAMES;
    AUTHORIZE(client, env, AOP_Stat, "access", inpath, error);

    EXEC_TIMING_BEGIN("IdMap");
    eos::common::Mapping::IdMap(client, ininfo, tident, vid);
    EXEC_TIMING_END("IdMap");
    gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);
    BOUNCE_NOT_ALLOWED;
    ACCESSMODE_R;
    MAYSTALL;
    MAYREDIRECT;
    eos_path = path;
    return SFS_OK;
  };

  if (basic_checks()) {
    eos_err("msg=\"failed basic checks for access privilege resolution\" "
            "path=\"%s\", user=\"%s\"", path.c_str(),
            (client->name ? client->name : ""));
    return XrdAccPriv_None;
  }

  return XrdAccPriv_All;
}
