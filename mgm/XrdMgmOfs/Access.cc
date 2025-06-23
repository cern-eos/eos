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

//------------------------------------------------------------------------------
// This file is included source code in XrdMgmOfs.cc to make the code more
// transparent without slowing down the compilation time.
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
//! Check access permissions for files/directories - XRootD API
//------------------------------------------------------------------------------
int
XrdMgmOfs::access(const char* inpath,
                  int mode,
                  XrdOucErrInfo& error,
                  const XrdSecEntity* client,
                  const char* ininfo)
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
  eos::common::Mapping::IdMap(client, ininfo, tident, vid,
                              gOFS->mTokenAuthz, AOP_Stat, inpath);
  EXEC_TIMING_END("IdMap");
  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);
  BOUNCE_NOT_ALLOWED;
  TOKEN_SCOPE;
  ACCESSMODE_R;
  MAYSTALL;
  MAYREDIRECT;
  return _access(path, mode, error, vid, ininfo);
}

//------------------------------------------------------------------------------
// Check access permissions for file/directories - EOS low-level API
//------------------------------------------------------------------------------
int
XrdMgmOfs::_access(const char* path, int mode, XrdOucErrInfo& error,
                   eos::common::VirtualIdentity& vid, const char* info)
{
  static const char* epname = "_access";
  eos_debug("path=%s mode=%x uid=%u gid=%u", path, mode, vid.uid, vid.gid);
  gOFS->MgmStats.Add("Access", vid.uid, vid.gid, 1);
  eos::common::Path cPath(path);
  bool permok = false;
  std::string attr_path = cPath.GetPath();
  std::shared_ptr<eos::IFileMD> fh;
  std::shared_ptr<eos::IContainerMD> dh;
  mode_t dh_mode {0};
  bool is_owner=false;
  eos::Prefetcher::prefetchItemAndWait(gOFS->eosView, cPath.GetPath());

  // Check for existing file
  try {
    fh = gOFS->eosView->getFile(cPath.GetPath());
  } catch (eos::MDException& e) {
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"", e.getErrno(),
              e.getMessage().str().c_str());
  }

  try {
    dh = gOFS->eosView->getContainer(cPath.GetPath());
  } catch (eos::MDException& e) {
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"", e.getErrno(),
              e.getMessage().str().c_str());
  }

  errno = 0;

  try {
    eos::IContainerMD::XAttrMap attrmap;
    eos::IContainerMD::XAttrMap fattrmap;

    if (fh || (!dh)) {
      std::string uri;

      // if this is a file or a not existing directory we check the access on the parent directory
      if (fh) {
        // Do not lock the file when calling getUri()!
        uri = gOFS->eosView->getUri(fh.get());
        // No need to explicitely lock the file as there is only one operation related to it.
        fattrmap = fh->getAttributes();
      } else {
        uri = cPath.GetPath();
      }

      eos::common::Path pPath(uri.c_str());
      dh = gOFS->eosView->getContainer(pPath.GetParentPath());
      attr_path = pPath.GetParentPath();
    }

    // ACL and permission check
    Acl acl(attr_path.c_str(), error, vid, attrmap);

    // Take into account file acls
    if (fattrmap.size()) {
      acl.SetFromAttrMap(attrmap, vid, &fattrmap);
    }

    eos_info("acl=%d r=%d w=%d wo=%d x=%d egroup=%d "
             "mutable=%d can_not_delete=%d token-issuer=%d",
             acl.HasAcl(), acl.CanRead(), acl.CanWrite(),
             acl.CanWriteOnce(), acl.CanBrowse(), acl.HasEgroup(),
             acl.IsMutable(), acl.CanNotDelete(), acl.CanIssueToken());
    {
      // In any case, we need to check the container access, read lock it to
      // check its access and release its lock afterwards
      eos::MDLocking::ContainerReadLockPtr dhLock = eos::MDLocking::readLock(
            dh.get());
      dh_mode = dh->getMode();
      is_owner = (dh->getCUid() == vid.uid);

      if (fh) {
	is_owner = (fh->getCUid() == vid.uid);
      }

      // check if people who don't own a file or directory can issue token
      if (!is_owner && (mode & T_OK) && !acl.CanIssueToken()) {
	errno = EPERM;
	return Emsg(epname, error, EPERM, "access - you cannot issue tokens", path);
      }
      
      if (!AccessChecker::checkContainer(dh.get(), acl, mode, vid)) {
        bool deny = true;

        if ((mode & D_OK) && acl.HasAcl() && acl.CanNotDelete()) {
          // The container cannot be accessed for deletion, however if the acl has !d, we need
          // to allow the owner of the file to delete it
          if (fh) {
            // Unlock the container before calling the file' getter
            dhLock.reset();

            if (fh->getCUid() == vid.uid) {
              deny = false;
            }
          }
        }

        if (deny) {
          errno = EPERM;
          return Emsg(epname, error, EPERM, "access", path);
        }
      }
    }

    if (fh) {
      // Check the file access, read lock it before and release the lock afterwards
      eos::MDLocking::FileReadLock fhLock(fh.get());

      if (!AccessChecker::checkFile(fh.get(), mode, dh_mode, vid)) {
        errno = EPERM;
        return Emsg(epname, error, EPERM, "access", path);
      }
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
    eos_debug("msg=\"access\" uid=%d gid=%d retc=%d mode=%o",
              vid.uid, vid.gid, permok, dh_mode);
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
  eos_debug("path=\"%s\" mode=%x uid=%u gid=%u", path, vid.uid, vid.gid);
  gOFS->MgmStats.Add("Access", vid.uid, vid.gid, 1);
  eos::common::Path cPath(path);
  std::shared_ptr<eos::IContainerMD> dh;
  std::shared_ptr<eos::IFileMD> fh;
  std::string attr_path = cPath.GetPath();
  uid_t dhCuid;
  bool r_ok = false;
  bool w_ok = false;
  bool x_ok = false;
  bool d_ok = false;
  bool d_perm_ok = false;
  // ---------------------------------------------------------------------------
  eos::Prefetcher::prefetchItemAndWait(gOFS->eosView, cPath.GetPath());

  // check for existing file
  try {
    fh = gOFS->eosView->getFile(cPath.GetPath());
  } catch (eos::MDException& e) {
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),
              e.getMessage().str().c_str());
  }

  if (!fh) {
    // check for existing dir if not a file
    try {
      dh = gOFS->eosView->getContainer(cPath.GetPath());
    } catch (eos::MDException& e) {
      eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),
                e.getMessage().str().c_str());
    }
  }

  try {
    eos::IContainerMD::XAttrMap attrmap;

    if (fh || (!dh)) {
      std::string uri;

      // if this is a file or a not existing directory we check the access on the parent directory
      if (fh) {
        // Do not lock the file before calling getUri()
        uri = gOFS->eosView->getUri(fh.get());
      } else {
        uri = cPath.GetPath();
      }

      eos::common::Path pPath(uri.c_str());
      dh = gOFS->eosView->getContainer(pPath.GetParentPath());
      attr_path = pPath.GetParentPath();
    }

    std::set<gid_t> gids;

    if (eos::common::Mapping::gSecondaryGroups) {
      gids = vid.allowed_gids;
    } else {
      gids.insert(vid.gid);
    }

    std::unique_ptr<Acl> aclPtr;
    {
      eos::MDLocking::ContainerReadLock dhLock(dh.get());

      dhCuid = dh->getCUid();
      if (!vid.token) {
	for (auto g : gids) {
	  if (dh->access(vid.uid, g, R_OK)) {
	    r_ok = true;
	  }
	  
	  if (dh->access(vid.uid, g, W_OK)) {
	    w_ok = true;
	    d_ok = true;
	    d_perm_ok = true;
	  }
	  
	  if (dh->access(vid.uid, g, X_OK)) {
	    x_ok = true;
	  }
	}   
      }

      //We prevent releasing the directory lock before calling the ACL constructor that will do
      //an _attr_ls on the directory
      aclPtr = std::make_unique<Acl>(attr_path.c_str(), error, vid, attrmap);
    }
    // ACL and permission check
    Acl& acl = *aclPtr;
    eos_debug("acl=%d r=%d w=%d wo=%d x=%d egroup=%d mutable=%d path=\"%s\"",
              acl.HasAcl(), acl.CanRead(), acl.CanWrite(), acl.CanWriteOnce(),
              acl.CanBrowse(), acl.HasEgroup(), acl.IsMutable(), path);

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
        // The user was probably allowed to delete from above logic, but the ACL prevents them to do so
        // however if this user is the owner of the directory, they should
        // be able to delete if the permissions of the directory allow it
        if(dhCuid == vid.uid && d_perm_ok) {
          d_ok = true;
        }
      }

      // the r/x are added to the posix permissions already set
      if (acl.CanRead()) {
        r_ok |= true;
      }

      if (acl.CanBrowse()) {
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
// Test if this is eosnobody accessing a squashfs file
//------------------------------------------------------------------------------
int
XrdMgmOfs::is_squashfs_access(const char* path,
                              eos::common::VirtualIdentity& vid)
{
  static int errc = 0;
  static int eosnobody = eos::common::Mapping::UserNameToUid(
                           std::string("eosnobody"), errc);

  if ((vid.prot == "sss") &&
      (eosnobody == vid.uid) && !errc) {
    // eosnobody can access all squash files
    eos::common::Path cPath(path);

    if (!cPath.isSquashFile()) {
      errno = EACCES;
      return 1;
    }

    return 2;
  }

  return 0;
}

//------------------------------------------------------------------------------
// Test if public access is allowed for a given path
//------------------------------------------------------------------------------
bool
XrdMgmOfs::allow_public_access(const char* path,
                               eos::common::VirtualIdentity& vid)
{
  int sq = is_squashfs_access(path, vid);

  if (sq == 2) {
    // eosnobody squashfs file access is allowed
    return true;
  }

  if (sq == 1) {
    // eosnobody access is not allowed in general
    return false;
  }

  // Check only for anonymous access
  // uid=99 for CentOS7
  // uid=65534 for >= Alma 9
  if ((vid.uid != 99) && (vid.uid != 65534)) {
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
