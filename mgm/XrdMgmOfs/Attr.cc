// ----------------------------------------------------------------------
// File: Attr.cc
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
// List extended attributes for a given file/directory - high-level API.
//------------------------------------------------------------------------------
int
XrdMgmOfs::attr_ls(const char* inpath,
                   XrdOucErrInfo& error,
                   const XrdSecEntity* client,
                   const char* ininfo,
                   eos::IContainerMD::XAttrMap& map)

{
  static const char* epname = "attr_ls";
  const char* tident = error.getErrUser();
  // use a thread private vid
  eos::common::VirtualIdentity vid;
  EXEC_TIMING_BEGIN("IdMap");
  eos::common::Mapping::IdMap(client, ininfo, tident, vid);
  EXEC_TIMING_END("IdMap");
  NAMESPACEMAP;
  BOUNCE_ILLEGAL_NAMES;
  XrdOucEnv access_Env(ininfo);
  AUTHORIZE(client, &access_Env, AOP_Stat, "access", inpath, error);
  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);
  BOUNCE_NOT_ALLOWED;
  return _attr_ls(path, error, vid, ininfo, map);
}

//------------------------------------------------------------------------------
// List extended attributes for a given file/directory - low-level API.
//------------------------------------------------------------------------------
int
XrdMgmOfs::_attr_ls(const char* path, XrdOucErrInfo& error,
                    const eos::common::VirtualIdentity& vid,
                    const char* info, eos::IContainerMD::XAttrMap& map,
                    bool take_lock, bool links)
{
  static const char* epname = "attr_ls";
  std::shared_ptr<eos::IContainerMD> dh;
  EXEC_TIMING_BEGIN("AttrLs");
  gOFS->MgmStats.Add("AttrLs", vid.uid, vid.gid, 1);
  eos::common::RWMutexReadLock ns_rd_lock;
  errno = 0;
  eos::Prefetcher::prefetchItemAndWait(gOFS->eosView, path);

  if (take_lock) {
    ns_rd_lock.Grab(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__, __FILE__);
  }

  try {
    eos::FileOrContainerMD item = gOFS->eosView->getItem(path).get();
    listAttributes(gOFS->eosView, item, map, links);
  } catch (eos::MDException& e) {
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),
              e.getMessage().str().c_str());
  }

  EXEC_TIMING_END("AttrLs");

  if (errno) {
    return Emsg(epname, error, errno, "list attributes", path);
  }

  return SFS_OK;
}


//------------------------------------------------------------------------------
// Set an extended attribute for a given file/directory - high-level API.
//------------------------------------------------------------------------------
int
XrdMgmOfs::attr_set(const char* inpath, XrdOucErrInfo& error,
                    const XrdSecEntity* client, const char* ininfo,
                    const char* key, const char* value)
{
  static const char* epname = "attr_set";
  const char* tident = error.getErrUser();
  // use a thread private vid
  eos::common::VirtualIdentity vid;
  EXEC_TIMING_BEGIN("IdMap");
  eos::common::Mapping::IdMap(client, ininfo, tident, vid);
  EXEC_TIMING_END("IdMap");
  NAMESPACEMAP;
  BOUNCE_ILLEGAL_NAMES;
  XrdOucEnv access_Env(ininfo);
  AUTHORIZE(client, &access_Env, AOP_Update, "update", inpath, error);
  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);
  BOUNCE_NOT_ALLOWED;
  return _attr_set(path, error, vid, ininfo, key, value);
}

//------------------------------------------------------------------------------
// Set an extended attribute for a given file/directory - low-level API.
//------------------------------------------------------------------------------
int
XrdMgmOfs::_attr_set(const char* path, XrdOucErrInfo& error,
                     eos::common::VirtualIdentity& vid,
                     const char* info, const char* key, const char* value,
                     bool take_lock)
{
  static const char* epname = "attr_set";
  EXEC_TIMING_BEGIN("AttrSet");
  gOFS->MgmStats.Add("AttrSet", vid.uid, vid.gid, 1);
  errno = 0;

  if (!key || !value) {
    errno = EINVAL;
    return Emsg(epname, error, errno, "set attribute", path);
  }

  XrdOucString Key = key;

  // Never put any attribute on version directories
  if (strstr(path, EOS_COMMON_PATH_VERSION_PREFIX) != 0) {
    return SFS_OK;
  }

  std::shared_ptr<eos::IContainerMD> dh;
  eos::common::RWMutexWriteLock ns_wr_lock;
  eos::Prefetcher::prefetchContainerMDAndWait(gOFS->eosView, path);

  if (take_lock) {
    ns_wr_lock.Grab(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__, __FILE__);
  }

  try {
    dh = gOFS->eosView->getContainer(path);
    eos::IContainerMD::XAttrMap xattrmap = dh->getAttributes();

    // get the ACL for this directory
    Acl acl(xattrmap, vid);

    // Check permissions in case of user attributes
    if (dh &&
	// any attribute not if not the owner and not a sudoer and not root
	(vid.uid != dh->getCUid()) && (!vid.sudoer && vid.uid) ||
	// sys.acl only by non sudoer/root if ACL allows
	((Key == "sys.acl") && (!vid.sudoer) && (vid.uid) && (!acl.CanSetAcl()))) {
      errno = EPERM;
    } else {
      XrdOucString val64 = value;
      XrdOucString ouc_val;
      eos::common::SymKey::DeBase64(val64, ouc_val);
      std::string val = ouc_val.c_str();
      Acl::AclType aclType;
      if ( (aclType = Acl::GetType(std::string(Key.c_str()))) != Acl::eNoAcl) {

        // Check format of acl
        if (!Acl::IsValid(val, error, aclType) &&
            !Acl::IsValid(val, error, aclType, true)) {
          errno = EINVAL;
          return Emsg(epname, error, errno, "set attribute", path);
        }

        // Convert to numeric representation
        if ( (aclType != Acl::eShareAcl) && Acl::ConvertIds(val)) {
          errno = EINVAL;
          return Emsg(epname, error, errno, "set attribute (failed id conver)", path);
        }
      }

      dh->setAttribute(key, val.c_str());

      if (Key != "sys.tmp.etag") {
        dh->setCTimeNow();
      }

      eosView->updateContainerStore(dh.get());
      eos::ContainerIdentifier d_id = dh->getIdentifier();
      eos::ContainerIdentifier d_pid = dh->getParentIdentifier();

      if (take_lock) {
        ns_wr_lock.Release();
      }

      gOFS->FuseXCastContainer(d_id);
      gOFS->FuseXCastRefresh(d_id, d_pid);
      errno = 0;
    }
  } catch (eos::MDException& e) {
    dh.reset();
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
              e.getErrno(), e.getMessage().str().c_str());
  }

  if (!dh) {
    if (take_lock) {
      ns_wr_lock.Release();
    }

    // pre-fetch file and parent
    eos::Prefetcher::prefetchFileMDAndWait(gOFS->eosView, path);

    if (take_lock) {
      ns_wr_lock.Grab(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__, __FILE__);
    }

    std::shared_ptr<eos::IFileMD> fmd;

    try {
      fmd = gOFS->eosView->getFile(path);
      eos::common::Path pPath(path);
      dh = gOFS->eosView->getContainer(pPath.GetParentPath());

      eos::IContainerMD::XAttrMap xattrmap = dh->getAttributes();

      Acl acl(xattrmap, vid);

      if (fmd &&
	  // any attribute not if not the owner and not a sudoer and not root
	  (vid.uid != fmd->getCUid()) && (!vid.sudoer && vid.uid) ||
	  // sys.acl only by non sudoer/root if ACL allows
	  ((Key == "sys.acl") && (!vid.sudoer) && (vid.uid) && (!acl.CanSetAcl()))) {
        errno = EPERM;
      } else {
        XrdOucString val64 = value;
        XrdOucString val;
        eos::common::SymKey::DeBase64(val64, val);
        fmd->setAttribute(key, val.c_str());

        if (Key != "sys.tmp.etag") {
          fmd->setCTimeNow();
        }

        eosView->updateFileStore(fmd.get());
        eos::FileIdentifier f_id = fmd->getIdentifier();

        if (take_lock) {
          ns_wr_lock.Release();
        }

        gOFS->FuseXCastFile(f_id);
        errno = 0;
      }
    } catch (eos::MDException& e) {
      fmd.reset();
      errno = e.getErrno();
      eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                e.getErrno(), e.getMessage().str().c_str());
    }
  }

  EXEC_TIMING_END("AttrSet");

  if (errno) {
    return Emsg(epname, error, errno, "set attributes", path);
  }

  return SFS_OK;
}

//------------------------------------------------------------------------------
// Get an extended attribute for a given entry by key - high-level API.
//------------------------------------------------------------------------------
int
XrdMgmOfs::attr_get(const char* inpath, XrdOucErrInfo& error,
                    const XrdSecEntity* client, const char* ininfo,
                    const char* key, XrdOucString& value)
{
  static const char* epname = "attr_get";
  const char* tident = error.getErrUser();
  // use a thread private vid
  eos::common::VirtualIdentity vid;
  EXEC_TIMING_BEGIN("IdMap");
  eos::common::Mapping::IdMap(client, ininfo, tident, vid);
  EXEC_TIMING_END("IdMap");
  NAMESPACEMAP;
  BOUNCE_ILLEGAL_NAMES;
  XrdOucEnv access_Env(ininfo);
  AUTHORIZE(client, &access_Env, AOP_Stat, "access", inpath, error);
  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);
  BOUNCE_NOT_ALLOWED;
  return _attr_get(path, error, vid, ininfo, key, value);
}

//------------------------------------------------------------------------------
// Get an extended attribute for a given entry by key - low-level API.
//------------------------------------------------------------------------------
int
XrdMgmOfs::_attr_get(const char* path, XrdOucErrInfo& error,
                     eos::common::VirtualIdentity& vid,
                     const char* info, const char* key, XrdOucString& value,
                     bool take_lock)
{
  static const char* epname = "attr_get";
  std::shared_ptr<eos::IContainerMD> dh;
  EXEC_TIMING_BEGIN("AttrGet");
  gOFS->MgmStats.Add("AttrGet", vid.uid, vid.gid, 1);
  errno = 0;

  if (!key) {
    return Emsg(epname, error, EINVAL, "get attribute", path);
  }

  value = "";
  XrdOucString link;
  bool b64 = false;

  if (info) {
    XrdOucEnv env(info);

    if (env.Get("eos.attr.val.encoding") &&
        (std::string(env.Get("eos.attr.val.encoding")) == "base64")) {
      b64 = true;
    }
  }

  eos::Prefetcher::prefetchContainerMDAndWait(gOFS->eosView, path);
  eos::common::RWMutexReadLock viewReadLock;

  if (take_lock) {
    viewReadLock.Grab(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__, __FILE__);
  }

  try {
    dh = gOFS->eosView->getContainer(path);
    value = (dh->getAttribute(key)).c_str();
  } catch (eos::MDException& e) {
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
              e.getErrno(), e.getMessage().str().c_str());
  }

  if (dh && errno) {
    // try linked attributes
    try {
      std::string lkey = "sys.attr.link";
      link = (dh->getAttribute(lkey)).c_str();
      dh = gOFS->eosView->getContainer(link.c_str());
      value = (dh->getAttribute(key)).c_str();
      errno = 0;
    } catch (eos::MDException& e) {
      dh.reset();
      errno = e.getErrno();
      eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                e.getErrno(), e.getMessage().str().c_str());
    }
  }

  if (!dh) {
    std::shared_ptr<eos::IFileMD> fmd;

    try {
      fmd = gOFS->eosView->getFile(path);
      value = (fmd->getAttribute(key)).c_str();
      errno = 0;
    } catch (eos::MDException& e) {
      errno = e.getErrno();
      eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                e.getErrno(), e.getMessage().str().c_str());
    }
  }

  viewReadLock.Release();
  // we always decode attributes here, even if they are stored as base64:
  XrdOucString val64 = value;
  eos::common::SymKey::DeBase64(val64, value);

  if (b64) {
    // on request do base64 encoding
    XrdOucString nb64 = value;
    eos::common::SymKey::Base64(nb64, value);
  }

  EXEC_TIMING_END("AttrGet");

  if (errno) {
    return Emsg(epname, error, errno, "get attributes", path);
  }

  return SFS_OK;
}


//------------------------------------------------------------------------------
// Get extended attribute for a given md object - low-level API.
//------------------------------------------------------------------------------
template<typename T>
static bool attrGetInternal(T& md, std::string key, std::string& rvalue)
{
  //------------------------------------------------------------------------------
  // First, check if the cmd itself contains the attribute.
  //------------------------------------------------------------------------------
  if (md.hasAttribute(key)) {
    rvalue = md.getAttribute(key);
    return true;
  }

  //----------------------------------------------------------------------------
  // Nope.. does the fmd have linked attributes?
  //----------------------------------------------------------------------------
  const std::string kMagicKey = "sys.attr.link";

  if (!md.hasAttribute(kMagicKey)) {
    // Nope
    return false;
  }

  //----------------------------------------------------------------------------
  // It does, fetch linked container
  //----------------------------------------------------------------------------
  std::string linkedContainer = md.getAttribute(kMagicKey);
  std::shared_ptr<eos::IContainerMD> dh;
  eos::Prefetcher::prefetchContainerMDAndWait(gOFS->eosView, linkedContainer);
  eos::common::RWMutexReadLock nsLock(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__, __FILE__);

  try {
    dh = gOFS->eosView->getContainer(linkedContainer.c_str());
  } catch (eos::MDException& e) {
    errno = e.getErrno();
    eos_static_err("msg=\"exception while following linked container\" ec=%d emsg=\"%s\"\n",
                   e.getErrno(), e.getMessage().str().c_str());
    return false;
  }

  //----------------------------------------------------------------------------
  // We have the linked container, lookup.
  //----------------------------------------------------------------------------
  if (!dh->hasAttribute(key)) {
    return false;
  }

  rvalue = dh->getAttribute(key);
  return true;
}

//------------------------------------------------------------------------------
// Get extended attribute for a given cmd - low-level API.
//------------------------------------------------------------------------------
bool
XrdMgmOfs::_attr_get(eos::IContainerMD& cmd, std::string key,
                     std::string& rvalue)
{
  return attrGetInternal(cmd, key, rvalue);
}

//------------------------------------------------------------------------------
// Get extended attribute for a given fmd - low-level API.
//------------------------------------------------------------------------------
bool
XrdMgmOfs::_attr_get(eos::IFileMD& fmd, std::string key, std::string& rvalue)
{
  return attrGetInternal(fmd, key, rvalue);
}

//------------------------------------------------------------------------------
// Remove an extended attribute for a given entry - high-level API.
//------------------------------------------------------------------------------
int
XrdMgmOfs::attr_rem(const char* inpath, XrdOucErrInfo& error,
                    const XrdSecEntity* client, const char* ininfo,
                    const char* key)
{
  static const char* epname = "attr_rm";
  const char* tident = error.getErrUser();
  // use a thread private vid
  eos::common::VirtualIdentity vid;
  EXEC_TIMING_BEGIN("IdMap");
  eos::common::Mapping::IdMap(client, ininfo, tident, vid);
  EXEC_TIMING_END("IdMap");
  NAMESPACEMAP;
  BOUNCE_ILLEGAL_NAMES;
  XrdOucEnv access_Env(ininfo);
  AUTHORIZE(client, &access_Env, AOP_Delete, "delete", inpath, error);
  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);
  BOUNCE_NOT_ALLOWED;
  return _attr_rem(path, error, vid, ininfo, key);
}

//------------------------------------------------------------------------------
// Remove an extended attribute for a given entry - low-level API.
//------------------------------------------------------------------------------
int
XrdMgmOfs::_attr_rem(const char* path, XrdOucErrInfo& error,
                     eos::common::VirtualIdentity& vid,
                     const char* info, const char* key)

{
  static const char* epname = "attr_rm";
  std::shared_ptr<eos::IContainerMD> dh;
  std::shared_ptr<eos::IFileMD> fmd;
  errno = 0;
  EXEC_TIMING_BEGIN("AttrRm");
  gOFS->MgmStats.Add("AttrRm", vid.uid, vid.gid, 1);

  if (!key) {
    return Emsg(epname, error, EINVAL, "delete attribute", path);
  }

  eos::Prefetcher::prefetchContainerMDAndWait(gOFS->eosView, path);
  eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__, __FILE__);

  try {
    dh = gOFS->eosView->getContainer(path);
    XrdOucString Key = key;

    if (Key.beginswith("sys.") && ((!vid.sudoer) && (vid.uid))) {
      errno = EPERM;
    } else {
      // TODO: REVIEW: check permissions
      if (dh && (!dh->access(vid.uid, vid.gid, X_OK | W_OK))) {
        errno = EPERM;
      } else {
        if (dh->hasAttribute(key)) {
          dh->removeAttribute(key);
          eosView->updateContainerStore(dh.get());
          eos::ContainerIdentifier d_id = dh->getIdentifier();
          eos::ContainerIdentifier d_pid = dh->getParentIdentifier();
          lock.Release();
          gOFS->FuseXCastContainer(d_id);
          gOFS->FuseXCastRefresh(d_id, d_pid);
        } else {
          errno = ENODATA;
        }
      }
    }
  } catch (eos::MDException& e) {
    dh.reset();
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),
              e.getMessage().str().c_str());
  }

  if (!dh) {
    try {
      fmd = gOFS->eosView->getFile(path);
      XrdOucString Key = key;

      if (Key.beginswith("sys.") && ((!vid.sudoer) && (vid.uid))) {
        errno = EPERM;
      } else {
        // check permissions
        if (vid.uid && (fmd->getCUid() != vid.uid)) {
          // TODO: REVIEW: only owner can set file attributes
          errno = EPERM;
        } else {
          if (fmd->hasAttribute(key)) {
            fmd->removeAttribute(key);
            eosView->updateFileStore(fmd.get());
            eos::FileIdentifier f_id = fmd->getIdentifier();
            lock.Release();
            gOFS->FuseXCastFile(f_id);
            errno = 0;
          } else {
            errno = ENODATA;
          }
        }
      }
    } catch (eos::MDException& e) {
      dh.reset();
      errno = e.getErrno();
      eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),
                e.getMessage().str().c_str());
    }
  }

  EXEC_TIMING_END("AttrRm");

  if (errno) {
    return Emsg(epname, error, errno, "remove attribute", path);
  }

  return SFS_OK;
}

//------------------------------------------------------------------------------
// Remove all extended attributes for a given file/directory - low-level API.
//------------------------------------------------------------------------------
int
XrdMgmOfs::_attr_clear(const char* path, XrdOucErrInfo& error,
                       eos::common::VirtualIdentity& vid,
                       const char* info, 
		       bool keep_acls)

{
  eos::IContainerMD::XAttrMap map;

  if (_attr_ls(path, error, vid, info, map)) {
    return SFS_ERROR;
  }

  int success = SFS_OK;

  for (auto it = map.begin(); it != map.end(); ++it) {
    if ( keep_acls && (
		       (it->first == "sys.acl") ||
		       (it->first == "user.acl") )) {
      continue;
    }
    success |= _attr_rem(path, error, vid, info, it->first.c_str());
  }

  return success;
}
