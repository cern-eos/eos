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
  eos::common::Mapping::VirtualIdentity vid;
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
                    eos::common::Mapping::VirtualIdentity& vid,
                    const char* info, eos::IContainerMD::XAttrMap& map,
                    bool take_lock, bool links)
{
  static const char* epname = "attr_ls";
  std::shared_ptr<eos::IContainerMD> dh;
  EXEC_TIMING_BEGIN("AttrLs");
  gOFS->MgmStats.Add("AttrLs", vid.uid, vid.gid, 1);
  eos::common::RWMutexReadLock ns_rd_lock;
  errno = 0;

  if (take_lock) {
    ns_rd_lock.Grab(gOFS->eosViewRWMutex);
  }

  try {
    dh = gOFS->eosView->getContainer(path);
    map = dh->getAttributes();
  } catch (eos::MDException& e) {
    dh.reset();
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),
              e.getMessage().str().c_str());
  }

  if (!dh) {
    std::shared_ptr<eos::IFileMD> fmd;

    try {
      fmd = gOFS->eosView->getFile(path);
      map = fmd->getAttributes();
      errno = 0;
    } catch (eos::MDException& e) {
      fmd.reset();
      errno = e.getErrno();
      eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),
                e.getMessage().str().c_str());
    }
  }

  // check for attribute references
  if (map.count("sys.attr.link")) {
    try {
      dh = gOFS->eosView->getContainer(map["sys.attr.link"]);
      eos::IFileMD::XAttrMap xattrs = dh->getAttributes();

      for (const auto& elem : xattrs) {
        XrdOucString key = elem.first.c_str();

        if (links) {
          key.replace("sys.", "sys.link.");
        }

        if (!map.count(elem.first)) {
          map[key.c_str()] = elem.second;
        }
      }
    } catch (eos::MDException& e) {
      dh.reset();
      std::string msg = map["sys.attr.link"];
      msg += " - not found";
      map["sys.attr.link"] = msg;
      eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),
                e.getMessage().str().c_str());
    }
  }

  EXEC_TIMING_END("AttrLs");

  if (errno) {
    return Emsg(epname, error, errno, "list attributes", path);
  }

  return SFS_OK;
}


//------------------------------------------------------------------------------
// Set an extended attribute for a given file/directory - high-level API.
//-----------------------------------------------------------------------------
int
XrdMgmOfs::attr_set(const char* inpath, XrdOucErrInfo& error,
                    const XrdSecEntity* client, const char* ininfo,
                    const char* key, const char* value)
{
  static const char* epname = "attr_set";
  const char* tident = error.getErrUser();
  // use a thread private vid
  eos::common::Mapping::VirtualIdentity vid;
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
                     eos::common::Mapping::VirtualIdentity& vid,
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

  if (Key.beginswith("sys.") && ((!vid.sudoer) && (vid.uid))) {
    errno = EPERM;
    return Emsg(epname, error, errno, "set attribute", path);
  }

  // Never put any attribute on version directories
  if (strstr(path, EOS_COMMON_PATH_VERSION_PREFIX) != 0) {
    return SFS_OK;
  }

  std::shared_ptr<eos::IContainerMD> dh;
  eos::common::RWMutexWriteLock ns_wr_lock;

  if (take_lock) {
    ns_wr_lock.Grab(gOFS->eosViewRWMutex);
  }

  try {
    dh = gOFS->eosView->getContainer(path);

    // Check permissions in case of user attributes
    if (dh && !Key.beginswith("sys.") && (vid.uid != dh->getCUid())
        && (!vid.sudoer && vid.uid)) {
      errno = EPERM;
    } else {
      XrdOucString val64 = value;
      XrdOucString ouc_val;
      eos::common::SymKey::DeBase64(val64, ouc_val);
      std::string val = ouc_val.c_str();

      if (Key.beginswith("sys.acl") || Key.beginswith("user.acl")) {
        bool is_sys_acl = Key.beginswith("sys.acl");

        // Check format of acl
        if (!Acl::IsValid(val, error, is_sys_acl) &&
            !Acl::IsValid(val, error, is_sys_acl, true)) {
          errno = EINVAL;
          return Emsg(epname, error, errno, "set attribute", path);
        }

        // Convert to numeric representation
        Acl::ConvertIds(val);
      }

      dh->setAttribute(key, val.c_str());
      dh->setMTimeNow();
      dh->notifyMTimeChange(gOFS->eosDirectoryService);
      eosView->updateContainerStore(dh.get());
      gOFS->FuseXCast(dh->getId());
      errno = 0;
    }
  } catch (eos::MDException& e) {
    dh.reset();
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
              e.getErrno(), e.getMessage().str().c_str());
  }

  if (!dh) {
    std::shared_ptr<eos::IFileMD> fmd;

    try {
      fmd = gOFS->eosView->getFile(path);

      // Check permissions in case of user attributes
      if (fmd && !Key.beginswith("sys.") && (vid.uid != fmd->getCUid())
          && (!vid.sudoer && vid.uid)) {
        errno = EPERM;
      } else {
        XrdOucString val64 = value;
        XrdOucString val;
        eos::common::SymKey::DeBase64(val64, val);
        fmd->setAttribute(key, val.c_str());
        fmd->setMTimeNow();
        eosView->updateFileStore(fmd.get());
        gOFS->FuseXCast(eos::common::FileId::FidToInode(fmd->getId()));
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
  eos::common::Mapping::VirtualIdentity vid;
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
                     eos::common::Mapping::VirtualIdentity& vid,
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

  if (take_lock) {
    gOFS->eosViewRWMutex.LockRead();
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

  if (take_lock) {
    gOFS->eosViewRWMutex.UnLockRead();
  }

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
static bool attrGetInternal(T &md, std::string key, std::string& rvalue) {
  //------------------------------------------------------------------------------
  // First, check if the cmd itself contains the attribute.
  //------------------------------------------------------------------------------
  if(md.hasAttribute(key)) {
    rvalue = md.getAttribute(key);
    return true;
  }

  //----------------------------------------------------------------------------
  // Nope.. does the fmd have linked attributes?
  //----------------------------------------------------------------------------
  const std::string kMagicKey = "sys.attr.link";
  if(!md.hasAttribute(kMagicKey)) {
    // Nope
    return false;
  }

  //----------------------------------------------------------------------------
  // It does, fetch linked container
  //----------------------------------------------------------------------------
  std::string linkedContainer = md.getAttribute(kMagicKey);
  std::shared_ptr<eos::IContainerMD> dh;
  eos::common::RWMutexReadLock nsLock(gOFS->eosViewRWMutex);

  try {
    dh = gOFS->eosView->getContainer(linkedContainer.c_str());
  }
  catch(eos::MDException &e) {
    errno = e.getErrno();
    eos_static_err("msg=\"exception while following linked container\" ec=%d emsg=\"%s\"\n",
              e.getErrno(), e.getMessage().str().c_str());
    return false;
  }

  nsLock.Release();

  //----------------------------------------------------------------------------
  // We have the linked container, lookup.
  //----------------------------------------------------------------------------
  if(!dh->hasAttribute(key)) {
    return false;
  }

  rvalue = dh->getAttribute(key);
  return true;
}

//------------------------------------------------------------------------------
// Get extended attribute for a given cmd - low-level API.
//------------------------------------------------------------------------------
bool
XrdMgmOfs::_attr_get(eos::IContainerMD &cmd, std::string key, std::string& rvalue) {
  return attrGetInternal(cmd, key, rvalue);
}

//------------------------------------------------------------------------------
// Get extended attribute for a given fmd - low-level API.
//------------------------------------------------------------------------------
bool
XrdMgmOfs::_attr_get(eos::IFileMD &fmd, std::string key, std::string& rvalue) {
  return attrGetInternal(fmd, key, rvalue);
}

//------------------------------------------------------------------------------
// Get extended attribute for a given inode - low-level API.
//------------------------------------------------------------------------------
bool
XrdMgmOfs::_attr_get(uint64_t cid, std::string key, std::string& rvalue)
{
  std::shared_ptr<eos::IContainerMD> dh;
  std::shared_ptr<eos::IFileMD> fmd;
  errno = 0;
  EXEC_TIMING_BEGIN("AttrGet");
  gOFS->MgmStats.Add("AttrGet", vid.uid, vid.gid, 1);

  if (!key.length()) {
    return false;
  }

  XrdOucString value = "";
  XrdOucString link;
  {
    eos::common::RWMutexReadLock nsLock(gOFS->eosViewRWMutex);

    try {
      dh = gOFS->eosDirectoryService->getContainerMD(cid);
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
      try {
        fmd = gOFS->eosFileService->getFileMD(cid);
        value = (fmd->getAttribute(key)).c_str();
        errno = 0;
      } catch (eos::MDException& e) {
        errno = e.getErrno();
        eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                  e.getErrno(), e.getMessage().str().c_str());
      }
    }
  }
  // Always decode attributes here, even if they are stored as base64:
  XrdOucString val64 = value;
  eos::common::SymKey::DeBase64(val64, value);
  rvalue = value.c_str();
  EXEC_TIMING_END("AttrGet");

  if (errno) {
    return false;
  }

  return true;
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
  eos::common::Mapping::VirtualIdentity vid;
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
                     eos::common::Mapping::VirtualIdentity& vid,
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

  eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);

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
                       eos::common::Mapping::VirtualIdentity& vid,
                       const char* info)

{
  eos::IContainerMD::XAttrMap map;

  if (_attr_ls(path, error, vid, info, map)) {
    return SFS_ERROR;
  }

  int success = SFS_OK;

  for (auto it = map.begin(); it != map.end(); ++it) {
    success |= _attr_rem(path, error, vid, info, it->first.c_str());
  }

  return success;
}
