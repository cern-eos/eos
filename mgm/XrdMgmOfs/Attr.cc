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

auto constexpr kAttrObfuscateKey = "user.obfuscate.key";

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
  eos::common::VirtualIdentity vid;
  EXEC_TIMING_BEGIN("IdMap");
  eos::common::Mapping::IdMap(client, ininfo, tident, vid, gOFS->mTokenAuthz,
                              AOP_Read, inpath);
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
                    bool links)
{
  static const char* epname = "attr_ls";
  std::shared_ptr<eos::IContainerMD> dh;
  EXEC_TIMING_BEGIN("AttrLs");
  gOFS->MgmStats.Add("AttrLs", vid.uid, vid.gid, 1);
  errno = 0;
  eos::Prefetcher::prefetchItemAndWait(gOFS->eosView, path);

  try {
    eos::FileOrContainerMD item = gOFS->eosView->getItem(path).get();
    listAttributes(gOFS->eosView, item, map, links);
    // we never show obfuscate key
    map.erase(kAttrObfuscateKey);
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
// Get an extended attribute for a given entry by key - high-level API.
//------------------------------------------------------------------------------
int
XrdMgmOfs::attr_get(const char* inpath, XrdOucErrInfo& error,
                    const XrdSecEntity* client, const char* ininfo,
                    const char* key, std::string& value)
{
  static const char* epname = "attr_get";
  const char* tident = error.getErrUser();
  eos::common::VirtualIdentity vid;
  EXEC_TIMING_BEGIN("IdMap");
  eos::common::Mapping::IdMap(client, ininfo, tident, vid, gOFS->mTokenAuthz,
                              AOP_Read, inpath);
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
// Get extended attribute for a given cmd - low-level API.
//------------------------------------------------------------------------------
bool
XrdMgmOfs::_attr_get(eos::IContainerMD& cmd, std::string key,
                     std::string& rvalue)
{
  return getAttribute(gOFS->eosView, cmd, key, rvalue);
}

//------------------------------------------------------------------------------
// Get extended attribute for a given fmd - low-level API.
//------------------------------------------------------------------------------
bool
XrdMgmOfs::_attr_get(eos::IFileMD& fmd, std::string key, std::string& rvalue)
{
  return getAttribute(gOFS->eosView, fmd, key, rvalue);
}

//------------------------------------------------------------------------------
// Get an extended attribute for a given entry by key - low-level API.
//------------------------------------------------------------------------------
int
XrdMgmOfs::_attr_get(const char* path, XrdOucErrInfo& error,
                     eos::common::VirtualIdentity& vid,
                     const char* info, const char* key, std::string& value)
{
  static const char* epname = "attr_get";
  std::shared_ptr<eos::IContainerMD> dh;
  EXEC_TIMING_BEGIN("AttrGet");
  gOFS->MgmStats.Add("AttrGet", vid.uid, vid.gid, 1);
  errno = 0;
  value.clear();

  if (!key) {
    return Emsg(epname, error, EINVAL, "get attribute", path);
  }

  // Never return the obfuscate key
  if (key == kAttrObfuscateKey) {
    return SFS_OK;
  }

  eos::Prefetcher::prefetchItemAndWait(gOFS->eosView, path);

  try {
    eos::FileOrContainerMD item = gOFS->eosView->getItem(path).get();

    if (item.file) {
      eos::IFileMD::IFileMDReadLocker tmp(item.file);

      if (!_attr_get(*item.file.get(), key, value)) {
        errno = ENODATA;
      }
    } else {
      eos::IContainerMD::IContainerMDReadLocker tmp(item.container);

      if (!_attr_get(*item.container.get(), key, value)) {
        errno = ENODATA;
      }
    }
  } catch (eos::MDException& e) {
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),
              e.getMessage().str().c_str());
  }

  EXEC_TIMING_END("AttrGet");

  if (errno) {
    return Emsg(epname, error, errno, "get attributes", path);
  }

  // Always decode attribute, even if they are not stores as base64
  std::string val64 = value;
  eos::common::SymKey::DeBase64(val64, value);

  if (info) {
    // Check if base64 encoding is requested
    XrdOucEnv env(info);
    const char* ptr = env.Get("eos.attr.val.encoding");

    if (ptr && (strncmp(ptr, "base64", 6) == 0)) {
      std::string nb64 = value;
      eos::common::SymKey::Base64(nb64, value);
    }
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
  eos::common::VirtualIdentity vid;
  EXEC_TIMING_BEGIN("IdMap");
  eos::common::Mapping::IdMap(client, ininfo, tident, vid, gOFS->mTokenAuthz,
                              AOP_Update, inpath);
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
                     bool exclusive)
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

  if (Key.beginswith("sys.") && (!vid.sudoer && vid.uid)) {
    errno = EPERM;
    return Emsg(epname, error, errno, "set attribute", path);
  }

  // Never put any attribute on version directories
  if ((strstr(path, EOS_COMMON_PATH_VERSION_PREFIX) != 0) &&
      ((Key.beginswith("sys.forced")) || (Key.beginswith("user.forced")))) {
    return SFS_OK;
  }

  std::shared_ptr<eos::IContainerMD> dh;
  eos::IContainerMD::IContainerMDWriteLockerPtr dhLock;
  eos::Prefetcher::prefetchContainerMDAndWait(gOFS->eosView, path);

  try {
    dhLock = gOFS->eosView->getContainerWriteLocked(path);
    dh = dhLock->getUnderlyingPtr();

    // Check permissions in case of user attributes
    if (!Key.beginswith("sys.") && (vid.uid != dh->getCUid())
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
          return Emsg(epname, error, errno, "set attribute (invalid acl format)", path);
        }

        // Convert to numeric representation
        if (Acl::ConvertIds(val)) {
          errno = EINVAL;
          return Emsg(epname, error, errno, "set attribute (failed id convert)", path);
        }
      }

      if (exclusive && dh->hasAttribute(Key.c_str())) {
        errno = EEXIST;
        return Emsg(epname, error, errno,
                    "set attribute (exclusive set for existing attribute)", path);
      }

      dh->setAttribute(key, val.c_str());

      if (Key != "sys.tmp.etag") {
        dh->setCTimeNow();
      }

      eos::ContainerIdentifier d_id = dh->getIdentifier();
      eos::ContainerIdentifier d_pid = dh->getParentIdentifier();
      eosView->updateContainerStore(dh.get());
      // Release the current lock on the object before broadcasting to fuse
      dhLock.reset(nullptr);
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
    std::shared_ptr<eos::IFileMD> fmd;
    eos::IFileMD::IFileMDWriteLockerPtr fmdLock;

    try {
      fmdLock = gOFS->eosView->getFileWriteLocked(path);
      fmd = fmdLock->getUnderlyingPtr();

      // Check permissions in case of user attributes
      if (!Key.beginswith("sys.") && (vid.uid != fmd->getCUid())
          && (!vid.sudoer && vid.uid)) {
        errno = EPERM;
      } else {
        if (exclusive && fmd->hasAttribute(Key.c_str())) {
          errno = EEXIST;
          return Emsg(epname, error, errno,
                      "set attribute (exclusive set for existing attribute)", path);
        }

        // screen for attribute locks
        if (Key == eos::common::EOS_APP_LOCK_ATTR) {
          errno = 0;
          eos::IContainerMD::XAttrMap map = fmd->getAttributes();
          XattrLock applock(map);

          if (applock.foreignLock(vid, true)) {
            errno = EBUSY;
            return Emsg(epname, error, errno,
                        "set attribute (foreign attribute lock existing)", path);
          }
        }

        XrdOucString val64 = value;
        XrdOucString val;
        eos::common::SymKey::DeBase64(val64, val);
        fmd->setAttribute(key, val.c_str());

        if (Key != "sys.tmp.etag") {
          fmd->setCTimeNow();
        }

        eos::FileIdentifier f_id = fmd->getIdentifier();
        eos::ContainerIdentifier c_id = eos::ContainerIdentifier(fmd->getContainerId());
        eosView->updateFileStore(fmd.get());
        // Release the current lock on the object before broadcasting to fuse
        fmdLock.reset(nullptr);
        gOFS->FuseXCastRefresh(f_id, c_id);
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
  eos::common::Mapping::IdMap(client, ininfo, tident, vid, gOFS->mTokenAuthz,
                              AOP_Update, inpath);
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

  try {
    auto dhLock = gOFS->eosView->getContainerWriteLocked(path);
    dh = dhLock->getUnderlyingPtr();
    XrdOucString Key = key;

    if (Key.beginswith("sys.") && ((!vid.sudoer) && (vid.uid))) {
      errno = EPERM;
    } else {
      // TODO: REVIEW: check permissions
      if (!dh->access(vid.uid, vid.gid, X_OK | W_OK)) {
        errno = EPERM;
      } else {
        if (dh->hasAttribute(key)) {
          dh->removeAttribute(key);
          eos::ContainerIdentifier d_id = dh->getIdentifier();
          eos::ContainerIdentifier d_pid = dh->getParentIdentifier();
          eosView->updateContainerStore(dh.get());
          // Release object lock before doing the fuse refresh
          dhLock.reset(nullptr);
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
      auto fmdLock = gOFS->eosView->getFileWriteLocked(path);
      fmd = fmdLock->getUnderlyingPtr();
      XrdOucString Key = key;

      if (Key.beginswith("sys.") && ((!vid.sudoer) && (vid.uid))) {
        errno = EPERM;
      } else {
        if ((vid.uid != fmd->getCUid())
            && (!vid.sudoer && vid.uid)) {
          // TODO: REVIEW: only owner/sudoer can delete file attributes
          errno = EPERM;
        } else {
          if (fmd->hasAttribute(key)) {
            fmd->removeAttribute(key);
            eosView->updateFileStore(fmd.get());
            eos::FileIdentifier f_id = fmd->getIdentifier();
            eos::ContainerIdentifier d_id(fmd->getContainerId());
            fmdLock.reset(nullptr);
            gOFS->FuseXCastRefresh(f_id, d_id);
            errno = 0;
          } else {
            errno = ENODATA;
          }
        }
      }
    } catch (eos::MDException& e) {
      fmd.reset();
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
    if (keep_acls && (
          (it->first == "sys.acl") ||
          (it->first == "user.acl"))) {
      continue;
    }

    success |= _attr_rem(path, error, vid, info, it->first.c_str());
  }

  return success;
}
