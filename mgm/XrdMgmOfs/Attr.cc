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
  EXEC_TIMING_BEGIN("AttrLs");
  gOFS->MgmStats.Add("AttrLs", vid.uid, vid.gid, 1);
  errno = 0;
  eos::Prefetcher::prefetchItemAndWait(gOFS->eosView, path);

  try {
    eos::FileOrContainerMD item = gOFS->eosView->getItem(path).get();
    listAttributes(gOFS->eosView, item, map, links);
    // we never show obfuscate key
    map.erase(eos::kAttrObfuscateKey);
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
  EXEC_TIMING_BEGIN("AttrGet");
  gOFS->MgmStats.Add("AttrGet", vid.uid, vid.gid, 1);
  value.clear();
  errno = 0;

  if (!key || (strlen(key) == 0)) {
    errno = EINVAL;
    return Emsg(epname, error, errno, "get attribute", path);
  }

  const std::string skey = key;

  // Never return the obfuscate key
  if (skey == eos::kAttrObfuscateKey) {
    return SFS_OK;
  }

  eos::Prefetcher::prefetchItemAndWait(gOFS->eosView, path);

  try {
    eos::FileOrContainerMD item = gOFS->eosView->getItem(path).get();

    if (item.file) {
      std::shared_ptr<eos::IFileMD> fmd = item.file;
      eos::IFileMD::IFileMDReadLocker fmd_lock(fmd);

      if (!_attr_get(*fmd.get(), skey, value)) {
        errno = ENODATA;
      }
    } else {
      std::shared_ptr<eos::IContainerMD> cmd = item.container;
      eos::IContainerMD::IContainerMDReadLocker cmd_lock(cmd);

      if (!_attr_get(*cmd.get(), skey, value)) {
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

  if (!key || (strlen(key) == 0) ||
      !value || (strlen(value) == 0)) {
    errno = EINVAL;
    return Emsg(epname, error, errno, "set attribute", path);
  }

  const std::string skey = key;

  if ((skey.find("sys.") == 0) && (!vid.sudoer && vid.uid)) {
    errno = EPERM;
    return Emsg(epname, error, errno, "set attribute", path);
  }

  // Never put any attribute on version directories
  if ((strstr(path, EOS_COMMON_PATH_VERSION_PREFIX) != 0) &&
      ((skey.find("sys.forced") == 0) ||
       (skey.find("user.forced") == 0))) {
    return SFS_OK;
  }

  // Base64 decode if necessary i.e. input value is prefixed with "base64:"
  std::string raw_val;
  (void) eos::common::SymKey::DeBase64(value, raw_val);

  // For ACL attr then check validity and convert to numeric format
  if ((skey.find("sys.acl") == 0) || (skey.find("user.acl") == 0)) {
    bool is_sys_acl = (skey.find("sys.acl") == 0);

    // Check format of acl
    if (!Acl::IsValid(raw_val, error, is_sys_acl) &&
        !Acl::IsValid(raw_val, error, is_sys_acl, true)) {
      errno = EINVAL;
      return Emsg(epname, error, errno, "set attribute (invalid acl format)", path);
    }

    // Convert to numeric representation
    if (Acl::ConvertIds(raw_val)) {
      errno = EINVAL;
      return Emsg(epname, error, errno, "set attribute (failed id convert)", path);
    }
  }

  eos::Prefetcher::prefetchItemAndWait(gOFS->eosView, path);

  try {
    eos::FileOrContainerMD item = gOFS->eosView->getItem(path).get();

    if (item.file) { // file
      std::shared_ptr<eos::IFileMD> fmd = item.file;
      auto fmd_lock = std::make_unique<eos::IFileMD::IFileMDWriteLocker>(fmd);

      if ((vid.uid != fmd->getCUid()) && (!vid.sudoer && vid.uid)) {
        errno = EPERM;
      } else {
        if (exclusive && fmd->hasAttribute(skey)) {
          errno = EEXIST;
          return Emsg(epname, error, errno, "set attribute (exclusive set "
                      "for existing attribute)", path);
        }

        // Handle attribute for application locks
        if (skey == eos::common::EOS_APP_LOCK_ATTR) {
          errno = 0;
          eos::IContainerMD::XAttrMap xattr_map = fmd->getAttributes();
          XattrLock app_lock(xattr_map);

          if (app_lock.foreignLock(vid, true)) {
            errno = EBUSY;
            return Emsg(epname, error, errno, "set attribute "
                        "(foreign attribute lock existing)", path);
          }
        }

        fmd->setAttribute(skey, raw_val);

        if (skey != eos::kAttrTmpEtagKey) {
          fmd->setCTimeNow();
        }

        const eos::FileIdentifier f_id(fmd->getIdentifier());
        const eos::ContainerIdentifier c_id(fmd->getContainerId());
        eosView->updateFileStore(fmd.get());
        // Release the current lock on the object before broadcasting to fuse
        fmd_lock.reset(nullptr);
        gOFS->FuseXCastRefresh(f_id, c_id);
        errno = 0;
      }
    } else { // container
      std::shared_ptr<eos::IContainerMD> cmd = item.container;
      auto cmd_lock = std::make_unique<eos::IContainerMD::IContainerMDWriteLocker>
                      (cmd);

      if ((vid.uid != cmd->getCUid()) && (!vid.sudoer && vid.uid)) {
        errno = EPERM;
      } else {
        if (exclusive && cmd->hasAttribute(skey)) {
          errno = EEXIST;
          return Emsg(epname, error, errno, "set attribute (exclusive set "
                      "for existing attribute)", path);
        }

        cmd->setAttribute(skey, raw_val);

        if (skey != eos::kAttrTmpEtagKey) {
          cmd->setCTimeNow();
        }

        const eos::ContainerIdentifier d_id(cmd->getIdentifier());
        const eos::ContainerIdentifier d_pid(cmd->getParentIdentifier());
        eosView->updateContainerStore(cmd.get());
        // Release the current lock on the object before broadcasting to fuse
        cmd_lock.reset(nullptr);
        gOFS->FuseXCastRefresh(d_id, d_pid);
        errno = 0;
      }
    }
  } catch (eos::MDException& e) {
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
              e.getErrno(), e.getMessage().str().c_str());
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
  EXEC_TIMING_BEGIN("AttrRm");
  gOFS->MgmStats.Add("AttrRm", vid.uid, vid.gid, 1);
  errno = 0;

  if (!key || (strlen(key) == 0)) {
    errno = EINVAL;
    return Emsg(epname, error, errno, "delete attribute", path);
  }

  const std::string skey = key;

  if ((skey.find("sys.") == 0) && (!vid.sudoer && vid.uid)) {
    errno = EPERM;
    return Emsg(epname, error, errno, "set attribute", path);
  }

  eos::Prefetcher::prefetchItemAndWait(gOFS->eosView, path);

  try {
    eos::FileOrContainerMD item = gOFS->eosView->getItem(path).get();

    if (item.file) { // file
      std::shared_ptr<eos::IFileMD> fmd = item.file;
      auto fmd_lock = std::make_unique<eos::IFileMD::IFileMDWriteLocker>(fmd);

      if ((vid.uid != fmd->getCUid()) && (!vid.sudoer && vid.uid)) {
        errno = EPERM;
      } else {
        if (!fmd->hasAttribute(skey)) {
          errno = ENODATA;
        } else {
          fmd->removeAttribute(skey);
          eosView->updateFileStore(fmd.get());
          eos::FileIdentifier f_id = fmd->getIdentifier();
          eos::ContainerIdentifier d_id(fmd->getContainerId());
          // Release object lock before doing the fuse refresh
          fmd_lock.reset(nullptr);
          gOFS->FuseXCastRefresh(f_id, d_id);
          errno = 0;
        }
      }
    } else { // container
      std::shared_ptr<eos::IContainerMD> cmd = item.container;
      auto cmd_lock = std::make_unique<eos::IContainerMD::IContainerMDWriteLocker>
                      (cmd);

      if (!cmd->access(vid.uid, vid.gid, X_OK | W_OK)) {
        errno = EPERM;
      } else {
        if (!cmd->hasAttribute(skey)) {
          errno = ENODATA;
        } else {
          cmd->removeAttribute(skey);
          eos::ContainerIdentifier d_id = cmd->getIdentifier();
          eos::ContainerIdentifier d_pid = cmd->getParentIdentifier();
          eosView->updateContainerStore(cmd.get());
          // Release object lock before doing the fuse refresh
          cmd_lock.reset(nullptr);
          gOFS->FuseXCastRefresh(d_id, d_pid);
          errno = 0;
        }
      }
    }
  } catch (eos::MDException& e) {
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),
              e.getMessage().str().c_str());
  }

  EXEC_TIMING_END("AttrRm");

  if (errno) {
    return Emsg(epname, error, errno, "remove attribute", path);
  }

  return SFS_OK;
}
