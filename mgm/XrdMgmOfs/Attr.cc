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
// Get extended attribute for a given metadata object - low-level API.
//------------------------------------------------------------------------------
bool
XrdMgmOfs::_attr_get(eos::FileOrContainerMD& item, std::string key,
                     std::string& rvalue)
{
  if (item.file) {
    return getAttribute(gOFS->eosView, *item.file.get(), key, rvalue);
  } else if (item.container) {
    return getAttribute(gOFS->eosView, *item.container.get(), key, rvalue);
  }

  return false;
}

//------------------------------------------------------------------------------
// Get extended attribute for a given file - low-level API.
//------------------------------------------------------------------------------
bool
XrdMgmOfs::_attr_get(eos::IFileMD& fmd, std::string key, std::string& rvalue)
{
  return getAttribute(gOFS->eosView, fmd, key, rvalue);
}

//------------------------------------------------------------------------------
// Get extended attribute for a given container - low-level API.
//------------------------------------------------------------------------------
bool
XrdMgmOfs::_attr_get(eos::IContainerMD& cmd, std::string key,
                     std::string& rvalue)
{
  return gOFS->getAttribute(gOFS->eosView, cmd, key, rvalue);
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
  errno = 0;
  value.clear();

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
      eos::MDLocking::FileReadLock fmd_lock(fmd.get());

      if (!_attr_get(*fmd.get(), skey, value)) {
        errno = ENODATA;
      }
    } else {
      std::shared_ptr<eos::IContainerMD> cmd = item.container;
      eos::MDLocking::ContainerReadLock cmd_lock(cmd.get());

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

  if (!key || (strlen(key) == 0) || !value) {
    errno = EINVAL;
    return Emsg(epname, error, errno, "set attribute (missing/empty)", path);
  }

  const std::string skey = key;

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
      eos_static_err("msg=\"invalid acl value\" value=\"%s\"", raw_val.c_str());
      return Emsg(epname, error, errno, "set attribute (invalid acl format)", path);
    }

    // Convert to numeric representation
    if (Acl::ConvertIds(raw_val)) {
      errno = EINVAL;
      eos_static_err("msg=\"invalid acl value\" value=\"%s\"", raw_val.c_str());
      return Emsg(epname, error, errno, "set attribute (failed id convert)", path);
    }
  }

  eos::mgm::FusexCastBatch fuse_batch;
  eos::Prefetcher::prefetchItemAndWait(gOFS->eosView, path);

  try {
    eos::FileOrContainerMD item = gOFS->eosView->getItem(path).get();
    // Capture previous value if any
    std::string prev_value;
    {
      if (item.file) {
        auto amap = item.file->getAttributes();
        if (amap.count(skey)) prev_value = amap[skey];
      } else if (item.container) {
        auto amap = item.container->getAttributes();
        if (amap.count(skey)) prev_value = amap[skey];
      }
    }
    eos::FileOrContWriteLocked item_wlock;

    if (item.file) {
      item_wlock.fileLock = eos::MDLocking::writeLock(item.file.get());
    } else {
      item_wlock.containerLock = eos::MDLocking::writeLock(item.container.get());
    }

    if (!_attr_set(item, skey, raw_val, exclusive, vid, fuse_batch)) {
      if (errno == EEXIST) {
        return Emsg(epname, error, errno, "set attribute (exclusive set for"
                    " existing attribute)", path);
      } else if (errno == EBUSY) {
        return Emsg(epname, error, errno, "set attribute (foreign attribute"
                    " lock existing)", path);
      }
    }
    // Fetch new value
    std::string new_value;
    {
      if (item.file) {
        auto amap = item.file->getAttributes();
        if (amap.count(skey)) new_value = amap[skey];
      } else if (item.container) {
        auto amap = item.container->getAttributes();
        if (amap.count(skey)) new_value = amap[skey];
      }
    }
    // Emit audit for attribute set
    if (mAudit) {
      EOS_AUDIT(mAudit, eos::audit::SET_XATTR, path, vid, std::string(logId), std::string(cident), "mgm",
                    std::string(), nullptr, nullptr, skey, prev_value, new_value);
      if (skey == "sys.acl" || skey == "user.acl") {
        EOS_AUDIT(mAudit, eos::audit::SET_ACL, path, vid, std::string(logId), std::string(cident), "mgm",
                      std::string(), nullptr, nullptr, skey, prev_value, new_value);
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
// Set an extended attribute for a given ContainerMD - low-level API.
//------------------------------------------------------------------------------
bool
XrdMgmOfs::_attr_set(eos::FileOrContainerMD& item, std::string_view key,
                     std::string_view value, bool exclusive,
                     eos::common::VirtualIdentity& vid,
                     eos::mgm::FusexCastBatch& fuse_batch)
{
  eos::IContainerMD::XAttrMap attr_map;
  bool has_attribute = false;
  uid_t cuid;

  if (item.file) {
    cuid = item.file->getCUid();
    attr_map = item.file->getAttributes();
    has_attribute = item.file->hasAttribute(key.data());
  } else {
    cuid = item.container->getCUid();
    attr_map = item.container->getAttributes();
    has_attribute = item.container->hasAttribute(key.data());
  }

  Acl acl(attr_map, vid);

  if ((vid.uid != cuid) && !acl.AllowXAttrUpdate(key, vid)) {
    errno = EPERM;
    return false;
  }

  if (exclusive && has_attribute) {
    errno = EEXIST;
    return false;
  }

  if (item.file) {
    // Handle attribute for application locks
    if (key == eos::common::EOS_APP_LOCK_ATTR) {
      errno = 0;
      XattrLock app_lock(attr_map);

      if (app_lock.foreignLock(vid, true)) {
        errno = EBUSY;
        return false;
      }
    }

    item.file->setAttribute(key.data(), value.data());

    if (key != eos::kAttrTmpEtagKey) {
      item.file->setCTimeNow();
    }

    const eos::FileIdentifier f_id(item.file->getIdentifier());
    const eos::ContainerIdentifier c_id(item.file->getContainerId());
    eosView->updateFileStore(item.file.get());
    fuse_batch.Register([&, f_id, c_id]() {
      gOFS->FuseXCastRefresh(f_id, c_id);
    });
  } else {
    item.container->setAttribute(key.data(), value.data());

    if (key != eos::kAttrTmpEtagKey) {
      item.container->setCTimeNow();
    }

    const eos::ContainerIdentifier d_id(item.container->getIdentifier());
    const eos::ContainerIdentifier d_pid(item.container->getParentIdentifier());
    eosView->updateContainerStore(item.container.get());
    fuse_batch.Register([&, d_id, d_pid]() {
      gOFS->FuseXCastRefresh(d_id, d_pid);
    });
  }

  errno = 0;
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
  eos::Prefetcher::prefetchItemAndWait(gOFS->eosView, path);

  try {
    eos::FileOrContainerMD item = gOFS->eosView->getItem(path).get();

    if (item.file) { // file
      std::shared_ptr<eos::IFileMD> fmd = item.file;
      auto fmd_lock = eos::MDLocking::writeLock(fmd.get());
      eos::IContainerMD::XAttrMap attr_map = fmd->getAttributes();
      Acl acl(attr_map, vid);

      if ((vid.uid != fmd->getCUid()) &&
          !acl.AllowXAttrUpdate(skey, vid)) {
        errno = EPERM;
      } else {
        if (!fmd->hasAttribute(skey)) {
          errno = ENODATA;
        } else {
          // Capture previous value
          std::string prev;
          {
            auto amap = fmd->getAttributes();
            if (amap.count(skey)) prev = amap[skey];
          }
          fmd->removeAttribute(skey);
          eosView->updateFileStore(fmd.get());
          eos::FileIdentifier f_id = fmd->getIdentifier();
          eos::ContainerIdentifier d_id(fmd->getContainerId());
          // Release object lock before doing the fuse refresh
          fmd_lock.reset(nullptr);
          gOFS->FuseXCastRefresh(f_id, d_id);
          errno = 0;
          if (mAudit) {
            EOS_AUDIT(mAudit, eos::audit::RM_XATTR, path, vid, std::string(logId), std::string(cident), "mgm",
                          std::string(), nullptr, nullptr, skey, prev, std::string());
          }
        }
      }
    } else { // container
      std::shared_ptr<eos::IContainerMD> cmd = item.container;
      auto cmd_lock = eos::MDLocking::writeLock(cmd.get());
      eos::IContainerMD::XAttrMap attr_map = cmd->getAttributes();
      Acl acl(attr_map, vid);

      if (vid.token || (!cmd->access(vid.uid, vid.gid, X_OK | W_OK) &&
                        !acl.AllowXAttrUpdate(skey, vid))) {
        errno = EPERM;
      } else {
        if (!cmd->hasAttribute(skey)) {
          errno = ENODATA;
        } else {
          std::string prev;
          {
            auto amap = cmd->getAttributes();
            if (amap.count(skey)) prev = amap[skey];
          }
          cmd->removeAttribute(skey);
          eos::ContainerIdentifier d_id = cmd->getIdentifier();
          eos::ContainerIdentifier d_pid = cmd->getParentIdentifier();
          eosView->updateContainerStore(cmd.get());
          // Release object lock before doing the fuse refresh
          cmd_lock.reset(nullptr);
          gOFS->FuseXCastRefresh(d_id, d_pid);
          errno = 0;
          if (mAudit) {
            EOS_AUDIT(mAudit, eos::audit::RM_XATTR, path, vid, std::string(logId), std::string(cident), "mgm",
                          std::string(), nullptr, nullptr, skey, prev, std::string());
          }
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

//----------------------------------------------------------------------------
// List attributes high-level function merging space and namespace attributes
//----------------------------------------------------------------------------
void
XrdMgmOfs::mergeSpaceAttributes(eos::IContainerMD::XAttrMap& out, bool prefix,
                                bool existing)
{
  std::string space = "default";

  if (out.count("sys.forced.space")) {
    space = out["sys.forced.space"];
  }

  std::map<std::string, std::string> attr;

  if (gOFS->mSpaceAttributes.count(space)) {
    // retrieve space arguments
    std::unique_lock<std::mutex> lock(gOFS->mSpaceAttributesMutex);
    attr = gOFS->mSpaceAttributes[space];
  }

  // loop over arguments
  for (const auto& x : attr) {
    if (x.first == "sys.forced.space") {
      // we ignore this
      continue;
    } else {
      if (existing && !out.count(x.first)) {
        // merge only existing attributes
        continue;
      }

      std::string inkey = x.first;
      std::string outkey = (prefix ? (std::string("sys.space.") + x.first) : x.first);

      if (x.first == "sys.acl") {
        // Special meaning of the first char:
        // > append acl to the existing ones
        // < prepend acl to the existing ones
        // | add only if acls not set at all
        // none of the above means overwrite existring acls
        char op = '\0';

        if (!x.second.empty()) {
          op = x.second[0];
        }

        std::string old_acls = out["sys.acl"];
        std::string space_acls = x.second;

        if ((op == '>') || (op == '<') || (op == '|')) {
          space_acls = space_acls.erase(0, 1);
        }

        // ACL handling
        if (((op != '>') && (op != '<') && (op != '|')) || // Full overwrite
            ((op == '|') && old_acls.empty())) {       // Overwrite if empty
          out[outkey] = space_acls;
        } else {
          // If existing acls already include the space acls then
          // remove them to avoid duplicates.
          auto pos = old_acls.find(space_acls);

          if ((pos != std::string::npos) && (op != '|')) {
            auto del_pos = pos;
            auto del_len = space_acls.length();

            // Either delete the command before or the comma after
            if (del_pos && (old_acls[del_pos - 1] == ',')) {
              --del_pos;
              ++del_len;
            } else if ((del_pos + del_len < old_acls.length()) &&
                       (old_acls[del_pos + del_len] == ',')) {
              ++del_len;
            }

            old_acls = old_acls.erase(del_pos, del_len);
          }

          if (op == '>') { // Append rule
            out[outkey] = old_acls + std::string(",") + space_acls;
          } else if (op == '<') { // Prepend rule
            out[outkey] = space_acls + std::string(",") + old_acls;
          }

          if (op == '|') {
            out[outkey] = old_acls;
          }
        }
      } else {
        // Normal attribute handling
        if (x.second.substr(0, 1) == "|") {
          // if not set rule
          if (!out[x.first].length()) {
            out[outkey] = x.second.substr(1);
          }
        } else {
          // overwrite rule
          out[outkey] = x.second;
        }
      }
    }
  }
}


void
XrdMgmOfs::listAttributes(eos::IView* view, eos::IContainerMD* target,
                          eos::IContainerMD::XAttrMap& out, bool prefixLinks)
{
  eos::listAttributes(view, target, out, prefixLinks);
  mergeSpaceAttributes(out);
}

void
XrdMgmOfs::listAttributes(eos::IView* view, eos::IFileMD* target,
                          eos::IContainerMD::XAttrMap& out, bool prefixLinks)
{
  eos::listAttributes(view, target, out, prefixLinks);
  mergeSpaceAttributes(out);
}

void
XrdMgmOfs::listAttributes(eos::IView* view, eos::FileOrContainerMD target,
                          eos::IContainerMD::XAttrMap& out, bool prefixLinks)
{
  eos::listAttributes(view, target, out, prefixLinks);
  mergeSpaceAttributes(out);
}

template<typename T>
bool XrdMgmOfs::getAttribute(eos::IView* view, T& md, std::string key,
                             std::string& rvalue)
{
  auto result = eos::getAttribute(view, md, key, rvalue);
  eos::IContainerMD::XAttrMap attr;

  if (!result) {
    attr[key] = "";
  } else {
    attr[key] = rvalue;
  }

  mergeSpaceAttributes(attr, false, true);
  rvalue = attr[key];

  if (!result) {
    return attr[key].length();
  } else {
    return true;
  }
}
