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

#include "fst/FmdClient.hh"

EOSFSTNAMESPACE_BEGIN

FmdClient gFmdClient; // Global fmd client

//------------------------------------------------------------------------------
// Convert an FST env representation to an Fmd struct
//------------------------------------------------------------------------------
bool
FmdClient::EnvFstToFmd(XrdOucEnv& env, struct Fmd& fmd)
{
  // Check that all tags are present
  if (!env.Get("id") ||
      !env.Get("cid") ||
      !env.Get("ctime") ||
      !env.Get("ctime_ns") ||
      !env.Get("mtime") ||
      !env.Get("mtime_ns") ||
      !env.Get("size") ||
      !env.Get("lid") ||
      !env.Get("uid") ||
      !env.Get("gid")) {
    return false;
  }

  fmd.set_fid(strtoull(env.Get("id"), 0, 10));
  fmd.set_cid(strtoull(env.Get("cid"), 0, 10));
  fmd.set_ctime(strtoul(env.Get("ctime"), 0, 10));
  fmd.set_ctime_ns(strtoul(env.Get("ctime_ns"), 0, 10));
  fmd.set_mtime(strtoul(env.Get("mtime"), 0, 10));
  fmd.set_mtime_ns(strtoul(env.Get("mtime_ns"), 0, 10));
  fmd.set_size(strtoull(env.Get("size"), 0, 10));
  fmd.set_lid(strtoul(env.Get("lid"), 0, 10));
  fmd.set_uid((uid_t) strtoul(env.Get("uid"), 0, 10));
  fmd.set_gid((gid_t) strtoul(env.Get("gid"), 0, 10));

  if (env.Get("checksum")) {
    fmd.set_checksum(env.Get("checksum"));

    if (fmd.checksum() == "none") {
      fmd.set_checksum("");
    }
  } else {
    fmd.set_checksum("");
  }

  return true;
}

//------------------------------------------------------------------------------
// Convert an MGM env representation to an Fmd struct
//------------------------------------------------------------------------------
bool
FmdClient::EnvMgmToFmd(XrdOucEnv& env, struct Fmd& fmd)
{
  // Check that all tags are present
  if (!env.Get("id") ||
      !env.Get("cid") ||
      !env.Get("ctime") ||
      !env.Get("ctime_ns") ||
      !env.Get("mtime") ||
      !env.Get("mtime_ns") ||
      !env.Get("size") ||
      !env.Get("checksum") ||
      !env.Get("lid") ||
      !env.Get("uid") ||
      !env.Get("gid")) {
    return false;
  }

  fmd.set_fid(strtoull(env.Get("id"), 0, 10));
  fmd.set_cid(strtoull(env.Get("cid"), 0, 10));
  fmd.set_ctime(strtoul(env.Get("ctime"), 0, 10));
  fmd.set_ctime_ns(strtoul(env.Get("ctime_ns"), 0, 10));
  fmd.set_mtime(strtoul(env.Get("mtime"), 0, 10));
  fmd.set_mtime_ns(strtoul(env.Get("mtime_ns"), 0, 10));
  fmd.set_mgmsize(strtoull(env.Get("size"), 0, 10));
  fmd.set_lid(strtoul(env.Get("lid"), 0, 10));
  fmd.set_uid((uid_t) strtoul(env.Get("uid"), 0, 10));
  fmd.set_gid((gid_t) strtoul(env.Get("gid"), 0, 10));
  fmd.set_mgmchecksum(env.Get("checksum"));
  fmd.set_locations(env.Get("location") ? env.Get("location") : "");
  return true;
}

//----------------------------------------------------------------------------
//! Convert namespace file metadata to an Fmd struct
//----------------------------------------------------------------------------
bool
FmdClient::NsFileMDToFmd(eos::IFileMD* file, struct Fmd& fmd)
{
  fmd.set_fid(file->getId());
  fmd.set_cid(file->getContainerId());
  eos::IFileMD::ctime_t ctime;
  eos::IFileMD::ctime_t mtime;
  (void) file->getCTime(ctime);
  (void) file->getMTime(mtime);
  fmd.set_ctime(ctime.tv_sec);
  fmd.set_ctime_ns(ctime.tv_nsec);
  fmd.set_mtime(mtime.tv_sec);
  fmd.set_mtime_ns(mtime.tv_nsec);
  fmd.set_mgmsize(file->getSize());
  fmd.set_lid(file->getLayoutId());
  fmd.set_uid(file->getCUid());
  fmd.set_gid(file->getCGid());
  std::string str_xs;
  eos::Buffer xs = file->getChecksum();
  uint8_t size = xs.size();

  for (uint8_t i = 0; i < size; i++) {
    char hx[3];
    hx[0] = 0;
    snprintf(static_cast<char*>(hx), sizeof(hx), "%02x",
             *(unsigned char*)(xs.getDataPtr() + i));
    str_xs += static_cast<char*>(hx);
  }

  fmd.set_mgmchecksum(str_xs);
  std::string slocations;
  auto locations = file->getLocations();

  for (const auto& loc : locations) {
    slocations += loc;
    slocations += " ";
  }

  if (!slocations.empty()) {
    slocations.pop_back();
  }

  fmd.set_locations(slocations);
  return true;
}

//------------------------------------------------------------------------------
// Return Fmd from MGM
//------------------------------------------------------------------------------
int
FmdClient::GetMgmFmd(const char* manager, eos::common::FileId::fileid_t fid,
                     struct Fmd& fmd)
{
  if ((!manager) || (!fid)) {
    return EINVAL;
  }

  int rc = 0;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;
  XrdCl::XRootDStatus status;
  char sfmd[1024];
  snprintf(sfmd, sizeof(sfmd) - 1, "%llu", fid);
  XrdOucString fmdquery = "/?mgm.pcmd=getfmd&mgm.getfmd.fid=";
  fmdquery += sfmd;
  XrdOucString address = "root://";
  address += manager;
  address += "//dummy";
  XrdCl::URL url(address.c_str());

  if (!url.IsValid()) {
    eos_err("error=URL is not valid: %s", address.c_str());
    return EINVAL;
  }

again:
  XrdCl::FileSystem* fs = new XrdCl::FileSystem(url);

  if (!fs) {
    eos_err("error=failed to get new FS object");
    return EINVAL;
  }

  arg.FromString(fmdquery.c_str());
  status = fs->Query(XrdCl::QueryCode::OpaqueFile, arg, response);

  if (status.IsOK()) {
    rc = 0;
    eos_static_debug("got replica file meta data from mgm %s for fid=%08llx",
                     manager, fid);
  } else {
    eos_static_err("msg=\"query error\" status=%d code=%d", status.status,
                   status.code);

    if ((status.code >= 100) &&
        (status.code <= 300)) {
      XrdSysTimer sleeper;
      sleeper.Snooze(1);
      eos_static_info("msg=\"retry query\" query=\"%s\"", fmdquery.c_str());
      goto again;
    }

    rc = ECOMM;
    eos_static_err("Unable to retrieve meta data from mgm %s for fid=%08llx",
                   manager, fid);
  }

  delete fs;

  if (rc) {
    delete response;
    return EIO;
  }

  // Check if response contains any data
  if (!response->GetBuffer()) {
    eos_static_info("Unable to retrieve meta data from mgm %s for fid=%08llx, "
                    "result data is empty", manager, fid);
    delete response;
    return ENODATA;
  }

  std::string sresult = response->GetBuffer();

  if ((sresult.find("getfmd: retc=0 ")) == std::string::npos) {
    // Remote side couldn't get the record
    eos_static_info("Unable to retrieve meta data on remote mgm %s for "
                    "fid=%08llx - result=%s", manager, fid,
                    response->GetBuffer());
    delete response;
    return ENODATA;
  } else {
    // Truncate 'getfmd: retc=0 ' away
    sresult.erase(0, 15);
  }

  // Get the remote file meta data into an env hash
  XrdOucEnv fmdenv(sresult.c_str());

  if (!EnvMgmToFmd(fmdenv, fmd)) {
    int envlen;
    eos_static_err("Failed to unparse file meta data %s", fmdenv.Env(envlen));
    delete response;
    return EIO;
  }

  // Basic check
  if (fmd.fid() != fid) {
    eos_static_err("Uups! Received wrong meta data from remote server - fid "
                   "is %lu instead of %lu !", fmd.fid(), fid);
    delete response;
    return EIO;
  }

  delete response;
  return 0;
}

//------------------------------------------------------------------------------
// Return a remote file attribute
//------------------------------------------------------------------------------
int
FmdClient::GetRemoteAttribute(const char* manager, const char* key,
                              const char* path, XrdOucString& attribute)
{
  if ((!key) || (!path)) {
    return EINVAL;
  }

  int rc = 0;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;
  XrdCl::XRootDStatus status;
  XrdOucString fmdquery = "/?fst.pcmd=getxattr&fst.getxattr.key=";
  fmdquery += key;
  fmdquery += "&fst.getxattr.path=";
  fmdquery += path;
  XrdOucString address = "root://";
  address += manager;
  address += "//dummy";
  XrdCl::URL url(address.c_str());

  if (!url.IsValid()) {
    eos_err("error=URL is not valid: %s", address.c_str());
    return EINVAL;
  }

  std::unique_ptr<XrdCl::FileSystem> fs(new XrdCl::FileSystem(url));

  if (!fs) {
    eos_err("error=failed to get new FS object");
    return EINVAL;
  }

  arg.FromString(fmdquery.c_str());
  status = fs->Query(XrdCl::QueryCode::OpaqueFile, arg, response);

  if (status.IsOK()) {
    rc = 0;
    eos_debug("got attribute meta data from server %s for key=%s path=%s"
              " attribute=%s", manager, key, path, response->GetBuffer());
  } else {
    rc = ECOMM;
    eos_err("Unable to retrieve meta data from server %s for key=%s path=%s",
            manager, key, path);
  }

  if (rc) {
    delete response;
    return EIO;
  }

  if (!strncmp(response->GetBuffer(), "ERROR", 5)) {
    // remote side couldn't get the record
    eos_info("Unable to retrieve meta data on remote server %s for key=%s "
             "path=%s", manager, key, path);
    delete response;
    return ENODATA;
  }

  attribute = response->GetBuffer();
  delete response;
  return 0;
}

//------------------------------------------------------------------------------
// Return Fmd from a remote filesystem
//------------------------------------------------------------------------------
int
FmdClient::GetRemoteFmdSqlite(const char* manager, const char* shexfid,
                              const char* sfsid, struct Fmd& fmd)
{
  if ((!manager) || (!shexfid) || (!sfsid)) {
    return EINVAL;
  }

  int rc = 0;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;
  XrdCl::XRootDStatus status;
  XrdOucString fmdquery = "/?fst.pcmd=getfmd&fst.getfmd.fid=";
  fmdquery += shexfid;
  fmdquery += "&fst.getfmd.fsid=";
  fmdquery += sfsid;
  XrdOucString address = "root://";
  address += manager;
  address += "//dummy";
  XrdCl::URL url(address.c_str());

  if (!url.IsValid()) {
    eos_static_err("error=URL is not valid: %s", address.c_str());
    return EINVAL;
  }

  std::unique_ptr<XrdCl::FileSystem> fs(new XrdCl::FileSystem(url));

  if (!fs) {
    eos_static_err("error=failed to get new FS object");
    return EINVAL;
  }

  arg.FromString(fmdquery.c_str());
  status = fs->Query(XrdCl::QueryCode::OpaqueFile, arg, response);

  if (status.IsOK()) {
    rc = 0;
    eos_static_debug("got replica file meta data from server %s for fid=%s fsid=%s",
                     manager, shexfid, sfsid);
  } else {
    rc = ECOMM;
    eos_static_err("Unable to retrieve meta data from server %s for fid=%s fsid=%s",
                   manager, shexfid, sfsid);
  }

  if (rc) {
    delete response;
    return EIO;
  }

  if (!strncmp(response->GetBuffer(), "ERROR", 5)) {
    // remote side couldn't get the record
    eos_static_info("Unable to retrieve meta data on remote server %s for fid=%s fsid=%s",
                    manager, shexfid, sfsid);
    delete response;
    return ENODATA;
  }

  // get the remote file meta data into an env hash
  XrdOucEnv fmdenv(response->GetBuffer());

  if (!EnvFstToFmd(fmdenv, fmd)) {
    int envlen;
    eos_static_err("Failed to unparse file meta data %s", fmdenv.Env(envlen));
    delete response;
    return EIO;
  }

  // very simple check
  if (fmd.fid() != eos::common::FileId::Hex2Fid(shexfid)) {
    eos_static_err("Uups! Received wrong meta data from remote server - fid "
                   "is %lu instead of %lu !", fmd.fid(),
                   eos::common::FileId::Hex2Fid(shexfid));
    delete response;
    return EIO;
  }

  delete response;
  return 0;
}

//------------------------------------------------------------------------------
// Call the 'auto repair' function e.g. 'file convert --rewrite'
//------------------------------------------------------------------------------
int
FmdClient::CallAutoRepair(const char* manager,
                          eos::common::FileId::fileid_t fid)
{
  if (!fid) {
    return EINVAL;
  }

  int rc = 0;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;
  XrdCl::XRootDStatus status;
  XrdOucString fmdquery = "/?mgm.pcmd=rewrite&mgm.fxid=";
  XrdOucString shexfid;
  eos::common::FileId::Fid2Hex(fid, shexfid);
  fmdquery += shexfid;
  XrdOucString address = "root://";
  address += manager;
  address += "//dummy";
  XrdCl::URL url(address.c_str());

  if (!url.IsValid()) {
    eos_static_err("error=URL is not valid: %s", address.c_str());
    return EINVAL;
  }

  std::unique_ptr<XrdCl::FileSystem> fs(new XrdCl::FileSystem(url));

  if (!fs) {
    eos_static_err("error=failed to get new FS object");
    return EINVAL;
  }

  arg.FromString(fmdquery.c_str());
  status = fs->Query(XrdCl::QueryCode::OpaqueFile, arg, response);

  if (status.IsOK()) {
    rc = 0;
    eos_static_debug("scheduled a repair at %s for fid=%s ",
                     manager, shexfid.c_str());
  } else {
    rc = ECOMM;
    eos_static_err("Unable to schedule repair at server %s for fid=%s",
                   manager, shexfid.c_str());
  }

  if (rc) {
    delete response;
    return EIO;
  }

  delete response;
  return 0;
}

EOSFSTNAMESPACE_END
