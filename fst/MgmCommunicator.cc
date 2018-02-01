//------------------------------------------------------------------------------
// File: MgmCommunicator.cc
// Author: Jozsef Makai - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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


#include "MgmCommunicator.hh"
#include "common/Logging.hh"
#include "XrdCl/XrdClBuffer.hh"
#include "XrdCl/XrdClXRootDResponses.hh"
#include "XrdCl/XrdClFileSystem.hh"

EOSFSTNAMESPACE_BEGIN

MgmCommunicator gMgmCommunicator;

int
eos::fst::MgmCommunicator::GetMgmFmd(const char* manager, eos::common::FileId::fileid_t fid, struct Fmd& fmd) {
  if ((!manager) || (!fid)) {
    return EINVAL;
  }

  int rc = 0;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = nullptr;
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
    eos_static_err("error=URL is not valid: %s", address.c_str());
    return EINVAL;
  }

again:
  std::unique_ptr<XrdCl::FileSystem> fs(new XrdCl::FileSystem(url));

  if (!fs) {
    eos_static_err("error=failed to get new FS object");
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

bool
eos::fst::MgmCommunicator::EnvMgmToFmd(XrdOucEnv& env, struct Fmd& fmd) {
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

int
MgmCommunicator::CallAutoRepair(const char* manager, eos::common::FileId::fileid_t fid)
{
  if (!fid) {
    return EINVAL;
  }

  int rc = 0;
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = nullptr;
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