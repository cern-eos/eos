//------------------------------------------------------------------------------
//! file ProcInterface.cc
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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
 * You should have received a copy of the AGNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include "ProcInterface.hh"
#include "common/ConsoleRequest.pb.h"
#include "mgm/proc/user/AclCmd.hh"
#include <iostream>
#include <fstream>
#include <json/json.h>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Factory method to get a ProcCommand object
//------------------------------------------------------------------------------
std::unique_ptr<IProcCommand>
ProcInterface::CreateProcCommand(eos::common::Mapping::VirtualIdentity& vid,
                                 const char* path, const char* opaque)
{
  std::unique_ptr<IProcCommand> cmd;

  if (!path || !opaque) {
    cmd.reset(new ProcCommand());
  } else {
    XrdOucEnv env(opaque);

    // New console implementation using ProtocolBuffer objects
    if (env.Get("mgm.cmd.proto")) {
      cmd = HandleProtobufRequest(path, opaque, vid);
    } else {
      cmd.reset(new ProcCommand());
    }
  }

  return cmd;
}

//----------------------------------------------------------------------------
// Handle protobuf request
//----------------------------------------------------------------------------
std::unique_ptr<IProcCommand>
ProcInterface::HandleProtobufRequest(const char* path, const char* opaque,
                                     eos::common::Mapping::VirtualIdentity& vid)
{
  using eos::console::RequestProto_OpType;
  std::unique_ptr<IProcCommand> cmd {nullptr};
  std::ostringstream oss;
  std::string raw_pb;
  XrdOucEnv env(opaque);
  const char* b64data = env.Get("mgm.cmd.proto");

  if (!eos::common::SymKey::Base64Decode(b64data, raw_pb)) {
    oss << "error: failed to base64decode request";
    eos_static_err("%s", oss.str().c_str());
    return cmd;
  }

  eos::console::RequestProto req;

  if (!req.ParseFromString(raw_pb)) {
    oss << "error: failed to deserialize ProtocolBuffer object: "
        << raw_pb;
    eos_static_err("%s", oss.str().c_str());
    return cmd;
  }

  switch (req.type()) {
  case RequestProto_OpType::RequestProto_OpType_ACL:
    eos_static_debug("handling acl command");
    cmd.reset(new AclCmd(std::move(req), vid));
    break;

  default:
    oss << "error: unknown request type";
    eos_static_err("%s", oss.str().c_str());
    break;
  }

  return cmd;
}

//------------------------------------------------------------------------------
// Check if a path indicates a proc command
//------------------------------------------------------------------------------
bool
ProcInterface::IsProcAccess(const char* path)
{
  return (strstr(path, "/proc/") == path);
}

//------------------------------------------------------------------------------
// Check if a proc command is a 'write' command modifying state of an MGM
//------------------------------------------------------------------------------
bool
ProcInterface::IsWriteAccess(const char* path, const char* info)
{
  XrdOucString inpath = (path ? path : "");
  XrdOucString ininfo = (info ? info : "");

  if (!inpath.beginswith("/proc/")) {
    return false;
  }

  XrdOucEnv procEnv(ininfo.c_str());
  XrdOucString cmd = procEnv.Get("mgm.cmd");
  XrdOucString subcmd = procEnv.Get("mgm.subcmd");

  // Filter here all namespace modifying proc messages
  if (((cmd == "file") &&
       ((subcmd == "adjustreplica") ||
        (subcmd == "drop") ||
        (subcmd == "layout") ||
        (subcmd == "verify") ||
        (subcmd == "version") ||
        (subcmd == "versions") ||
        (subcmd == "rename"))) ||
      ((cmd == "attr") &&
       ((subcmd == "set") ||
        (subcmd == "rm"))) ||
      ((cmd == "archive") &&
       ((subcmd == "create") ||
        (subcmd == "get")  ||
        (subcmd == "purge")  ||
        (subcmd == "delete"))) ||
      ((cmd == "backup")) ||
      ((cmd == "mkdir")) ||
      ((cmd == "rmdir")) ||
      ((cmd == "rm")) ||
      ((cmd == "chown")) ||
      ((cmd == "chmod")) ||
      ((cmd == "fs") &&
       ((subcmd == "config") ||
        (subcmd == "boot") ||
        (subcmd == "dropdeletion") ||
        (subcmd == "add") ||
        (subcmd == "mv") ||
        (subcmd == "rm"))) ||
      ((cmd == "space") &&
       ((subcmd == "config") ||
        (subcmd == "define") ||
        (subcmd == "set") ||
        (subcmd == "rm") ||
        (subcmd == "quota"))) ||
      ((cmd == "node") &&
       ((subcmd == "rm") ||
        (subcmd == "config") ||
        (subcmd == "set") ||
        (subcmd == "register") ||
        (subcmd == "gw"))) ||
      ((cmd == "group") &&
       ((subcmd == "set") ||
        (subcmd == "rm"))) ||
      ((cmd == "map") &&
       ((subcmd == "link") ||
        (subcmd == "unlink"))) ||
      ((cmd == "quota") &&
       ((subcmd != "ls"))) ||
      ((cmd == "vid") &&
       ((subcmd != "ls"))) ||
      ((cmd == "transfer") &&
       ((subcmd != ""))) ||
      ((cmd == "recycle") &&
       ((subcmd != "ls")))) {
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Authorize a proc command based on the client's VID
//------------------------------------------------------------------------------
bool
ProcInterface::Authorize(const char* path, const char* info,
                         eos::common::Mapping::VirtualIdentity& vid,
                         const XrdSecEntity* entity)
{
  XrdOucString inpath = path;

  // Administrator access
  if (inpath.beginswith("/proc/admin/")) {
    // Hosts with 'sss' authentication can run 'admin' commands
    std::string protocol = entity ? entity->prot : "";

    // We allow sss only with the daemon login is admin
    if ((protocol == "sss") &&
        (eos::common::Mapping::HasUid(DAEMONUID, vid.uid_list))) {
      return true;
    }

    // Root can do it
    if (!vid.uid) {
      return true;
    }

    // One has to be part of the virtual users 2(daemon)/3(adm)/4(adm)
    return ((eos::common::Mapping::HasUid(DAEMONUID, vid.uid_list)) ||
            (eos::common::Mapping::HasUid(3, vid.uid_list)) ||
            (eos::common::Mapping::HasGid(4, vid.gid_list)));
  }

  // User access
  if (inpath.beginswith("/proc/user/")) {
    return true;
  }

  return false;
}

EOSMGMNAMESPACE_END
