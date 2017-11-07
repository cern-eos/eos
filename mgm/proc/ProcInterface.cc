//------------------------------------------------------------------------------
//! @file ProcInterface.cc
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
#include "mgm/proc/admin/NsCmd.hh"
#include <iostream>
#include <fstream>
#include <json/json.h>
#include <google/protobuf/util/json_util.h>

EOSMGMNAMESPACE_BEGIN

std::mutex ProcInterface::mMutexCmds;
std::list<std::unique_ptr<IProcCommand>> ProcInterface::mCmdToDel;
std::unordered_map<std::string, std::unique_ptr<IProcCommand>>
    ProcInterface::mMapCmds;
eos::common::ThreadPool ProcInterface::sProcThreads;

//------------------------------------------------------------------------------
// Factory method to get a ProcCommand object
//------------------------------------------------------------------------------
std::unique_ptr<IProcCommand>
ProcInterface::GetProcCommand(const char* tident,
                              eos::common::Mapping::VirtualIdentity& vid,
                              const char* path, const char* opaque)
{
  // Check if this is an already submmited command
  std::unique_ptr<IProcCommand> pcmd = GetSubmittedCmd(tident);

  if (pcmd) {
    return pcmd;
  }

  if (!path || !opaque) {
    // Return old style proc command which is populated in the open
    pcmd.reset(new ProcCommand());
  } else {
    XrdOucEnv env(opaque);

    // New proc command implementation using ProtocolBuffer objects
    if (env.Get("mgm.cmd.proto")) {
      pcmd = HandleProtobufRequest(path, opaque, vid);
    } else {
      pcmd.reset(new ProcCommand());
    }
  }

  return pcmd;
}

//------------------------------------------------------------------------------
// Get asynchronous executing command, submitted earlier by the same client
//------------------------------------------------------------------------------
std::unique_ptr<IProcCommand>
ProcInterface::GetSubmittedCmd(const char* tident)
{
  std::unique_ptr<IProcCommand> pcmd;
  std::lock_guard<std::mutex> lock(mMutexCmds);
  auto it = mMapCmds.find(tident);

  if (it != mMapCmds.end()) {
    pcmd.swap(it->second);
    mMapCmds.erase(it);
  }

  return pcmd;
}

//------------------------------------------------------------------------------
// Save asynchronous executing command, so we can stall the client and
// return later on the result.
//------------------------------------------------------------------------------
bool
ProcInterface::SaveSubmittedCmd(const char* tident,
                                std::unique_ptr<IProcCommand>&& pcmd)
{
  std::lock_guard<std::mutex> lock(mMutexCmds);

  if (mMapCmds.count(tident)) {
    return false;
  }

  mMapCmds.insert(std::make_pair(std::string(tident), std::move(pcmd)));
  return true;
}

//------------------------------------------------------------------------------
// Drop asynchronous executing command since the client disconnected
//------------------------------------------------------------------------------
void
ProcInterface::DropSubmittedCmd(const char* tident)
{
  std::lock_guard<std::mutex> lock(mMutexCmds);

  // Drop any long running commands without connected clients
  for (auto it = mCmdToDel.begin(); it != mCmdToDel.end(); /* empty */) {
    if ((*it)->KillJob()) {
      mCmdToDel.erase(it++);
    } else {
      ++it;
    }
  }

  // Check if this client has any executing command
  auto it = mMapCmds.find(tident);

  if (it != mMapCmds.end()) {
    std::unique_ptr<IProcCommand> tmp_cmd;
    tmp_cmd.swap(it->second);
    mMapCmds.erase(it);

    if (!tmp_cmd->KillJob()) {
      mCmdToDel.push_back(std::move(tmp_cmd));
    }
  }
}

//----------------------------------------------------------------------------
// Handle protobuf request
//----------------------------------------------------------------------------
std::unique_ptr<IProcCommand>
ProcInterface::HandleProtobufRequest(const char* path, const char* opaque,
                                     eos::common::Mapping::VirtualIdentity& vid)
{
  using eos::console::RequestProto;
  std::unique_ptr<IProcCommand> cmd;
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

  // Log the type of command that we received
  std::string json_out;
  (void) google::protobuf::util::MessageToJsonString(req, &json_out);
  eos_static_info("cmd_proto=%s", json_out.c_str());

  switch (req.command_case()) {
  case RequestProto::kAcl:
    cmd.reset(new AclCmd(std::move(req), vid));
    break;

  case RequestProto::kNs:
    cmd.reset(new NsCmd(std::move(req), vid));
    break;

  default:
    eos_static_err("error: unknown request type");
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

  // @todo (esindril): review this and do it in a smart way
  if (procEnv.Get("mgm.cmd.proto")) {
    return false;
  }

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
