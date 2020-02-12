//------------------------------------------------------------------------------
//! @file ProcInterface.cc
//! @author Elvin Sindrilaru - CERN
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
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include "ProcInterface.hh"
#include "mgm/proc/user/AclCmd.hh"
#include "mgm/proc/user/FindCmd.hh"
#include "mgm/proc/user/RmCmd.hh"
#include "mgm/proc/user/TokenCmd.hh"
#include "mgm/proc/user/RouteCmd.hh"
#include "mgm/proc/user/RecycleCmd.hh"
#include "mgm/proc/admin/FsCmd.hh"
#include "mgm/proc/admin/NsCmd.hh"
#include "mgm/proc/admin/StagerRmCmd.hh"
#include "mgm/proc/admin/IoCmd.hh"
#include "mgm/proc/admin/GroupCmd.hh"
#include "mgm/proc/admin/DebugCmd.hh"
#include "mgm/proc/admin/NodeCmd.hh"
#include "mgm/proc/admin/QuotaCmd.hh"
#include "mgm/proc/admin/SpaceCmd.hh"
#include "mgm/proc/admin/ConfigCmd.hh"
#include "mgm/proc/admin/AccessCmd.hh"
#include "mgm/proc/admin/FsckCmd.hh"
#include <google/protobuf/util/json_util.h>
#include "XrdOuc/XrdOucEnv.hh"

EOSMGMNAMESPACE_BEGIN
thread_local eos::common::LogId ProcInterface::tlLogId;
std::mutex ProcInterface::mMutexCmds;
std::list<std::unique_ptr<IProcCommand>> ProcInterface::mCmdToDel;
std::unordered_map<std::string, std::unique_ptr<IProcCommand>>
    ProcInterface::mMapCmds;
eos::common::ThreadPool ProcInterface::sProcThreads(
  std::max(std::thread::hardware_concurrency() / 10, 64u),
  std::max(std::thread::hardware_concurrency() / 4, 256u),
  3, 2, 2, "proc_pool");

//------------------------------------------------------------------------------
// Factory method to get a ProcCommand object
//------------------------------------------------------------------------------
std::unique_ptr<IProcCommand>
ProcInterface::GetProcCommand(const char* tident,
                              eos::common::VirtualIdentity& vid,
                              const char* path, const char* opaque,
                              const char* log_id)
{
  tlLogId.SetLogId((log_id ? log_id : ""), vid, tident);
  // Check if this is an already submitted command
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
      pcmd = HandleProtobufRequest(opaque, vid);
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
unique_ptr<IProcCommand>
ProcInterface::HandleProtobufRequest(const char* opaque,
                                     eos::common::VirtualIdentity& vid)
{
  using eos::console::RequestProto;
  std::unique_ptr<IProcCommand> cmd;
  std::ostringstream oss;
  std::string raw_pb;
  XrdOucEnv env(opaque);
  const char* b64data = env.Get("mgm.cmd.proto");

  if (!eos::common::SymKey::Base64Decode(b64data, raw_pb)) {
    oss << "error: failed to base64decode request";
    eos_thread_err("%s", oss.str().c_str());
    return cmd;
  }

  eos::console::RequestProto req;

  if (!req.ParseFromString(raw_pb)) {
    oss << "error: failed to deserialize ProtocolBuffer object: "
        << raw_pb;
    eos_thread_err("%s", oss.str().c_str());
    return cmd;
  }

  return HandleProtobufRequest(req, vid);
}


std::unique_ptr<IProcCommand>
ProcInterface::HandleProtobufRequest(eos::console::RequestProto& req,
				       eos::common::VirtualIdentity& vid)
{
  using eos::console::RequestProto;
  std::unique_ptr<IProcCommand> cmd;
  // Log the type of command that we received
  std::string json_out;
  (void) google::protobuf::util::MessageToJsonString(req, &json_out);
  eos_thread_info("cmd_proto=%s", json_out.c_str());

  switch (req.command_case()) {
  case RequestProto::kAcl:
    cmd.reset(new AclCmd(std::move(req), vid));
    break;

  case RequestProto::kNs:
    cmd.reset(new NsCmd(std::move(req), vid));
    break;

  case RequestProto::kFind:
    cmd.reset(new FindCmd(std::move(req), vid));
    break;

  case RequestProto::kFs:
    cmd.reset(new FsCmd(std::move(req), vid));
    break;

  case RequestProto::kRm:
    cmd.reset(new RmCmd(std::move(req), vid));
    break;

  case RequestProto::kToken:
    cmd.reset(new TokenCmd(std::move(req), vid));
    break;

  case RequestProto::kStagerRm:
    cmd.reset(new StagerRmCmd(std::move(req), vid));
    break;

  case RequestProto::kRoute:
    cmd.reset(new RouteCmd(std::move(req), vid));
    break;

  case RequestProto::kRecycle:
    cmd.reset(new RecycleCmd(std::move(req), vid));
    break;

  case RequestProto::kIo:
    cmd.reset(new IoCmd(std::move(req), vid));
    break;

  case RequestProto::kGroup:
    cmd.reset(new GroupCmd(std::move(req), vid));
    break;

  case RequestProto::kDebug:
    cmd.reset(new DebugCmd(std::move(req), vid));
    break;

  case RequestProto::kNode:
    cmd.reset(new NodeCmd(std::move(req), vid));
    break;

  case RequestProto::kFsck:
    cmd.reset(new FsckCmd(std::move(req), vid));
    break;

  case RequestProto::kQuota:
    cmd.reset(new QuotaCmd(std::move(req), vid));
    break;

  case RequestProto::kSpace:
    cmd.reset(new SpaceCmd(std::move(req), vid));
    break;

  case RequestProto::kConfig:
    cmd.reset(new ConfigCmd(std::move(req), vid));
    break;

  case RequestProto::kAccess:
    cmd.reset(new AccessCmd(std::move(req), vid));
    break;

  default:
    eos_static_err("error: unknown request type");
    break;
  }


  return cmd;
}

//----------------------------------------------------------------------------
// Inspect protobuf request if this modifies the namespace
//----------------------------------------------------------------------------
bool
ProcInterface::ProtoIsWriteAccess(const char* opaque)
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
    return false;
  }

  eos::console::RequestProto req;

  if (!req.ParseFromString(raw_pb)) {
    oss << "error: failed to deserialize ProtocolBuffer object: " << raw_pb;
    eos_static_err("%s", oss.str().c_str());
    return false;
  }

  // Log the type of command that we received
  std::string json_out;
  (void)google::protobuf::util::MessageToJsonString(req, &json_out);

  /* being conservative, true by default. Add false clauses explicitly */
  switch (req.command_case()) {

  // always false
  case RequestProto::kNs:
  case RequestProto::kFind:
  case RequestProto::kIo:
  case RequestProto::kDebug:
  case RequestProto::kConfig:
  case RequestProto::kToken:

    return false;

  // conditional on the subcommand
  case RequestProto::kAcl:
    switch (req.acl().op()) {
    case eos::console::AclProto::NONE:
    case eos::console::AclProto::LIST:
      return false;
    default:
      return true;
    }
  case RequestProto::kRecycle:
    switch (req.recycle().subcmd_case()) {
    case eos::console::RecycleProto::kLs:
      return false;
    default:
      return true;
    }
  case RequestProto::kFs:
    switch (req.fs().subcmd_case()) {
    case eos::console::FsProto::kClone:
    case eos::console::FsProto::kCompare:
    case eos::console::FsProto::kDumpmd:
    case eos::console::FsProto::kLs:
    case eos::console::FsProto::kStatus:
      return false;
      default:
        return true;
    }
  case RequestProto::kRoute:
    switch (req.route().subcmd_case()) {
    case eos::console::RouteProto::kList:
      return false;
      default:
        return true;
    }
  case RequestProto::kGroup:
    switch (req.group().subcmd_case()) {
    case eos::console::GroupProto::kLs:
      return false;
      default:
        return true;
    }
  case RequestProto::kNode:
    switch (req.node().subcmd_case()) {
    case eos::console::NodeProto::kLs:
    case eos::console::NodeProto::kStatus:
      return false;
      default:
        return true;
    }
  case RequestProto::kQuota:
    switch (req.quota().subcmd_case()) {
    case eos::console::QuotaProto::kLs:
    case eos::console::QuotaProto::kLsuser:
      return false;
      default:
        return true;
    }
  case RequestProto::kSpace:
    switch (req.space().subcmd_case()) {
    case eos::console::SpaceProto::kLs:
    case eos::console::SpaceProto::kStatus:
    case eos::console::SpaceProto::kNodeGet:
      return false;
      default:
        return true;
    }
  case RequestProto::kAccess:
    switch (req.access().subcmd_case()) {
    case eos::console::AccessProto::kLs:
      return false;
      default:
        return true;
    }

  // always true
  case RequestProto::kRm:
  case RequestProto::kStagerRm:
  case RequestProto::kDrain: // @note where is it?
  default:
    return true;
  }

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

  // Filter protobuf requests
  // @TODO: avoid parsing proto buf requests twice (here and later when running the request)
  if (procEnv.Get("mgm.cmd.proto")) {
    return ProtoIsWriteAccess(ininfo.c_str());
  }

  XrdOucString cmd = procEnv.Get("mgm.cmd");
  XrdOucString subcmd = procEnv.Get("mgm.subcmd");

  // Filter here all namespace modifying proc messages
  if (((cmd == "file") &&
       ((subcmd == "adjustreplica") ||
        (subcmd == "drop") ||
        (subcmd == "layout") ||
        (subcmd == "touch") ||
        (subcmd == "verify") ||
        (subcmd == "version") ||
        (subcmd == "versions") ||
        (subcmd == "move") ||
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
      ((cmd == "fuseX")) ||
      ((cmd == "fusex")) ||
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
                         eos::common::VirtualIdentity& vid,
                         const XrdSecEntity* entity)
{
  XrdOucString inpath = path;

  // Administrator access
  if (inpath.beginswith("/proc/admin/")) {
    // Hosts with 'sss' authentication can run 'admin' commands
    std::string protocol = entity ? entity->prot : "";

    // We allow sss only with the daemon login is admin
    if ((protocol == "sss") &&
        (vid.hasUid(DAEMONUID))) {
      return true;
    }

    // Root can do it
    if (!vid.uid) {
      return true;
    }

    // One has to be part of the virtual users 2(daemon)/3(adm)/4(adm)
    return ((vid.hasUid(DAEMONUID)) ||
            (vid.hasUid(3)) ||
            (vid.hasGid(4)));
  }

  // User access
  if (inpath.beginswith("/proc/user/")) {
    return true;
  }

  return false;
}

EOSMGMNAMESPACE_END
