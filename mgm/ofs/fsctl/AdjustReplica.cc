// ----------------------------------------------------------------------
// File: AdjustReplica.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

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

#include "common/Logging.hh"
#include "common/SymKeys.hh"
#include "mgm/macros/Macros.hh"
#include "mgm/ofs/XrdMgmOfs.hh"
#include "mgm/proc/user/FileCmd.hh"
#include "mgm/stat/Stat.hh"
#include "proto/ConsoleRequest.pb.h"

#include <XrdOuc/XrdOucEnv.hh>

//----------------------------------------------------------------------------
// Adjust replica (repairOnClose from FST). The request is a protobuf
// 'file adjustreplica' command in mgm.cmd.proto. Older FSTs only send the
// path in mgm.path, the same command is built for them.
//----------------------------------------------------------------------------
int
XrdMgmOfs::AdjustReplica(const char* path,
                         const char* ininfo,
                         XrdOucEnv& env,
                         XrdOucErrInfo& error,
                         eos::common::VirtualIdentity& vid,
                         const XrdSecEntity* client)
{
  static const char* epname = "AdjustReplica";
  REQUIRE_SSS_OR_LOCAL_AUTH;
  ACCESSMODE_W;
  MAYSTALL;
  const char* inpath = path;
  MAYREDIRECT;
  EXEC_TIMING_BEGIN("AdjustReplica");
  vid = eos::common::VirtualIdentity::Root();
  gOFS->MgmStats.Add("AdjustReplica", 0, 0, 1);
  eos::console::RequestProto req;
  const char* b64data = env.Get("mgm.cmd.proto");
  const char* spath = env.Get("mgm.path");

  if (b64data) {
    std::string raw_pb;

    if (!eos::common::SymKey::Base64Decode(b64data, raw_pb) ||
        !req.ParseFromString(raw_pb)) {
      eos_thread_err("%s", "msg=\"adjustreplica failed to decode request\"");
      return Emsg(epname, error, EINVAL, "decode adjustreplica request",
                  spath ? spath : "");
    }
  } else if (spath && strlen(spath)) {
    req.mutable_file()->mutable_md()->set_path(spath);
    req.mutable_file()->mutable_adjustreplica();
  } else {
    eos_thread_err("%s", "msg=\"adjustreplica failed - no given path\"");
    return Emsg(epname, error, EIO, "repair [EIO]", "no path");
  }

  // This entry point runs as root, it must not run any other command
  if (!req.has_file() || !req.file().has_adjustreplica()) {
    eos_thread_err("%s", "msg=\"adjustreplica got a different command\"");
    return Emsg(epname, error, EINVAL, "accept non adjustreplica request",
                spath ? spath : "");
  }

  const std::string target =
      (req.file().md().id() ? eos::common::FileId::Fid2Hex(req.file().md().id())
                            : req.file().md().path());
  eos::mgm::FileCmd cmd(std::move(req), vid);
  const eos::console::ReplyProto reply = cmd.ProcessRequest();

  if (reply.retc()) {
    eos_thread_err("msg=\"adjustreplica failed\" target=\"%s\" retc=%i err=\"%s\"",
                   target.c_str(), reply.retc(), reply.std_err().c_str());
    return Emsg(epname, error, EIO, "repair [EIO]", target.c_str());
  }

  eos_thread_debug("msg=\"adjustreplica succeeded\" target=\"%s\" out=\"%s\"",
                   target.c_str(), reply.std_out().c_str());
  const char* ok = "OK";
  error.setErrInfo(strlen(ok) + 1, ok);
  EXEC_TIMING_END("AdjustReplica");
  return SFS_DATA;
}
