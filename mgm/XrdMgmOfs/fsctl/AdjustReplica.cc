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
#include "mgm/stat/Stat.hh"
#include "mgm/ofs/XrdMgmOfs.hh"
#include "mgm/macros/Macros.hh"

#include <XrdOuc/XrdOucEnv.hh>

//----------------------------------------------------------------------------
// Adjust replica (repairOnClose from FST)
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
  MAYREDIRECT;
  EXEC_TIMING_BEGIN("AdjustReplica");
  // TODO(gbitzes): If vid is replaced with root, why is it even a parameter?!
  vid = eos::common::VirtualIdentity::Root();
  // Execute a proc command
  ProcCommand Cmd;
  XrdOucString info = "mgm.cmd=file&mgm.subcmd=adjustreplica&mgm.path=";
  char* spath = env.Get("mgm.path");

  if (spath) {
    info += spath;
    info += "&mgm.format=fuse";
    Cmd.open("/proc/user", info.c_str(), vid, &error);
    Cmd.close();
    gOFS->MgmStats.Add("AdjustReplica", 0, 0, 1);

    if (Cmd.GetRetc()) {
      eos_thread_err("msg=\"adjustreplica failed\" path=\"%s\"", spath);
      return Emsg(epname, error, EIO, "repair [EIO]", spath);
    }
  } else {
    eos_thread_err("msg=\"adjustreplica failed - no given path\"");
    return Emsg(epname, error, EIO, "repair [EIO]", "no path");
  }

  eos_thread_debug("msg=\"adjustreplica succeeded\" path=%s", spath);
  const char* ok = "OK";
  error.setErrInfo(strlen(ok) + 1, ok);
  EXEC_TIMING_END("AdjustReplica");
  return SFS_DATA;
}
