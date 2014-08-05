// ----------------------------------------------------------------------
// File: Adjustreplica.cc
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


// -----------------------------------------------------------------------
// This file is included source code in XrdMgmOfs.cc to make the code more
// transparent without slowing down the compilation time.
// -----------------------------------------------------------------------

{
  REQUIRE_SSS_OR_LOCAL_AUTH;
  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;

  EXEC_TIMING_BEGIN("AdjustReplica");

  // execute adjust replica
  eos::common::Mapping::VirtualIdentity vid;
  eos::common::Mapping::Root(vid);

  // execute a proc command
  ProcCommand Cmd;
  XrdOucString info = "mgm.cmd=file&mgm.subcmd=adjustreplica&mgm.path=";
  char* spath = env.Get("mgm.path");
  if (spath)
  {
    info += spath;
    info += "&mgm.format=fuse";
    Cmd.open("/proc/user", info.c_str(), vid, &error);
    Cmd.close();
    gOFS->MgmStats.Add("AdjustReplica", 0, 0, 1);
  }
  if (Cmd.GetRetc())
  {
    // the adjustreplica failed
    return Emsg(epname, error, EIO, "[EIO] repair", spath);
  }
  else
  {
    // the adjustreplica succeede!
    const char* ok = "OK";
    error.setErrInfo(strlen(ok) + 1, ok);
    EXEC_TIMING_END("AdjustReplica");
    return SFS_DATA;
  }
}
