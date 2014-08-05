// ----------------------------------------------------------------------
// File: Rewrite.cc
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

  EXEC_TIMING_BEGIN("Rewrite");

  bool IsEnabledAutoRepair = false;
  {
    // check if 'autorepair' is enabled
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
    if (FsView::gFsView.mSpaceView.count("default") && (FsView::gFsView.mSpaceView["default"]->GetConfigMember("autorepair") == "on"))
      IsEnabledAutoRepair = true;
    else
      IsEnabledAutoRepair = false;

  }

  char* hexfid = env.Get("mgm.fxid");

  if (!IsEnabledAutoRepair)
  {
    eos_thread_info("msg=\"suppressing auto-repair\" fxid=\"%s\"", (hexfid) ?
                    hexfid : "<missing>");
    // the rewrite was suppressed!
    const char* ok = "OK";
    error.setErrInfo(strlen(ok) + 1, ok);
    EXEC_TIMING_END("Rewrite");
    return SFS_DATA;
  }
  eos::common::Mapping::VirtualIdentity vid;
  eos::common::Mapping::Root(vid);

  // convert fxid to path
  const char* spath = 0;
  errno = 0;
  std::string fullpath = "";
  eos::common::FileId::fileid_t fid = strtoul(hexfid, 0, 16);
  if (!errno && fid)
  {
    eos::common::RWMutexReadLock nslock(gOFS->eosViewRWMutex);
    eos::FileMD* fmd = 0;
    try
    {
      fmd = gOFS->eosFileService->getFileMD(fid);
      fullpath = gOFS->eosView->getUri(fmd);
      spath = fullpath.c_str();
    }
    catch (eos::MDException &e)
    {
      eos_thread_err("msg=\"unable to reference fid=%lu in namespacen", fid);
      return Emsg(epname, error, EIO, "[EIO] rewrite", spath);
    }
  }
  // execute a proc command
  ProcCommand Cmd;
  XrdOucString info = "mgm.cmd=file&mgm.subcmd=convert&";
  info += "mgm.path=";
  info += spath;
  info += "&mgm.option=rewrite&mgm.format=fuse";
  if (spath)
  {
    Cmd.open("/proc/user", info.c_str(), vid, &error);
    Cmd.close();
    gOFS->MgmStats.Add("Rewrite", 0, 0, 1);
  }
  if (Cmd.GetRetc())
  {
    // the rewrite failed
    return Emsg(epname, error, EIO, "[EIO] rewrite", spath);
  }
  else
  {
    // the rewrite succeeded!
    const char* ok = "OK";
    error.setErrInfo(strlen(ok) + 1, ok);
    EXEC_TIMING_END("Rewrite");
    return SFS_DATA;
  }
}
