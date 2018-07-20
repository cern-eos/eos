// ----------------------------------------------------------------------
// File: Rewrite.cc
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
#include "namespace/interface/IView.hh"
#include "namespace/interface/IFileMD.hh"
#include "namespace/interface/IFileMDSvc.hh"
#include "mgm/Stat.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Macros.hh"
#include "mgm/FsView.hh"

#include <XrdOuc/XrdOucEnv.hh>

//----------------------------------------------------------------------------
// Repair file.
// Used to repair after scan error (E.g.: use the converter to rewrite)
//----------------------------------------------------------------------------
int
XrdMgmOfs::Rewrite(const char* path,
                   const char* ininfo,
                   XrdOucEnv& env,
                   XrdOucErrInfo& error,
                   eos::common::LogId& ThreadLogId,
                   eos::common::Mapping::VirtualIdentity& vid,
                   const XrdSecEntity* client)
{
  static const char* epname = "Rewrite";
  REQUIRE_SSS_OR_LOCAL_AUTH;
  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;
  EXEC_TIMING_BEGIN("Rewrite");
  bool IsEnabledAutoRepair = false;
  {
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);

    // Check if 'autorepair' is enabled
    if ((FsView::gFsView.mSpaceView.count("default")) &&
        (FsView::gFsView.mSpaceView["default"]->GetConfigMember("autorepair") ==
         "on")) {
      IsEnabledAutoRepair = true;
    }
  }
  // @todo(esindril): Transition
  char* hexfid = env.Get("mgm.fid"); // try to use new parameter

  if (!hexfid) {
    // Legacy, drop once fst/FmdDbMap.cc no longer uses mgm.fxid
    hexfid = env.Get("mgm.fxid");
  }

  if (!IsEnabledAutoRepair) {
    eos_thread_info("msg=\"suppressing auto-repair\" fxid=\"%s\"",
                    (hexfid) ? hexfid : "<missing>");
  } else {
    eos::common::Mapping::VirtualIdentity rvid;
    eos::common::Mapping::Root(rvid);
    // Convert fxid to path
    errno = 0;
    const char* spath = 0;
    std::string fullpath = 0;
    eos::common::FileId::fileid_t fid = strtoul(hexfid, 0, 16);

    if (fid && !errno) {
      eos::common::RWMutexReadLock nslock(gOFS->eosViewRWMutex);

      try {
        std::shared_ptr<eos::IFileMD> fmd = gOFS->eosFileService->getFileMD(fid);
        fullpath = gOFS->eosView->getUri(fmd.get());
        spath = fullpath.c_str();
      } catch (eos::MDException& e) {
        eos_thread_err("msg=\"no reference for file in namespace\" fid=%08llx", fid);
        return Emsg(epname, error, EIO, "rewrite [EIO]", spath);
      }
    }

    if (spath) {
      // Execute a proc command
      XrdOucString info = "mgm.cmd=file&mgm.subcmd=convert";
      info += "&mgm.path=";
      info += spath;
      info += "&mgm.option=rewrite&mgm.format=fuse";
      ProcCommand procCommand;
      procCommand.open("/proc/user", info.c_str(), rvid, &error);
      procCommand.close();

      if (procCommand.GetRetc()) {
        // Rewrite failed
        return Emsg(epname, error, EIO, "rewrite [EIO]", spath);
      }
    }
  }

  gOFS->MgmStats.Add("Rewrite", 0, 0, 1);
  const char* ok = "OK";
  error.setErrInfo(strlen(ok) + 1, ok);
  EXEC_TIMING_END("Rewrite");
  return SFS_DATA;
}
