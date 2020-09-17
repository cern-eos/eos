// ----------------------------------------------------------------------
// File: Getfmd.cc
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
#include "common/Path.hh"
#include "namespace/interface/IView.hh"
#include "namespace/interface/IFileMD.hh"
#include "namespace/interface/IFileMDSvc.hh"
#include "mgm/Stat.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Macros.hh"
#include "namespace/Prefetcher.hh"

#include <XrdOuc/XrdOucEnv.hh>

//----------------------------------------------------------------------------
// Return metadata in env representation
//----------------------------------------------------------------------------
int
XrdMgmOfs::Getfmd(const char* path,
                  const char* ininfo,
                  XrdOucEnv& env,
                  XrdOucErrInfo& error,
                  eos::common::VirtualIdentity& vid,
                  const XrdSecEntity* client)
{
  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;
  gOFS->MgmStats.Add("GetMd", 0, 0, 1);
  char* afid = env.Get("mgm.getfmd.fid"); // decimal fid
  eos::common::FileId::fileid_t fid = afid ? strtoull(afid, 0, 10) : 0;
  XrdOucString response;

  if (fid) {
    std::string fullpath;
    std::shared_ptr<eos::IFileMD> fmd;

    // Use prefetching for QDB namespace
    if (!gOFS->eosView->inMemory()) {
      eos::Prefetcher::prefetchFileMDWithParentsAndWait(gOFS->eosView, fid);
    }

    eos::common::RWMutexReadLock vlock(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__,
                                       __FILE__);

    try {
      fmd = gOFS->eosFileService->getFileMD(fid);
      fullpath = gOFS->eosView->getUri(fmd.get());
    } catch (eos::MDException& e) {
      response = "getfmd: retc=";
      response += e.getErrno();
      error.setErrInfo(response.length() + 1, response.c_str());
      return SFS_DATA;
    }

    eos::common::Path cPath(fullpath.c_str());
    std::string fmdEnv = "";
    fmd->getEnv(fmdEnv, true);
    fmdEnv += "&container=";
    // Patch parent name
    XrdOucString safepath = cPath.GetParentPath();

    while (safepath.replace("&", "#AND#")) {}

    fmdEnv += safepath.c_str();
    response = "getfmd: retc=0 ";
    response += fmdEnv.c_str();

    if (response.find("checksum=&") != STR_NPOS) {
      // XrdOucEnv does not deal with empty values [... sigh ...]
      response.replace("checksum=&", "checksum=none&");
    }

    // Patch the file name
    safepath = cPath.GetName();

    if (safepath.find("&") != STR_NPOS) {
      XrdOucString initial_name = "name=";
      initial_name += safepath;

      while (safepath.replace("&", "#AND#")) {}

      XrdOucString safe_name = "name=";
      safe_name += safepath;
      response.replace(initial_name, safe_name);
    }
  } else {
    response = "getfmd: retc=";
    response += EINVAL;
  }

  error.setErrInfo(response.length() + 1, response.c_str());
  return SFS_DATA;
}
