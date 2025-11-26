// ----------------------------------------------------------------------
// File: Redirect.cc
// Author: Geoffray Adde - CERN
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
#include "mgm/XrdMgmOfs.hh"
#include "mgm/XrdMgmOfsFile.hh"
#include "mgm/macros/Macros.hh"

#include <XrdOuc/XrdOucEnv.hh>

//----------------------------------------------------------------------------
// Get open redirect
//----------------------------------------------------------------------------
int
XrdMgmOfs::Redirect(const char* path,
                    const char* ininfo,
                    XrdOucEnv& env,
                    XrdOucErrInfo& error,
                    eos::common::VirtualIdentity& vid,
                    const XrdSecEntity* client)
{
  gOFS->MgmStats.Add("OpenRedirect", vid.uid, vid.gid, 1);
  XrdMgmOfsFile* file = new XrdMgmOfsFile(const_cast<char*>(client->tident));
  int retc = SFS_ERROR;

  if (file) {
    XrdSfsFileOpenMode oflags = SFS_O_RDONLY;
    mode_t omode = 0;

    if (env.Get("eos.client.openflags")) {
      std::string openflags = env.Get("eos.client.openflags");

      if (openflags.find("wo") != std::string::npos) {
        oflags |= SFS_O_WRONLY;
      }

      if (openflags.find("rw") != std::string::npos) {
        oflags |= SFS_O_RDWR;
      }

      if (openflags.find("cr") != std::string::npos) {
        oflags |= SFS_O_CREAT;
      }

      if (openflags.find("tr") != std::string::npos) {
        oflags |= SFS_O_TRUNC;
      }

      std::string openmode = env.Get("eos.client.openmode");
      omode = (mode_t) strtol(openmode.c_str(), NULL, 8);
    }

    if ((oflags & SFS_O_CREAT) || (oflags & SFS_O_RDWR) ||
        (oflags & SFS_O_TRUNC)) {
      ACCESSMODE_W;
      MAYSTALL;
      MAYREDIRECT;
    } else {
      ACCESSMODE_R;
      MAYSTALL;
      MAYREDIRECT;
    }

    int rc = file->open(path, oflags, omode, client, ininfo);
    std::string emsg = file->error.getErrText();

    if (rc == SFS_REDIRECT) {
      eos_thread_debug("success redirect=%s", error.getErrText());
      char buf[1024];
      snprintf(buf, 1024, ":%d/%s?", file->error.getErrInfo(), path);
      emsg.replace(emsg.find("?"), 1 , buf);
      error.setErrInfo(emsg.size() + 1, emsg.c_str());
      retc = SFS_DATA;
    } else {
      eos_thread_debug("failed redirect=%s", error.getErrText());
      error.setErrInfo(emsg.size() + 1, emsg.c_str());
      error.setErrCode(file->error.getErrInfo());
    }

    delete file;
  } else {
    const char* emsg = "allocate file object";
    error.setErrInfo(strlen(emsg) + 1, emsg);
    error.setErrCode(ENOMEM);
  }

  return retc;
}
