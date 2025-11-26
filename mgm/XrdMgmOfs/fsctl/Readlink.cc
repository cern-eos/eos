// ----------------------------------------------------------------------
// File: Readlink.cc
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
#include "mgm/XrdMgmOfs.hh"
#include "mgm/macros/Macros.hh"

#include <XrdOuc/XrdOucEnv.hh>

//----------------------------------------------------------------------------
// Resolve symbolic link
//----------------------------------------------------------------------------
int
XrdMgmOfs::Readlink(const char* path,
                    const char* ininfo,
                    XrdOucEnv& env,
                    XrdOucErrInfo& error,
                    eos::common::VirtualIdentity& vid,
                    const XrdSecEntity* client)
{
  ACCESSMODE_R;
  MAYSTALL;
  MAYREDIRECT;
  gOFS->MgmStats.Add("Fuse-Readlink", vid.uid, vid.gid, 1);
  XrdOucString link = "";
  int retc = 0;

  if (readlink(path, error, link, client)) {
    retc = (error.getErrInfo()) ? error.getErrInfo() : -1;
  }

  XrdOucString response = "readlink: retc=";
  response += retc;

  if (!retc) {
    if (env.Get("eos.encodepath")) {
      link = eos::common::StringConversion::curl_escaped(link.c_str()).c_str();
    }

    response += " ";
    response += link.c_str();
  }

  error.setErrInfo(response.length() + 1, response.c_str());
  return SFS_DATA;
}
