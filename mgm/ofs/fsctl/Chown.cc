// ----------------------------------------------------------------------
// File: Chown.cc
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
// Chown of a file or directory
//----------------------------------------------------------------------------
int
XrdMgmOfs::Chown(const char* path,
                 const char* ininfo,
                 XrdOucEnv& env,
                 XrdOucErrInfo& error,
                 eos::common::VirtualIdentity& vid,
                 const XrdSecEntity* client)
{
  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;
  gOFS->MgmStats.Add("Fuse-Chown", vid.uid, vid.gid, 1);
  char* suid = env.Get("uid");
  char* sgid = env.Get("gid");
  int retc = 0;

  if (suid && sgid) {
    uid_t uid = (uid_t) atoi(suid);
    uid_t gid = (uid_t) atoi(sgid);

    if (_chown(path, uid, gid, error, vid)) {
      retc = error.getErrInfo();
    }
  } else {
    retc = EINVAL;
  }

  XrdOucString response = "chown: retc=";
  response += retc;
  error.setErrInfo(response.length() + 1, response.c_str());
  return SFS_DATA;
}
