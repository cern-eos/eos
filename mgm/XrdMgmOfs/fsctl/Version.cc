// ----------------------------------------------------------------------
// File: Version.cc
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
#include "mgm/macros/Macros.hh"

#include <XrdOuc/XrdOucEnv.hh>

//----------------------------------------------------------------------------
// Get EOS version and features
//----------------------------------------------------------------------------
int
XrdMgmOfs::Version(const char* path,
                   const char* ininfo,
                   XrdOucEnv& env,
                   XrdOucErrInfo& error,
                   eos::common::VirtualIdentity& vid,
                   const XrdSecEntity* client)
{
  ACCESSMODE_R;
  MAYSTALL;
  MAYREDIRECT;
  gOFS->MgmStats.Add("Version", 0, 0, 1);
  bool features = env.Get("mgm.version.features");
  XrdOucString response = "version: retc=";
  int retc = 0;
  XrdOucErrInfo errInfo;
  ProcCommand procCommand;
  const char* cmdInfo = features ? "mgm.cmd=version&mgm.option=f"
                        : "mgm.cmd=version";

  if (procCommand.open("/proc/user", cmdInfo, vid, &errInfo)) {
    retc = EINVAL;
  }

  response += retc;

  if (!retc) {
    char buff[4096];
    response += " ";

    while (int nread = procCommand.read(0, buff, 4095)) {
      buff[nread] = '\0';
      response += buff;

      if (nread != 4095) {
        break;
      }
    }
  }

  error.setErrInfo(response.length() + 1, response.c_str());
  return SFS_DATA;
}
