// ----------------------------------------------------------------------
// File: Version.cc
// Author: Geoffray Adde - CERN
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
  ACCESSMODE_R;
  MAYSTALL;
  MAYREDIRECT;

  gOFS->MgmStats.Add("Version", 0, 0, 1);

  bool features = env.Get("mgm.version.features"); // decimal fid

  XrdOucString response;

  ProcCommand pc;
  XrdOucErrInfo err;
  if(pc.open ("/proc/user",
          features?"mgm.cmd=version&mgm.option=f":"mgm.cmd=version",
          vid,
          &err))
  {
    response = "version: retc=";
    response += EINVAL;
    error.setErrInfo(response.length() + 1, response.c_str());
    return SFS_DATA;
  }

  char buf[4096];
  while(int nread=pc.read (0, buf, 4095))
  {
    buf[nread]='\0';
    response += buf;
    if(nread!=4095)
    break;
  }

  response.insert("version: retc=0 ",0);
  error.setErrInfo(response.length() + 1, response.c_str());
  return SFS_DATA;
}
