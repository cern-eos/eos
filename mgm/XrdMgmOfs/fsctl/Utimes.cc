// ----------------------------------------------------------------------
// File: Utimes.cc
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
  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;

  gOFS->MgmStats.Add("Fuse-Utimes", vid.uid, vid.gid, 1);

  char* tv1_sec;
  char* tv1_nsec;
  char* tv2_sec;
  char* tv2_nsec;

  tv1_sec = env.Get("tv1_sec");
  tv1_nsec = env.Get("tv1_nsec");
  tv2_sec = env.Get("tv2_sec");
  tv2_nsec = env.Get("tv2_nsec");

  struct timespec tvp[2];
  if (tv1_sec && tv1_nsec && tv2_sec && tv2_nsec)
  {
    tvp[0].tv_sec = strtol(tv1_sec, 0, 10);
    tvp[0].tv_nsec = strtol(tv1_nsec, 0, 10);
    tvp[1].tv_sec = strtol(tv2_sec, 0, 10);
    tvp[1].tv_nsec = strtol(tv2_nsec, 0, 10);
    
    int retc = utimes(spath.c_str(),
		      tvp,
		      error,
		      client,
		      0);
    
    XrdOucString response = "utimes: retc=";
    response += retc;
    error.setErrInfo(response.length() + 1, response.c_str());
    return SFS_DATA;
  }
  else
  {
    XrdOucString response = "utimes: retc=";
    response += EINVAL;
    error.setErrInfo(response.length() + 1, response.c_str());
    return SFS_DATA;
  }
}
