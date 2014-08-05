// ----------------------------------------------------------------------
// File: Statvfs.cc
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
  ACCESSMODE_R;
  MAYSTALL;
  MAYREDIRECT;

  gOFS->MgmStats.Add("Fuse-Statvfs", vid.uid, vid.gid, 1);

  XrdOucString space = env.Get("path");
  static XrdSysMutex statvfsmutex;
  static unsigned long long freebytes = 0;
  static unsigned long long freefiles = 0;
  static unsigned long long maxbytes = 0;
  static unsigned long long maxfiles = 0;

  static time_t laststat = 0;

  XrdOucString response = "";

  if (!space.length())
  {
    response = "df: retc=";
    response += EINVAL;
  }
  else
  {
    statvfsmutex.Lock();

    // here we put some cache to avoid too heavy space recomputations
    if ((time(NULL) - laststat) > (10 + (int) rand() / RAND_MAX))
    {
      // take the sum's from all file systems in 'default'
      if (FsView::gFsView.mSpaceView.count("default"))
      {
        eos::common::RWMutexReadLock vlock(FsView::gFsView.ViewMutex);
        freebytes = FsView::gFsView.mSpaceView["default"]->SumLongLong("stat.statfs.freebytes");
        freefiles = FsView::gFsView.mSpaceView["default"]->SumLongLong("stat.statfs.ffree");

        maxbytes = FsView::gFsView.mSpaceView["default"]->SumLongLong("stat.statfs.capacity");
        maxfiles = FsView::gFsView.mSpaceView["default"]->SumLongLong("stat.statfs.files");
      }
    }
    statvfsmutex.UnLock();
    response = "statvfs: retc=0";
    char val[1025];
    snprintf(val, 1024, "%llu", freebytes);
    response += " f_avail_bytes=";
    response += val;
    snprintf(val, 1024, "%llu", freefiles);
    response += " f_avail_files=";
    response += val;
    snprintf(val, 1024, "%llu", maxbytes);
    response += " f_max_bytes=";
    response += val;
    snprintf(val, 1024, "%llu", maxfiles);
    response += " f_max_files=";
    response += val;
    error.setErrInfo(response.length() + 1, response.c_str());
  }
  return SFS_DATA;
}