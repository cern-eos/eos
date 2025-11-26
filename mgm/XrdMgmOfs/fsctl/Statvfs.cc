// ----------------------------------------------------------------------
// File: Statvfs.cc
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
#include "common/utils/RandUtils.hh"
#include "mgm/stat/Stat.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/macros/Macros.hh"
#include "mgm/quota/Quota.hh"
#include "mgm/fsview/FsView.hh"

#include <XrdOuc/XrdOucEnv.hh>

//----------------------------------------------------------------------------
// Virtual filesystem stat
//----------------------------------------------------------------------------
int
XrdMgmOfs::Statvfs(const char* path,
                   const char* ininfo,
                   XrdOucEnv& env,
                   XrdOucErrInfo& error,
                   eos::common::VirtualIdentity& vid,
                   const XrdSecEntity* client)
{
  ACCESSMODE_R;
  MAYSTALL;
  MAYREDIRECT;
  gOFS->MgmStats.Add("Fuse-Statvfs", vid.uid, vid.gid, 1);
  XrdOucString space = env.Get("path");

  if (env.Get("eos.encodepath")) {
    space = eos::common::StringConversion::curl_unescaped(space.c_str()).c_str();
  }

  static XrdSysMutex statvfsmutex;
  static time_t laststat = 0;
  static long long freebytes = 0;
  static long long freefiles = 0;
  static long long maxbytes = 0;
  static long long maxfiles = 0;
  long long l_freebytes = 0;
  long long l_freefiles = 0;
  long long l_maxbytes = 0;
  long long l_maxfiles = 0;
  int retc = 0;

  if (space.length()) {
    int deepness = 0;
    int spos = 0;

    while ((spos = space.find("/", spos)) != STR_NPOS) {
      deepness++;
      spos++;
    }

    if ((!getenv("EOS_MGM_STATVFS_ONLY_QUOTA") && (deepness < 4)) ||
        (getenv("EOS_MGM_STATVFS_ONLY_SPACE"))) {
      statvfsmutex.Lock();
      time_t now = time(NULL);

      // Use caching to avoid often expensive space recomputations
      if ((now - laststat) > eos::common::getRandom(5, 15)) {
        // Take the sums from all file systems in 'default' space
        eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);

        if (FsView::gFsView.mSpaceView.count("default")) {
          freebytes =
            FsView::gFsView.mSpaceView["default"]->SumLongLong("stat.statfs.freebytes",
                false);
          freefiles =
            FsView::gFsView.mSpaceView["default"]->SumLongLong("stat.statfs.ffree", false);
          maxbytes =
            FsView::gFsView.mSpaceView["default"]->SumLongLong("stat.statfs.capacity",
                false);
          maxfiles =
            FsView::gFsView.mSpaceView["default"]->SumLongLong("stat.statfs.files", false);
        }

        laststat = now;
      }

      l_freebytes = freebytes;
      l_freefiles = freefiles;
      l_maxbytes  = maxbytes;
      l_maxfiles  = maxfiles;
      statvfsmutex.UnLock();
    } else {
      const std::string sspace = space.c_str();
      Quota::GetIndividualQuota(vid, sspace, l_maxbytes, l_freebytes,
                                l_maxfiles, l_freefiles, true);
    }
  } else {
    retc = EINVAL;
  }

  XrdOucString response = "statvfs: retc=";
  response += retc;

  if (!retc) {
    char val[1025];
    snprintf(val, 1024, "%lld", l_freebytes);
    response += " f_avail_bytes=";
    response += val;
    snprintf(val, 1024, "%lld", l_freefiles);
    response += " f_avail_files=";
    response += val;
    snprintf(val, 1024, "%lld", l_maxbytes);
    response += " f_max_bytes=";
    response += val;
    snprintf(val, 1024, "%lld", l_maxfiles);
    response += " f_max_files=";
    response += val;
  }

  error.setErrInfo(response.length() + 1, response.c_str());
  return SFS_DATA;
}
