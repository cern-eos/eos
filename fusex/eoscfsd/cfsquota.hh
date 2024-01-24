//------------------------------------------------------------------------------
//! @file cfsquota.hh
//! @author Andreas-Joachim Peters CERN
//! @brief Class providing the quota en-/disabling
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2022 CERN/Switzerland                                  *
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

#pragma once

class cfsquota
{
public:
  cfsquota(const char* uqpath = "/@eoscfsd/.cfsd/quota/user/",
           const char* gqpath = "/@eoscfsd/.cfsd/quota/group/") : userquotapath(uqpath),
    groupquotapath(gqpath)  {};


  bool hasquota(uid_t uid, gid_t gid)
  {
    time_t now = time(NULL);
    // fprintf(stderr,"lookup for %d %d\n", uid,gid);
    // user quota
    {
      auto entry = userquotamap.find(uid);

      if ((entry != userquotamap.end()) && (now < entry->second.valid)) {
        // serve from cache
        return true;
      }

      // refresh from fs
      std::string lookup = userquotapath + std::string("/") + std::to_string((
                             int)uid);
      struct stat buf;

      if (!::lstat(lookup.c_str(), &buf)) {
        userquotamap[uid].valid = now + 60;
        return true;
      } else {
        userquotamap.erase(uid);
      }
    }
    // group quota
    {
      auto entry = groupquotamap.find(gid);

      if ((entry != groupquotamap.end()) && (now < entry->second.valid)) {
        // serve from cache
        return true;
      }

      // refresh from fs
      std::string lookup = groupquotapath + std::string("/") + std::to_string((
                             int)gid);
      struct stat buf;

      if (!::lstat(lookup.c_str(), &buf)) {
        groupquotamap[gid].valid = now + 60;
        return true;
      } else {
        groupquotamap.erase(gid);
      }
    }
    return false;
  }

  class quotaentry
  {
  public:
    quotaentry() {}
    quotaentry(time_t v) : valid(v) {}
    time_t valid;
  };

private:
  std::string userquotapath;
  std::string groupquotapath;

  std::map<uid_t, quotaentry> userquotamap;
  std::map<gid_t, quotaentry> groupquotamap;
};
