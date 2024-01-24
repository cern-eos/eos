//------------------------------------------------------------------------------
//! @file cfsmapping.hh
//! @author Andreas-Joachim Peters CERN
//! @brief Class providing the username mapping to uid/gid with configurion on fs
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

class cfsmapping
{
public:
  cfsmapping(const char* path = "/@eoscfsd/.cfsd/mapping/name/") : namepath(
      path) {};
  void translate(std::string& name, uid_t& uid, gid_t& gid)
  {
    time_t now = time(NULL);
    auto entry = namemap.find(name);

    if ((entry != namemap.end()) && (now < entry->second.valid)) {
      // serve from cache
      uid = entry->second.uid;
      gid = entry->second.gid;
      return;
    }

    // refresh from fs
    std::string lookup = namepath + std::string("/") + name;
    struct stat buf;

    if (!::lstat(lookup.c_str(), &buf)) {
      uid = buf.st_uid;
      gid = buf.st_gid;
    } else {
      uid = 99;
      gid = 99;
    }

    // store in cache for 60s
    namemap[name].valid = now + 60;
    namemap[name].uid = uid;
    namemap[name].gid = gid;
  }

  class mapentry
  {
  public:
    mapentry() {}
    mapentry(time_t v, uid_t u, gid_t g) : valid(v), uid(u), gid(g) {}
    time_t valid;
    uid_t uid;
    gid_t gid;
  };

private:
  std::string namepath;
  std::map<std::string, mapentry> namemap;
};
