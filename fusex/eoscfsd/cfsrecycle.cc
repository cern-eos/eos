//------------------------------------------------------------------------------
//! @file cfsrecycle.cc
//! @author Andreas-Joachim Peters CERN
//! @brief Class providing username, executable from process/credentials
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

#include "cfsrecycle.hh"
#include <unistd.h>
#include "common/Path.hh"
#include "eoscfsd.hh"

int
cfsrecycle::provideBin(uid_t uid, ino_t ino)
{
  std::string binPath = recyclepath + std::string("/") + std::to_string(uid);
  {
    FsID rootId(0, 0);
    char srecycleuser[4096];
    time_t now = time(NULL);
    struct tm nowtm;
    localtime_r(&now, &nowtm);
    size_t index = ino;
    snprintf(srecycleuser, sizeof(srecycleuser) - 1,
             "%s/uid:%u/%04u/%02u/%u/%lu.#_recycle_#/",
             recyclepath.c_str(),
             uid,
             1900 + nowtm.tm_year,
             nowtm.tm_mon + 1,
             nowtm.tm_mday,
             index);
    struct stat buf;
    std::cerr << "# recycle " << srecycleuser << std::endl;

    // if i_index is not -1, we just compute the path for the given index and return if it exists already
    if (!::stat(srecycleuser, &buf)) {
      // great that exists already
    } else {
      // create the path
      std::string mpath = std::string(srecycleuser) + std::string("/dummy");
      eos::common::Path cpath(mpath.c_str());
      eos::common::Path ppath(srecycleuser);

      if (!cpath.MakeParentPath(S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH)) {
        std::cerr << "error: failed to create recycle bin path '" << cpath.GetPath() <<
                  "'\n";
        return -1;
      }

      if (chown(srecycleuser, uid, 0)) {
        std::cerr << "error: failed to chown reyccle bin path '" << cpath.GetPath() <<
                  "'\n";
        return -1;
      }

      if (chown(ppath.GetParentPath(), uid, 0)) {
        std::cerr << "error: failed to chown reyccle bin path '" << cpath.GetPath() <<
                  "'\n";
        return -1;
      }

      if (chmod(srecycleuser, S_IRWXU | S_IRGRP | S_IXGRP)) {
        std::cerr << "error: failed to chmod recycle bin path '" << cpath.GetPath() <<
                  "'\n";
        return -1;
      }

      if (chmod(ppath.GetParentPath(), S_IRWXU | S_IRGRP | S_IXGRP)) {
        std::cerr << "error: failed to chmod recycle bin path '" << cpath.GetPath() <<
                  "'\n";
        return -1;
      }
    }

    return ::open(srecycleuser, O_PATH | O_NOFOLLOW);
  }
}

int
cfsrecycle::moveBin(uid_t uid, ino_t parent, int source_fd, const char* name)
{
  struct stat buf;

  if (::fstat(source_fd, &buf)) {
    return -1;
  }

  int target_fd = provideBin(uid, buf.st_ino);

  if (target_fd >= 0) {
    struct stat buf;
    ::fstatat(source_fd, name, &buf, AT_SYMLINK_NOFOLLOW);
    std::string newname = std::string(name) + std::string(".") + std::to_string((
                            unsigned long) buf.st_ino) + std::string(".#_recycle_#");
    int rc = ::renameat(source_fd, name, target_fd, newname.c_str());
    rc |= close(target_fd);
    return rc;
  } else {
    return -1;
  }
}

bool
cfsrecycle::shouldRecycle(uid_t uid, ino_t parent, int source_fd,
                          const char* name)
{
  if (std::string(name).find(".#_recycle_#") == std::string::npos) {
    return true;
  } else {
    return false;
  }
}
