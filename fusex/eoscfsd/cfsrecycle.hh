//------------------------------------------------------------------------------
//! @file cfsrecycle.hh
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

#include <string>

class cfsrecycle
{
public:
  cfsrecycle(const char* rpath = "/@eoscfsd/.cfsd/recycle/") : recyclepath(
      rpath) {};

  int provideBin(uid_t uid, ino_t parent);
  int moveBin(uid_t uid, ino_t parent, int source_fd, const char* name);
  bool shouldRecycle(uid_t uid, ino_t parent, int source_fd, const char* name);
private:
  std::string recyclepath;
};
