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

//------------------------------------------------------------------------------
// author: Lukasz Janyst <ljanyst@cern.ch>
// desc:   TestHelpers
//------------------------------------------------------------------------------

#include "namespace/utils/TestHelpers.hh"
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <unistd.h>

//------------------------------------------------------------------------------
// Create a temporary file name
//------------------------------------------------------------------------------
std::string getTempName(std::string dir, std::string prefix)
{
  dir += "/" + prefix;
  dir += "XXXXXX";
  size_t sz = 4 * 1024;
  char tmp_name[sz];
  strncpy(tmp_name, dir.c_str(), std::min(dir.length(), sz));
  tmp_name[std::min(dir.length(), sz)] = '\0';
  int tmp_fd = mkstemp(tmp_name);

  if (tmp_fd == -1) {
    return "";
  }

  (void) close(tmp_fd);
  return std::string(tmp_name);
}
