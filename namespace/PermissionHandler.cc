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

//------------------------------------------------------------------------------
//! @author: Georgios Bitzes <georgios.bitzes@cern.ch>
//! @brief  Collection of functions to do permission checking
//------------------------------------------------------------------------------

#include "namespace/PermissionHandler.hh"
#include <sys/stat.h>

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Convert "user" mode_t permission bits to internally-used representation.
//------------------------------------------------------------------------------
char PermissionHandler::convertModetUser(mode_t mode) {
  char perms = 0;

  if ((mode & S_IRUSR) != 0u) {
    perms |= CANREAD;
  }

  if ((mode & S_IWUSR) != 0u) {
    perms |= CANWRITE;
  }

  if ((mode & S_IXUSR) != 0u) {
    perms |= CANENTER;
  }

  return perms;
}

//------------------------------------------------------------------------------
//! Convert "group" mode_t permission bits to internally-used representation.
//------------------------------------------------------------------------------
char PermissionHandler::convertModetGroup(mode_t mode) {
  char perms = 0;

  if ((mode & S_IRGRP) != 0u) {
    perms |= CANREAD;
  }

  if ((mode & S_IWGRP) != 0u) {
    perms |= CANWRITE;
  }

  if ((mode & S_IXGRP) != 0u) {
    perms |= CANENTER;
  }

  return perms;
}

//------------------------------------------------------------------------------
//! Convert "other" mode_t permission bits to internally-used representation.
//------------------------------------------------------------------------------
char PermissionHandler::convertModetOther(mode_t mode) {
  char perms = 0;

  if ((mode & S_IROTH) != 0u) {
    perms |= CANREAD;
  }

  if ((mode & S_IWOTH) != 0u) {
    perms |= CANWRITE;
  }

  if ((mode & S_IXOTH) != 0u) {
    perms |= CANENTER;
  }

  return perms;
}

//------------------------------------------------------------------------------
//! Check permissions and decide whether to allow or not.
//------------------------------------------------------------------------------
bool PermissionHandler::checkPerms(char actual, char requested) {
  for (int i = 0; i < 3; ++i) {
    if ((requested & (1 << i)) != 0) {
      if ((actual & (1 << i)) == 0) {
        return false;
      }
    }
  }

  return true;
}

//------------------------------------------------------------------------------
//! Convert requested permissions to internal representation. Ready to pass
//! onto checkPerms then.
//------------------------------------------------------------------------------
char PermissionHandler::convertRequested(mode_t requested) {
  char convFlags = 0;

  if ((requested & R_OK) != 0) {
    convFlags |= CANREAD;
  }

  if ((requested & W_OK) != 0) {
    convFlags |= CANWRITE;
  }

  if ((requested & X_OK) != 0) {
    convFlags |= CANENTER;
  }

  return convFlags;
}

//------------------------------------------------------------------------------
//! Parse octal mask
//------------------------------------------------------------------------------
bool PermissionHandler::parseOctalMask(const std::string &str, mode_t &out) {
  char *endptr = NULL;
  out = strtoll(str.c_str(), &endptr, 8);
  if(endptr != str.c_str() + str.size() || out == LLONG_MIN || out == LONG_LONG_MAX) {
    return false;
  }
  return true;
}

EOSNSNAMESPACE_END
