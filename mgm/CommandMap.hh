// ----------------------------------------------------------------------
// File: CommandMap.hh
// Author: Georgios Bitzes - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#ifndef __EOSMGM_COMMANDMAP__HH__
#define __EOSMGM_COMMANDMAP__HH__

#include "mgm/Namespace.hh"
#include <string>
#include <map>

EOSMGMNAMESPACE_BEGIN

enum class FsctlCommand {
  INVALID = 0,
  access,
  adjustreplica,
  checksum,
  chmod,
  chown,
  commit,
  drop,
  event,
  getfmd,
  getfusex,
  is_master,
  mkdir,
  open,
  readlink,
  redirect,
  schedule2balance,
  schedule2delete,
  stat,
  statvfs,
  symlink,
  txstate,
  utimes,
  version,
  xattr
};

FsctlCommand lookupFsctl(const std::string& cmd);

EOSMGMNAMESPACE_END

#endif
