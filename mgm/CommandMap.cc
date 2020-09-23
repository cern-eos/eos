// ----------------------------------------------------------------------
// File: CommandMap.cc
// Author: Georgios Bitzes - CERN
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

#include "mgm/CommandMap.hh"

namespace
{

using namespace eos::mgm;

std::map<std::string, FsctlCommand> fsctlCommandMap;

struct fsctlMapInit {
  fsctlMapInit()
  {
    fsctlCommandMap["access"] = FsctlCommand::access;
    fsctlCommandMap["adjustreplica"] = FsctlCommand::adjustreplica;
    fsctlCommandMap["checksum"] = FsctlCommand::checksum;
    fsctlCommandMap["chmod"] = FsctlCommand::chmod;
    fsctlCommandMap["chown"] = FsctlCommand::chown;
    fsctlCommandMap["commit"] = FsctlCommand::commit;
    fsctlCommandMap["drop"] = FsctlCommand::drop;
    fsctlCommandMap["event"] = FsctlCommand::event;
    fsctlCommandMap["getfmd"] = FsctlCommand::getfmd;
    fsctlCommandMap["getfusex"] = FsctlCommand::getfusex;
    fsctlCommandMap["is_master"] = FsctlCommand::is_master;
    fsctlCommandMap["mastersignalbounce"] = FsctlCommand::mastersignalbounce;
    fsctlCommandMap["mastersignalreload"] = FsctlCommand::mastersignalreload;
    fsctlCommandMap["mkdir"] = FsctlCommand::mkdir;
    fsctlCommandMap["open"] = FsctlCommand::open;
    fsctlCommandMap["readlink"] = FsctlCommand::readlink;
    fsctlCommandMap["redirect"] = FsctlCommand::redirect;
    fsctlCommandMap["schedule2balance"] = FsctlCommand::schedule2balance;
    fsctlCommandMap["schedule2delete"] = FsctlCommand::schedule2delete;
    fsctlCommandMap["query2delete"] = FsctlCommand::schedule2delete;
    fsctlCommandMap["stat"] = FsctlCommand::stat;
    fsctlCommandMap["statvfs"] = FsctlCommand::statvfs;
    fsctlCommandMap["symlink"] = FsctlCommand::symlink;
    fsctlCommandMap["txstate"] = FsctlCommand::txstate;
    fsctlCommandMap["utimes"] = FsctlCommand::utimes;
    fsctlCommandMap["version"] = FsctlCommand::version;
    fsctlCommandMap["xattr"] = FsctlCommand::xattr;
  }

} fsctl_map_init_object;

}

EOSMGMNAMESPACE_BEGIN

FsctlCommand lookupFsctl(const std::string& cmd)
{
  auto it = fsctlCommandMap.find(cmd);

  if (it == fsctlCommandMap.end()) {
    return FsctlCommand::INVALID;
  }

  return it->second;
}

EOSMGMNAMESPACE_END
