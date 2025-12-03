//------------------------------------------------------------------------------
//! @file com_proto_recycle.cc
//------------------------------------------------------------------------------

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

#include "console/ConsoleMain.hh"
#include "console/commands/helpers/RecycleHelper.hh"
#include <sstream>

extern int com_recycle(char*);
void com_recycle_help();

//------------------------------------------------------------------------------
// Recycle command entrypoint
//------------------------------------------------------------------------------
int com_protorecycle(char* arg)
{
  if (wants_help(arg)) {
    com_recycle_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  RecycleHelper recycle(gGlobalOpts);

  if (!recycle.ParseCommand(arg)) {
    com_recycle_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  global_retc = recycle.Execute(true, true);
  return global_retc;
}

//------------------------------------------------------------------------------
// Print help message
//------------------------------------------------------------------------------
void com_recycle_help()
{
  std::ostringstream oss;
  oss << "Usage: recycle [ls|purge|restore|config ...]\n"
      << "    provides recycle bin functionality\n"
      << "  recycle [-m]\n"
      << "    print status of recycle bin and config status if executed by root\n"
      << "    -m     : display info in monitoring format\n"
      << std::endl
      << "  recycle ls [<date> [<limit>]] [-m] [-n] [--all] [--uid] [--rid <val>] \n"
      << "    list files in the recycle bin\n"
      << "    <date>      : can be <year>, <year>/<month> or <year>/<month>/<day> or"
      << "                   <year>/<month>/<day>/<index>\n"
      << "    <limit>     : maximum number of entries to return when listing\n"
      << "                  e.g.: recycle ls 2018/08/12 1000\n"
      << "    -m          : display info in monitoring format\n"
      << "    -n          : display numeric uid/gid(s) instead of names\n"
      << "    --all       : display entries of all users - only if root or admin\n"
      << "    --uid       : display entries for the current user id [default]\n"
      << "    --rid <val> : display entries corresponding to the given recycle id\n"
      << "                  which represents the container id of the top directory\n"
      << "                  e.g. recycle ls --rid 1001\n"
      << std::endl
      << "  recycle purge [--all] [--uid] [--rid <val>] <date> | -k <key>\n"
      << "    purge files in the recycle bin either by date or by key\n"
      << "    --all       : purge entries of all users - only if root or admin\n"
      << "    --uid       : purge entries for the current user [default] \n"
      << "    --rid <val> : purge entries corresponding to the given recycle id\n"
      << "    <date>      : can be <year>, <year>/<month> or <year>/<month>/<day>\n"
      << "                  and can't be used together with a recycle key\n"
      << "    -k <key>    : purge only the given key\n"
      << std::endl
      << "  recycle restore [-p] [-f|--force-original-name] [-r|--restore-versions] <key>\n"
      << "    undo the deletion identified by the recycle <key>\n"
      << "    -p          : create all missing parent directories\n"
      << "    -f          : move deleted files/dirs back to their original location\n"
      << "                  (otherwise the key entry will have a <.inode> suffix)\n"
      << "    -r          : restore all previous versions of a file\n"
      << std::endl
      << "  recycle project --path <path> [--acl <val>]\n"
      << "    setup a recycle id that will group all the recycled paths from\n"
      << "    the given top level directory <path>. Optionally, specify a list\n"
      << "    of ACLs that are appended to the recycle location and control the \n"
      << "    access to the recycled entries. The recycle id is represented by the\n"
      << "    container id of <path> and is used to construct the recycle path:\n"
      << "    /eos/<instance>/proc/recycle/rid:<cid_value>/2025...\n"
      << "    ACL val is the usual string representation of ACLs e.g u:1234:rx\n"
      << std::endl
      << "  recycle config <key> <value>\n"
      << "    where <key> and <value> need to be one of the following:\n"
      << "    --dump\n"
      << "      dump the current recycle policy configuration\n"
      << "    [--add-bin|--remove-bin] <sub-tree>\n"
      << "      --add-bin    : enable recycle bin for deletion in <sub-tree>\n"
      << "      --remove-bin : disable recycle bin for <sub-tree>\n"
      << "    --lifetime <seconds>\n"
      << "      configure FIFO lifetime for the recycle bin\n"
      << "    --ratio <0..1.0>\n"
      << "      configure the volume/inode keep ratio. E.g.: 0.8 means files\n"
      << "      will only be recycled if more than 80% of the volume/inodes\n"
      << "      quota is used. The low-watermark is by default 10% below the\n"
      << "      the given ratio.\n"
      << "    --size <value>[K|M|G]\n"
      << "      configure the quota for the maximum size of the recycle bin\n"
      << "      If no unit is set explicitly then bytes is assumed.\n"
      << "    --inodes <value>[K|M|G]\n"
      << "      configure the quota for the maximum number of inodes in the\n"
      << "      recycle bin.\n"
      << "    --dry-run <yes/no>\n"
      << "      when dry-run mode is enabled, no removal of entries is performed\n"
      << "    --collect-interval <seconds>\n"
      << "      how ofen the recycler collects new entries to be removed from\n"
      << "      the recycle bin. Default once per day i.e 86400 seconds.\n"
      << "      Change only for testing!\n"
      << "    --remove-interval <seconds>\n"
      << "      how often the recycler removes collected entries. The collected\n"
      << "      container ids to be removed are sharded and the removal is spread\n"
      << "      evenly across collect-interval/remove-interval slots. Default once\n"
      << "      every hour i.e. 3600. Change only for testing!\n"
      << "    Note: The last two parameters should be changed only for testing\n"
      << "    and while maintaining the following order: \n"
      << "    remove-interval << collection-interval\n"
      << std::endl;
  std::cerr << oss.str() << std::endl;
}
