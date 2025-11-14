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
      << "  recycle ls [-g|<date> [<limit>]] [-m] [-n]\n"
      << "    list files in the recycle bin\n"
      << "    -g     : list files of all users (if done by root or admin)\n"
      << "    <date> : can be <year>, <year>/<month> or <year>/<month>/<day> or"
      << "             <year>/<month>/<day>/<index>\n"
      << "   <limit> : maximum number of entries to return when listing\n"
      << "             e.g.: recycle ls 2018/08/12\n"
      << "    -m     : display info in monitoring format\n"
      << "    -n     : display numeric uid/gid(s) instead of names\n"
      << std::endl
      << "  recycle purge [-g|<date>] [-k <key>]\n"
      << "    purge files in the recycle bin\n"
      << "    -g       : empty recycle bin of all users (if done by root or admin)\n"
      << "    -k <key> : purge only the given key\n"
      << "    <date>   : can be <year>, <year>/<month> or <year>/<month>/<day>\n"
      << std::endl
      << "  recycle restore [-p] [-f|--force-original-name] [-r|--restore-versions] <recycle-key>\n"
      << "    undo the deletion identified by the <recycle-key>\n"
      << "    -p : create all missing parent directories\n"
      << "    -f : move deleted files/dirs back to their original location (otherwise\n"
      << "          the key entry will have a <.inode> suffix)" << std::endl
      << "     -r : restore all previous versions of a file" << std::endl
      << std::endl
      << "  recycle config <key> <value>\n"
      << "    where <key> and <value> need to be one of the following:\n"
      << std::endl
      << "    --dump\n"
      << "      dump the current recycle policy configuration\n"
      << std::endl
      << "    [--add-bin|--remove-bin] <sub-tree>\n"
      << "      --add-bin    : enable recycle bin for deletion in <sub-tree>\n"
      << "      --remove-bin : disable recycle bin for <sub-tree>\n"
      << std::endl
      << "    --lifetime <seconds>\n"
      << "      configure FIFO lifetime for the recycle bin\n"
      << std::endl
      << "    --ratio <0..1.0>\n"
      << "      configure the volume/inode keep ratio. E.g.: 0.8 means files\n"
      << "      will only be recycled if more than 80% of the volume/inodes\n"
      << "      quota is used. The low-watermark is by default 10% below the\n"
      << "      the given ratio.\n"
      << std::endl
      << "    --size <value>[K|M|G]\n"
      << "      configure the quota for the maximum size of the recycle bin\n"
      << "      If no unit is set explicitly then bytes is assumed.\n"
      << std::endl
      << "    --inodes <value>[K|M|G]\n"
      << "      configure the quota for the maximum number of inodes in the\n"
      << "      recycle bin.\n"
      << std::endl
      << "    --dry-run <yes/no>\n"
      << "      when dry-run mode is enabled, no removal of entries is performed\n"
      << std::endl
      << "    --collect-interval <seconds>\n"
      << "      how ofen the recycler collects new entries to be removed from\n"
      << "      the recycle bin. Default once per day i.e 86400 seconds.\n"
      << "      Change only for testing!\n"
      << std::endl
      << "    --remove-interval <seconds>\n"
      << "      how often the recycler removes collected entries. The collected\n"
      << "      container ids to be removed are sharded and the removal is spread\n"
      << "      evenly across collect-interval/remove-interval slots. Default once\n"
      << "      every hour i.e. 3600. Change only for testing!\n"
      << std::endl
      << "    Note: The last two parameters should be changed only for testing\n"
      << "    and while maintaining the following order: \n"
      << "    remove-interval << collection-interval\n"
      << std::endl;
  std::cerr << oss.str() << std::endl;
}
