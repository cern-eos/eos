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

  global_retc = recycle.Execute(false, true);

  if (global_retc) {
    std::cerr << recycle.GetError();
  }

  return global_retc;
}

//------------------------------------------------------------------------------
// Print help message
//------------------------------------------------------------------------------
void com_recycle_help()
{
  std::ostringstream oss;
  oss << "Usage: recycle [ls|purge|restore|config ...]" << std::endl
      << "    provides recycle bin functionality" << std::endl
      << "  recycle [-m]" << std::endl
      << "    print status of recycle bin and config status if executed by root"
      << std::endl
      << "    -m     : display info in monitoring format" << std::endl
      << std::endl
      << "  recycle ls [-g|<date>] [-m] [-n]" << std::endl
      << "    list files in the recycle bin" << std::endl
      << "    -g     : list files of all users (if done by root or admin)"
      << std::endl
      << "    <date> : can be <year>, <year>/<month> or <year>/<month>/<day>"
      << std::endl
      << "             e.g.: recycle ls 2018/08/12" << std::endl
      << "    -m     : display info in monitoring format" << std::endl
      << "    -n     : dispaly numeric uid/gid(s) instead of names" << std::endl
      << std::endl
      << "  recycle purge [-g|<date>] [-k <key>]" << std::endl
      << "    purge files in the recycle bin" << std::endl
      << "    -g       : empty recycle bin of all users (if done by root or admin)"
      << std::endl
      << "    -k <key> : purge only the given key"
      << std::endl
      << "    <date>   : can be <year>, <year>/<month> or <year>/<month>/<day>"
      << std::endl
      << std::endl
      << "  recycle restore [-p] [-f|--force-original-name] [-r|--restore-versions] <recycle-key>"
      << std::endl
      << "    undo the deletion identified by the <recycle-key>" << std::endl
      << "    -p : create all missing parent directories\n"
      << "    -f : move deleted files/dirs back to their original location (otherwise"
      << std::endl
      << "          the key entry will have a <.inode> suffix)" << std::endl
      << "     -r : restore all previous versions of a file" << std::endl
      << std::endl
      << "  recycle config [--add-bin|--remove-bin] <sub-tree>" << std::endl
      << "    --add-bin    : enable recycle bin for deletions in <sub-tree>" <<
      std::endl
      << "    --remove-bin : disable recycle bin for deletions in <sub-tree>" <<
      std::endl
      << std::endl
      << "  recycle config --lifetime <seconds>" << std::endl
      << "    configure FIFO lifetime for the recycle bin" << std::endl
      << std::endl
      << "  recycle config --ratio <0..1.0>" << std::endl
      << "    configure the volume/inode keep ratio. E.g: 0.8 means files will only"
      << std::endl
      << "    be recycled if more than 80% of the volume/inodes quota is used. The"
      << std::endl
      << "    low watermark is by default 10% below the given ratio."
      << std::endl
      << std::endl
      << "  recycle config --size <value>[K|M|G]" << std::endl
      << "    configure the quota for the maximum size of the recycle bin. "
      << std::endl
      << "    If no unit is set explicitly then we assume bytes." << std::endl
      << std::endl
      << "  recycle config --inodes <value>[K|M|G]" << std::endl
      << "    configure the quota for the maximum number of inodes in the recycle"
      << std::endl
      << "    bin." << std::endl;
  std::cerr << oss.str() << std::endl;
}
