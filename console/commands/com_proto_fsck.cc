//------------------------------------------------------------------------------
//! @file com_fsck.cc
//! @autor Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

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

#include "console/ConsoleMain.hh"
#include "console/commands/helpers/FsckHelper.hh"

void com_fsck_help();

//------------------------------------------------------------------------------
// Fsck command entry point
//------------------------------------------------------------------------------
int com_proto_fsck(char* arg)
{
  if (wants_help(arg)) {
    com_fsck_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  FsckHelper fsck(gGlobalOpts);

  if (!fsck.ParseCommand(arg)) {
    com_fsck_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  global_retc = fsck.Execute();
  return global_retc;
}

//------------------------------------------------------------------------------
// Print help message
//------------------------------------------------------------------------------
void com_fsck_help()
{
  std::ostringstream oss;
  oss << "Usage: fsck [stat|config|report|repair]\n"
      << "    control and display file system check information\n"
      << std::endl
      << "  fsck stat\n"
      << "    print summary of consistency checks\n"
      << std::endl
      << "  fsck config <key> [<value>]\n"
      << "    configure the fsck with the following possible options:"
      << std::endl
      << "    toggle-collect       : enable/disable error collection thread, <value> represents\n"
      << "                           the collection interval in minutes [default 30]\n"
      << "    toggle-repair        : enable/disable repair thread, no <value> required\n"
      << "    show-dark-files      : yes/no [default no]\n"
      << "    show-offline         : yes/no [default no]\n"
      << "    show-no-replica      : yes/no [default no]\n"
      << "    max-queued-jobs      : maximum number of queued jobs\n"
      << "    max-thread-pool-size : maximum number of threads in the fsck pool\n"
      << std::endl
      << "  fsck report [-a] [-h] [-i] [-l] [-j|--json] [--error <tag1> <tag2> ...]"
      << std::endl
      << "    report consistency check results, with the following options"
      << std::endl
      << "    -a         : break down statistics per file system" << std::endl
      << "    -i         : display file identifiers" << std::endl
      << "    -l         : display logical file name" << std::endl
      << "    -j|--json  : display in JSON output format" << std::endl
      << "    --error    : dispaly information about the following error tags"
      << std::endl
      << std::endl
      << "  fsck repair --fxid <val> [--async]\n"
      << "    repair the given file if there are any errors\n"
      << "    --fxid  : hexadecimal file identifier\n"
      << "    --async : job queued and ran by the repair thread if enabled\n"
      << std::endl;
  std::cerr << oss.str() << std::endl;
}
