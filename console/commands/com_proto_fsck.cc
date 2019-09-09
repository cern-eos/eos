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
  oss << "Usage: fsck [stat|enable|disable|report|repair|search]" << std::endl
      << "    control and display file system check information" << std::endl
      << std::endl
      << "  fsck enable [<interval>]" << std::endl
      << "    enable fsck with interval in minutes (default 30 minutes)" << std::endl
      << std::endl
      << "  fsck disable" << std::endl
      << "    disable fsck" << std::endl
      << std::endl
      << "  fsck stat" << std::endl
      << "    print summary of consistency checks" << std::endl
      << std::endl
      << "  fsck config <key> [<value>]" << std::endl
      << "    configure the fsck with the following possible options:"
      << std::endl
      << "    show-dark-files : yes/no [default no]" << std::endl
      << "    show-offline    : yes/no [default no]" << std::endl
      << "    toggle-repair   : start/stop repair thread, no <value> required"
      << std::endl
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
      << "  fsck repair [--checksum|--checksum-commit|--resync|--unlink-unregistered|"
      << std::endl
      << "               --unlink-orphans|--adjust-replicas[-nodrop]|--drop-missing-replicas|"
      << std::endl
      << "               --unlink-zero-replicas|--replace-damaged-replicas|--all]"
      << std::endl
      << "    trigger the repair procedure for the various types of errors. Options:"
      << std::endl
      << "    checksum                  : issue 'verify' operation on all files with checksum errors"
      << std::endl
      << "    checksum-commit           : issue 'verify' operation on all files with checkusm errors"
      << std::endl
      << "                                and force a commit of size and checksum to the MGM"
      << std::endl
      << "    resync                    : issue a 'resync' operation on all files with any errors."
      << std::endl
      << "                                This will resync the MGM metadata to the storage node and will clean-up"
      << std::endl
      << "                                'ghost' entries from the FST metadata cache."
      << std::endl
      << "     unlink-unregistered      : unlink replicas which are not connected to their"
      << std::endl
      << "                                logical name"
      << std::endl
      << "     unlink-orphans           : unlink replicas which don't belong to any logical name"
      << std::endl
      << "     adjust-replicas          : try to fix all replica inconsistencies. If 'nodrop' is used"
      << std::endl
      << "                                replicas are only added and never removed"
      << std::endl
      << "     drop-missing-replicas    : drop replicas from the namespace if they can not"
      << std::endl
      << "                                be found on disk"
      << std::endl
      << "     unlink-zero-replicas     : drop all files that have no replicas attached"
      << std::endl
      << "                                and are older than 48 hours"
      << std::endl
      << "     replace-damaged-replicas : drop damaged replicas of a files and replace"
      << std::endl
      << "                                with healthy ones if possible"
      << std::endl;
  std::cerr << oss.str() << std::endl;
}
