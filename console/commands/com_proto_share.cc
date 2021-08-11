//------------------------------------------------------------------------------
//! @file com_proto_share.cc
//! @author Andreas-Joachim Peteres - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2021 CERN/Switzerland                                  *
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
#include "console/commands/helpers/ShareHelper.hh"

void com_share_help();

//------------------------------------------------------------------------------
// Share command entry point
//------------------------------------------------------------------------------
int com_proto_share(char* arg)
{
  if (wants_help(arg)) {
    com_share_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  ShareHelper share(gGlobalOpts);

  if (!share.ParseCommand(arg)) {
    com_share_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  global_retc = share.Execute(true, true);

  if (global_retc) {
    std::cerr << share.GetError();
  }

  return global_retc;
}

//------------------------------------------------------------------------------
// Print help message
//------------------------------------------------------------------------------
void com_share_help()
{
  std::ostringstream oss;
  oss << "Usage: share ls|access|create|modify|remove|share|unshare\n"
      << "  share access <name> <username>|<uid> <gid>\n"
      << "    dump all ACL permission when <username> or <uid>/<gid> access the share <name>\n"
      << "\n"
      << "  share create <name> <acl> <path> \n"
      << "    create a share with name <name>, acl <acl> under path <path>\n"
      << "\n"
      << "  share ls\n"
      << "    list my sharesn\n"
      << "\n"
      << "  share modify <name> <acl>\n"
      << "    modify the acl of the existing share <name>\n"
      << "\n"
      << "  share remove <name>\n"
      << "    remove share with name <name>\n"
      << "\n"
      << "  share share <name> <acl> <path>\n"
      << "    share the existing share with name <name> using <acl> under <path>\n"
      << "\n"
      << "  share unshare <name>\n"
      << "    unshare the existing share with name <name>\n"
      << "\n"
      << "Examples:\n"
      << "          eos share ls [-m] \n"
      << "                                           : list all my shares [-m monitoring format] \n"
      << "                                           : list all shares with 'root' role \n"
      << std::endl;
  std::cerr << oss.str() << std::endl;
}
