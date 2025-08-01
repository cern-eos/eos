//------------------------------------------------------------------------------
//! @file com_acl.cc
//! @author Stefan Isidorovic <stefan.isidorovic@comtrade.com>
//!         Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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
#include "console/commands/helpers/AclHelper.hh"
#include <sstream>

//! Forward declaration
void com_acl_help();

//------------------------------------------------------------------------------
// Acl command entrypoint
//------------------------------------------------------------------------------
int com_acl(char* arg)
{
  if (wants_help(arg)) {
    com_acl_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  AclHelper acl(gGlobalOpts);

  if (!acl.ParseCommand(arg)) {
    com_acl_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  global_retc = acl.Execute(true, true);
  return global_retc;
}

//------------------------------------------------------------------------------
// Print help message
//------------------------------------------------------------------------------
void com_acl_help()
{
  std::ostringstream oss;
  oss
      << "Usage: eos acl [-l|--list] [-R|--recursive]"
      << " [-p|--position <pos>] [-f|--front] "
      << "[--sys|--user] [<rule>] <identifier>" << std::endl
      << "  atomically set and modify ACLs for the given directory path/sub-tree\n"
      << std::endl
      << "  -h, --help      : print help message" << std::endl
      << "  -R, --recursive : apply to directories recursively" << std::endl
      << "  -l, --list      : list ACL rules" << std::endl
      << "  -p, --position  : add the acl rule at specified position" << std::endl
      << "  -f, --front     : add the acl rule at the front position" << std::endl
      << "      --user      : handle user.acl rules on directory" << std::endl
      << "      --sys       : handle sys.acl rules on directory - admin only\n"
      << std::endl
      << "  <identifier> can be one of <path>|cid:<cid-dec>|cxid:<cid-hex>\n"
      << std::endl
      << "  <rule> is created similarly to chmod rules. Every rule begins with"
      << std::endl
      << "    [u|g|egroup] followed by \":\" or \"=\" and an identifier."
      << std::endl
      << "    \":\" is used to for modifying permissions while" << std::endl
      << "    \"=\" is used for setting/overwriting permissions." << std::endl
      << "    When modifying permissions every ACL flag can be added with" <<
      std::endl
      << "    \"+\" or removed with \"-\"." << std::endl
      << "    By default rules are appended at the end of acls" << std::endl
      << "    This ordering can be changed via --position flag" << std::endl
      << "    which will add the new rule at a given position starting at 1 or" <<
      std::endl
      << "    the --front flag which adds the rule at the front instead" << std::endl
      << std::endl
      << "Examples:" << std::endl
      << "  acl --user u:1001=rwx /eos/dev/" << std::endl
      << "    Set ACLs for user id 1001 to rwx" << std::endl
      << "  acl --user u:1001:-w /eos/dev" << std::endl
      << "    Remove \'w\' flag for user id 1001" << std::endl
      << "  acl --user u:1001:+m /eos/dev" << std::endl
      << "    Add change mode permission flag for user id 1001" << std::endl
      << "  acl --user u:1010= /eos/dev" << std::endl
      << "    Remove all ACls for user id 1001" << std::endl
      << "  acl --front --user u:1001=rwx /eos/dev" << std::endl
      << "     Add the user id 1001 rule to the front of ACL rules" << std::endl;
  std::cerr << oss.str() << std::endl;
}
