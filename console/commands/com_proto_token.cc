//------------------------------------------------------------------------------
//! @file com_proto_token.cc
//! @author Andreas-Joachim Peteres - CERN
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
#include "console/commands/helpers/TokenHelper.hh"

void com_token_help();

//------------------------------------------------------------------------------
// Token command entry point
//------------------------------------------------------------------------------
int com_proto_token(char* arg)
{
  if (wants_help(arg)) {
    com_token_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  TokenHelper token(gGlobalOpts);

  if (!token.ParseCommand(arg)) {
    com_token_help();
    global_retc = EINVAL;
    return EINVAL;
  }

  global_retc = token.Execute(true, true);
  return global_retc;
}

//------------------------------------------------------------------------------
// Print help message
//------------------------------------------------------------------------------
void com_token_help()
{
  std::ostringstream oss;
  oss << "Usage: token --token  <token> | --path <path> --expires <expires> [--permission <perm>] [--owner <owner>] [--group <group>] [--tree] [--origin <origin1> [--origin <origin2>] ...]] \n"
      << "    get or show a token\n\n"
      << "       token --token <token> \n"
      << "                                           : provide a JSON dump of a token - independent of validity\n"
      << "             --path <path>                 : define the namespace restriction - if ending with '/' this is a directory or tree, otherwise it references a file\n"
      << "             --permission <perm>           : define the token bearer permissions e.g 'rx' 'rwx' 'rwx!d' 'rwxq' - see acl command for permissions\n"
      << "             --owner <owner>               : identify the bearer with as user <owner> \n"
      << "             --group <group>               : identify the beaere with a group <group> \n"
      << "             --tree                        : request a subtree token granting permissions for the whole tree under <path>\n"
      << "              --origin <origin>            : restrict token usage to <origin> - multiple origin parameters can be provided\n"
      << "                                             <origin> := <regexp:hostname>:<regex:username>:<regex:protocol>\n"
      << "                                             - described by three regular extended expressions matching the \n"
      << "                                               bearers hostname, possible authenticated name and protocol\n"
      << "                                             - default is .*:.*:.* (be careful with proper shell escaping)"
      << "\n"
      << "Examples:\n"
      << "          eos token --path /eos/ --permission rx --tree\n"
      << "                                           : token with browse permission for the whole /eos/ tree\n"
      << "          eos token --path /eos/file --permission rwx --owner foo --group bar\n"
      << "                                           : token granting write permission for /eos/file as user foo:bar\n"
      << "          eos token --token zteos64:...\n"
      << "                                           : dump the given token\n"
      << std::endl;
  std::cerr << oss.str() << std::endl;
}
