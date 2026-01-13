// ----------------------------------------------------------------------
// File: com_member.cc
// Author: Andreas-Joachim Peters - CERN
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

#include "console/ConsoleMain.hh"
#include "common/StringTokenizer.hh"

//------------------------------------------------------------------------------
// Print help message
//------------------------------------------------------------------------------
void com_member_help()
{
  std::ostringstream oss;
  oss << "Usage: member [--update] <egroup>\n"
      << "   show the (cached) information about egroup membership for the\n"
      << "   current user running the command. If the check is required for\n"
      << "   a different user then please use the \"eos -r <uid> <gid>\"\n"
      << "   command to switch to a different role.\n"
      << " Options:\n"
      << "    --update : Refresh cached egroup information\n";
  std::cerr << oss.str() << std::endl;
}

//------------------------------------------------------------------------------
// Egroup member
//------------------------------------------------------------------------------
int
com_member(char* arg)
{
  if (!arg || wants_help(arg)) {
    com_member_help();
    return (global_retc = EINVAL);
  }

  bool update = false;
  const char* option = nullptr;
  std::string soption;
  std::string egroup = "";
  XrdOucString in = "";
  eos::common::StringTokenizer subtokenizer(arg);
  subtokenizer.GetLine();

  do {
    option = subtokenizer.GetToken();

    if (!option || !strlen(option)) {
      break;
    }

    soption = option;

    if ((soption == "--help") || (soption == "-h")) {
      com_member_help();
      return (global_retc = 0);
    }

    if (soption == "--update") {
      update = true;
      continue;
    }

    if (egroup.empty()) {
      egroup = option;
    } else {
      std::cerr << "error: command accepts only one egroup argument" << std::endl;
      return (global_retc = EINVAL);
    }
  } while (option && strlen(option));

  if (egroup.empty()) {
    std::cerr << "error: no egroup argument given" << std::endl;
    return (global_retc = EINVAL);
  }

  std::cout << "egroup: " << egroup << std::endl;
  in = "mgm.cmd=member";
  in += "&mgm.egroup=";
  in += egroup.c_str();

  if (update) {
    in += "&mgm.egroupupdate=true";
  }

  return (global_retc = output_result(client_command(in)));
}
