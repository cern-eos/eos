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

/*----------------------------------------------------------------------------*/
#include "console/ConsoleMain.hh"
#include "common/StringTokenizer.hh"
/*----------------------------------------------------------------------------*/

/* Egroup member -  Interface */
int
com_member(char* arg1)
{
  eos::common::StringTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();

  XrdOucString option = "";
  bool update = false;
  XrdOucString egroup = "";
  XrdOucString in = "";

  do {
    option = subtokenizer.GetToken();

    if(!option.length()) {
      break;
    }

    if(option == "--help" || option == "-h") {
      goto com_member_usage;
    }

    if(option == "--update") {
      update = true;
      continue;
    }

    egroup = option;
  } while(option.length());

  std::cout << "egroup: " << egroup << std::endl;

  if(!egroup.length()) {
    goto com_member_usage;
  }

  in = "mgm.cmd=member";
  in += "&mgm.egroup=";
  in += egroup;

  if(update) {
    in += "&mgm.egroupupdate=true";
  }

  global_retc = output_result(client_command(in));
  return (0);
com_member_usage:
  fprintf(stdout,
          "usage: member [--update] [<egroup>]                                :  show the (cached) information about egroup membership\n"
          "                      --update : Refresh cached egroup information\n");
  global_retc = 0;
  return (0);
}
