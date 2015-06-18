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
/*----------------------------------------------------------------------------*/

/* Egroup member -  Interface */
int
com_member (char* arg1)
{
  eos::common::StringTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString egroup = "";

  XrdOucString in = "";

  if (wants_help(arg1))
    goto com_member_usage;

  in = "mgm.cmd=member";
  do
  {
    egroup = subtokenizer.GetToken();
    break;
  }
  while (1);

  if (egroup.length())
  {
    in += "&mgm.egroup=";
    in += egroup;
  }

  global_retc = output_result(client_user_command(in));
  return (0);

com_member_usage:
  fprintf(stdout, "usage: member [<egroup>]                                :  show the (cached) information about egroup membership\n");
  return (0);
}
