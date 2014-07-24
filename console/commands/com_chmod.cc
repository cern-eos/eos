// ----------------------------------------------------------------------
// File: com_chmod.cc
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

/* Mode Interface */
int
com_chmod (char* arg1)
{
  eos::common::StringTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString mode = subtokenizer.GetToken();
  XrdOucString option = "";
  XrdOucString in = "mgm.cmd=chmod";
  XrdOucString arg = "";

  if (mode.beginswith("-"))
  {
    option = mode;
    option.erase(0, 1);
    mode = subtokenizer.GetToken();
    in += "&mgm.option=";
    in += option;
  }

  XrdOucString path = subtokenizer.GetToken();

  if (wants_help(arg1))
    goto com_chmod_usage;

  if (!path.length() || !mode.length())
    goto com_chmod_usage;

  path = abspath(path.c_str());

  in += "&mgm.path=";
  in += path;
  in += "&mgm.chmod.mode=";
  in += mode;

  global_retc = output_result(client_user_command(in));
  return (0);

com_chmod_usage:
  fprintf(stdout, "usage: chmod [-r] <mode> <path>                             : set mode for <path> (-r recursive)\n");
  fprintf(stdout, "                 <mode> can be only numerical like 755, 644, 700\n");
  fprintf(stdout, "                 <mode> are automatically changed to 2755, 2644, 2700 respectivly\n");
  fprintf(stdout, "                 <mode> to disable attribute inheritance use 4755, 4644, 4700 ...\n");
  return (0);
}
