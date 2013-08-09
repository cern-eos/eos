// ----------------------------------------------------------------------
// File: com_rmdir.cc
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

/* Remove a directory */
int
com_rmdir (char* arg1)
{
  // split subcommands
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString path = subtokenizer.GetToken();
  XrdOucString in = "mgm.cmd=rmdir&";

  if (wants_help(arg1))
    goto com_rmdir_usage;

  if ((path == "--help") || (path == "-h"))
  {
    goto com_rmdir_usage;
  }

  do
  {
    XrdOucString param;
    param = subtokenizer.GetToken();
    if (param.length())
    {
      path += " ";
      path += param;
    }
    else
    {
      break;
    }
  }
  while (1);

  // remove escaped blanks
  while (path.replace("\\ ", " "))
  {
  }

  if (!path.length())
  {
    goto com_rmdir_usage;

  }
  else
  {
    path = abspath(path.c_str());
    in += "mgm.path=";
    in += path;

    global_retc = output_result(client_user_command(in));
    return (0);
  }

com_rmdir_usage:
  fprintf(stdout, "usage: rmdir <path>                                                   :  remote directory <path>\n");
  return (0);

}
