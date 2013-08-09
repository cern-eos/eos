// ----------------------------------------------------------------------
// File: com_mkdir.cc
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

/* Create a directory */
int
com_mkdir (char* arg1)
{
  // split subcommands
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString path = subtokenizer.GetToken();
  XrdOucString in = "mgm.cmd=mkdir";

  if (wants_help(arg1))
    goto com_mkdir_usage;

  if (path == "-p")
  {
    path = subtokenizer.GetToken();
    in += "&mgm.option=p";
  }
  else
  {
    if (path.beginswith("-"))
    {
      goto com_mkdir_usage;
    }
  }

  do
  {
    // read space seperated names as a single directory name
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
    goto com_mkdir_usage;

  }
  else
  {
    path = abspath(path.c_str());
    in += "&mgm.path=";
    in += path;

    global_retc = output_result(client_user_command(in));
    return (0);
  }

com_mkdir_usage:
  fprintf(stdout, "usage: mkdir -p <path>                                                :  create directory <path>\n");
  return (0);

}
