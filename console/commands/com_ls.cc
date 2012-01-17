// ----------------------------------------------------------------------
// File: com_ls.cc
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

/* List a directory */
int
com_ls (char* arg1) {
  // split subcommands
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString param="";
  XrdOucString option="";
  XrdOucString path="";
  XrdOucString in = "mgm.cmd=ls"; 

  do {
    param = subtokenizer.GetToken();
    if (!param.length())
      break;
    if (param.beginswith("-")) {
      option+= param;
      if ( (option.find("&")) != STR_NPOS) {
        goto com_ls_usage;
      }
    } else {
      path = param;
      break;
    }
  } while(1);

  if (!path.length()) {
    path = pwd;
  } 

  path = abspath(path.c_str());

  in += "&mgm.path=";
  in += path;
  in += "&mgm.option=";
  in += option;
  global_retc = output_result(client_user_command(in));
  return (0);

 com_ls_usage:
  fprintf(stdout,"usage: ls [-lan] <path>                                                  :  list directory <path>\n");
  fprintf(stdout,"                    -l : show long listing\n");
  fprintf(stdout,"                    -a : show hidden files\n");
  fprintf(stdout,"                    -n : show numerical user/group ids\n");
  fprintf(stdout,"                    -s : checks only if the directory exists without listing\n");
  return (0);
}
