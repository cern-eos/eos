// ----------------------------------------------------------------------
// File: com_chown.cc
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

/* Owner Interface */
int 
com_chown (char* arg1) {
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString owner = subtokenizer.GetToken();
  XrdOucString option="";
  XrdOucString in = "mgm.cmd=chown";
  XrdOucString arg = "";

  if (owner.beginswith("-")) {
    option = owner;
    option.erase(0,1);
    owner = subtokenizer.GetToken();
    in += "&mgm.chown.option=";
    in += option;
  }

  XrdOucString path = subtokenizer.GetToken();

  if ( !path.length() || !owner.length() ) 
    goto com_chown_usage;

  path = abspath(path.c_str());

  in += "&mgm.path="; in += path;
  in += "&mgm.chown.owner="; in += owner;

  global_retc = output_result(client_admin_command(in));
  return (0);

 com_chown_usage:
  fprintf(stdout,"Usage: chown [-r] <owner>[:<group>] <path>\n");
  fprintf(stdout,"'[eos] chown ..' provides the change owner interface of EOS.\n");
  fprintf(stdout,"<path> is the file/directory to modify, <owner> has to be a user id or user name. <group> is optional and has to be a group id or group name.\n");
  fprintf(stdout,"Remark: EOS does access control on directory level - the '-r' option only applies to directories! It is not possible to set uid!=0 and gid=0!\n\n");
  fprintf(stdout,"Options:\n");
  fprintf(stdout,"                  -r : recursive\n");
  return (0);
}
