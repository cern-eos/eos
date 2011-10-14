// ----------------------------------------------------------------------
// File: com_group.cc
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

using namespace eos::common;

/* Group listing, configuration, manipulation */
int
com_group (char* arg1) {
  XrdOucString in = "";
  bool silent=false;
  bool printusage=false;
  bool highlighting=true;
  XrdOucString option="";
  XrdOucEnv* result=0;
  bool ok=false;
  bool sel=false;
  // split subcommands
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString subcommand = subtokenizer.GetToken();
  if ( subcommand == "ls" ) {
    in ="mgm.cmd=group&mgm.subcmd=ls";
    option="";

    do {
      ok=false;
      subtokenizer.GetLine();
      option = subtokenizer.GetToken();
      if (option.length()) {
        if (option == "-m") {
          in += "&mgm.outformat=m";
          ok=true;
          highlighting=false;
        } 
        if (option == "-l") {
          in += "&mgm.outformat=l";
          ok=true;
        }
        if (option == "--io") {
          in += "&mgm.outformat=io";
          ok=true;
        }       
        if (option == "--IO") {
          in += "&mgm.outformat=IO";
          ok=true;
        }
        if (option == "-s") {
          silent=true;
          ok=true;
        }
        if (!option.beginswith("-")) {
          in += "&mgm.selection=";
          in += option;
          if (!sel) 
            ok=true;
          sel=true;
        }

        if (!ok) 
          printusage=true;
      } else {
        ok=true;
      }
    } while(option.length());
  }

  if ( subcommand == "set" ) {
    in ="mgm.cmd=group&mgm.subcmd=set";
    XrdOucString nodename = subtokenizer.GetToken();
    XrdOucString active= subtokenizer.GetToken();

    if ( (active != "on") && (active != "off") ) {
      printusage=true;
    }

    if (!nodename.length())
      printusage=true;
    in += "&mgm.group=";
    in += nodename;
    in += "&mgm.group.state=";
    in += active;
    ok = true;
  }

  if ( subcommand == "rm" ) {
    in ="mgm.cmd=group&mgm.subcmd=rm";
    XrdOucString groupname = subtokenizer.GetToken();

    if (!groupname.length())
      printusage=true;
    in += "&mgm.group=";
    in += groupname;
    ok = true;
  }
    

  if (printusage ||  (!ok))
    goto com_group_usage;

  result = client_admin_command(in);
  
  if (!silent) {
    global_retc = output_result(result, highlighting);
  } else {
    if (result) {
      global_retc = 0;
    } else {
      global_retc = EINVAL;
    }
  }
  
  return (0);

 com_group_usage:

  printf("usage: group ls                                                      : list groups\n");
  printf("usage: group ls [-s] [-m|-l|--io] [<group>]                          : list groups or only <group>\n");
  printf("                                                                  -s : silent mode\n");
  printf("                                                                  -m : monitoring key=value output format\n");
  printf("                                                                  -l : long output - list also file systems after each group\n");
  printf("                                                                --io : print IO statistics for the group\n");
  printf("                                                                --IO : print IO statistics for each filesystem\n");
  printf("       group rm <group-name>                                         : remove group\n");
  printf("       group set <group-name> on|off                                 : activate/deactivate group\n");
  return (0);
}
