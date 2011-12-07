// ----------------------------------------------------------------------
// File: com_access.cc
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

/* access (deny/bounce/redirect) -  Interface */
int 
com_access (char* arg1) {
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString option="";
  XrdOucString options="";
  bool ok=false;
  XrdOucString in ="";
  in = "mgm.cmd=access";

  XrdOucString subcmd = subtokenizer.GetToken();

  if (subcmd == "ban") {
    ok = true;
    in += "&mgm.subcmd=ban";
  } 

  if (subcmd == "unban") {
    in += "&mgm.subcmd=unban";
    ok = true;
  }

  if (subcmd == "allow") {
    in += "&mgm.subcmd=allow";
    ok = true;
  }

  if (subcmd == "unallow") {
    in += "&mgm.subcmd=unallow";
    ok = true;
  }

  if (subcmd == "ls") {
    in += "&mgm.subcmd=ls";
    ok = true;
  }

  if (subcmd == "set") {
    in += "&mgm.subcmd=set";
    ok = true;
  }

  if (subcmd == "rm") {
    in += "&mgm.subcmd=rm";
    ok = true;
  }

  if (ok) {
    ok = false;
    XrdOucString type = "";
    XrdOucString maybeoption = subtokenizer.GetToken();
    while (maybeoption.beginswith("-")) {
      if ( (subcmd == "ls") && (maybeoption != "-m") && (maybeoption != "-n") )
        goto com_access_usage;
      if ( (subcmd != "ls") ) 
        goto com_access_usage;

      maybeoption.replace("-","");
      option += maybeoption;
      maybeoption = subtokenizer.GetToken();
    } 
    
    if (subcmd == "ls") {
      ok = true;
    }

    if ( (subcmd == "ban") || (subcmd == "unban") || (subcmd == "allow") || (subcmd == "unallow")) {
      type = maybeoption;
      XrdOucString id   = subtokenizer.GetToken();
      if ((!type.length()) || (!id.length())) 
        goto com_access_usage;
      
      if (type == "host") {
        in += "&mgm.access.host="; in += id;
        ok = true;
      }
      if (type == "user") {
        in += "&mgm.access.user="; in += id;
        ok = true;
      }
      if (type == "group") {
        in += "&mgm.access.group="; in += id;
        ok = true;
      }
    }

    if ( (subcmd == "set") || (subcmd == "rm") ) {
      type = maybeoption;
      XrdOucString id   = subtokenizer.GetToken();
      if ((subcmd != "rm") && ((!type.length()) || (!id.length())))
        goto com_access_usage;

      XrdOucString rtype   = subtokenizer.GetToken();
      if (subcmd == "rm") {
	rtype = id;
      }

      if (!id.length()) {
        id = "dummy";
      }
      if (type == "redirect") {
        in += "&mgm.access.redirect="; in += id;
	if (rtype.length()) {
	  if (rtype == "r") {
	    in += "&mgm.access.type=r";
	    ok = true;
	  } else {
	    if (rtype == "w") {
	      in += "&mgm.access.type=w";
	      ok = true;
	    }
	  }
	} else {
	  ok = true;
	}
      }
      if (type == "stall") {
        in += "&mgm.access.stall="; in += id;
	if (rtype.length()) {
	  if (rtype == "r") {
	    in += "&mgm.access.type=r";
	    ok = true;
	  } else {
	    if (rtype == "w") {
	      in += "&mgm.access.type=w";
	      ok = true;
	    } 
	  }
	} else {
	  ok = true;
	}
      }

    }
    if (!ok) 
      goto com_access_usage;
  } else {
    goto com_access_usage;
  }

  in += "&mgm.access.option="; in += option;


  global_retc = output_result(client_admin_command(in));
  return (0);

 com_access_usage:
  printf("'[eos] access ..' provides the access interface of EOS to allow/disallow hosts and/or users\n");
  printf("Usage: access ban|unban|allow|unallow|set|rm|ls ...\n\n");
  printf("Options:\n");
  printf("access ban user|group|host <identifier> : \n");
  
  printf("                                                  ban user,group or host with identifier <identifier>\n");
  printf("                                   <identifier> : can be a user name, user id, group name, group id, hostname or IP \n");
  printf("access unban user|group|host <identifier> :\n");
  printf("                                                  unban user,group or host with identifier <identifier>\n");
  printf("                                   <identifier> : can be a user name, user id, group name, group id, hostname or IP \n");
  printf("access allow user|group|host <identifier> :\n");
  printf("                                                  allows this user,group or host access\n");
  printf("                                   <identifier> : can be a user name, user id, group name, group id, hostname or IP \n");
  printf("access unallow user|group|host <identifier> :\n");
  printf("                                                  unallows this user,group or host access\n");
  printf("                                   <identifier> : can be a user name, user id, group name, group id, hostname or IP \n");
  printf("HINT:  if you add any 'allow' the instance allows only the listed users.\nA banned identifier will still overrule an allowed identifier!\n\n");
  printf("access set redirect <target-host> :\n");
  printf("                                                  allows to set a global redirection to <target-host>\n");
  printf("                                  <target-host> : hostname to which all requests get redirected\n");
  printf("access rm  redirect :\n");
  printf("                                                  removes global redirection\n");
  printf("access set stall <stall-time> [r|w]:\n");
  printf("                                                  allows to set a global stall time\n");
  printf("                                   <stall-time> : time in seconds after which clients should rebounce\n");
  printf("                                          [r|w] : optional set stall time for read/write requests seperatly\n");
  printf("access rm  stall [r|w]:\n");
  printf("                                                  removes global stall time\n");
  printf("                                          [r|w] : removes stall time for read or write requests\n");
  printf("access ls [-m] [-n] :\n");
  printf("                                                  print banned,unbanned user,group, hosts\n");
  printf("                                                                  -m    : output in monitoring format with <key>=<value>\n");
  printf("                                                                  -n    : don't translate uid/gids to names\n");
  printf("Examples:\n");
  printf("  access ban foo           Ban host foo\n");
  printf("  access set redirect foo  Redirect all requests to host foo\n");
  printf("  access rm redirect       Remove redirection to previously defined host foo\n");
  printf("  access set stall 60      Stall all clients by 60 seconds\n");
  printf("  access ls                Print all defined access rules\n");
  return (0);
}
