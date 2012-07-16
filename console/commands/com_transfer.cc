// ----------------------------------------------------------------------
// File: com_transfers.cc
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

/* Transfer Interface */
int
com_transfer (char* argin) {
  // split subcommands
  XrdOucTokenizer subtokenizer(argin);
  subtokenizer.GetLine();
  XrdOucString subcmd = subtokenizer.GetToken();
  XrdOucString rate ="0";
  XrdOucString streams="0";
  XrdOucString group="";
  XrdOucString in = "mgm.cmd=transfer&mgm.subcmd=";
  XrdOucString option="";
  XrdOucString foption="";
  XrdOucString xid="";
  XrdOucString arg1="";
  XrdOucString arg2="";

  if ( (subcmd != "submit") && (subcmd != "cancel") && (subcmd != "ls") && (subcmd != "enable") && (subcmd != "disable" ) && (subcmd != "reset" ) && (subcmd != "clear") && (subcmd != "log") && (subcmd != "resubmit") && (subcmd != "kill") && (subcmd != "purge") )
    goto com_usage_transfer;

  in += subcmd;
    
  do {
    option = subtokenizer.GetToken();
    if (!option.length())
      break;
    if (option.beginswith("--rate=")) {
      rate = option;
      rate.replace("--rate=","");
    } else {
      if (option.beginswith("--streams=")) {
	streams = option;
	streams.replace("--streams=","");
      } else {
	if (option.beginswith("--group=")) {
	  group = option;
	  group.replace("--group=","");
	} else {
	  if (option == "-a") {
	    foption +="a";
	  } else {
	    if (option == "-m") {
	      foption = "m";
	    } else {
	      if (option == "-s") {
		foption ="s";
	      } else {
		if (option.beginswith("-")) {
		  goto com_usage_transfer;
		} else {
		  arg1=option;
		  arg2=subtokenizer.GetToken();
		  break;
		}
	      }
	    }
	  }
	}
      }
    }
  } while(1);

  if (subcmd == "submit") {

    if ( (!arg2.beginswith("root://")) &&
	 (!arg2.beginswith("as3://")) &&
	 (!arg2.beginswith("gsiftp://")) &&
	 (!arg2.beginswith("http://")) &&
	 (!arg2.beginswith("https://")) &&
	 (!arg2.beginswith("/eos/")) ) {
      goto com_usage_transfer;
    }

    if ( (!arg1.beginswith("/eos/")) &&
	 (!arg1.beginswith("as3://")) &&
	 (!arg1.beginswith("gsiftp://")) &&
	 (!arg1.beginswith("http://")) &&
	 (!arg1.beginswith("https://")) &&
	 (!arg1.beginswith("root://")) ){
      goto com_usage_transfer;
    }
    
    if (foption.length()) {
      goto com_usage_transfer;
    }

    in += "&mgm.txsrc="; in += XrdMqMessage::Seal(arg1);
    in += "&mgm.txdst="; in += XrdMqMessage::Seal(arg2);
    in += "&mgm.txrate="; in += rate;
    in += "&mgm.txstreams="; in += streams;
    in += "&mgm.txgroup="; in += group;

    global_retc = output_result(client_admin_command(in));
    return (0);
  }

  if (subcmd == "ls") {
    if (arg1.length() || arg2.length()) {
      goto com_usage_transfer;
    }

    in += "&mgm.txoption="; in += foption;
    in += "&mgm.txgroup="; in += group;
    
    global_retc = output_result(client_admin_command(in));
    return (0);
  }

  if ( (subcmd == "enable") || (subcmd == "disable") || (subcmd == "clear") )  {
    global_retc = output_result(client_admin_command(in));
    return (0);
  }
  
  if ( (subcmd == "cancel") || (subcmd == "log") || (subcmd == "resubmit") || (subcmd == "kill") || (subcmd == "purge") ) {
    xid = arg1;
    if ((subcmd != "purge") && ((subcmd != "reset")) && (!xid.length() && (!group.length()))) {
      goto com_usage_transfer;
    }
    if (!xid.length()) {
      in += "&mgm.txgroup="; in += group;
    } else {
      in += "&mgm.txid="; in += xid;
    }

    global_retc = output_result(client_admin_command(in));
    return (0);
  }
  
 com_usage_transfer:
  fprintf(stdout,"Usage: transfer submit|cancel|ls|enable|disable|reset|clear|resubmit|log ..");
  fprintf(stdout,"'[eos] transfer ..' provides the transfer interface of EOS.\n");
  fprintf(stdout,"Options:\n");
  fprintf(stdout,"transfer submit [--rate=<rate>] [--streams=<#>] [--group=<groupname>] <URL1> <URL2> :\n");
  fprintf(stdout,"                                                  transfer a file from URL1 to URL2\n");
  fprintf(stdout,"                                                             <URL> can be root://<host>/<path> or a local path /eos/...\n");
  fprintf(stdout,"       --rate          : limit the transfer rate to <rate>\n");
  fprintf(stdout,"       --streams       : use <#> parallel streams\n\n");
  fprintf(stdout,"       --group         : set the group name for this transfer\n");
  fprintf(stdout,"transfer cancel <id>|--group=<groupname>\n");
  fprintf(stdout,"                                                  cancel transfer with ID <id> or by group <groupname>\n");
  fprintf(stdout,"       <id>=*          : cancel all transfers (only root can do that)\n\n");
  fprintf(stdout,"transfer ls [-a] [-m] [s] [--group=<groupname>] \n");
  fprintf(stdout,"       -a              : list all transfers not only of the current role\n");
  fprintf(stdout,"       -m              : list all transfers in monitoring format (key-val pairs)\n");
  fprintf(stdout,"       -s              : print transfer summary\n");
  fprintf(stdout,"       --group         : list all transfers in this group\n");
  fprintf(stdout,"\n");
  fprintf(stdout,"transfer enable\n");
  fprintf(stdout,"                       : start the transfer engine (you have to be root to do that)\n");
  fprintf(stdout,"transfer disable\n");  
  fprintf(stdout,"                       : stop the transfer engine (you have to be root to do that)\n");
  fprintf(stdout,"transfer reset [<id>|--group=<groupname>]\n");    
  fprintf(stdout,"                       : reset all transfers to 'inserted' state (you have to be root to do that)\n");
  fprintf(stdout,"transfer clear \n");    
  fprintf(stdout,"                       : clear's the transfer database (you have to be root to do that)\n");
  fprintf(stdout,"transfer resubmit <id> [--group=<groupname>]\n");
  fprintf(stdout,"                       : resubmit's a transfer\n");
  fprintf(stdout,"transfer kill <id>|--group=<groupname>\n");
  fprintf(stdout,"                       : kill a running transfer\n");
  fprintf(stdout,"transfer purge [<id>|--group=<groupname>]\n");
  fprintf(stdout,"                       : remove 'failed' transfers from the transfer queue by id, group or all if not specified\n");
  return (0);
}
