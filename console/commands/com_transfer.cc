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

  if ( (subcmd != "submit") && (subcmd != "cancel") && (subcmd != "ls") ) 
    goto com_usage_transfer;

  if (subcmd == "submit") 
    in += "submit";
  if (subcmd == "cancel")
    in += "cancel";
  if (subcmd == "ls") 
    in += "ls";
    
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
  } while(1);

  if (subcmd == "submit") {
    if ((arg1.find("&")!= STR_NPOS)) {
      fprintf(stderr,"error: & is not allowed in a path!\n");
      goto com_usage_transfer;
    }

    if ( (!arg2.beginswith("root://")) &&
	 (!arg2.beginswith("/eos/")) ) {
      goto com_usage_transfer;
    }

    if ( (!arg1.beginswith("/eos/")) &&
	 (!arg1.beginswith("root://")) ){
      goto com_usage_transfer;
    }
    
    if (foption.length()) {
      goto com_usage_transfer;
    }

    in += "&mgm.src="; in += arg1;
    in += "&mgm.dst="; in += arg2;
    in += "&mgm.rate="; in += rate;
    in += "&mgm.streams="; in += streams;
    in += "&mgm.group="; in += group;

    global_retc = output_result(client_admin_command(in));
    return (0);
  }

  if (subcmd == "ls") {
    if (arg1.length() || arg2.length()) {
      goto com_usage_transfer;
    }

    in += "&mgm.option="; in += foption;
    in += "&mgm.group="; in += group;
    
    global_retc = output_result(client_admin_command(in));
    return (0);
  }
  
  if (subcmd == "cancel") {
    xid = arg1;
    if (arg2.length() || rate.length())
      goto com_usage_transfer;

    in += "&mgm.txid="; in += xid;

    global_retc = output_result(client_admin_command(in));
    return (0);
  }
  
 com_usage_transfer:
  printf("Usage: transfer submit|cancel|ls ..");
  printf("'[eos] transfer ..' provides the transfer interface of EOS.\n");
  printf("Options:\n");
  printf("transfer submit [--rate=<rate>] [--streams=<#>] [--group=<groupname>] <URL1> <URL2> :\n");
  printf("                                                  transfer a file from URL1 to URL2\n");
  printf("                                                             <URL> can be root://<host>/<path> or a local path /eos/...\n");
  printf("       --rate          : limit the transfer rate to <rate>\n");
  printf("       --streams       : use <#> parallel streams\n\n");
  printf("       --group         : set the group name for this transfer\n");
  printf("transfer get [--rate=<rate>] [--streams=<#>] [--group=<groupname>] root://<host>//<path> /eos/.. :\n");
  printf("                                                  transfer the file from RUL root://<host>//<path> to /eos/..\n");
  printf("       --rate          : limit the transfer rate to <rate>\n");
  printf("       --streams       : use <#> parallel streams\n\n");
  printf("       --group         : set the group name for this transfer\n");
  printf("transfer cancel <id>\n");
  printf("                                                  cancel transfer with ID <id>\n");
  printf("       <id>=*          : cancel all transfers (only root can do that)\n\n");
  printf("transfer ls [-a] [-m] [--group=<groupname>] \n");
  printf("       -a              : list all transfers not only of the current role\n");
  printf("       --group         : list all transfers in this group\n");
  printf("       -m              : list all transfers in monitoring format (key-val pairs)\n");
  
  return (0);
}
