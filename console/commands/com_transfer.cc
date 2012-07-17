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

/* Control-C handler for interactive transfers */

bool txcancel=false;

void txcancel_handler(int) {
  txcancel=true;
  signal (SIGINT,  exit_handler);
}

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
  bool sync=false;

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
      if (option == "--sync") {
	sync = true;
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
		if (option == "-p") {
		  foption = "mp";
		} else {
		  if (option == "-s") {
		    foption ="s";
		  } else {
		    if (option == "-n") {
		      foption ="n";
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
    
    bool noprogress=false;
    
    if ( (foption.find("s")) != STR_NPOS) {
      while(foption.replace("s","")) {}
      silent = true;
    }

    if ( (foption.find("n")) != STR_NPOS) {
      while(foption.replace("n","")) {}
      noprogress = true;
    }

    if (foption.length()) {
      goto com_usage_transfer;
    }

    in += "&mgm.txsrc="; in += XrdMqMessage::Seal(arg1);
    in += "&mgm.txdst="; in += XrdMqMessage::Seal(arg2);
    in += "&mgm.txrate="; in += rate;
    in += "&mgm.txstreams="; in += streams;
    in += "&mgm.txgroup="; in += group;
    
    if (!sync) {
      global_retc = output_result(client_admin_command(in));
    } else {
      signal(SIGINT, txcancel_handler);

      time_t starttime=time(NULL);
      in += "&mgm.txoption=s";

      XrdOucEnv* result = client_admin_command(in);
      std::vector<std::string> lines;
      command_result_stdout_to_vector(lines);
      global_retc = output_result(result);
      if (!global_retc) {
	// scan for success: submitted transfer id=<#>
	if (lines.size()==2) {
	  std::string id;
	  if ( (lines[1].find(" id="))!=std::string::npos) {
	    id = lines[1];
	    id.erase(0, lines[1].find(" id=")+4);
	  }
	  // now poll the state

	  errno=0;
	  strtol(id.c_str(),0,10);
	  if (errno) {
	    fprintf(stderr,"error: submission of transfer probably failed - check with 'transfer ls'\n");
	    global_retc=EFAULT;
	    return (0);
	  }
	  // prepare the get progress command
	  in = "mgm.cmd=transfer&mgm.subcmd=ls&mgm.txoption=mp&mgm.txid=";in += id.c_str();
	  XrdOucString incp=in;
	  while (1) {
	    lines.clear();
	    XrdOucEnv* result = client_admin_command(in);
	    in = incp;
	    command_result_stdout_to_vector(lines);
	    if (result) delete result;
	    if (lines.size()==2) {
	      // this transfer is in the queue
	      XrdOucString info = lines[1].c_str();
	      while(info.replace(" ","&")) {}
	      XrdOucEnv txinfo(info.c_str());

	      XrdOucString status=txinfo.Get("tx.status");

	      if (!noprogress) {
		if ( (status!= "done") && (status!= "failed") ) {
		  fprintf(stdout,"[eoscp TX] [ %-10s ]\t|", txinfo.Get("tx.status"));
		  int progress = atoi(txinfo.Get("tx.progress"));
		  for (int l=0; l< 20; l++) {
		    if (l < ( (int) (0.2 * progress))) {
		      fprintf(stdout,"=");
		    }
		    if (l ==( (int) (0.2 * progress))) {
		      fprintf(stdout,">");
		    }
		    if (l > ( (int) (0.2 * progress))) {
		      fprintf(stdout,".");
		    }
		  }
		  fprintf(stdout,"| %5s%% : %us\r",txinfo.Get("tx.progress"), (unsigned int)(time(NULL)-starttime));
		  fflush(stdout);
		}
	      }

	      if ( (status=="done") || (status=="failed") ) {

		if (!noprogress) {
		  fprintf(stdout,"[eoscp TX] [ %-10s ]\t|", txinfo.Get("tx.status"));
		  int progress = 0;
		  if (status=="done") {
		    progress = 100;
		  }
		  for (int l=0; l< 20; l++) {
		    if (l < ( (int) (0.2 * progress))) {
		      fprintf(stdout,"=");
		    }
		    if (l ==( (int) (0.2 * progress))) {
		      fprintf(stdout,">");
		  }
		    if (l > ( (int) (0.2 * progress))) {
		      fprintf(stdout,".");
		    }
		  }
		  if (status=="done") {
		    fprintf(stdout,"|  100.0%% : %us\n", (unsigned int)(time(NULL)-starttime));
		  } else {
		    fprintf(stdout,"|    0.0%% : %us\n", (unsigned int)(time(NULL)-starttime));
		  }
		  fflush(stdout);
		}

		if (!silent) {
		  // get the log
		  in = "mgm.cmd=transfer&mgm.subcmd=log&mgm.txid="; in += id.c_str();
		  output_result(client_admin_command(in));
		}
		if (status=="done") {
		  global_retc= 0;
		} else {
		  global_retc= EFAULT;
		}
		return (0);
	      }
	      for (size_t i=0; i< 10; i++) {
		usleep(100000);
		if (txcancel) {
		  fprintf(stdout,"\n<Control-C>\n");
		  in = "mgm.cmd=transfer&mgm.subcmd=cancel&mgm.txid="; in += id.c_str();
		  output_result(client_admin_command(in));
		  global_retc=ECONNABORTED;
		  return (0);
		}
	      }
	    } else {
	      fprintf(stderr,"error: transfer has been canceled externnaly!\n");
	      global_retc= EFAULT;
	      return (0);
	    }
	  }
	}
      }
    }
    return (0);
  }

  if (subcmd == "ls") {
    if (arg2.length()) {
      goto com_usage_transfer;
    }

    in += "&mgm.txoption="; in += foption;
    in += "&mgm.txgroup="; in += group;
    in += "&mgm.txid="; in += arg1;
    
    global_retc = output_result(client_admin_command(in));
    return (0);
  }

  if ( (subcmd == "enable") || (subcmd == "disable") || (subcmd == "clear") )  {
    global_retc = output_result(client_admin_command(in));
    return (0);
  }
  
  if ( (subcmd == "cancel") || (subcmd == "log") || (subcmd == "resubmit") || (subcmd == "kill") || (subcmd == "purge") || (subcmd == "reset")) {
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
  fprintf(stdout,"transfer submit [--rate=<rate>] [--streams=<#>] [--group=<groupname>] [--sync] <URL1> <URL2> :\n");
  fprintf(stdout,"                                                  transfer a file from URL1 to URL2\n");
  fprintf(stdout,"                                                             <URL> can be root://<host>/<path> or a local path /eos/...\n");
  fprintf(stdout,"       --rate          : limit the transfer rate to <rate>\n");
  fprintf(stdout,"       --streams       : use <#> parallel streams\n\n");
  fprintf(stdout,"       --group         : set the group name for this transfer\n");
  fprintf(stdout,"transfer cancel <id>|--group=<groupname>\n");
  fprintf(stdout,"                                                  cancel transfer with ID <id> or by group <groupname>\n");
  fprintf(stdout,"       <id>=*          : cancel all transfers (only root can do that)\n\n");
  fprintf(stdout,"transfer ls [-a] [-m] [s] [--group=<groupname>] [id] \n");
  fprintf(stdout,"       -a              : list all transfers not only of the current role\n");
  fprintf(stdout,"       -m              : list all transfers in monitoring format (key-val pairs)\n");
  fprintf(stdout,"       -s              : print transfer summary\n");
  fprintf(stdout,"       --group         : list all transfers in this group\n");
  fprintf(stdout,"       --sync          : follow the transfer in interactive mode (like interactive third party 'cp')\n");
  fprintf(stdout,"                  <id> : id of the transfer to list\n");
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
