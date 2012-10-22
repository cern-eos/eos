// ----------------------------------------------------------------------
// File: com_node.cc
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

/* Node listing, configuration, manipulation */
int
com_node (char* arg1) {
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
    in ="mgm.cmd=node&mgm.subcmd=ls";
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
	if (option == "--sys") {
	  in += "&mgm.outformat=sys";
	  ok=true;
	}
        if (option == "--fsck") {
          in += "&mgm.outformat=fsck";
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
    in ="mgm.cmd=node&mgm.subcmd=set";
    XrdOucString nodename = subtokenizer.GetToken();
    XrdOucString active= subtokenizer.GetToken();

    if ( (active != "on") && (active != "off") ) {
      printusage=true;
    }

    if (!nodename.length())
      printusage=true;
    in += "&mgm.node=";
    in += nodename;
    in += "&mgm.node.state=";
    in += active;
    ok = true;
  }

  if ( subcommand == "status" ) {
    in ="mgm.cmd=node&mgm.subcmd=status";
    XrdOucString nodename = subtokenizer.GetToken();

    if (!nodename.length())
      printusage=true;
    in += "&mgm.node=";
    in += nodename;
    ok = true;
  }

  if ( subcommand == "gw" ) {
    in ="mgm.cmd=node&mgm.subcmd=set";
    XrdOucString nodename = subtokenizer.GetToken();
    XrdOucString active= subtokenizer.GetToken();

    if ( (active != "on") && (active != "off") ) {
      printusage=true;
    }

    if (!nodename.length())
      printusage=true;
    in += "&mgm.node=";
    in += nodename;
    in += "&mgm.node.txgw=";
    in += active;
    ok = true;
  }
  
  if ( subcommand == "rm" ) {
    in ="mgm.cmd=node&mgm.subcmd=rm";
    XrdOucString nodename = subtokenizer.GetToken();

    if (!nodename.length())
      printusage=true;
    in += "&mgm.node=";
    in += nodename;
    ok = true;
  }
    
  if ( subcommand == "config" ) {
    XrdOucString nodename = subtokenizer.GetToken();
    XrdOucString keyval   = subtokenizer.GetToken();
    
    if ( (!nodename.length()) || (!keyval.length()) ) {
      goto com_node_usage;
    }
    
    if ( (keyval.find("=")) == STR_NPOS) {
      // not like <key>=<val>
      goto com_node_usage;
    }
    
    std::string is = keyval.c_str();
    std::vector<std::string> token;
    std::string delimiter = "=";
    eos::common::StringConversion::Tokenize(is, token, delimiter);
    
    if (token.size() != 2) 
      goto com_node_usage;
    
    XrdOucString in = "mgm.cmd=node&mgm.subcmd=config&mgm.node.name=";
    in += nodename;
    in += "&mgm.node.key="; in += token[0].c_str();
    in += "&mgm.node.value="; in += token[1].c_str();
    
    global_retc = output_result(client_admin_command(in));
    return (0);
  }

  if ( subcommand == "register" ) {
    XrdOucString nodename = subtokenizer.GetToken();
    XrdOucString path2register = subtokenizer.GetToken();
    XrdOucString space2register = subtokenizer.GetToken();
    XrdOucString flag1 = subtokenizer.GetToken();
    XrdOucString flag2 = subtokenizer.GetToken();
    bool forceflag=false;
    bool rootflag=false;

    if (flag1.length()) {
      if (flag1 == "--force") {
	forceflag = true;
      } else {
	if (flag1 == "--root") {
	  rootflag = true;
	} else {
	  goto com_node_usage;
	}
      }
      if (flag2.length()) {
	if (flag2 == "--force") {
	  forceflag = true;
	} else {
	  if (flag2 == "--root") {
	    rootflag = true;
	  } else {
	    goto com_node_usage;
	  }
	}
      }
    }
      
    if ( (!nodename.length()) || (!path2register.length()) || (!space2register.length()) ) {
      goto com_node_usage;
    }

    XrdOucString in = "mgm.cmd=node&mgm.subcmd=register&mgm.node.name=";in+=nodename;
    in += "&mgm.node.path2register=";  in += path2register;
    in += "&mgm.node.space2register="; in += space2register;

    if (forceflag) {
      in += "&mgm.node.force=true";
    } 

    if (rootflag) {
      in += "&mgm.node.root=true";
    }
    global_retc = output_result(client_admin_command(in));
    return (0);
  }
  
  if (printusage ||  (!ok))
    goto com_node_usage;

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

 com_node_usage:

  fprintf(stdout,"usage: node ls [-s] [-m|-l|--sys|--io|--fsck] [<node>]                     : list all nodes or only <node>\n");
  fprintf(stdout,"                                                                  -s : silent mode\n");
  fprintf(stdout,"                                                                  -m : monitoring key=value output format\n");
  fprintf(stdout,"                                                                  -l : long output - list also file systems after each node\n");
  fprintf(stdout,"                                                                --io : print IO statistics\n");
  fprintf(stdout,"                                                              --sys  : print SYS statistics (memory + threads)\n");
  fprintf(stdout,"                                                              --fsck : print filesystem check statistcis\n");
  fprintf(stdout,"       node config <host:port> <key>=<value>                    : configure file system parameters for each filesystem of this node\n");
  fprintf(stdout,"                                                               <key> : gw.rate=<mb/s> - set the transfer speed per gateway transfer\n");
  fprintf(stdout,"                                                               <key> : gw.ntx=<#>     - set the number of concurrent transfers for a gateway node\n");
  fprintf(stdout,"                                                               <key> : error.simulation=io_read|io_write|xs_read|xs_write\n");
  fprintf(stdout,"                                                                       io_read  : simulate read  errors\n");
  fprintf(stdout,"                                                                       io_write : simulate write errors\n");
  fprintf(stdout,"                                                                       xs_read  : simulate checksum errors when reading a file\n");
  fprintf(stdout,"                                                                       xs_write : simulate checksum errors when writing a file\n");
  fprintf(stdout,"                                                                       <none>   : disable error simulation (every value than the previous ones are fine!)\n");
  fprintf(stdout,"                                                               <key> : publish.interval=<sec> - set the filesystem state publication interval to <sec> seconds\n");
  fprintf(stdout,"                                                               <key> : debug.level=<level> - set the node into debug level <level> [default=notice] -> see debug --help for available levels\n");
  fprintf(stdout,"                                                               <key> : for other keys see help of 'fs config' for details\n");
  fprintf(stdout,"\n");
  fprintf(stdout,"       node set <queue-name>|<host:port> on|off                 : activate/deactivate node\n");
  fprintf(stdout,"       node rm  <queue-name>|<host:port>                        : remove a node\n");
  fprintf(stdout,"       node register <host:port|*> <path2register> <space2register> [--force] [--root]\n");
  fprintf(stdout,"       node gw <queue-name>|<host:port> <on|off>                : enable (on) or disable (off) node as a transfer gateway\n");
  fprintf(stdout,"                                                                : register filesystems on node <host:port>\n");
  fprintf(stdout,"                                                                  <path2register> is used as match for the filesystems to register e.g. /data matches filesystems /data01 /data02 etc. ... /data/ registers all subdirectories in /data/\n");
  fprintf(stdout,"                                                                  <space2register> is formed as <space>:<n> where <space> is the space name and <n> must be equal to the number of filesystems which are matched by <path2register> e.g. data:4 or spare:22 ...\n");
  fprintf(stdout,"                                                                --force : removes any existing filesystem label and re-registers\n");
  fprintf(stdout,"                                                                --root  : allows to register paths on the root partition\n");
  fprintf(stdout,"       node status <queue-name>|<host:port>                     : print's all defined variables for a node\n");
  return (0);
}
