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

  printf("usage: node ls [-s] [-m|-l|--io] [<node>]                            : list all nodes or only <node>\n");
  printf("                                                                  -s : silent mode\n");
  printf("                                                                  -m : monitoring key=value output format\n");
  printf("                                                                  -l : long output - list also file systems after each node\n");
  printf("                                                                --io : print IO statistics\n");
  printf("       node config <host:port> <key>=<value>                    : configure file system parameters for each filesystem of this node (see help of 'fs config' for details)\n");
  printf("       node set <queue-name>|<host:port> on|off                 : activate/deactivate node\n");
  printf("       node rm  <queue-name>|<host:port>                        : remove a node\n");
  return (0);
}
