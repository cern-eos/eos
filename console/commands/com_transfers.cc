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
com_transfers (char* arg1) {
  // split subcommands
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString subcmd = subtokenizer.GetToken();
  XrdOucString nodes = subtokenizer.GetToken();
  XrdOucString selection = subtokenizer.GetToken();

  XrdOucString in = "mgm.cmd=";

  if ( (subcmd != "drop") && (subcmd != "ls") ) 
    goto com_usage_transfers;

  if (subcmd == "drop")
    in += "droptransfers";
  if (subcmd == "ls") 
    in += "listtransfers";
  
  in += "&mgm.subcmd=";

  if (nodes.length()) {
    in += nodes;
    if (selection.length()) {
      in += "&mgm.nodename=";
      in += selection;
    }
    
    global_retc = output_result(client_admin_command(in));
    return (0);
  }
  
 com_usage_transfers:
  printf("       transfers drop fst *                 : drop transfers on all fst nodes !\n");
  printf("       transfers ls fst *                   : list transfers on all fst nodes !\n");
  return (0);
}
