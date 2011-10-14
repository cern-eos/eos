// ----------------------------------------------------------------------
// File: com_map.cc
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

/* Map ls, link, unlink */
int 
com_map (char* arg1) {
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString subcommand = subtokenizer.GetToken();
  XrdOucString option="";
  XrdOucString optionstring="";
  XrdOucString in = "mgm.cmd=map";
  XrdOucString arg = "";
  
  if (subcommand.beginswith("-")) {
    option = subcommand;
    option.erase(0,1);
    optionstring += subcommand; optionstring += " ";
    subcommand = subtokenizer.GetToken();
    arg = subtokenizer.GetToken();
    in += "&mgm.option=";
    in += option;
  } else {
    arg = subtokenizer.GetToken();
  }
  
  if ((!subcommand.length()) ||
      ( (subcommand != "ls") && (subcommand != "link") && (subcommand != "unlink")))
    goto com_map_usage;
  
  if ( subcommand == "ls") {
    in += "&mgm.subcmd=ls";
  }
  
  if ( subcommand == "link") {
    XrdOucString key   = arg;
    XrdOucString value = subtokenizer.GetToken();

    if ((!key.length()) || (!value.length()))
      goto com_map_usage;
    
    in += "&mgm.subcmd=link&mgm.map.src="; in += key;
    in += "&mgm.map.dest="; in += value;
  }

  if ( subcommand == "unlink") {
    XrdOucString key   = arg;
    if (!key.length())
      goto com_map_usage;
    in += "&mgm.subcmd=unlink&mgm.map.src="; in += key;
  }
 
  global_retc = output_result(client_user_command(in));
  return (0); 

 com_map_usage:
  printf("'[eos] map ..' provides a namespace mapping interface for directories in EOS.\n");
  printf("Usage: map [OPTIONS] ls|link|unlink ...\n");
  printf("Options:\n");
  
  printf("map ls :\n");
  printf("                                                : list all defined mappings\n");  
  printf("map link <source-path> <destination-path> :\n");
  printf("                                                : create a symbolic link from source-path to destination-path\n");
  printf("map unlink <source-path> :\n");
  printf("                                                : remove symbolic link from source-path\n");
  return (0);
}
