// ----------------------------------------------------------------------
// File: com_vst.cc
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

/* VST Interface */
int
com_vst (char* arg1)
{
  // split subcommands
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString subcommand = subtokenizer.GetToken();

  if (wants_help(arg1))
    goto com_vst_usage;

  if (subcommand == "ls")
  {
    XrdOucString in = "mgm.cmd=vst&mgm.subcmd=ls";
    XrdOucString soption = "";
    XrdOucString option = "";
    do
    {
      option = subtokenizer.GetToken();
      if (option.beginswith("-"))
      {
	if (option.beginswith("--"))
	  option.erase(0, 2);
	else
	  option.erase(0, 1);
        soption += option;
        if (option.beginswith("h") || option.beginswith("-h"))
          goto com_vst_usage;
      }
    }
    while (option.length());

    if (soption.length())
    {
      in += "&mgm.option=";
      in += soption;
    }
    global_retc = output_result(client_admin_command(in));
    return (0);
  }
  
  if (subcommand == "--udp")
  {
    XrdOucString in = "mgm.cmd=vst&mgm.subcmd=udp";    
    XrdOucString target = subtokenizer.GetToken();
    XrdOucString myself = subtokenizer.GetToken();
    if (target.length())
    {
      in += "&mgm.vst.target=";
      in += target;
    }
    if (myself.length() && (myself != "--self"))
      goto com_vst_usage;

    if (myself.length() )
    {
      in += "&mgm.vst.self=true";
    }
    global_retc = output_result(client_admin_command(in));
    return (0);
  }

com_vst_usage:
  fprintf(stdout, "usage: vst ls [-m] [--io]                                       : list VSTs\n");
  fprintf(stdout, "                                        -m : monitoring format\n");
  fprintf(stdout, "                                      --io : IO format\n");
  fprintf(stdout, "       vst --udp [<host:port>] [--self]                         : list[set] VST influxdb target\n");
  fprintf(stdout, "\n");
  return (0);
}
