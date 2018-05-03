// ----------------------------------------------------------------------
// File: com_route.cc
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
#include "common/StringTokenizer.hh"
/*----------------------------------------------------------------------------*/

/* Route ls, link, unlink */
int
com_route(char* arg1)
{
  eos::common::StringTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString subcommand = subtokenizer.GetToken();
  XrdOucString option = "";
  XrdOucString optionstring = "";
  XrdOucString in = "mgm.cmd=route";
  XrdOucString arg = "";

  if (wants_help(arg1)) {
    goto com_route_usage;
  }

  if (subcommand.beginswith("-")) {
    option = subcommand;
    option.erase(0, 1);
    optionstring += subcommand;
    optionstring += " ";
    subcommand = subtokenizer.GetToken();
    arg = subtokenizer.GetToken();
    in += "&mgm.option=";
    in += option;
  } else {
    arg = subtokenizer.GetToken();
  }

  if ((!subcommand.length()) ||
      ((subcommand != "ls") && (subcommand != "link") && (subcommand != "unlink"))) {
    goto com_route_usage;
  }

  if (subcommand == "ls") {
    in += "&mgm.subcmd=ls";
  }

  if (subcommand == "link") {
    XrdOucString key = arg;
    XrdOucString value = subtokenizer.GetToken();

    if ((!key.length()) || (!value.length())) {
      goto com_route_usage;
    }

    in += "&mgm.subcmd=link&mgm.route.src=";
    in += key;
    in += "&mgm.route.dest=";
    in += value;
  }

  if (subcommand == "unlink") {
    XrdOucString key = arg;

    if (!key.length()) {
      goto com_route_usage;
    }

    in += "&mgm.subcmd=unlink&mgm.route.src=";
    in += key;
  }

  global_retc = output_result(client_command(in));
  return (0);
com_route_usage:
  fprintf(stdout,
          "'[eos] route ..' provides a namespace routing interface for directories to redirect to external instances.\n");
  fprintf(stdout, "Usage: route [OPTIONS] ls|link|unlink ...\n");
  fprintf(stdout, "Options:\n");
  fprintf(stdout, "route ls :\n");
  fprintf(stdout, "                                                : list all defined routings\n");
  fprintf(stdout, "route link <source-path> <destination-host>[:xrdport[:httpport]] :\n");
  fprintf(stdout, "                                                : create a routing from source-path to destination url\n");
  fprintf(stdout, "                                                  - you can define the XRootd and HTTP ports by adding the optional [:port] arguments e.g default ports would be 'foo.bar:1094:8000'\n");
  fprintf(stdout, "route unlink <source-path> :\n");
  fprintf(stdout, "                                                : remove routing from source-path\n");
  global_retc = 0;
  return (0);
}
