// ----------------------------------------------------------------------
// File: com_map.cc
// Author: Andreas-Joachim Peters - CERN
// Author: Joaquim Rocha - CERN
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
com_map (char* arg1)
{
  XrdOucString in = "mgm.cmd=map";
  ConsoleCliCommand *parsedCmd, *mapCmd, *lsSubCmd, *linkSubCmd, *unlinkSubCmd;

  mapCmd = new ConsoleCliCommand("map", "provides a namespace mapping "
                                 "interface for directories in EOS");

  lsSubCmd = new ConsoleCliCommand("ls", "list all defined mappings");
  mapCmd->addSubcommand(lsSubCmd);

  linkSubCmd = new ConsoleCliCommand("link", "create a symbolic link from "
                                     "<source-path> to <destination-path>");
  linkSubCmd->addOptions({{"src-path", "", 1, 1, "<source-path>", true},
                          {"dst-path", "", 2, 1, "<destination-path>", true},
                         });
  mapCmd->addSubcommand(linkSubCmd);

  unlinkSubCmd = new ConsoleCliCommand("unlink", "remove symbolic link from "
                                       "source-path");
  unlinkSubCmd->addOption({"src-path", "", 1, 1, "<source-path>", true});
  mapCmd->addSubcommand(unlinkSubCmd);

  addHelpOptionRecursively(mapCmd);

  parsedCmd = mapCmd->parse(arg1);

  if (checkHelpAndErrors(parsedCmd))
    goto bailout;

  if (parsedCmd == lsSubCmd)
  {
    in += "&mgm.subcmd=ls";
  }
  else if (parsedCmd == linkSubCmd)
  {
    in += "&mgm.subcmd=link&mgm.map.src=";
    in += linkSubCmd->getValue("src-path").c_str();
    in += "&mgm.map.dest=";
    in += linkSubCmd->getValue("dst-path").c_str();
  }
  else if (parsedCmd == unlinkSubCmd)
  {
    in += "&mgm.subcmd=unlink&mgm.map.src=";
    in += linkSubCmd->getValue("src-path").c_str();;
  }

  global_retc = output_result(client_user_command(in));

 bailout:
  delete mapCmd;

  return (0);
}
