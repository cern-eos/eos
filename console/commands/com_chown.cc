// ----------------------------------------------------------------------
// File: com_chown.cc
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
#include "console/ConsoleCliCommand.hh"
/*----------------------------------------------------------------------------*/

/* Owner Interface */
int
com_chown (char* arg1)
{
  XrdOucString in = "mgm.cmd=chown";

  CliOption helpOption("help", "print help", "-h,--help");
  helpOption.setHidden(true);

  ConsoleCliCommand chownCmd("chown", "provides the change owner "
                             "interface of EOS");
  chownCmd.addOption(helpOption);
  chownCmd.addOption({"recursive", "change mode recursively", "-r"});
  chownCmd.addOptions({{"owner-group", "<owner> has to be a user id or user "
                        "name;\n<group> is optional and has to be a group id "
                        "or group name", 1, 1, "<owner>[:<group>]", true},
                       {"path", "the file/directory to modify", 2, 1,
                        "<path>", true}
                      });

  chownCmd.parse(arg1);

  if (chownCmd.hasValue("help"))
  {
    chownCmd.printUsage();
    return 0;
  }
  else if (chownCmd.hasErrors())
  {
    chownCmd.printErrors();
    chownCmd.printUsage();
    return 0;
  }

  if (chownCmd.hasValue("recursive"))
    in += "&mgm.chown.option=r";

  in += "&mgm.path=" + cleanPath(chownCmd.getValue("path"));
  in += "&mgm.chown.owner=";
  in += chownCmd.getValue("owner-group").c_str();

  global_retc = output_result(client_user_command(in));
  return (0);
}
