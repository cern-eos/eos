// ----------------------------------------------------------------------
// File: com_chmod.cc
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

/* Mode Interface */
int
com_chmod (char* arg1)
{
  XrdOucString in = "mgm.cmd=chmod";

  CliOption helpOption("help", "print help", "-h,--help");
  helpOption.setHidden(true);

  ConsoleCliCommand chmodCmd("chmod", "set mode for <path>");
  chmodCmd.addOption(helpOption);
  chmodCmd.addOption({"recursive", "change mode recursively", "-r"});

  CliPositionalOption modeOption("mode",
                                 "can be only numerical like 755, 644, 700 "
                                 "are automatically changed to 2755, 2644, "
                                 "2700, respectively;\n"
                                 "to disable attribute inheritance use 4755, "
                                 "4644, 4700, ...", 1, 1, "<mode>", true);
  modeOption.addEvalFunction(optionIsPositiveNumberEvalFunc, 0);
  chmodCmd.addOption(modeOption);
  chmodCmd.addOption({"path", "", 2, 1, "<path>", true});

  chmodCmd.parse(arg1);

  if (chmodCmd.hasValue("help"))
  {
    chmodCmd.printUsage();
    return 0;
  }
  else if (chmodCmd.hasErrors())
  {
    chmodCmd.printErrors();
    chmodCmd.printUsage();
    return 0;
  }

  if (chmodCmd.hasValue("recursive"))
    in += "&mgm.option=r";

  in += "&mgm.path=" + cleanPath(chmodCmd.getValue("path"));
  in += "&mgm.chmod.mode=";
  in += chmodCmd.getValue("mode").c_str();

  global_retc = output_result(client_user_command(in));
  return (0);
}
