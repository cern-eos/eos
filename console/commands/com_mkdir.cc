// ----------------------------------------------------------------------
// File: com_mkdir.cc
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

/* Create a directory */
int
com_mkdir (char* arg1)
{
  XrdOucString path;
  XrdOucString in = "mgm.cmd=mkdir";

  ConsoleCliCommand mkdirCmd("mkdir", "set mode for <path>");
  mkdirCmd.addOption({"parents", "make parent directories as needed", "-p"});
  mkdirCmd.addOption({"path", "", 1, 1, "<path>", true});

  addHelpOptionRecursively(&mkdirCmd);

  mkdirCmd.parse(arg1);

  if (checkHelpAndErrors(&mkdirCmd))
    return 0;

  if (mkdirCmd.hasValue("parents"))
    in += "&mgm.option=p";

  path = cleanPath(mkdirCmd.getValue("path"));
  in += "&mgm.path=" + path;

  global_retc = output_result(client_user_command(in));
  return (0);
}
