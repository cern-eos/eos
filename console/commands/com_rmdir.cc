// ----------------------------------------------------------------------
// File: com_rmdir.cc
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

/* Remove a directory */
int
com_rmdir (char* arg1)
{
  XrdOucString path;
  XrdOucString in = "mgm.cmd=rmdir&";

  ConsoleCliCommand rmdirCmd("rmdir", "remote directory <path>");
  rmdirCmd.addOption({"path", "", 1, 1, "<path>", true});

  addHelpOptionRecursively(&rmdirCmd);

  rmdirCmd.parse(arg1);

  if (checkHelpAndErrors(&rmdirCmd))
    return 0;

  path = cleanPath(rmdirCmd.getValue("path"));
  in += "mgm.path=" + path;

  global_retc = output_result(client_user_command(in));
  return (0);
}
