// ----------------------------------------------------------------------
// File: com_rm.cc
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
#include "common/Path.hh"
/*----------------------------------------------------------------------------*/

/* Remove a file */
int
com_rm (char* arg1)
{
  XrdOucString path;
  XrdOucString option = "";
  eos::common::Path* cPath = 0;
  XrdOucString in = "mgm.cmd=rm&";
  ConsoleCliCommand cliCommand("rm", "remove file <path>");
  cliCommand.addOption({"recursive", "delete recursively", "-r"});
  cliCommand.addOption({"path", "", 1, -1, "<path>", true});

  addHelpOptionRecursively(&cliCommand);

  cliCommand.parse(arg1);

  if (checkHelpAndErrors(&cliCommand))
    return 0;

  if (cliCommand.hasValue("recursive"))
    option = "r";

  path = cleanPath(cliCommand.getValue("path"));

  in += "mgm.path=" + path;
  in += "&mgm.option=" + option;

  cPath = new eos::common::Path(path.c_str());

  if ( (option == "r") && (cPath->GetSubPathSize() < 4) )
  {
    string s;
    fprintf(stdout, "Do you really want to delete ALL files starting at %s ?\n", path.c_str());
    fprintf(stdout, "Confirm the deletion by typing => ");
    XrdOucString confirmation = "";
    for (int i = 0; i < 10; i++)
    {
      confirmation += (int) (9.0 * rand() / RAND_MAX);
    }
    fprintf(stdout, "%s\n", confirmation.c_str());
    fprintf(stdout, "                               => ");
    getline(std::cin, s);
    std::string sconfirmation = confirmation.c_str();
    if (s == sconfirmation)
    {
      fprintf(stdout, "\nDeletion confirmed\n");
      in += "&mgm.deletion=deep";
      delete cPath;
    }
    else
    {
      fprintf(stdout, "\nDeletion aborted\n");
      global_retc = EINTR;
      delete cPath;
      return (0);
    }
  }

  global_retc = output_result(client_user_command(in));

  return (0);
}
