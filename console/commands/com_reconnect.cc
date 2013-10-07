// ----------------------------------------------------------------------
// File: com_reconnect.cc
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
#include "XrdClient/XrdClientConn.hh"
/*----------------------------------------------------------------------------*/

/* Force a reconnection/reauthentication */
int
com_reconnect (char* arg1)
{
  XrdOucString param("");
  const char *options[] = {"gsi", "krb5", "unix", "sss", 0};
  ConsoleCliCommand reconnectCmd("reconnect", "reconnect to the management "
                                 "node (using the specified protocol)");

  std::vector<CliOption> optionsVector;
  for (int i = 0; options[i]; i++)
    optionsVector.push_back({options[i], "", options[i]});

  reconnectCmd.addGroupedOptions(optionsVector);

  addHelpOptionRecursively(&reconnectCmd);

  reconnectCmd.parse(arg1);

  if (checkHelpAndErrors(&reconnectCmd))
    return 0;

  for (int i = 0; options[i]; i++)
  {
    if (reconnectCmd.hasValue(options[i]))
    {
      param = options[i];
      break;
    }
  }

  if (param.length())
  {
    fprintf(stdout, "# reconnecting to %s with <%s> authentication\n",
            serveruri.c_str(), param.c_str());
    setenv("XrdSecPROTOCOL", param.c_str(), 1);
  }
  else
    fprintf(stdout, "# reconnecting to %s\n", serveruri.c_str());

  XrdOucString path = serveruri;
  path += "//proc/admin/";

  /* - NOT SUPPORTED IN THE NEW CLIENT 
     XrdClientAdmin admin(path.c_str());
     admin.Connect();
     if (admin.GetClientConn()) {
     admin.GetClientConn()->Disconnect(true);
     }
  */

  if (debug)
    fprintf(stdout, "debug: %s\n", path.c_str());

  return (0);
}
