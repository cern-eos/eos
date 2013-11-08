// ----------------------------------------------------------------------
// File: com_who.cc
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

/* Who is connected -  Interface */
int
com_who (char* arg1)
{
  XrdOucString option = "";
  XrdOucString options = "";
  XrdOucString in = "";
  ConsoleCliCommand cliCommand("who",
                               "print statistics about active users "
                               "(idle<5min)");
  cliCommand.addOptions(std::vector<CliOption>
			{{"client", "break down by client host", "-c"},
                         {"ids", "print id's instead of names", "-n"},
                         {"auth", "print auth protocols", "-z"},
                         {"all", "print all", "-a"},
                         {"monitor", "print in monitoring format <key>=<value>",
                                                                          "-m"},
                         {"summary", "print summary for clients", "-s"}
                        });

  addHelpOptionRecursively(&cliCommand);

  cliCommand.parse(arg1);

  if (checkHelpAndErrors(&cliCommand))
    return 0;

  in = "mgm.cmd=who";
  const char *commandsAndOption[] = {"client", "c",
                                     "ids", "n",
                                     "auth", "z",
                                     "all", "a",
                                     "monitor", "m",
                                     "summary", "s",
                                     0};

  for (int i = 0; commandsAndOption[i]; i += 2)
  {
    if (cliCommand.hasValue(commandsAndOption[i]))
      options += commandsAndOption[i + 1];
  }

  if (options.length())
  {
    in += "&mgm.option=";
    in += options;
  }

  global_retc = output_result(client_user_command(in));
  return (0);
}
