// ----------------------------------------------------------------------
// File: com_io.cc
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

/* Namespace Interface */
int
com_io (char* arg1)
{
  XrdOucString options = "";
  XrdOucString path = "";
  XrdOucString in = "";
  XrdOucString target = "";
  ConsoleCliCommand *parsedCmd, *ioCmd, *statSubCmd, *enableSubCmd,
    *disableSubCmd, *reportSubCmd, *nsSubCmd;

  ioCmd = new ConsoleCliCommand("io", "IO related functions");

  statSubCmd = new ConsoleCliCommand("stat", "print IO statistics");
  statSubCmd->addOptions({{"summary", "show summary information (this is the "
                           "default if -t,-d,-x is not selected)", "-l"},
                          {"all", "break down by uid/gid", "-a"},
                          {"monitor", "print in <key>=<val> monitoring format",
                           "-m"},
                          {"numerical", "print numerical uid/gids", "-n"},
                          {"top", "print top user stats", "-t"},
                          {"domains", "break down by domains", "-d"},
                          {"app", "break down by application", "-x"},
                         });
  ioCmd->addSubcommand(statSubCmd);

  enableSubCmd = new ConsoleCliCommand("enable", "enable collection of IO "
                                       "reports");
  enableSubCmd->addOptions({{"reports", "enable collection of IO reports", "-r"},
                            {"popularity", "enable popularity accounting", "-p"},
                            {"namespace", "enable report namespace", "-n"}
                           });
  enableSubCmd->addOption({"target", "add a UDP message target for IO UDP "
                           "packets (the configured targets are shown by "
                           "'io stat -l'", "--udp=", "<address>", false});
  ioCmd->addSubcommand(enableSubCmd);

  disableSubCmd = new ConsoleCliCommand("disable", "disable collection of IO "
                                       "reports");
  disableSubCmd->addOptions({{"reports", "disable collection of IO reports",
                              "-r"},
                             {"popularity", "disable popularity accounting",
                              "-p"},
                             {"namespace", "disable report namespace", "-n"}
                            });
  disableSubCmd->addOption({"target", "remove a UDP message target for IO UDP "
                            "packets (the configured targets are shown by "
                            "'io stat -l'", "--udp=", "<address>", false});
  ioCmd->addSubcommand(disableSubCmd);

  reportSubCmd = new ConsoleCliCommand("report", "show contents of report "
                                       "namespace for <path>");
  reportSubCmd->addOption({"path", "", 1, 1, "<path>", true});
  ioCmd->addSubcommand(reportSubCmd);

  nsSubCmd = new ConsoleCliCommand("ns", "show namespace IO ranking "
                                   "(popularity)");
  nsSubCmd->addOptions({{"all", "don't limit the output list", "-a"},
                        {"monitor", "print in <key>=<val> monitoring format",
                         "-m"},
                        {"accesses", "show ranking by number of accesses", "-n"},
                        {"bytes", "show ranking by number of bytes", "-b"},
                        {"week", "show history for the last 7 days", "-w"}
                       });
  nsSubCmd->addGroupedOptions({{"100", "show the first 100 in the ranking",
                                "-100"},
                               {"1000", "show the first 1000 in the ranking",
                                "-1000,-1k"},
                               {"1000", "show the first 10000 in the ranking",
                                "-10000,-10k"}
                              });
  ioCmd->addSubcommand(nsSubCmd);

  addHelpOptionRecursively(ioCmd);

  parsedCmd = ioCmd->parse(arg1);

  if (parsedCmd == ioCmd)
  {
    if (!checkHelpAndErrors(ioCmd))
      ioCmd->printUsage();
    goto bailout;
  }
  else if (checkHelpAndErrors(parsedCmd))
    goto bailout;

  in = "mgm.cmd=io&";

  if (parsedCmd == enableSubCmd || parsedCmd == disableSubCmd)
  {
    in += "mgm.subcmd=";
    in += parsedCmd == enableSubCmd ? "enable" : "disable";

    if (parsedCmd->hasValue("target"))
      target = parsedCmd->getValue("target").c_str();

    const char *enableOptions[] = {"namespace", "n",
                                   "reports", "r",
                                   "popularity", "p",
                                   NULL
                                  };

    for (int i = 0; enableOptions[i] != NULL; i += 2)
    {
      if (parsedCmd->hasValue(enableOptions[i]))
        options += enableOptions[i + 1];
    }
  }
  else if (parsedCmd == statSubCmd)
  {
    in += "mgm.subcmd=stat";
    const char *statOptions[] = {"summary", "l",
                                 "all", "a",
                                 "monitor", "m",
                                 "numerical", "n",
                                 "top", "t",
                                 "domains", "d",
                                 "app", "x",
                                 NULL
                                };
    for (int i = 0; statOptions[i] != NULL; i += 2)
    {
      if (statSubCmd->hasValue(statOptions[i]))
        options += statOptions[i + 1];
    }
  }
  else if (parsedCmd == reportSubCmd)
  {
    in += "mgm.subcmd=report";
    in += "&mgm.io.path=";
    in += reportSubCmd->getValue("path").c_str();
  }
  else if (parsedCmd == nsSubCmd)
  {
    in += "mgm.subcmd=ns";

    const char *nsOptions[] = {"all", "a",
                               "monitor", "m",
                               "accesses", "n",
                               "bytes", "b",
                               "week", "w",
                               "100", "-100",
                               "1000", "-1000",
                               "10000", "-10000",
                               NULL
                              };
    for (int i = 0; nsOptions[i] != NULL; i += 2)
    {
      if (nsSubCmd->hasValue(nsOptions[i]))
        options += nsOptions[i + 1];
    }

  }

  if (options.length())
  {
    in += "&mgm.option=";
    in += options;
  }
  if (target.length())
  {
    in += "&mgm.udptarget=";
    in += target;
  }

  global_retc = output_result(client_admin_command(in));

 bailout:
  delete ioCmd;

  return (0);
}
