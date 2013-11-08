// ----------------------------------------------------------------------
// File: com_rtlog.cc
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

/* Retrieve realtime log output */

int
com_rtlog (char* arg1)
{
  XrdOucString tag("err");
  XrdOucString lines("10");
  XrdOucString queue(".");
  XrdOucString in = "mgm.cmd=rtlog&mgm.rtlog.queue=";

  ConsoleCliCommand rtlogCmd("rtlog", "real-time log");
  rtlogCmd.addOptions(std::vector<CliPositionalOption>
                      {{"queue", "if '*' is used, it will query all nodes;\n"
                        "if '.' is used, it will query only the connected mgm\n"
                        "if no argument is given, '.' is assumed", 1, 1,
                        "<queue>|*|.", false},
                       {"filter", "filter by word <filter-word>", 4, 1,
                        "<filter-word>", false}
                      });
  CliPositionalOption linesOption("lines", "print the last <lines> of the log "
                                  "(default = 10)", 2, 1, "<lines>");
  linesOption.addEvalFunction(optionIsPositiveNumberEvalFunc, 0);
  rtlogCmd.addOption(linesOption);
  CliPositionalOption debugOption("debug", "filter by debug level "
                                  "<debug-level> (default = err)",
                                  3, 1, "<debug-level>");
  std::vector<std::string> choices = {"info", "debug", "err", "emerg",
                                      "alert", "crit", "warning"};
  debugOption.addEvalFunction(optionIsChoiceEvalFunc, &choices);
  rtlogCmd.addOption(debugOption);

  addHelpOptionRecursively(&rtlogCmd);

  rtlogCmd.parse(arg1);

  if (checkHelpAndErrors(&rtlogCmd))
    return 0;

  if (rtlogCmd.hasValue("queue"))
    queue = rtlogCmd.getValue("queue").c_str();

  in += queue;

  if (rtlogCmd.hasValue("lines"))
    lines = rtlogCmd.getValue("lines").c_str();
  else
    goto call_command;

  if (rtlogCmd.hasValue("debug"))
    tag = rtlogCmd.getValue("debug").c_str();
  else
    goto call_command;

  if (rtlogCmd.hasValue("filter"))
  {
    in += "&mgm.rtlog.filter=";
    in += rtlogCmd.getValue("filter").c_str();
  }

 call_command:
  in += "&mgm.rtlog.lines=" + lines;
  in += "&mgm.rtlog.tag=" + tag;

  global_retc = output_result(client_admin_command(in));
  return (0);
}
