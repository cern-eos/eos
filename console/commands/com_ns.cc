// ----------------------------------------------------------------------
// File: com_ns.cc
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
#include "common/RWMutex.hh"
/*----------------------------------------------------------------------------*/

/* Namespace Interface */
int
com_ns (char* arg1)
{
  XrdOucString option = "";
  XrdOucString options = "";
  XrdOucString in = "";
  XrdOucString interval;
  ConsoleCliCommand *parsedCmd, *nsCmd, *statSubCmd,
    *compactSubCmd, *compactOnSubCmd, *compactOffSubCmd, *masterSubCmd;
#ifdef EOS_INSTRUMENTED_RWMUTEX
  ConsoleCliCommand *mutexSubCmd;
#endif

  nsCmd = new ConsoleCliCommand("ns", "print basic namespace parameters");

  statSubCmd = new ConsoleCliCommand("stat", "print namespace statistics");
  statSubCmd->addOptions({{"all", "break down by uid/gid", "-a"},
                          {"monitor", "print in <key>=<val> monitoring format",
                           "-m"},
                          {"numerical", "print numerical uid/gids", "-n"},
                          {"reset", "reset namespace counter", "--reset"}
                         });
  nsCmd->addSubcommand(statSubCmd);

#ifdef EOS_INSTRUMENTED_RWMUTEX
  mutexSubCmd = new ConsoleCliCommand("mutex", "print namespace mutexistics");
  mutexSubCmd->addOptions({{"timing", "toggle the timing", "--toggletiming"},
                           {"order", "toggle the order checking",
                            "--toggleorder"},
                          {"rate1", "set the timing sample rate at 1% "
                           "(default, almost no slow-down)", "--smplrate1"},
                          {"rate10", "set the timing sample rate at 10% "
                           "(medium no slow-down)", "--smplrate10"},
                          {"rate100", "set the timing sample rate at 100% "
                           "(severe slow-down)", "--smplrate100"}
                         });
  nsCmd->addSubcommand(mutexSubCmd);
#endif // EOS_INSTRUMENTED_RWMUTEX

  compactSubCmd = new ConsoleCliCommand("compact", "");
  nsCmd->addSubcommand(compactSubCmd);

  compactOnSubCmd = new ConsoleCliCommand("on", "enable online "
                                          "compactification after <delay> "
                                          "seconds");
  CliPositionalOption delayOption("delay", "", 1, 1, "<delay>", true);
  delayOption.addEvalFunction(optionIsPositiveNumberEvalFunc, 0);
  compactOnSubCmd->addOption(delayOption);
  CliPositionalOption intervalOption("interval", "if <interval> is >0 the "
                                     "compactifcation is repeated "
                                     "automatically after <interval> seconds!",
                                     2, 1, "<interval>", false);
  intervalOption.addEvalFunction(optionIsPositiveNumberEvalFunc, 0);
  compactOnSubCmd->addOption(intervalOption);
  compactSubCmd->addSubcommand(compactOnSubCmd);

  compactOffSubCmd = new ConsoleCliCommand("off", "disable online "
                                           "compactification");
  compactSubCmd->addSubcommand(compactOffSubCmd);

  masterSubCmd = new ConsoleCliCommand("master", "master/slave operation");
  OptionsGroup *group = masterSubCmd->addGroupedOptions({
      {"log", "show the master log (this also happens if no option is given)",
       "--log"},
      {"log-clear", "clean the master log", "--log-clear"},
      {"disable", "disable the slave/master supervisor thread modifying "
       "stall/redirection variables", "--disable"},
      {"enable", "enable the slave/master supervisor thread modifying "
       "stall/redirection variables", "--enable"}
     });
  group->addOption({"name", "set the host name of the MGM RW master daemon",
                    "--set=", "<master-hostname>", false});
  nsCmd->addSubcommand(masterSubCmd);

  addHelpOptionRecursively(nsCmd);

  parsedCmd = nsCmd->parse(arg1);

  if (parsedCmd == compactSubCmd)
  {
    fprintf(stdout, "Use one of the following subcommands:\n");
    compactOnSubCmd->printUsage();
    compactOffSubCmd->printUsage();
    goto bailout;
  }
  if (checkHelpAndErrors(parsedCmd))
    goto bailout;

  in = "mgm.cmd=ns&";

  if (parsedCmd == statSubCmd)
  {
    in += "mgm.subcmd=stat";

    if (statSubCmd->hasValue("all"))
      options += "a";
    if (statSubCmd->hasValue("monitor"))
      options += "m";
    if (statSubCmd->hasValue("numerical"))
      options += "n";
    if (statSubCmd->hasValue("reset"))
      options += "r";
  }
  else if (parsedCmd == compactOnSubCmd || parsedCmd == compactOffSubCmd)
  {
    in += "mgm.subcmd=compact";
    in += "&mgm.ns.compact=";
    in += parsedCmd == compactOnSubCmd ? "on" : "off";

    if (parsedCmd == compactOnSubCmd)
    {
      in += "&mgm.ns.compact.delay=";
      in += compactOnSubCmd->getValue("delay").c_str();

      if (compactOnSubCmd->hasValue("interval"))
        interval = compactOnSubCmd->getValue("interval").c_str();
      else
        interval = "0";

      in += "&mgm.ns.compact.interval=" + interval;
    }
  }
  else if (parsedCmd == masterSubCmd)
  {
    in += "mgm.subcmd=master";
    in += "&mgm.master=";

    if (masterSubCmd->hasValue("log-clear"))
      in += "--log-clear";
    else if (masterSubCmd->hasValue("disable"))
      in += "--disable";
    else if (masterSubCmd->hasValue("enable"))
      in += "--enable";
    else if (masterSubCmd->hasValue("name"))
      in += masterSubCmd->getValue("name").c_str();
    else
      in += "--log";
  }
#ifdef EOS_INSTRUMENTED_RWMUTEX
  else if (parsedCmd == mutexSubCmd)
  {
    in += "mgm.subcmd=mutex";

    if (statSubCmd->hasValue("timing"))
      options += "t";
    if (statSubCmd->hasValue("order"))
      options += "o";
    if (statSubCmd->hasValue("rate1"))
      options += "1";
    if (statSubCmd->hasValue("rate10"))
      options += "s";
    if (statSubCmd->hasValue("rate100"))
      options += "f";
  }
#endif // EOS_INSTRUMENTED_RWMUTEX

  if (options.length())
  {
    in += "&mgm.option=";
    in += options;
  }

  global_retc = output_result(client_admin_command(in));

 bailout:
  delete nsCmd;

  return (0);
}
