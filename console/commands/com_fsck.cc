// ----------------------------------------------------------------------
// FiBle: com_fsck.cc
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

/* Namespace Interface */
int
com_fsck (char* arg1)
{
  XrdOucString option;
  XrdOucString options = "";
  XrdOucString path = "";
  XrdOucString in = "";
  ConsoleCliCommand *parsedCmd, *fsckCmd, *statSubCmd, *enableSubCmd,
    *disableSubCmd, *reportSubCmd, *repairSubCmd;

  fsckCmd = new ConsoleCliCommand("fsck", "filesystem consistency check");

  statSubCmd = new ConsoleCliCommand("stat", "print status of consistency "
                                     "check");
  fsckCmd->addSubcommand(statSubCmd);

  enableSubCmd = new ConsoleCliCommand("enable", "enable fsck");
  CliPositionalOption interval("interval", "check interval in minutes "
                               "(default: 30 minutes)", 1, 1, "<interval>");
  interval.addEvalFunction(optionIsPositiveNumberEvalFunc, 0);
  enableSubCmd->addOption(interval);
  fsckCmd->addSubcommand(enableSubCmd);

  disableSubCmd = new ConsoleCliCommand("disable", "stop fsck");
  fsckCmd->addSubcommand(disableSubCmd);

  reportSubCmd = new ConsoleCliCommand("report", "report consistency check "
                                       "results");
  reportSubCmd->addOptions({{"all", "break down statistics per filesystem", "-a"},
                            {"ids", "print concerned file ids", "-i"},
                            {"logical", "print concerned logical names", "-l"},
                            {"json", "select JSON output format", "--json"}
                           });
  reportSubCmd->addOption({"error", "select to report only error tag <tag>",
                           "--error=", "<tag>", false});
  fsckCmd->addSubcommand(reportSubCmd);

  repairSubCmd = new ConsoleCliCommand("repair", "filesystem consistency "
                                       "repair functions");
  repairSubCmd->addOptions({{"checksum", "issues a 'verify' operation on all "
                             "files with checksum errors", "--checksum"},
                            {"checksum-commit", "issues a 'verify' operation "
                             "on all files with checksum errors and forces a "
                             "commit of size and checksum to the MGM",
                             "--checksum-commit"},
                            {"resync", "issues a 'resync' operation on all "
                             "files with any error. This will resync the MGM "
                             "meta data to the storage node and will clean-up "
                             "'ghost' entries in the FST meta data cache.",
                             "--resync"},
                            {"unlink-unregistered", "unlink replicas which are "
                             "not connected/registered to their logical name",
                             "--unlink-unregistered"},
                            {"unlink-orphans", "unlink replicas which don't "
                             "belong to any logical name", "--unlink-orphans"},
                            {"adjust-replicas", "try to fix all replica "
                             "inconsistencies", "--adjust-replicas"},
                            {"drop-missing-replicas", "just drop replicas from "
                             "the namespace if they cannot be found on disk",
                             "--drop-missing-replicas"},
                            {"unlink-zero-replicas", "drop all files which "
                             "have no replica's attached and are older than 48 "
                             "hours!", "--unlink-zero-replicas"},
                            {"all", "do all the repair actions above, besides "
                             "--checksum-commit", "--all"}
                           });
  repairSubCmd->addOption({"error", "select to repair only error tag <tag>",
                           "--error=", "<tag>", false});
  fsckCmd->addSubcommand(repairSubCmd);

  addHelpOptionRecursively(fsckCmd);

  parsedCmd = fsckCmd->parse(arg1);

  if (parsedCmd == fsckCmd)
  {
    if (!checkHelpAndErrors(parsedCmd))
      fsckCmd->printUsage();
    goto bailout;
  }
  else if (checkHelpAndErrors(parsedCmd))
    goto bailout;

  in = "mgm.cmd=fsck&";

  if (parsedCmd == enableSubCmd)
  {
    XrdOucString interval("");

    in += "mgm.subcmd=enable";
    if (enableSubCmd->hasValue("interval"))
    {
      in += "&mgm.fsck.interval=";
      in += enableSubCmd->getValue("interval").c_str();
    }
  }
  else if (parsedCmd == disableSubCmd)
  {
    in += "mgm.subcmd=disable";
  }
  else if (parsedCmd == statSubCmd)
    in += "mgm.subcmd=stat";
  else if (parsedCmd == reportSubCmd)
  {
    in += "mgm.subcmd=report";

    if (reportSubCmd->hasValue("error"))
    {
      in += "&mgm.fsck.selection=";
      in += reportSubCmd->getValue("error").c_str();
    }

    if (reportSubCmd->hasValue("a"))
      options += "a";
    if (reportSubCmd->hasValue("ids"))
      options += "i";
    if (reportSubCmd->hasValue("logical"))
      options += "l";
    if (reportSubCmd->hasValue("json"))
      options += "json";
  }
  else if (parsedCmd == repairSubCmd)
  {
    in += "mgm.subcmd=repair";

    const char* repairOptions[] = {"checksum", "checksum-commit", "resync",
                                   "unlink-unregistered", "unlink-orphans",
                                   "unlink-replicas", "drop-missing-replicas",
                                   "unlink-zero-replicas", "all"};
    for (int i = 0; repairOptions[i]; i++)
    {
      if (repairSubCmd->hasValue(repairOptions[i]))
        options += repairOptions[i];
    }

    if (options.length() == 0)
    {
      repairSubCmd->printUsage();
      goto bailout;
    }
  }

  if (options.length())
  {
    in += "&mgm.option=";
    in += options;
  }

  global_retc = output_result(client_admin_command(in));

 bailout:
  delete fsckCmd;

  return (0);
}
