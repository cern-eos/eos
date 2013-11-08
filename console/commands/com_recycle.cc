// ----------------------------------------------------------------------
// File: com_recycle.cc
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

/* Recycle a file/directory and configure recycling */
int
com_recycle (char* arg1)
{
  XrdOucString in = "mgm.cmd=recycle&";
  ConsoleCliCommand *parsedCmd, *recycleCmd, *lsSubCmd, *purgeSubCmd,
    *restoreSubCmd, *configSubCmd;

  recycleCmd = new ConsoleCliCommand("recycle", "print status of recycle bin "
                                     "and if executed by root the recycle bin "
                                     "configuration settings");

  CliOption monitorOption("monitor", "print information in monitoring "
                          "<key>=<value> format", "-m");

  lsSubCmd = new ConsoleCliCommand("ls", "list files in the recycle bin");
  lsSubCmd->addOption(monitorOption);
  lsSubCmd->addOption({"numerical", "print uid+gid number", "-n"});
  recycleCmd->addSubcommand(lsSubCmd);

  purgeSubCmd = new ConsoleCliCommand("purge", "purge files in the recycle bin");
  recycleCmd->addSubcommand(purgeSubCmd);

  restoreSubCmd = new ConsoleCliCommand("restore", "undo the deletion "
                                        "identified by <recycle-key>");
  restoreSubCmd->addOption({"force", "move's deleted files/dirs back to the "
                            "original location (otherwise the key entry will "
                            "have a <.inode> suffix",
                            "--force-original-name,-f"});
  restoreSubCmd->addOption({"key", "", 1, 1, "<recycle-key>", true});
  recycleCmd->addSubcommand(restoreSubCmd);

  configSubCmd = new ConsoleCliCommand("config", "configuration functions for "
                                       "the recycle bin");
  configSubCmd->addOption(monitorOption);
  CliOptionWithArgs lifetimeOption("lifetime", "configure the FIFO lifetime "
                                   "of the recycle bin", "--lifetime",
                                   "<seconds>", false);
  lifetimeOption.addEvalFunction(optionIsPositiveNumberEvalFunc, 0);
  OptionsGroup *group =
    configSubCmd->addGroupedOptions(std::vector<CliOptionWithArgs>
    {{"add-bin", "configures to use the recycle bin for deletions in "
       "<sub-tree>", "--add-bin", "<sub-tree>", false},
      {"remove-bin", "disables usage of recycle bin for <sub-tree>",
       "--remove-bin", "<sub-tree>", false},
      {"size", "set the size of the recycle bin", "--size", "<size>", false}
    });
  group->addOption(lifetimeOption);
  group->setRequired(true);
  recycleCmd->addSubcommand(configSubCmd);

  addHelpOptionRecursively(recycleCmd);

  parsedCmd = recycleCmd->parse(arg1);

  if (checkHelpAndErrors(parsedCmd))
      goto bailout;

  if (parsedCmd == lsSubCmd)
  {
    in += "&mgm.subcmd=ls";

    if (lsSubCmd->hasValue("numerical"))
      in += "&mgm.recycle.format=n";

    if (lsSubCmd->hasValue("monitor"))
      in += "&mgm.recycle.format=m";
  }
  else if (parsedCmd == purgeSubCmd)
    in += "&mgm.subcmd=purge";
  else if (parsedCmd == restoreSubCmd)
  {
    in += "&mgm.subcmd=restore";
    in += "&mgm.recycle.arg=";
    in += restoreSubCmd->getValue("key").c_str();

    if (restoreSubCmd->hasValue("force"))
      in += "&mgm.option=-f";
  }
  else if (parsedCmd == configSubCmd)
  {
    in += "&mgm.subcmd=config";
    in += "&mgm.recycle.arg=";

    if (configSubCmd->hasValue("add-bin"))
      in += cleanPath(configSubCmd->getValue("add-bin"));
    else if (configSubCmd->hasValue("remove-bin"))
      in += cleanPath(configSubCmd->getValue("remove-bin"));
    else if (configSubCmd->hasValue("lifetime"))
      in += configSubCmd->getValue("lifetime").c_str();
    else if (configSubCmd->hasValue("size"))
      in += configSubCmd->getValue("size").c_str();

    if (configSubCmd->hasValue("monitor"))
      in += "&mgm.recycle.format=m";
  }

  global_retc = output_result(client_user_command(in));

 bailout:
  delete recycleCmd;

  return (0);
}
