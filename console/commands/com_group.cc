// ----------------------------------------------------------------------
// File: com_group.cc
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

using namespace eos::common;

/* Group listing, configuration, manipulation */
int
com_group (char* arg1)
{
  XrdOucString in = "";
  bool silent = false;
  bool highlighting = true;
  XrdOucString option = "";
  XrdOucEnv* result = 0;
  ConsoleCliCommand *parsedCmd, *groupCmd, *lsSubCmd, *rmSubCmd, *setSubCmd;

  groupCmd = new ConsoleCliCommand("group", "group related functions");

  CliPositionalOption groupOption("group", "", 1, 1, "<group>", false);

  lsSubCmd = new ConsoleCliCommand("ls", "list groups or only <group>");
  lsSubCmd->addOption({"silent", "silent mode", "-s"});
  lsSubCmd->addGroupedOptions({{"monitor", "monitoring key=value output format",
                                "-m"},
                               {"long", "long output - list also file systems "
                                "after each group", "-l"},
                               {"io", "print IO statistics for the group",
                                "--io"},
                               {"IO", "print IO statistics for each "
                                "filesystem", "--IO"}
                           });
  lsSubCmd->addOption(groupOption);
  groupCmd->addSubcommand(lsSubCmd);

  groupOption.setRequired(true);

  rmSubCmd = new ConsoleCliCommand("rm", "remove group");
  rmSubCmd->addOption(groupOption);
  groupCmd->addSubcommand(rmSubCmd);

  setSubCmd = new ConsoleCliCommand("set", "activate/deactivate a group\nasas");
  setSubCmd->addOption(groupOption);
  CliPositionalOption activeOption("active", "", 2, 1, "on|off", true);
  std::vector<std::string> choices = {"on", "off"};
  activeOption.addEvalFunction(optionIsChoiceEvalFunc, &choices);
  setSubCmd->addOption(activeOption);
  groupCmd->addSubcommand(setSubCmd);

  addHelpOptionRecursively(groupCmd);

  parsedCmd = groupCmd->parse(arg1);

  if (parsedCmd == groupCmd)
  {
    if (!checkHelpAndErrors(groupCmd))
      groupCmd->printUsage();
    goto bailout;
  }
  if (checkHelpAndErrors(parsedCmd))
    goto bailout;

  if (parsedCmd == lsSubCmd)
  {
    in = "mgm.cmd=group&mgm.subcmd=ls";
    option = "";

    silent = lsSubCmd->hasValue("silent");

    if (lsSubCmd->hasValue("monitor"))
    {
      in += "&mgm.outformat=m";
      highlighting = false;
    }
    else if (lsSubCmd->hasValue("long"))
      in += "&mgm.outformat=l";
    else if (lsSubCmd->hasValue("io"))
      in += "&mgm.outformat=io";
    else if (lsSubCmd->hasValue("IO"))
      in += "&mgm.outformat=IO";

    if (lsSubCmd->hasValue("group"))
    {
      in += "&mgm.selection=";
      in += lsSubCmd->getValue("group").c_str();
    }
  }
  else if (parsedCmd == setSubCmd)
  {
    in = "mgm.cmd=group&mgm.subcmd=set";
    in += "&mgm.group=";
    in += setSubCmd->getValue("group").c_str();
    in += "&mgm.group.state=";
    in += setSubCmd->getValue("active").c_str();
  }
  else if (parsedCmd == rmSubCmd)
  {
    in = "mgm.cmd=group&mgm.subcmd=rm";
    in += "&mgm.group=";
    in += rmSubCmd->getValue("group").c_str();
  }

  result = client_admin_command(in);

  if (!silent)
  {
    global_retc = output_result(result, highlighting);
  }
  else
  {
    if (result)
    {
      global_retc = 0;
    }
    else
    {
      global_retc = EINVAL;
    }
  }

 bailout:
  delete groupCmd;

  return (0);
}
