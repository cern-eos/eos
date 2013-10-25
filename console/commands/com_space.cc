// ----------------------------------------------------------------------
// File: com_space.cc
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
#include <climits>
/*----------------------------------------------------------------------------*/

using namespace eos::common;

static void
printConfigHelp()
{

  std::vector<std::pair<std::string, std::string>> configHelp =
    {{"space.nominalsize=<value>", "configure the nominal size for this space"},
     {"space.balancer=on|off ",
      "enable/disable the space balancer [default=off]"},
     {"space.balancer.threshold=<percent>",
      "configure the used bytes deviation which triggers balancing "
      "[ default=20 (%%) ]"},
     {"space.balancer.node.rate=<MB/s>",
      "configure the nominal transfer bandwith per running transfer on a node "
      "[ default=25 (MB/s) ]"},
     {"space.balancer.node.ntx=<#>",
      "configure the number of parallel balancing transfers per node "
      "[ default=2 (streams) ]"},
     {"space.converter=on|off ",
      "enable/disable the space converter [default=off]"},
     {"space.converter.ntx=<#>",
      "configure the number of parallel conversions per space "
      "[ default=2 (streams) ]"},
     {"space.drainer.node.rate=<MB/s>",
      "configure the nominal transfer bandwith per running transfer on a node "
      "[ default=25 (MB/s) ]"},
     {"space.drainer.node.ntx=<#>",
      "configure the number of parallel draining transfers per node "
      "[ default=2 (streams) ]"},
     {"space.headroom=<size>",
      "configure the default disk headroom if not defined on a filesystem "
      "(see fs for details)"},
     {"space.scaninterval=<sec>",
      "configure the default scan interval if not defined on a filesystem "
      "(see fs for details)"},
     {"space.drainperiod=<sec>",
      "configure the default drain  period if not defined on a filesystem "
      "(see fs for details)"},
     {"space.graceperiod=<sec>",
      "configure the default grace  period if not defined on a filesystem "
      "(see fs for details)"},
     {"space.autorepair=on|off",
      "enable auto-repair of faulty replica's/files "
      "(the converter has to be enabled too)"},
     {"fs.<key>=<value>",
      "configure file system parameters for each filesystem in this space "
      "(see help of 'fs config' for detail"}};

  fprintf(stdout, "  KEY=VALUE can be one of:\n");

  for (size_t i = 0; i < configHelp.size(); i++)
    fprintf(stdout, "\t%-*s\t\t- %s\n",
            35, configHelp[i].first.c_str(), configHelp[i].second.c_str());
  fprintf(stdout,
          "  (size can be given also like 10T, 20G, 2P ... "
          "without space before the unit)\n");
}

/* Space listing, configuration, manipulation */
int
com_space (char* arg1)
{
  std::string in("");
  bool silent = false;
  bool highlighting = true;
  XrdOucString command;
  XrdOucEnv* result = 0;
  ConsoleCliCommand *parsedCmd, *spaceCmd, *lsSubCmd, *statusSubCmd, *rmSubCmd,
    *defineSubCmd, *setSubCmd, *quotaSubCmd, *configSubCmd;

  spaceCmd = new ConsoleCliCommand("space", "space configuration");

  lsSubCmd = new ConsoleCliCommand("ls", "list spaces");
  lsSubCmd->addOption({"silent", "silent mode", "-s"});
  lsSubCmd->addGroupedOptions({
        {"monitor", "print in monitoring format <key>=<value>", "-m"},
        {"long", "long output - list also file systems after each space", "-l"},
        {"io", "print IO satistics", "--io"},
        {"fsck", "print filesystem check statistics", "--fsck"}
       });
  lsSubCmd->addOption({"space", "a specific space to list", 1, "<space>"});
  spaceCmd->addSubcommand(lsSubCmd);

  statusSubCmd = new ConsoleCliCommand("status", "print all defined variables "
                                       "for a space");
  statusSubCmd->addOption({"space", "", 1, 1, "<space>", true});
  spaceCmd->addSubcommand(statusSubCmd);

  rmSubCmd = new ConsoleCliCommand("rm", "remove space");
  rmSubCmd->addOption({"space", "", 1, 1, "<space>", true});
  spaceCmd->addSubcommand(rmSubCmd);

  defineSubCmd = new ConsoleCliCommand("define", "define how many filesystems "
                                       "can end up in one scheduling group "
                                       "<group-size> (default=0)");
  defineSubCmd->addOption({"space", "the name for the space", 1, 1, "<space>",
                           true});
  CliPositionalOption *groupSize = new CliPositionalOption("groupsize",
                           "a value of 0 means that no groups are built "
                           "within a space, otherwise it should be the maximum "
                           "number of nodes in a scheduling group",
                           2, 1,
                           "<group-size>", false);
  groupSize->addEvalFunction(optionIsIntegerEvalFunc, 0);
  std::pair<float, float> groupSizeRange = {0.0, 1024};
  groupSize->addEvalFunction(optionIsNumberInRangeEvalFunc, &groupSizeRange);
  defineSubCmd->addOption(groupSize);
  CliPositionalOption *groupMod = new CliPositionalOption("groupmod",
                           "defines the maximun number of filesystems per node",
                           3, 1, "<group-mod>", false);
  groupMod->addEvalFunction(optionIsIntegerEvalFunc, 0);
  std::pair<float, float> groupModRange = {0.0, 256.0};
  groupMod->addEvalFunction(optionIsNumberInRangeEvalFunc, &groupModRange);
  defineSubCmd->addOption(groupMod);
  spaceCmd->addSubcommand(defineSubCmd);

  setSubCmd = new ConsoleCliCommand("set", "enables/disables all groups under "
                                    "that space (not the nodes!)");
  CliPositionalOption activeOption = {"active","", 2, 1,"on|off", true};
  std::vector<std::string> choices = {"on", "off"};
  activeOption.addEvalFunction(optionIsChoiceEvalFunc, &choices);
  setSubCmd->addOption({"space", "", 1, 1, "<space>", true});
  setSubCmd->addOption(activeOption);
  spaceCmd->addSubcommand(setSubCmd);

  quotaSubCmd = new ConsoleCliCommand("quota", "enables/disables quota");
  quotaSubCmd->addOption({"space", "", 1, 1, "<space>", true});
  quotaSubCmd->addOption(activeOption);
  spaceCmd->addSubcommand(quotaSubCmd);

  configSubCmd = new ConsoleCliCommand("config", "enables/disables quota");
  configSubCmd->addOption({"space", "", 1, 1, "<space>", true});
  configSubCmd->addOption({"config_keyValue", "", 2, 1, "KEY=VALUE", true});
  spaceCmd->addSubcommand(configSubCmd);

  addHelpOptionRecursively(spaceCmd);

  parsedCmd = spaceCmd->parse(arg1);

  if (checkHelpAndErrors(parsedCmd))
  {
    if (parsedCmd == configSubCmd)
      printConfigHelp();

    goto bailout;
  }

  if (parsedCmd == lsSubCmd)
  {
    in = "mgm.cmd=space&mgm.subcmd=ls";

    silent = lsSubCmd->hasValue("silent");

    if (lsSubCmd->hasValue("monitor"))
    {
      in += "&mgm.outformat=m";
      highlighting = false;
    }
    if (lsSubCmd->hasValue("long"))
      in += "&mgm.outformat=l";
    if (lsSubCmd->hasValue("io"))
      in += "&mgm.outformat=io";
    if (lsSubCmd->hasValue("fsck"))
      in += "&mgm.outformat=fsck";
    if (lsSubCmd->hasValue("space"))
    {
      in += "&mgm.selection=";
      in += lsSubCmd->getValue("space");
    }
  }
  else if (parsedCmd == defineSubCmd)
  {
    in = "mgm.cmd=space&mgm.subcmd=define&mgm.space=";
    in += defineSubCmd->getValue("space");

    in += "&mgm.space.groupsize=";
    if (defineSubCmd->hasValue("groupsize"))
      in += defineSubCmd->getValue("groupsize");
    else
      in += "0";

    in += "&mgm.space.groupmod=";
    if (defineSubCmd->hasValue("groupmod"))
      in += defineSubCmd->getValue("groupmod");
    else
      in += "24";
  }
  else if (parsedCmd == setSubCmd || parsedCmd == quotaSubCmd)
  {
    std::string active, realCmd;

    if (parsedCmd->hasValue("active"))
      active = parsedCmd->getValue("active");

    in = "mgm.cmd=space&mgm.subcmd=";
    if (parsedCmd == setSubCmd)
    {
      in += "set";
      realCmd = "state";
    }
    else
    {
      in += "quota";
      realCmd = "quota";
    }

    in += "&mgm.space=";
    in += parsedCmd->getValue("space");

    in += "&mgm.space.";
    in += realCmd;
    in += "=";
    in += active;
  }
  else if (parsedCmd == statusSubCmd || parsedCmd == rmSubCmd)
  {
    in = "mgm.cmd=space&mgm.subcmd=";

    if (parsedCmd == rmSubCmd)
      in += "rm";
    else
      in += "status";

    if (parsedCmd->hasValue("space"))
    {
      in += "&mgm.space=";
      in += parsedCmd->getValue("space");
    }
  }
  else if (parsedCmd == configSubCmd)
  {
    configSubCmd->hasValue("config_keyValue");
    std::string value = configSubCmd->getValue("config_keyValue");

    std::vector<std::string> token;
    std::string delimiter = "=";
    eos::common::StringConversion::Tokenize(value, token, delimiter);

    if (token.size() != 2)
    {
      parsedCmd->printUsage();
      printConfigHelp();

      goto bailout;
    }

    in = "mgm.cmd=space&mgm.subcmd=config&mgm.space.name=";
    in += configSubCmd->getValue("space");
    in += "&mgm.space.key=";
    in += token[0];
    in += "&mgm.space.value=";
    in += token[1];
  }

  command = in.c_str();
  result = client_admin_command(command);

  if (!silent)
  {
    global_retc = output_result(result, highlighting);
  }
  else if (result)
  {
    global_retc = 0;
  }
  else
  {
    global_retc = EINVAL;
  }

 bailout:
  delete spaceCmd;

  return (0);
}
