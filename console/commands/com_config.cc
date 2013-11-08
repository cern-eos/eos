// ----------------------------------------------------------------------
// File: com_config.cc
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

/* Configuration System listing, configuration, manipulation */
int
com_config (char* arg1)
{
  XrdOucString in;
  ConsoleCliCommand *configCmd, *parsedCmd, *lsSubCmd, *dumpSubCmd, *saveSubCmd,
    *loadSubCmd, *resetSubCmd, *diffSubCmd, *changelogSubCmd, *autosaveSubCmd;

  configCmd = new ConsoleCliCommand("config", "provides the configuration "
                                    "interface to EOS");
  lsSubCmd = new ConsoleCliCommand("ls", "list existing configurations");
  lsSubCmd->addOption({"backup", "show also backup & autosave files",
                       "-b,--backup"});
  configCmd->addSubcommand(lsSubCmd);

  dumpSubCmd = new ConsoleCliCommand("dump", "dump current configuration or "
                                     "configuration with name <name>");
  dumpSubCmd->addOptions(std::vector<CliOption>
                         {{"fs", "dump only file system config", "-f,--fs"},
                          {"vid", "dump only virtual id config", "-v,--vid"},
                          {"quota", "dump only quota config", "-q,--quota"},
                          {"comment", "dump only comment config", "-c,--comment"},
                          {"policy", "dump only policy config", "-p,--policy"},
                          {"global", "dump only global config", "-g,--global"},
                          {"access", "dump only access config", "-a,--access"},
                          {"mapping", "dump only mapping config", "-m,--mapping"}
                         });
  dumpSubCmd->addOption({"name", "", 1, 1, "<name>", false});
  configCmd->addSubcommand(dumpSubCmd);

  saveSubCmd = new ConsoleCliCommand("save", "save config (optionally under "
                                     "<name>)");
  saveSubCmd->addOption({"force", "overwrite existing config name and create "
                         "a timestamped backup", "-f,--force"});
  saveSubCmd->addOption({"name", "the name for the configuration file;\n"
                         "IMPORTANT: if no name is specified the current "
                         "config file is overwritten", 1, 1, "<name>", false});
  saveSubCmd->addOption({"comment", "an optional comment about this "
                         "configuration", "-c,--comment=", 1, "<comment>", false});
  configCmd->addSubcommand(saveSubCmd);

  loadSubCmd = new ConsoleCliCommand("load", "load configuration");
  loadSubCmd->addOption({"name", "name of the configuration file",
                         1, 1, "<name>", true});
  configCmd->addSubcommand(loadSubCmd);

  resetSubCmd = new ConsoleCliCommand("reset", "reset all configuration to "
                                      "empty state");
  configCmd->addSubcommand(resetSubCmd);

  diffSubCmd = new ConsoleCliCommand("diff", "show changes since last "
                                     "load/save operation");
  configCmd->addSubcommand(diffSubCmd);

  changelogSubCmd = new ConsoleCliCommand("changelog", "show the last <#> "
                                          "lines from the changelog - default "
                                          "is -10");
  CliPositionalOption nrLines("nr-lines", "", 1, 1, "-#lines", false);
  std::pair<float, float> range = {-100.0, 100.0};
  nrLines.addEvalFunction(optionIsNumberInRangeEvalFunc, &range);
  changelogSubCmd->addOption(nrLines);
  configCmd->addSubcommand(changelogSubCmd);

  autosaveSubCmd = new ConsoleCliCommand("autosave", "without on/off just "
                                         "prints the state otherwise set's "
                                         "autosave to on or off");
  CliPositionalOption activeOption("active", "", 1, 1, "on|off", true);
  std::vector<std::string> choices = {"on", "off"};
  activeOption.addEvalFunction(optionIsChoiceEvalFunc, &choices);
  autosaveSubCmd->addOption(activeOption);
  configCmd->addSubcommand(autosaveSubCmd);

  addHelpOptionRecursively(configCmd);

  parsedCmd = configCmd->parse(arg1);

  if (checkHelpAndErrors(parsedCmd))
    goto bailout;

  if (parsedCmd == dumpSubCmd)
  {
    in = "mgm.cmd=config&mgm.subcmd=dump";

    if (dumpSubCmd->hasValue("fs"))
      in += "&mgm.config.fs=1";

    if (dumpSubCmd->hasValue("vid"))
      in += "&mgm.config.vid=1";

    if (dumpSubCmd->hasValue("quota"))
      in += "&mgm.config.quota=1";

    if (dumpSubCmd->hasValue("comment"))
      in += "&mgm.config.comment=1";

    if (dumpSubCmd->hasValue("policy"))
      in += "&mgm.config.policy=1";

    if (dumpSubCmd->hasValue("global"))
      in += "&mgm.config.global=1";

    if (dumpSubCmd->hasValue("mapping"))
      in += "&mgm.config.map=1";

    if (dumpSubCmd->hasValue("access"))
      in += "mgm.config.access=1";

    if (dumpSubCmd->hasValue("name"))
    {
      in += "&mgm.config.file=";
      in += dumpSubCmd->getValue("name").c_str();
    }
  }
  else if (parsedCmd == lsSubCmd)
  {
    in = "mgm.cmd=config&mgm.subcmd=ls";

    if (lsSubCmd->hasValue("backup"))
      in += "&mgm.config.showbackup=1";
  }
  else if (parsedCmd == loadSubCmd)
  {
    in = "mgm.cmd=config&mgm.subcmd=load&mgm.config.file=";
    in += loadSubCmd->getValue("name").c_str();
  }
  else if (parsedCmd == autosaveSubCmd)
  {
    in = "mgm.cmd=config&mgm.subcmd=autosave&mgm.config.state=";
    in += autosaveSubCmd->getValue("active").c_str();
  }
  else if (parsedCmd == resetSubCmd)
  {
    in = "mgm.cmd=config&mgm.subcmd=reset";
  }
  else if (parsedCmd == saveSubCmd)
  {
    in = "mgm.cmd=config&mgm.subcmd=save";

    if (saveSubCmd->hasValue("force"))
      in += "&mgm.config.force=1";

    if (saveSubCmd->hasValue("comment"))
    {
      in += "&mgm.config.comment=";
      in += saveSubCmd->getValue("comment").c_str();
    }

    if (saveSubCmd->hasValue("name"))
    {
      in += "&mgm.config.file=";
      in += saveSubCmd->getValue("name").c_str();
    }
  }
  else if (parsedCmd == diffSubCmd)
  {
    in = "mgm.cmd=config&mgm.subcmd=diff";
  }
  else if (parsedCmd == changelogSubCmd)
  {
    in = "mgm.cmd=config&mgm.subcmd=changelog";

    if (changelogSubCmd->hasValue("nr-lines"))
    {
      std::string nrLines = changelogSubCmd->getValue("nr-lines");
      in += "&mgm.config.lines=";

      if (nrLines[0] == '-')
        nrLines.erase(0, 1);
      in += nrLines.c_str();
    }
  }

  global_retc = output_result(client_admin_command(in));

 bailout:
  delete configCmd;

  return (0);
}
