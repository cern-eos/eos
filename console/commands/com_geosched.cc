// ----------------------------------------------------------------------
// File: com_geosched.cc
// Author: Geoffray Adde - CERN
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
com_geosched (char* arg1)
{
  XrdOucString option = "";
  XrdOucString options = "";
  XrdOucString in = "";
  XrdOucString interval;
  std::set<std::string> supportedParam = {"skipSaturatedPlct","skipSaturatedAccess",
	"skipSaturatedDrnAccess","skipSaturatedBlcAccess",
	"skipSaturatedDrnPlct","skipSaturatedBlcPlct",
	"plctDlScorePenalty","plctUlScorePenalty",
	"accessDlScorePenalty","accessUlScorePenalty",
	"fillRatioLimit","fillRatioCompTol","saturationThres",
	"timeFrameDurationMs"};

  ConsoleCliCommand *parsedCmd, *gsCmd, *showSubCmd, *showTreeSubCmd, *showSnapshotSubCmd, *showStateSubCmd,
  *setSubCmd, *updaterSubCmd, *updtPauseSubCmd, *updtResumeSubCmd;

  gsCmd = new ConsoleCliCommand("geosched", "interact with the geoscheduling engine");

  showSubCmd = new ConsoleCliCommand("show", "show geosched state");showSubCmd = new ConsoleCliCommand("show", "show geosched internals");
  showTreeSubCmd = new ConsoleCliCommand("tree", "show current scheduling tree(s)");
  showTreeSubCmd->addOption({"group", "show the tree only for the specified <group>",
    1, 1, "<group>", false});
  showSnapshotSubCmd = new ConsoleCliCommand("snapshot", "show current scheduling snapshot(s)");
  showSnapshotSubCmd->addOption({"group", "show the snapshot(s) only for the specified <group>",
    1, 1, "<group>", false});
  showSnapshotSubCmd->addOption({"optype", "show the snapshot(s) only for the specified operation type <optype> {plct,accsro,accsrw,accsdrain,plctdrain,accsblc,plctblc}",
    2, 1, "<optype>", false});
  showStateSubCmd = new ConsoleCliCommand("state", "list scheduling engine parameters and groups");
  showSubCmd->addSubcommand(showTreeSubCmd);
  showSubCmd->addSubcommand(showSnapshotSubCmd);
  showSubCmd->addSubcommand(showStateSubCmd);

  setSubCmd = new ConsoleCliCommand("set", "set parameters of the geosched");
  setSubCmd->addOptions( std::vector<CliPositionalOption>{
    {"parameter", "name of the parameter (all names can be listed with geosched show ls)",1, 1, "<parameter>", true},
    {"value", "value of the parameter",2, 1, "<value>", true}
  });

  updaterSubCmd = new ConsoleCliCommand("updater", "interact with the scheduling engine state updater");
  updtPauseSubCmd = new ConsoleCliCommand("pause", "pause scheduling engine state updater");
  updtResumeSubCmd = new ConsoleCliCommand("resume", "resume scheduling engine state updater");
  updaterSubCmd->addSubcommand(updtPauseSubCmd);
  updaterSubCmd->addSubcommand(updtResumeSubCmd);

  gsCmd->addSubcommand(showSubCmd);
  gsCmd->addSubcommand(setSubCmd);
  gsCmd->addSubcommand(updaterSubCmd);

  addHelpOptionRecursively(gsCmd);

  parsedCmd = gsCmd->parse(arg1);

  if (parsedCmd == gsCmd && !gsCmd->hasValues())
  {
    gsCmd->printUsage();
    goto bailout;
  }

  if (checkHelpAndErrors(parsedCmd))
    goto bailout;

  in = "mgm.cmd=geosched";

  if (parsedCmd == showStateSubCmd)
  {
    in += "&mgm.subcmd=showstate";
  }

  if (parsedCmd == showTreeSubCmd)
  {
    in += "&mgm.subcmd=showtree";
    in += "&mgm.schedgroup=";
    if(parsedCmd->hasValue("group"))
      in += parsedCmd->getValue("group").c_str();
  }

  if (parsedCmd == showSnapshotSubCmd)
  {
    in += "&mgm.subcmd=showsnapshot";
    in += "&mgm.schedgroup=";
    if(parsedCmd->hasValue("group"))
      in += parsedCmd->getValue("group").c_str();
    in += "&mgm.optype=";
    if(parsedCmd->hasValue("optype"))
      in += parsedCmd->getValue("optype").c_str();
  }

  if(parsedCmd == setSubCmd)
  {
    if(supportedParam.find(parsedCmd->getValue("parameter"))==supportedParam.end())
    {
      fprintf(stderr, "Error: parameter %s not supported\n",parsedCmd->getValue("parameter").c_str());
      goto bailout;
    }

    if(!XrdOucString(parsedCmd->getValue("value").c_str()).isdigit())
    {
      fprintf(stderr, "Error: parameter %s should have a numeric value, %s was provided\n",
	      parsedCmd->getValue("parameter").c_str(),parsedCmd->getValue("value").c_str());
      goto bailout;
    }

    in += "&mgm.subcmd=set";
    in += "&mgm.param=";
    in += parsedCmd->getValue("parameter").c_str();
    in += "&mgm.value=";
    in += parsedCmd->getValue("value").c_str();
  }

  if(parsedCmd == updtPauseSubCmd)
  {
    in += "&mgm.subcmd=updtpause";
  }

  if(parsedCmd == updtResumeSubCmd)
  {
    in += "&mgm.subcmd=updtresume";
  }

  global_retc = output_result(client_admin_command(in));

bailout:
  delete gsCmd;

  return (0);
}
