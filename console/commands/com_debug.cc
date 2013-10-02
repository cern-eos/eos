// ----------------------------------------------------------------------
// File: com_debug.cc
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

/* Debug Level Setting */
int
com_debug (char* arg1)
{
  XrdOucString in;
  std::string level;

  CliOption helpOption("help", "print help", "-h,--help");
  helpOption.setHidden(true);

  ConsoleCliCommand debugCmd("debug", "allows to modify the verbosity of the "
                             "EOS log files in MGM and FST services");
  debugCmd.addOption(helpOption);
  debugCmd.addOption({"filter", "a comma seperated list of strings of software "
                      "units which should be filtered out in the message log;\n"
                      "The default filter list is 'Process,AddQuota,UpdateHint,"
                      "UpdateQuotaStatus,SetConfigValue,Deletion,GetQuota,"
                      "PrintOut,RegisterNode,SharedHash'", "--filter=,-f",
                      1, "<unit-list>", false});

  CliPositionalOption levelOption("level", "the debug level to set;\n"
                                  "The allowed values are: debug, info, warning, "
                                  "notice, err, crit, alert, emerg or this\n"
                                  "The level 'this' will toggle the shell's "
                                  "debug mode", 1, 1, "<level>", true);
  std::vector<std::string> choices = {"debug", "info", "warning", "notice",
                                      "err", "crit", "alert", "emerg", "this"};
  levelOption.addEvalFunction(optionIsChoiceEvalFunc, &choices);
  debugCmd.addOption(levelOption);
  debugCmd.addOption({"node-queue", "the <node-queue> to which the debug "
                      "level should be set.\n<node-queue> are internal EOS "
                      "names e.g. '/eos/<hostname>:<port>/fst",
                      2, 1, "<node-queue>", false});

  debugCmd.parse(arg1);

  if (debugCmd.hasValue("help"))
  {
    debugCmd.printUsage();
    goto com_debugexamples;
  }
  else if (debugCmd.hasErrors())
  {
    debugCmd.printErrors();
    debugCmd.printUsage();
    return 0;
  }

  level = debugCmd.getValue("level");
  if (level == "this")
  {
    fprintf(stdout, "info: toggling shell debugmode to debug=%d\n", debug);
    debug = !debug;

    if (debug)
      eos::common::Logging::SetLogPriority(LOG_DEBUG);
    else
      eos::common::Logging::SetLogPriority(LOG_NOTICE);

    return (0);
  }

  in = "mgm.cmd=debug&mgm.debuglevel=";
  in += level.c_str();

  if (debugCmd.hasValue("filter"))
  {
    in += "&mgm.filter=";
    in += debugCmd.getValue("filter").c_str();
  }

  if (debugCmd.hasValue("node-queue"))
  {
    in += "&mgm.nodename=";
    in += debugCmd.getValue("node-queue").c_str();
  }

  global_retc = output_result(client_admin_command(in));

  return (0);

 com_debugexamples:
  fprintf(stdout, "Examples:\n");
  fprintf(stdout, "  debug info *                        set MGM & all FSTs into debug mode 'info'\n\n");
  fprintf(stdout, "  debug err /eos/*/fst                set all FSTs into debug mode 'info'\n\n");
  fprintf(stdout, "  debug crit /eos/*/mgm               set MGM into debug mode 'crit'\n\n");
  fprintf(stdout, "  debug debug -filter MgmOfsMessage   set MGM into debug mode 'debug' and filter only messages comming from unit 'MgmOfsMessage'.\n\n");

  return (0);
}
