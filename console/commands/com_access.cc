// ----------------------------------------------------------------------
// File: com_access.cc
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

/* access (deny/bounce/redirect) -  Interface */
int
com_access (char* arg1)
{
  ConsoleCliCommand *parsedCmd, *accessCmd, *banSubCmd, *unbanSubCmd, *lsSubCmd,
    *allowSubCmd, *disallowSubCmd, *setSubCmd, *rmSubCmd, *setRedirectSubCmd,
    *setStallSubCmd, *setLimitSubCmd, *rmRedirectSubCmd, *rmStallSubCmd,
    *rmLimitSubCmd;
  XrdOucString in = "";
  in = "mgm.cmd=access";

  accessCmd = new ConsoleCliCommand("access", "provides the access interface "
                                    "of EOS to allow/disallow hosts and/or "
                                    "users");

  CliPositionalOption idOption("id", "can be a user name, user id, group name, "
                               "group id, hostname or IP", 1, 1,
                               "<identifier>", true);
  banSubCmd = new ConsoleCliCommand("ban", "ban user,group or host with "
                                    "identifier <identifier>");
  banSubCmd->addGroupedOptions(std::vector<CliOption>
                               {{"user", "", "user"},
                                {"group", "", "group"},
                                {"host", "", "host"}
                               })->setRequired(true);
  banSubCmd->addOption(idOption);
  accessCmd->addSubcommand(banSubCmd);

  unbanSubCmd = new ConsoleCliCommand(*banSubCmd);
  unbanSubCmd->setName("unban");
  unbanSubCmd->setDescription("unban user,group or host with "
                              "identifier <identifier>");

  allowSubCmd = new ConsoleCliCommand(*banSubCmd);
  allowSubCmd->setName("allow");
  allowSubCmd->setDescription("allow user,group or host with "
                              "identifier <identifier>");
  CliOption *idOptionPtr = allowSubCmd->getOption("id");
  idOptionPtr->setDescription(std::string(idOptionPtr->description()) +
                              "\nHINT:  if you add any 'allow' the instance "
                              "allows only the listed users;\nA banned "
                              "identifier will still overrule an allowed "
                              "identifier!");

  disallowSubCmd = new ConsoleCliCommand(*banSubCmd);
  disallowSubCmd->setName("disallow");
  disallowSubCmd->setDescription("disallow user,group "
                                 "or host with identifier <identifier>");

  lsSubCmd = new ConsoleCliCommand("ls", "print banned,unbanned user, group, "
                                   "hosts");
  lsSubCmd->addOptions(std::vector<CliOption>
                       {{"monitor", "output in monitoring format with "
                         "<key>=<value>", "-m"},
                        {"numerical", "don't translate uid/gids to names", "-n"}
                       });
  accessCmd->addSubcommand(lsSubCmd);

  setSubCmd = new ConsoleCliCommand("set", "");
  accessCmd->addSubcommand(setSubCmd);

  setRedirectSubCmd = new ConsoleCliCommand("redirect", "allows to set a global "
                                            "redirection to <target-host>");
  setRedirectSubCmd->addGroupedOptions(std::vector<CliOption>
                                       {{"read", "set stall time for read "
                                         "requests", "-r,--read"},
                                        {"write", "set stall time for write "
                                         "requests", "-w,--write"},
                                        {"ENOENT", "set a redirect if a file "
                                         "does not exist", "--ENOENT"},
                                        {"ENONET", "set a redirect if a file "
                                         "is offline", "--ENONET"}
                                       });
  setRedirectSubCmd->addOption({"target", "hostname to which all requests "
                                "get redirected", 1, 1, "<target-host>", true});
  setSubCmd->addSubcommand(setRedirectSubCmd);

  setStallSubCmd = new ConsoleCliCommand("stall", "allows to set a global "
                                         "stall time");
  setStallSubCmd->addGroupedOptions(std::vector<CliOption>
                                    {{"read", "set stall time for read "
                                      "requests", "-r,--read"},
                                     {"write", "set stall time for write "
                                      "requests", "-w,--write"},
                                     {"ENOENT", "set a stall if a file "
                                      "does not exist", "--ENOENT"},
                                     {"ENONET", "set a stall if a file "
                                      "is offline", "--ENONET"}
                                    });
  setStallSubCmd->addOption({"target", "time in seconds after which clients "
                             "should rebounce", 1, 1, "<stall-time>", true});
  setSubCmd->addSubcommand(setStallSubCmd);

  setLimitSubCmd = new ConsoleCliCommand("limit", "stall the defined user "
                                         "group for 5s if the <counter> "
                                         "exceeds a frequency of <frequency> "
                                         "in a 5s interval");
  setLimitSubCmd->addOptions(std::vector<CliPositionalOption>
                             {{"frequency", "", 1, 1, "<frequency>", true},
                              {"rate", "the instantanious rate can exceed this "
                               "value by 33%;\nrate:user:*:<counter> : apply "
                               "to all users based on user counter;\n"
                               "rate:group:*:<counter>: apply to all groups "
                               "based on group counter", 2, 1,
                               "rate:{user,group}:{name}:<counter>", true}
                             });
  setSubCmd->addSubcommand(setLimitSubCmd);

  rmSubCmd = new ConsoleCliCommand("rm", "");
  accessCmd->addSubcommand(rmSubCmd);

  rmRedirectSubCmd = new ConsoleCliCommand("redirect", "removes global "
                                           "redirection");
  rmSubCmd->addSubcommand(rmRedirectSubCmd);

  rmStallSubCmd = new ConsoleCliCommand("stall", "removes global stall time");
  rmStallSubCmd->addGroupedOptions(std::vector<CliOption>
                                   {{"read", "remove stall time for read "
                                      "requests", "-r,--read"},
                                     {"write", "remove stall time for write "
                                      "requests", "-w,--write"},
                                     {"ENOENT", "remove stall if a file "
                                      "does not exist", "--ENOENT"},
                                     {"ENONET", "remove stall if a file "
                                      "is offline", "--ENONET"}
                                    });
  rmSubCmd->addSubcommand(rmStallSubCmd);

  rmLimitSubCmd = new ConsoleCliCommand("limit", "remove rate limitation");
  rmLimitSubCmd->addOption({"rate", "", 1, 1,
                            "rate:{user,group}:{name}:<counter>", true});
  rmSubCmd->addSubcommand(rmLimitSubCmd);

  addHelpOptionRecursively(accessCmd);

  parsedCmd = accessCmd->parse(arg1);

  if (parsedCmd == accessCmd || parsedCmd == setSubCmd || parsedCmd == rmSubCmd)
  {
    if (!checkHelpAndErrors(parsedCmd))
      parsedCmd->printUsage();
    goto com_access_examples;
  }
  if (checkHelpAndErrors(parsedCmd))
    goto com_access_examples;

  if (parsedCmd == banSubCmd)
    in += "&mgm.subcmd=ban";
  else if (parsedCmd == unbanSubCmd)
    in += "&mgm.subcmd=unban";
  else if (parsedCmd == allowSubCmd)
    in += "&mgm.subcmd=allow";
  else if (parsedCmd == disallowSubCmd)
    in += "&mgm.subcmd=unallow";
  else if (parsedCmd == lsSubCmd)
    in += "&mgm.subcmd=ls";
  else if (parsedCmd->parent() == setSubCmd)
    in += "&mgm.subcmd=set";
  else if (parsedCmd->parent() == rmSubCmd)
    in += "&mgm.subcmd=rm";

  if (parsedCmd == lsSubCmd)
  {
    in += "&mgm.subcmd=ls";
    XrdOucString option("");

    if (lsSubCmd->hasValue("monitor"))
      option += "m";
    if (lsSubCmd->hasValue("numerical"))
      option += "n";

    if (option != "")
    {
      in += "&mgm.access.option=";
      in += option;
    }
  }
  else if (parsedCmd == banSubCmd || parsedCmd == unbanSubCmd ||
           parsedCmd == allowSubCmd || parsedCmd == disallowSubCmd)
  {
    if (parsedCmd->hasValue("host"))
      in += "&mgm.access.host=";
    if (parsedCmd->hasValue("user"))
      in += "&mgm.access.user=";
    if (parsedCmd->hasValue("group"))
      in += "&mgm.access.group=";

    in += parsedCmd->getValue("id").c_str();
  }
  else if (parsedCmd == setLimitSubCmd)
  {
    in += "&mgm.access.stall=";
    in += parsedCmd->getValue("frequency").c_str();

    XrdOucString rate = parsedCmd->getValue("rate").c_str();
    if ((rate.beginswith("rate:user:") || rate.beginswith("rate:group:")) &&
        rate.find(":", 11) != STR_NPOS)
    {
      in += "&mgm.access.type=" + rate;
    }
  }
  else
  {
    if (parsedCmd == setRedirectSubCmd || parsedCmd == rmRedirectSubCmd)
      in += "&mgm.access.redirect=";
    if (parsedCmd == setStallSubCmd || parsedCmd == rmStallSubCmd)
      in += "&mgm.access.stall=";

    if (parsedCmd == setRedirectSubCmd || parsedCmd == setStallSubCmd)
      in += parsedCmd->getValue("target").c_str();
    else
      in += "dummy";

    if (parsedCmd->hasValue("read"))
      in += "&mgm.access.type=r";
    else if (parsedCmd->hasValue("write"))
      in += "&mgm.access.type=w";
    else if (parsedCmd->hasValue("ENOENT"))
      in += "&mgm.access.type=ENOENT";
    else if (parsedCmd->hasValue("ENONET"))
      in += "&mgm.access.type=ENONET";
  }

  global_retc = output_result(client_admin_command(in));
  goto bailout;

com_access_examples:
  fprintf(stdout, "\nExamples:\n");
  fprintf(stdout, "  access ban foo           Ban host foo\n");
  fprintf(stdout, "  access set redirect foo  Redirect all requests to host foo\n");
  fprintf(stdout, "  access rm redirect       Remove redirection to previously defined host foo\n");
  fprintf(stdout, "  access set stall 60      Stall all clients by 60 seconds\n");
  fprintf(stdout, "  access ls                Print all defined access rules\n");
  fprintf(stdout, "  access set limit 100  rate:user:*:OpenRead      Limit the rate of open for read to a frequency of 100 Hz for all users\n");
  fprintf(stdout, "  access set limit 2000 rate:group:zp:Stat        Limit the stat rate for the zp group to 2kHz\n");
  fprintf(stdout, "  access rm limit rate:user:*:OpenRead            Removes the defined limit\n");

 bailout:
  delete accessCmd;

  return (0);
}
