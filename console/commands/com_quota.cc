// ----------------------------------------------------------------------
// File: com_quota.cc
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

/* Quota System listing, configuration, manipulation */
int
com_quota (char* arg1)
{
  XrdOucString in("");
  bool highlighting = true;
  ConsoleCliCommand *parsedCmd, *quotaCmd, *lsSubCmd, *setSubCmd, *rmSubCmd,
    *rmnodeSubCmd;
  CliOption monitorOption("monitor", "print information in monitoring "
			  "<key>=<value> format", "-m");

  quotaCmd = new ConsoleCliCommand("quota", "show personal quota for all or "
				   "only the quota node responsible for "
				   "<path>");
  quotaCmd->addOption(monitorOption);
  quotaCmd->addOption({"path", "", 1, 1, "<path>"});

  lsSubCmd = new ConsoleCliCommand("ls", "list configured quota and quota "
                                   "node(s)");
  lsSubCmd->addOption(monitorOption);
  lsSubCmd->addOption({"numerical", "don't translate ids, print uid+gid "
                       "number", "-n"});
  lsSubCmd->addOptions({{"uid", "print information only for uid <uid>",
                         "-u,--uid", "<uid>", false},
                        {"gid", "print information only for gid <gid>",
                         "-g,--gid", "<gid>", false}
                       });
  lsSubCmd->addOption({"path", "print information only for path <path>",
                       1, 1, "<path>"});
  quotaCmd->addSubcommand(lsSubCmd);

  rmSubCmd = new ConsoleCliCommand("rm", "list configured quota and quota "
                                   "node(s)");
  rmSubCmd->addGroupedOptions({{"uid", "", "-u,--uid", "<uid>", false},
                               {"gid", "", "-g,--gid", "<gid>", false}
                              });
  rmSubCmd->addOption({"path", "", 1, 1, "<path>", true});
  quotaCmd->addSubcommand(rmSubCmd);

  setSubCmd = new ConsoleCliCommand("set", "set volume and/or inode quota by "
                                    "uid or gid");
  setSubCmd->addGroupedOptions({{"uid", "", "-u,--uid", "<uid>", false},
                                {"gid", "", "-g,--gid", "<gid>", false}
                               });
  setSubCmd->addOptions({{"volume", "set the volume limit to <bytes>",
                          "-v,--volume", "<bytes>", false},
                         {"inodes", "set the inodes limit to <inodes>",
                          "-i,--inodes", "<inodes>", false}
                        });
  setSubCmd->addOption({"path", "if omitted, 'default' is used instead",
                        1, 1, "<path>", false});
  quotaCmd->addSubcommand(setSubCmd);

  rmnodeSubCmd = new ConsoleCliCommand("rmnode", "remove quota node and every "
                                       "defined quota on that node");
  rmnodeSubCmd->addOption({"path", "", 1, 1, "<path>"});
  quotaCmd->addSubcommand(rmnodeSubCmd);

  quotaCmd->setStandalone(true);

  addHelpOptionRecursively(quotaCmd);

  parsedCmd = quotaCmd->parse(arg1);

  if (checkHelpAndErrors(parsedCmd))
    goto bailout;

  if (parsedCmd == quotaCmd)
  {
    in = "mgm.cmd=quota&mgm.subcmd=lsuser";

    if (quotaCmd->hasValue("path"))
    {
      in += "&mgm.quota.space=";
      in += quotaCmd->getValue("path").c_str();
    }

    if (quotaCmd->hasValue("monitor"))
      in += "&mgm.quota.format=m";
  }
  else if (parsedCmd == lsSubCmd)
  {
    in = "mgm.cmd=quota&mgm.subcmd=ls";

    if (lsSubCmd->hasValue("uid"))
    {
      in += "&mgm.quota.uid=";
      in += lsSubCmd->getValue("uid").c_str();
    }

    if (lsSubCmd->hasValue("gid"))
    {
      in += "&mgm.quota.gid=";
      in += lsSubCmd->getValue("gid").c_str();
    }

    if (lsSubCmd->hasValue("path"))
    {
      in += "&mgm.quota.space=";
      in += lsSubCmd->getValue("path").c_str();
    }

    if (lsSubCmd->hasValue("numerical"))
      in += "&mgm.quota.printid=n";

    if (lsSubCmd->hasValue("monitor"))
    {
      in += "&mgm.quota.format=m";
      highlighting = false;
    }
  }
  else if (parsedCmd == setSubCmd)
  {
    in = "mgm.cmd=quota&mgm.subcmd=set";
    XrdOucString space = "default";

    if (setSubCmd->hasValue("uid"))
    {
      in += "&mgm.quota.uid=";
      in += setSubCmd->getValue("uid").c_str();
    }
    else if (setSubCmd->hasValue("gid"))
    {
      in += "&mgm.quota.gid=";
      in += setSubCmd->getValue("gid").c_str();
    }

    if (setSubCmd->hasValue("volume"))
    {
        in += "&mgm.quota.maxbytes=";
        in += setSubCmd->getValue("volume").c_str();
    }

    if (setSubCmd->hasValue("inodes"))
    {
        in += "&mgm.quota.maxinodes=";
        in += setSubCmd->getValue("inodes").c_str();
    }

    if (setSubCmd->hasValue("path"))
    {
      in += "&mgm.quota.space=";
      in += setSubCmd->getValue("path").c_str();
    }
  }
  else if (parsedCmd == rmSubCmd)
  {
    in = "mgm.cmd=quota&mgm.subcmd=rm";

    if (rmSubCmd->hasValue("uid"))
    {
      in += "&mgm.quota.uid=";
      in += rmSubCmd->getValue("uid").c_str();
    }
    else if (rmSubCmd->hasValue("gid"))
    {
      in += "&mgm.quota.gid=";
      in += rmSubCmd->getValue("gid").c_str();
    }

    in += "&mgm.quota.space=";
    in += rmSubCmd->getValue("path").c_str();
  }
  else if (parsedCmd == rmnodeSubCmd)
  {
    in = "mgm.cmd=quota&mgm.subcmd=rmnode";
    XrdOucString space = rmnodeSubCmd->getValue("path").c_str();

    in += "&mgm.quota.space=" + space;

    string s;
    fprintf(stdout, "Do you really want to delete the quota node under path %s ?\n", space.c_str());
    fprintf(stdout, "Confirm the deletion by typing => ");
    XrdOucString confirmation = "";
    for (int i = 0; i < 10; i++)
    {
      confirmation += (int) (9.0 * rand() / RAND_MAX);
    }

    fprintf(stdout, "%s\n", confirmation.c_str());
    fprintf(stdout, "                               => ");
    getline(std::cin, s);
    std::string sconfirmation = confirmation.c_str();
    if (s == sconfirmation)
    {
      fprintf(stdout, "\nDeletion confirmed\n");
      global_retc = output_result(client_admin_command(in));
    }
    else
    {
      fprintf(stdout, "\nDeletion aborted!\n");
      global_retc = -1;
    }

    goto bailout;
  }

  global_retc = output_result(client_user_command(in), highlighting);

 bailout:
  delete quotaCmd;

  return (0);
}
