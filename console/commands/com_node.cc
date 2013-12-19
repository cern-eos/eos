// ----------------------------------------------------------------------
// File: com_node.cc
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

/* Node listing, configuration, manipulation */
int
com_node (char* arg1)
{
  XrdOucString in = "";
  bool silent = false;
  bool highlighting = true;
  XrdOucEnv* result = 0;
  ConsoleCliCommand *parsedCmd, *nodeCmd, *lsSubCmd, *configSubCmd, *setSubCmd,
    *rmSubCmd, *registerSubCmd, *gwSubCmd, *statusSubCmd;

  nodeCmd = new ConsoleCliCommand("node", "node related functions");

  lsSubCmd = new ConsoleCliCommand("ls", "list all nodes or only <node>");
  CliOptionWithArgs geodepth({"geodepth", "aggregate group information along the instance topology tree up to geodepth", "-g,--geodepth=","<geodepth>",false});
  geodepth.addEvalFunction(optionIsIntegerEvalFunc, 0);
  geodepth.addEvalFunction(optionIsPositiveNumberEvalFunc, 0);
  lsSubCmd->addOption(geodepth);
  lsSubCmd->addGroupedOptions(std::vector<CliOption>
                              {{"monitor", "monitoring key=value output format",
                                "-m"},
                               {"long", "long output - list also file systems "
                                "after each node", "-l"},
                               {"io", "print IO statistics", "--io"},
                               {"sys", "print SYS statistics (memory + threads)",
                                "--sys"},
                               {"fsck", "print filesystem check statistics",
                                "--fsck"},
                              });
  lsSubCmd->addOption({"silent", "run in silent mode", "-s"});
  lsSubCmd->addOption({"node", "", 1, 1, "<node>"});
  nodeCmd->addSubcommand(lsSubCmd);

  configSubCmd = new ConsoleCliCommand("config", "configure file system "
				       "parameters for each filesystem of "
				       "this node");
  configSubCmd->addOptions(std::vector<CliPositionalOption>
                           {{"node", "", 1, 1, "<host:port>", true},
	                    {"key-value", "the key to set and its value, e.g.:\n"
			     "gw.rate=<mb/s> - set the transfer speed per "
			     "gateway transfer\n"
			     "gw.ntx=<#>     - set the number of concurrent "
			     "transfers for a gateway node\n"
			     "error.simulation=io_read|io_write|xs_read|xs_write\n"
			     "\tio_read  : simulate read  errors\n"
			     "\tio_write : simulate write errors\n"
			     "\txs_read  : simulate checksum errors when "
			     "reading a file\n"
			     "\txs_write : simulate checksum errors when "
			     "writing a file\n"
			     "<none>   : disable error simulation (every value "
			     "than the previous ones are fine!)\n"
			     "publish.interval=<sec> - set the filesystem "
			     "state publication interval to <sec> seconds\n"
			     "debug.level=<level> - set the node into debug "
			     "level <level> [default=notice] -> see debug "
			     "--help for available levels\n"
			     "for other keys see help of 'fs config' for details",
			     2, 1, "<key>=<value>", true}
                           });
  nodeCmd->addSubcommand(configSubCmd);

  setSubCmd = new ConsoleCliCommand("set", "activate/deactivate node");
  setSubCmd->addOption({"node", "", 1, 1, "<queue-name>|<host:port>", true});
  CliPositionalOption activeOption("active", "", 2, 1, "on|off", true);
  std::vector<std::string> choices = {"on", "off"};
  activeOption.addEvalFunction(optionIsChoiceEvalFunc, &choices);
  setSubCmd->addOption(activeOption);
  nodeCmd->addSubcommand(setSubCmd);

  rmSubCmd = new ConsoleCliCommand("rm", "remove a node");
  rmSubCmd->addOption({"node", "", 1, 1, "<queue-name>|<host:port>", true});
  nodeCmd->addSubcommand(rmSubCmd);

  registerSubCmd = new ConsoleCliCommand("register", "register filesystems on "
					 "node <host:port>");
  registerSubCmd->addOptions(std::vector<CliOption>
                             {{"force", "removes any existing filesystem label "
	                       "and re-registers", "--force,-f"},
	                      {"root", "allows to register paths on the root "
			       "partition", "--root"}
	                     });
  registerSubCmd->addOptions(std::vector<CliPositionalOption>
                             {{"node", "", 1, 1, "<queue-name>|<host:port>",
                               true},
                              {"path2reg", "used as a match for the filesystems "
                               "to register e.g. ""/data matches filesystems "
                               "/data01 /data02 etc. ... /data/ registers all "
                               "subdirectories in /data/", 2, 1,
                               "<path2register>", true},
                              {"space2reg", "formed as <space>:<n> where "
                               "<space> is the space name and <n> must be "
                               "equal to the number of filesystems which are "
                               "matched by <path2register> e.g. data:4 or "
                               "spare:22 ...", 3, 1, "<space2register>", true}
                             });
  nodeCmd->addSubcommand(registerSubCmd);

  gwSubCmd = new ConsoleCliCommand("gw", "enable (on) or disable (off) node as "
				   "a transfer gateway");
  gwSubCmd->addOption({"node", "", 1, 1, "<queue-name>|<host:port>", true});
  gwSubCmd->addOption(activeOption);
  nodeCmd->addSubcommand(gwSubCmd);

  statusSubCmd = new ConsoleCliCommand("status", "print's all defined "
				       "variables for a node");
  statusSubCmd->addOption({"node", "", 1, 1, "<queue-name>|<host:port>", true});
  nodeCmd->addSubcommand(statusSubCmd);

  addHelpOptionRecursively(nodeCmd);

  parsedCmd = nodeCmd->parse(arg1);

  if (checkHelpAndErrors(parsedCmd))
    goto bailout;

  if (parsedCmd == lsSubCmd)
  {
    in = "mgm.cmd=node&mgm.subcmd=ls";

    if (lsSubCmd->hasValue("monitor"))
    {
      in += "&mgm.outformat=m";
      highlighting = false;
    }

    if (lsSubCmd->hasValue("long"))
      in += "&mgm.outformat=l";

    if (lsSubCmd->hasValue("io"))
      in += "&mgm.outformat=io";

    if (lsSubCmd->hasValue("sys"))
      in += "&mgm.outformat=sys";

    if (lsSubCmd->hasValue("fsck"))
      in += "&mgm.outformat=fsck";

    silent = lsSubCmd->hasValue("silent");

    if (lsSubCmd->hasValue("node"))
    {
      in += "&mgm.selection=";
      in += lsSubCmd->getValue("node").c_str();
    }

    if (lsSubCmd->hasValue("geodepth"))
    {
      in += "&mgm.outdepth=";
      in += lsSubCmd->getValue("geodepth").c_str();
    }

  }
  else if (parsedCmd == setSubCmd)
  {
    in = "mgm.cmd=node&mgm.subcmd=set";
    in += "&mgm.node=";
    in += setSubCmd->getValue("node").c_str();
    in += "&mgm.node.state=";
    in += setSubCmd->getValue("active").c_str();
  }
  else if (parsedCmd == statusSubCmd)
  {
    in = "mgm.cmd=node&mgm.subcmd=status";
    in += "&mgm.node=";
    in += statusSubCmd->getValue("node").c_str();
  }
  else if (parsedCmd == gwSubCmd)
  {
    in = "mgm.cmd=node&mgm.subcmd=set";
    in += "&mgm.node=";
    in += gwSubCmd->getValue("node").c_str();
    in += "&mgm.node.txgw=";
    in += gwSubCmd->getValue("active").c_str();
  }
  else if (parsedCmd == rmSubCmd)
  {
    in = "mgm.cmd=node&mgm.subcmd=rm";
    in += "&mgm.node=";
    in += rmSubCmd->getValue("node").c_str();
  }
  else if (parsedCmd == configSubCmd)
  {
    XrdOucString nodename = configSubCmd->getValue("node").c_str();
    XrdOucString keyval = configSubCmd->getValue("key-value").c_str();

    if ((keyval.find("=")) == STR_NPOS)
    {
      // not like <key>=<val>
      parsedCmd->printUsage();
      goto bailout;
    }

    std::string is = keyval.c_str();
    std::vector<std::string> token;
    std::string delimiter = "=";
    eos::common::StringConversion::Tokenize(is, token, delimiter);

    if (token.size() != 2)
    {
      parsedCmd->printUsage();
      goto bailout;
    }

    XrdOucString in = "mgm.cmd=node&mgm.subcmd=config&mgm.node.name=";
    in += nodename;
    in += "&mgm.node.key=";
    in += token[0].c_str();
    in += "&mgm.node.value=";
    in += token[1].c_str();

    global_retc = output_result(client_admin_command(in));
    return (0);
  }
  else if (parsedCmd == registerSubCmd)
  {
    XrdOucString in = "mgm.cmd=node&mgm.subcmd=register&mgm.node.name=";
    in += registerSubCmd->getValue("node").c_str();
    in += "&mgm.node.path2register=";
    in += registerSubCmd->getValue("path2reg").c_str();
    in += "&mgm.node.space2register=";
    in += registerSubCmd->getValue("space2reg").c_str();

    if (registerSubCmd->hasValue("force"))
      in += "&mgm.node.force=true";

    if (registerSubCmd->hasValue("root"))
      in += "&mgm.node.root=true";

    global_retc = output_result(client_admin_command(in));
    return (0);
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
  delete nodeCmd;

  return (0);
}
