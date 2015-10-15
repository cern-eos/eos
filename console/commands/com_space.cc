// ----------------------------------------------------------------------
// File: com_space.cc
// Author: Andreas-Joachim Peters - CERN
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

#include <streambuf>
#include <string>
#include <cerrno>

using namespace eos::common;

/* Space listing, configuration, manipulation */
int
com_space (char* arg1)
{
  XrdOucString in = "";
  bool silent = false;
  bool printusage = false;
  bool highlighting = true;
  XrdOucString option = "";
  XrdOucEnv* result = 0;
  bool ok = false;
  bool sel = false;
  // split subcommands
  eos::common::StringTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString subcommand = subtokenizer.GetToken();

  if (wants_help(arg1))
    goto com_space_usage;

  if (subcommand == "ls")
  {
    in = "mgm.cmd=space&mgm.subcmd=ls";
    option = "";

    do
    {
      ok = false;
      subtokenizer.GetLine();
      option = subtokenizer.GetToken();
      if (option.length())
      {
        if (option == "-m")
        {
          in += "&mgm.outformat=m";
          ok = true;
          highlighting = false;
        }
        if (option == "-l")
        {
          in += "&mgm.outformat=l";
          ok = true;
        }
        if (option == "--io")
        {
          in += "&mgm.outformat=io";
          ok = true;
        }
        if (option == "--fsck")
        {
          in += "&mgm.outformat=fsck";
          ok = true;
        }
        if (option == "-s")
        {
          silent = true;
          ok = true;
        }
        if (!option.beginswith("-"))
        {
          in += "&mgm.selection=";
          in += option;
          if (!sel)
            ok = true;
          sel = true;
        }

        if (!ok)
          printusage = true;
      }
      else
      {
        ok = true;
      }
    }
    while (option.length());
  }

  if (subcommand == "reset")
  {
    in = "mgm.cmd=space&mgm.subcmd=reset";
    XrdOucString spacename = subtokenizer.GetToken();
    XrdOucString option = subtokenizer.GetToken();
    while (option.replace("-", ""))
    {
    }

    if (!spacename.length())
      printusage = true;

    if (option.length() &&
        (option != "egroup") &&
        (option != "drain") &&
        (option != "scheduledrain") &&
        (option != "schedulebalance"))
      printusage = true;

    in += "&mgm.space=";
    in += spacename;
    if (option.length())
    {
      in += "&mgm.option=";
      in += option;
    }

    ok = true;
  }

  if (subcommand == "define")
  {
    in = "mgm.cmd=space&mgm.subcmd=define";
    XrdOucString nodename = subtokenizer.GetToken();
    XrdOucString groupsize = subtokenizer.GetToken();
    XrdOucString groupmod = subtokenizer.GetToken();

    if (groupsize == "")
    {
      groupsize = "0";
    }

    if (groupmod == "")
    {
      groupmod = 24;
    }

    if (!nodename.length())
      printusage = true;

    in += "&mgm.space=";
    in += nodename;
    in += "&mgm.space.groupsize=";
    in += groupsize;
    in += "&mgm.space.groupmod=";
    in += groupmod;
    ok = true;
  }

  if (subcommand == "set")
  {
    in = "mgm.cmd=space&mgm.subcmd=set";
    XrdOucString nodename = subtokenizer.GetToken();
    XrdOucString active = subtokenizer.GetToken();

    if ((!nodename.length()) || (!active.length()))
      printusage = true;

    if ((active != "on") && (active != "off"))
    {
      printusage = true;
    }

    in += "&mgm.space=";
    in += nodename;
    in += "&mgm.space.state=";
    in += active;
    ok = true;
  }

  if (subcommand == "rm")
  {
    in = "mgm.cmd=space&mgm.subcmd=rm";
    XrdOucString spacename = subtokenizer.GetToken();

    if (!spacename.length())
      printusage = true;
    in += "&mgm.space=";
    in += spacename;
    ok = true;
  }


  if (subcommand == "status")
  {
    in = "mgm.cmd=space&mgm.subcmd=status";
    XrdOucString spacename = subtokenizer.GetToken();

    if (!spacename.length())
      printusage = true;
    in += "&mgm.space=";
    in += spacename;
    ok = true;

    std::string contents = eos::common::StringConversion::StringFromShellCmd("cat /var/eos/md/stacktrace");
  }

  if (subcommand == "node-set")
  {
    in = "mgm.cmd=space&mgm.subcmd=node-set";

    XrdOucString spacename = subtokenizer.GetToken();
    XrdOucString key = subtokenizer.GetToken();
    XrdOucString file = subtokenizer.GetToken();
    std::string val;

    if (!spacename.length())
      printusage = true;

    if (!key.length())
      printusage = true;

    if (!file.length())
      printusage = true;

    in += "&mgm.space=";
    in += spacename;

    in += "&mgm.space.node-set.key=";
    in += key;
    in += "&mgm.space.node-set.val=";

    if (file.length())
    {
      if (file.beginswith("/"))
      {
	std::ifstream ifs(file.c_str(), std::ios::in | std::ios::binary);
	if (!ifs)
	{
	  fprintf(stderr, "error: unable to read %s - errno=%d\n", file.c_str(), errno);
	  global_retc = errno;
	  return (0);
	}
	
	val = std::string((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());
	if (val.length()>512)
	{
	  fprintf(stderr, "error: the file contents exceeds 0.5 kB - configure a file hosted on the MGM using file:<mgm-path>\n");
	  global_retc = EINVAL;
	  return (0);
	}
	// store the value b64 encoded
	XrdOucString val64;
	
	eos::common::SymKey::Base64Encode((char*) val.c_str(), val.length(), val64);
	while (val64.replace("=",":")) {}
	in += "base64:";
	in += val64.c_str();
	ok = true;
      }
      else
      {
	val = file.c_str();
	in += val.c_str();
	ok = true;
      }
    }
  }

  if (subcommand == "node-get")
  {
    in = "mgm.cmd=space&mgm.subcmd=node-get";

    XrdOucString spacename = subtokenizer.GetToken();
    XrdOucString key = subtokenizer.GetToken();

    if (!spacename.length())
      printusage = true;

    if (!key.length())
      printusage = true;

    in += "&mgm.space=";
    in += spacename;

    in += "&mgm.space.node-get.key=";
    in += key;
    ok = true;
    result = client_admin_command(in);
    if (result->Get("mgm.proc.stdout"))
    {
      XrdOucString val = result->Get("mgm.proc.stdout");
      eos::common::StringTokenizer subtokenizer(val.c_str());
      
      while (subtokenizer.GetLine())
      {
	XrdOucString nodeline = subtokenizer.GetToken();
	XrdOucString node = nodeline;
	node.erase(nodeline.find(":"));
	nodeline.erase(0, nodeline.find(":="));
	nodeline.erase(0,9);
	// base 64 decode
	XrdOucString val64 = nodeline.c_str();
	char* valout = 0;
	unsigned int valout_len = 0;
	eos::common::SymKey::Base64Decode(val64, valout, valout_len);
	if (valout)
	{
	  val.assign(valout, 0, valout_len-1);
	  free(valout);
	  if (node == "*")
	  {
	    fprintf(stdout,"%s\n", val.c_str());
	  }
	  else
	  {
	    fprintf(stdout,"# [ %s ]\n %s\n", node.c_str(), val.c_str());
	  }
	  global_retc =0;
	  return (0);
	} 
	else
	{
	  fprintf(stderr,"error: unable to base64 decode result!\n");
	  global_retc = EINVAL;
	  return (0);
	}
      }
    }
  }

  if (subcommand == "quota")
  {
    in = "mgm.cmd=space&mgm.subcmd=quota";
    XrdOucString spacename = subtokenizer.GetToken();
    XrdOucString onoff = subtokenizer.GetToken();
    if ((!spacename.length()) || (!onoff.length()))
    {
      goto com_space_usage;
    }

    in += "&mgm.space=";
    in += spacename;
    in += "&mgm.space.quota=";
    in += onoff;
    ok = true;
  }

  if (subcommand == "config")
  {
    XrdOucString spacename = subtokenizer.GetToken();
    XrdOucString keyval = subtokenizer.GetToken();

    if ((!spacename.length()) || (!keyval.length()))
    {
      goto com_space_usage;
    }

    if ((keyval.find("=")) == STR_NPOS)
    {
      // not like <key>=<val>
      goto com_space_usage;
    }

    std::string is = keyval.c_str();
    std::vector<std::string> token;
    std::string delimiter = "=";
    eos::common::StringConversion::Tokenize(is, token, delimiter);

    if (token.size() != 2)
      goto com_space_usage;

    XrdOucString in = "mgm.cmd=space&mgm.subcmd=config&mgm.space.name=";
    in += spacename;
    in += "&mgm.space.key=";
    in += token[0].c_str();
    in += "&mgm.space.value=";
    in += token[1].c_str();

    global_retc = output_result(client_admin_command(in));
    return (0);
  }

  if (printusage || (!ok))
    goto com_space_usage;

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

  return (0);

com_space_usage:

  fprintf(stdout, "usage: space ls                                                  : list spaces\n");
  fprintf(stdout, "usage: space ls [-s] [-m|-l|--io|--fsck] [<space>]                   : list in all spaces or select only <space>. <space> is a substring match and can be a comma seperated list\n");
  fprintf(stdout, "                                                                  -s : silent mode\n");
  fprintf(stdout, "                                                                  -m : monitoring key=value output format\n");
  fprintf(stdout, "                                                                  -l : long output - list also file systems after each space\n");
  fprintf(stdout, "                                                                --io : print IO satistics\n");
  fprintf(stdout, "                                                              --fsck : print filesystem check statistics\n");
  fprintf(stdout, "       space config <space-name> space.nominalsize=<value>           : configure the nominal size for this space\n");
  fprintf(stdout, "       space config <space-name> space.balancer=on|off               : enable/disable the space balancer [default=off]\n");
  fprintf(stdout, "       space config <space-name> space.balancer.threshold=<percent>  : configure the used bytes deviation which triggers balancing            [ default=20 (%%)     ] \n");
  fprintf(stdout, "       space config <space-name> space.balancer.node.rate=<MB/s>     : configure the nominal transfer bandwith per running transfer on a node [ default=25 (MB/s)   ]\n");
  fprintf(stdout, "       space config <space-name> space.balancer.node.ntx=<#>         : configure the number of parallel balancing transfers per node          [ default=2 (streams) ]\n");
  fprintf(stdout, "       space config <space-name> space.converter=on|off              : enable/disable the space converter [default=off]\n");
  fprintf(stdout, "       space config <space-name> space.converter.ntx=<#>             : configure the number of parallel conversions per space                 [ default=2 (streams) ]\n");
  fprintf(stdout, "       space config <space-name> space.drainer.node.rate=<MB/s >     : configure the nominal transfer bandwith per running transfer on a node [ default=25 (MB/s)   ]\n");
  fprintf(stdout, "       space config <space-name> space.drainer.node.ntx=<#>          : configure the number of parallel draining transfers per node           [ default=2 (streams) ]\n");
  fprintf(stdout, "       space config <space-name> space.lru=on|off                    : enable/disable the LRU policy engine [default=off]\n");
  fprintf(stdout, "       space config <space-name> space.lru.interval=<sec>            : configure the default lru scan interval\n");
  fprintf(stdout, "       space config <space-name> space.headroom=<size>               : configure the default disk headroom if not defined on a filesystem (see fs for details)\n");
  fprintf(stdout, "       space config <space-name> space.scaninterval=<sec>            : configure the default scan interval if not defined on a filesystem (see fs for details)\n");
  fprintf(stdout, "       space config <space-name> space.drainperiod=<sec>             : configure the default drain  period if not defined on a filesystem (see fs for details)\n");
  fprintf(stdout, "       space config <space-name> space.graceperiod=<sec>             : configure the default grace  period if not defined on a filesystem (see fs for details)\n");
  fprintf(stdout, "       space config <space-name> space.autorepair=on|off             : enable auto-repair of faulty replica's/files (the converter has to be enabled too)");
  fprintf(stdout, "                                                                       => size can be given also like 10T, 20G, 2P ... without space before the unit \n");
  fprintf(stdout, "       space config <space-name> space.geo.access.policy.write.exact=on|off   : if 'on' use exact matching geo replica (if available) , 'off' uses weighting [ for write case ]\n");
  fprintf(stdout, "       space config <space-name> space.geo.access.policy.read.exact=on|off    : if 'on' use exact matching geo replica (if available) , 'off' uses weighting [ for read case  ]\n");
  fprintf(stdout, "\n");
  fprintf(stdout, "       space config <space-name> fs.<key>=<value>                    : configure file system parameters for each filesystem in this space (see help of 'fs config' for details)\n");
  fprintf(stdout, "\n");
  fprintf(stdout, "       space define <space-name> [<groupsize> [<groupmod>]]          : define how many filesystems can end up in one scheduling group <groupsize> [default=0]\n");
  fprintf(stdout, "\n");
  fprintf(stdout, "                                                                       => <groupsize>=0 means, that no groups are built within a space, otherwise it should be the maximum number of nodes in a scheduling group\n");
  fprintf(stdout, "                                                                       => <groupmod> defines the maximun number of filesystems per node\n");
  fprintf(stdout, "\n");
  fprintf(stdout, "       space node-set <space-name> <node.key> <file-name>            : store the contents of <file-name> into the node configuration variable <node.key> visibile to all FSTs\n");
  fprintf(stdout, "                                                                       => if <file-name> matches file:<path> the file is loaded from the MGM and not from the client\n");
  fprintf(stdout, "                                                                       => local files cannot exceed 512 bytes - MGM files can be arbitrary length\n");
  fprintf(stdout, "                                                                       => the contents gets base64 encoded by default\n");
  fprintf(stdout, "\n");
  fprintf(stdout, "       space node-get <space-name> <node.key>                        : get the value of <node.key> and base64 decode before output\n");
  fprintf(stdout, "                                                                     : if the value for <node.key> is identical for all nodes in the referenced space, it is dumped only once, otherwise the value is dumped for each node separately\n");
  fprintf(stdout, "       space reset <space-name>  [--egroup|drain|scheduledrain|schedulebalance] \n");
  fprintf(stdout, "                                                                     : reset a space e.g. recompute the drain state machine\n");
  fprintf(stdout, "       space status <space-name>                                     : print's all defined variables for space\n");
  fprintf(stdout, "       space set <space-name> on|off                                 : enables/disabels all groups under that space ( not the nodes !) \n");
  fprintf(stdout, "       space rm <space-name>                                         : remove space\n");
  fprintf(stdout, "\n");
  fprintf(stdout, "       space quota <space-name> on|off                               : enable/disable quota\n");
  return (0);
}
