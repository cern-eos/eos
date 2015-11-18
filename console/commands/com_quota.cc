// ----------------------------------------------------------------------
// File: com_quota.cc
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
#include <iomanip>
/*----------------------------------------------------------------------------*/

//------------------------------------------------------------------------------
// Quota System listing, configuration and manipulation
//------------------------------------------------------------------------------
int
com_quota(char* arg1)
{
  // Split subcommands
  eos::common::StringTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString subcommand = subtokenizer.GetToken();
  XrdOucString arg = subtokenizer.GetToken();
  bool highlighting = true;

  if (subcommand == "-m")
  {
    subcommand = "";
    arg = "-m";
  }

  if (subcommand == "" || subcommand.beginswith("/"))
  {
    XrdOucString in = "mgm.cmd=quota&mgm.subcmd=lsuser";

    if (subcommand.beginswith("/"))
    {
      in += "&mgm.quota.space=";
      in += subcommand;
    }

    if (arg == "-m")
      in += "&mgm.quota.format=m";

    global_retc = output_result(client_user_command(in));
    return (0);
  }

  bool has_space = false;

  if (wants_help(arg1))
    goto com_quota_usage;

  if (subcommand == "ls")
  {
    XrdOucString in = "mgm.cmd=quota&mgm.subcmd=ls";

    if (arg.length())
      do
      {
	if ((arg == "--uid") || (arg == "-u"))
	{
	  XrdOucString uid = subtokenizer.GetToken();

	  if (!uid.length())
	    goto com_quota_usage;

	  in += "&mgm.quota.uid=";
	  in += uid;
	  arg = subtokenizer.GetToken();
	}
	else if ((arg == "--gid") || (arg == "-g"))
	{
	  XrdOucString gid = subtokenizer.GetToken();

	  if (!gid.length())
	    goto com_quota_usage;

	  in += "&mgm.quota.gid=";
	  in += gid;
	  arg = subtokenizer.GetToken();
	}
	else if ((arg == "--path") || (arg == "-p"))
	{
	  if (has_space)
	    goto com_quota_usage;

	  XrdOucString space = subtokenizer.GetToken();

	  if (space.c_str())
	  {
	    in += "&mgm.quota.space=";
	    in += space;
	    arg = subtokenizer.GetToken();
	    has_space = true;
	  }
	}
	else if ((arg == "-m"))
	{
	  in += "&mgm.quota.format=m";
	  arg = subtokenizer.GetToken();
	  highlighting = false;
	}
	else if ((arg == "-n"))
	{
	  in += "&mgm.quota.printid=n";
	  arg = subtokenizer.GetToken();
	}
	else
	{
	  if ((arg.beginswith("/")) && (!has_space))
	  {
	    in += "&mgm.quota.space=";
	    in += arg;
	    has_space = true;
	    arg = subtokenizer.GetToken();
	  }
	  else
	    goto com_quota_usage;
	}
      }
      while (arg.length());

    global_retc = output_result(client_user_command(in), highlighting);
    return (0);
  }

  if (subcommand == "set")
  {
    XrdOucString in = "mgm.cmd=quota&mgm.subcmd=set";
    XrdOucString space = "default";

    do
    {
      if ((arg == "--uid") || (arg == "-u"))
      {
	XrdOucString uid = subtokenizer.GetToken();

	if (!uid.length())
	  goto com_quota_usage;

	in += "&mgm.quota.uid=";
	in += uid;
	arg = subtokenizer.GetToken();
      }
      else if ((arg == "--gid") || (arg == "-g"))
      {
	XrdOucString gid = subtokenizer.GetToken();

	if (!gid.length())
	  goto com_quota_usage;

	in += "&mgm.quota.gid=";
	in += gid;
	arg = subtokenizer.GetToken();
      }
      else if ((arg == "--path") || (arg == "-p"))
      {
	if (has_space)
	  goto com_quota_usage;

	space = subtokenizer.GetToken();

	if (!space.length())
	  goto com_quota_usage;

	in += "&mgm.quota.space=";
	in += space;
	arg = subtokenizer.GetToken();
	has_space = true;
      }
      else if ((arg == "--volume") || (arg == "-v"))
      {
	XrdOucString bytes = subtokenizer.GetToken();

	if (!bytes.length())
	  goto com_quota_usage;

	in += "&mgm.quota.maxbytes=";
	in += bytes;
	arg = subtokenizer.GetToken();
      }
      else if ((arg == "--inodes") || (arg == "-i"))
      {
	XrdOucString inodes = subtokenizer.GetToken();

	if (!inodes.length())
	  goto com_quota_usage;

	in += "&mgm.quota.maxinodes=";
	in += inodes;
	arg = subtokenizer.GetToken();
      }
      else
      {
	if ((arg.beginswith("/")) && (!has_space))
	{
	  in += "&mgm.quota.space=";
	  in += arg;
	  has_space = true;
	  arg = subtokenizer.GetToken();
	}
	else
	  goto com_quota_usage;
      }
    }
    while (arg.length());

    global_retc = output_result(client_user_command(in));
    return (0);
  }

  if (subcommand == "rm")
  {
    XrdOucString in = "mgm.cmd=quota&mgm.subcmd=rm";

    do
    {
      if ((arg == "--uid") || (arg == "-u"))
      {
	XrdOucString uid = subtokenizer.GetToken();

	if (!uid.length())
	  goto com_quota_usage;

	in += "&mgm.quota.uid=";
	in += uid;
	arg = subtokenizer.GetToken();
      }
      else if ((arg == "--gid") || (arg == "-g"))
      {
	XrdOucString gid = subtokenizer.GetToken();

	if (!gid.length())
	  goto com_quota_usage;

	in += "&mgm.quota.gid=";
	in += gid;
	arg = subtokenizer.GetToken();
      }
      else if ((arg == "--path") || (arg == "-p"))
      {
	if (has_space)
	  goto com_quota_usage;

	XrdOucString space = subtokenizer.GetToken();

	if (!space.length())
	  goto com_quota_usage;

	in += "&mgm.quota.space=";
	in += space;
	arg = subtokenizer.GetToken();
	has_space = true;
      }
      else if ((arg == "--inode") || (arg == "-i"))
      {
	in += "&mgm.quota.type=inode";
	arg = subtokenizer.GetToken();
      }
      else if ((arg == "--volume") || (arg == "-v"))
      {
	in += "&mgm.quota.type=volume";
	arg = subtokenizer.GetToken();
      }
      else
      {
	if ((arg.beginswith("/")) && (!has_space))
	{
	  in += "&mgm.quota.space=";
	  in += arg;
	  has_space = true;
	  arg = subtokenizer.GetToken();
	}
	else
	  goto com_quota_usage;
      }
    }
    while (arg.length());

    global_retc = output_result(client_user_command(in));
    return (0);
  }

  if (subcommand == "rmnode")
  {
    XrdOucString in = "mgm.cmd=quota&mgm.subcmd=rmnode";
    XrdOucString space = "";

    do
    {
      if ((arg == "--path") || (arg == "-p"))
      {
	space = subtokenizer.GetToken();

	if (!space.length())
	  goto com_quota_usage;

	in += "&mgm.quota.space=";
	in += space;
	arg = subtokenizer.GetToken();
      }
      else
	goto com_quota_usage;
    }
    while (arg.length());

    if (!space.length())
      goto com_quota_usage;

    string s;
    fprintf(stdout, "Do you really want to delete the quota node under path %s ?\n",
	    space.c_str());
    fprintf(stdout, "Confirm the deletion by typing => ");
    XrdOucString confirmation = "";

    for (int i = 0; i < 10; i++)
    {
      confirmation += (int)(9.0 * rand() / RAND_MAX);
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

    return 0;
  }

com_quota_usage:
  std::ostringstream oss;
  std::vector<std::uint32_t> col_size = {0 , 0};
  std::map<std::string, std::string> map_cmds =
  {
    {
      "quota [<path>]",
      ": show personal quota for all or only the quota node responsible for <path>"
    },
    {
      "quota ls [-n] [-m] [-u <uid>] [-g <gid>] [-p <path>]",
      ": list configured quota and quota node(s)"
    },
    {
      "quota ls [-n] [-m] [-u <uid>] [-g <gid>] [<path>]",
      ": list configured quota and quota node(s)"
    },
    {
      "quota set -u <uid>|-g <gid> [-v <bytes>] [-i <inodes>] -p <path>",
      ": set volume and/or inode quota by uid or gid"
    },
    {
      "quota set -u <uid>|-g <gid> [-v <bytes>] [-i <inodes>] <path>",
      ": set volume and/or inode quota by uid or gid"
    },
    {
      "quota rm -u <uid>|-g <gid> [-v] [-i] -p <path>",
      ": remove configured quota type(s) for uid/gid in path"
    },
    {
      "quota rm -u <uid>|-g <gid> [-v] [-i] <path>",
      ": remove configured quota type(s) for uid/gid in path"
    },
    {
      "quota rmnode -p <path>",
      ": remove quota node and every defined quota on that node"
    }
  };

  // Compute max width for command and description table
  for (auto it = map_cmds.begin(); it != map_cmds.end(); ++it)
  {
    if (col_size[0] < it->first.length())
      col_size[0] = it->first.length() + 1;

    if (col_size[1] < it->second.length())
      col_size[1] = it->second.length() + 1;
  }

  std::int8_t tab_size = 2;
  std::string usage_txt = "Usage:";
  std::string opt_txt = "General options:";
  std::string notes_txt = "Notes:";
  oss << usage_txt << std::endl;

  // Print the command and their description
  for (auto it = map_cmds.begin(); it != map_cmds.end(); ++it)
  {
    oss << std::setw(usage_txt.length()) << ""
	<< std::setw(col_size[0]) << std::setiosflags(std::ios_base::left)
	<< it->first
	<< std::setw(col_size[1]) << std::setiosflags(std::ios_base::left)
	<< it->second
	<< std::endl;
  }

  std::uint32_t indent_len = usage_txt.length() + tab_size;

  // Print general options
  oss << std::endl << std::setw(usage_txt.length()) << ""
      << opt_txt << std::endl
      << std::setw(indent_len) << ""
      << "-m : print information in monitoring <key>=<value> format" << std::endl
      << std::setw(indent_len) << ""
      << "-n : don't translate ids, print uid and gid number" << std::endl
      << std::setw(indent_len) << ""
      << "-u/--uid <uid> : print information only for uid <uid>" << std::endl
      << std::setw(indent_len) << ""
      << "-g/--gid <gid> : print information only for gid <gid>" << std::endl
      << std::setw(indent_len) << ""
      << "-p/--path <path> : print information only for path <path> - this "
      << "can also be given without -p or --path" << std::endl
      << std::setw(indent_len) << ""
      << "-v/--volume <bytes> : refer to volume limit in <bytes>" << std::endl
      << std::setw(indent_len) << ""
      << "-i/--inodes <inodes> : refer to inode limit in number of <inodes>"
      << std::endl;
  indent_len = usage_txt.length() + tab_size;

  // Print extra notes
  oss << std::endl << std::setw(usage_txt.length()) << ""
      << notes_txt << std::endl
      << std::setw(indent_len) << ""
      << "=> you have to specify either the user or the group identified by the "
      << "unix id or the user/group name" << std::endl
      << std::setw(indent_len) << ""
      << "=> the space argument is by default assumed as 'default'" << std::endl
      << std::setw(indent_len) << ""
      << "=> you have to specify at least a volume or an inode limit to set quota" <<
      std::endl
      << std::setw(indent_len) << ""
      << "=> for convenience all commands can just use <path> as last argument "
      << "ommitting the -p|--path e.g. quota ls /eos/ ..." << std::endl
      << std::setw(indent_len) << ""
      << "=> if <path> is not terminated with a '/' it is assumed to be a file "
      << "so it won't match the quota node with <path>/ !" << std::endl;
  fprintf(stdout, "%s", oss.str().c_str());
  return 0;
}
