// ----------------------------------------------------------------------
// File: com_recycle.cc
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
#include "common/Path.hh"
/*----------------------------------------------------------------------------*/

/* Recycle a file/directory and configure recycling */
int
com_recycle (char* arg1)
{
 // split subcommands
 XrdOucTokenizer subtokenizer(arg1);
 subtokenizer.GetLine();
 XrdOucString in = "mgm.cmd=recycle&";
 std::vector<std::string> args;
 std::vector<std::string> options;

 bool monitoring = false;
 bool translateids = false;
 
 XrdOucString subcmd = subtokenizer.GetToken();

 if ( (subcmd != "" ) &&
      (subcmd != "config") &&
      (subcmd != "ls") &&
      (subcmd != "purge") &&
      (subcmd != "restore") &&
      (!subcmd.beginswith("-")) )
   goto com_recycle_usage;


 do
 {
   XrdOucString param = subtokenizer.GetToken();
   if (!param.length()) {
     if  (subcmd.beginswith("-")) 
     {
       param = subcmd;
       subcmd = "";
     } 
     else 
     {
     break;
     }
   }
   if (param.beginswith("-"))
   {
     if (param == "-m")
     {
       monitoring = true;
     }
     else
     {
       if (param == "-n")
       {
         translateids = true;
       }
       else
       {
         options.push_back(param.c_str());
       }
     }
   }
   else
   {
     args.push_back(param.c_str());
   }
 }
 while (1);

 if ((subcmd == "ls") && options.size())
   goto com_recycle_usage;

 if ((subcmd == "ls") && args.size())
   goto com_recycle_usage;

 if ((subcmd == "purge") && options.size())
   goto com_recycle_usage;

 if ((subcmd == "purge") && args.size())
   goto com_recycle_usage;

 if ((subcmd == "config") && (options.size() > 0) &&
     (options[0] != "--add-bin") &&
     (options[0] != "--remove-bin") &&
     (options[0] != "--lifetime") &&
     (options[0] != "--size"))
   goto com_recycle_usage;

 if ((subcmd == "config") && (options.size() == 1) && (args.size() != 1))
   goto com_recycle_usage;
 if ((subcmd == "config") && (options.size() > 1))
   goto com_recycle_usage;

 if ((subcmd == "restore") && (options.size() > 0) &&
     (options[0] != "--force-original-name") &&
     (options[0] != "-f"))
   goto com_recycle_usage;

 if ((subcmd == "restore") && (args.size() != 1))
   goto com_recycle_usage;

 for (size_t i = 0; i < options.size(); i++)
 {
   if ((options[i] == "-h") || (options[i] == "--help"))
     goto com_recycle_usage;
 }

 in += "&mgm.subcmd=";
 in += subcmd;
 if (options.size())
 {
   in += "&mgm.option=";
   in += options[0].c_str();
 }

 if (args.size())
 {
   in += "&mgm.recycle.arg=";
   if ( (options.size()) && 
	((options[0] == "--add-bin") ||
	 (options[0] == "--remove-bin")) ) 
   {
     args[0] = abspath(args[0].c_str());
   }
   in += args[0].c_str();
 }

 if (monitoring)
 {
   in += "&mgm.recycle.format=m";
 }
 
 if (translateids)
 {
   in += "&mgm.recycle.printid=n";
 }

 global_retc = output_result(client_user_command(in));
 return (0);


com_recycle_usage:
 fprintf(stdout, "Usage: recycle ls|purge|restore|config ...\n");
 fprintf(stdout, "'[eos] recycle ..' provides recycle bin functionality to EOS.\n");
 fprintf(stdout, "Options:\n");
 fprintf(stdout, "recycle :\n");
 fprintf(stdout, "                                                  print status of recycle bin and if executed by root the recycle bin configuration settings.\n");
 fprintf(stdout, "recycle ls :\n");
 fprintf(stdout, "                                                  list files in the recycle bin\n");
 fprintf(stdout, "recycle purge :\n");
 fprintf(stdout, "                                                  purge files in the recycle bin\n");
 fprintf(stdout, "recycle restore [--force-original-name|-f] <recycle-key> :\n");
 fprintf(stdout, "                                                  undo the deletion identified by <recycle-key>\n");
 fprintf(stdout, "       --force-original-name : move's deleted files/dirs back to the original location (otherwise the key entry will have a <.inode> suffix\n");
 fprintf(stdout, "recycle config --add-bin <sub-tree>:\n");
 fprintf(stdout, "                                                  configures to use the recycle bin for deletions in <sub-tree>\n");
 fprintf(stdout, "recycle config --remove-bin <sub-tree> :\n");
 fprintf(stdout, "                                                  disables usage of recycle bin for <sub-tree>\n");
 fprintf(stdout, "recycle config --lifetime <seconds> :\n");
 fprintf(stdout, "                                                  configure the FIFO lifetime of the recycle bin\n");
 fprintf(stdout, "recycle config --size <size> :\n");
 fprintf(stdout, "                                                  set the size of the recycle bin\n");
 fprintf(stdout, "'ls' and 'config' support the '-m' flag to give monitoring format output!\n");
 fprintf(stdout, "'ls' supports the '-n' flag to give numeric user/group ids instead of names!\n");
 return (0);
}

