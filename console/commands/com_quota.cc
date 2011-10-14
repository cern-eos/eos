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
/*----------------------------------------------------------------------------*/

/* Quota System listing, configuration, manipulation */
int
com_quota (char* arg1) {
  // split subcommands
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString subcommand = subtokenizer.GetToken();
  XrdOucString arg = subtokenizer.GetToken();
  bool highlighting = true;

  if ( subcommand == "") {
    XrdOucString in ="mgm.cmd=quota&mgm.subcmd=ls";
    global_retc = output_result(client_user_command(in));
    return (0);
  }
  if ( subcommand == "ls" ) {
    XrdOucString in ="mgm.cmd=quota&mgm.subcmd=ls";
    if (arg.length())
      do {
        if ((arg == "--uid") || (arg == "-u") ) {
          XrdOucString uid = subtokenizer.GetToken();
          if (!uid.length()) 
            goto com_quota_usage;
          in += "&mgm.quota.uid=";
          in += uid;
          arg = subtokenizer.GetToken();
        } else 
          if ((arg == "--gid") || (arg == "-g")) {
            XrdOucString gid = subtokenizer.GetToken();
            if (!gid.length()) 
              goto com_quota_usage;
            in += "&mgm.quota.gid=";
            in += gid;
            arg = subtokenizer.GetToken();
          } else 
            if ((arg == "--path") || (arg == "-p")) {
              XrdOucString space = subtokenizer.GetToken();
              if (space.c_str()) {
                in += "&mgm.quota.space=";
                in += space;
                arg = subtokenizer.GetToken();
              }
            } else 
              if ((arg == "-m")) {
                in += "&mgm.quota.format=m";
                arg = subtokenizer.GetToken();
                highlighting = false;
              } else 
                if ((arg == "-n")) {
                  in += "&mgm.quota.printid=n";
                  arg = subtokenizer.GetToken();
                } else 
                  goto com_quota_usage;
      } while (arg.length());
    
    
    global_retc = output_result(client_admin_command(in), highlighting);
    return (0);
  }
  
  if ( subcommand == "set" ) {
    XrdOucString in ="mgm.cmd=quota&mgm.subcmd=set";
    XrdOucString space ="default";
    do {
      if ((arg == "--uid") || (arg == "-u")) {
        XrdOucString uid = subtokenizer.GetToken();
        if (!uid.length()) 
          goto com_quota_usage;
        in += "&mgm.quota.uid=";
        in += uid;
        arg = subtokenizer.GetToken();
      } else
        if ((arg == "--gid") || (arg == "-g")) {
          XrdOucString gid = subtokenizer.GetToken();
          if (!gid.length()) 
            goto com_quota_usage;
          in += "&mgm.quota.gid=";
          in += gid;
          arg = subtokenizer.GetToken();
        } else
          if ((arg == "--path") || (arg == "-p")) {
            space = subtokenizer.GetToken();
            if (!space.length()) 
              goto com_quota_usage;
             
            in += "&mgm.quota.space=";
            in += space;
            arg = subtokenizer.GetToken();
          } else
            if ((arg == "--volume") || (arg =="-v")) {
              XrdOucString bytes = subtokenizer.GetToken();
              if (!bytes.length()) 
                goto com_quota_usage;
              in += "&mgm.quota.maxbytes=";
              in += bytes;
              arg = subtokenizer.GetToken();
            } else
              if ((arg == "--inodes") || (arg == "-i")) {
                XrdOucString inodes = subtokenizer.GetToken();
                if (!inodes.length()) 
                  goto com_quota_usage;
                in += "&mgm.quota.maxinodes=";
                in += inodes;
                arg = subtokenizer.GetToken();
              } else 
                goto com_quota_usage;
    } while (arg.length());

    global_retc = output_result(client_admin_command(in));
    return (0);
  }

  if ( subcommand == "rm" ) {
    XrdOucString in ="mgm.cmd=quota&mgm.subcmd=rm";
    do {
      if ((arg == "--uid") || (arg == "-u")) {
        XrdOucString uid = subtokenizer.GetToken();
        if (!uid.length()) 
          goto com_quota_usage;
        in += "&mgm.quota.uid=";
        in += uid;
        arg = subtokenizer.GetToken();
      } else 
        if ((arg == "--gid") || (arg == "-g")) {
          XrdOucString gid = subtokenizer.GetToken();
          if (!gid.length()) 
            goto com_quota_usage;
          in += "&mgm.quota.gid=";
          in += gid;
          arg = subtokenizer.GetToken();
        } else 
          if ((arg == "--path") || (arg == "-p")) {
            XrdOucString space = subtokenizer.GetToken();
            if (!space.length()) 
              goto com_quota_usage;
            
            in += "&mgm.quota.space=";
            in += space;
            arg = subtokenizer.GetToken();
          } else {
            goto com_quota_usage;
          }
    } while (arg.length());
    
    
    global_retc = output_result(client_admin_command(in));
    return (0);
  }

  if ( subcommand == "rmnode" ) {
    XrdOucString in ="mgm.cmd=quota&mgm.subcmd=rmnode";
    XrdOucString space="";
    do {
      if ((arg == "--path") || (arg == "-p")) {
        space = subtokenizer.GetToken();
        if (!space.length()) 
          goto com_quota_usage;
        
        in += "&mgm.quota.space=";
        in += space;
        arg = subtokenizer.GetToken();
      } else {
        goto com_quota_usage;
      }
    } while (arg.length());
    
    if (!space.length())
      goto com_quota_usage;
    
    string s;
    printf("Do you really want to delete the quota node under path %s ?\n" , space.c_str());
    printf("Confirm the deletion by typing => ");
    XrdOucString confirmation="";
    for (int i=0; i<10; i++) {
      confirmation += (int) (9.0 * rand()/RAND_MAX);
    }

    printf("%s\n", confirmation.c_str());
    printf("                               => ");
    getline( std::cin, s );
    std::string sconfirmation = confirmation.c_str();
    if ( s == sconfirmation) {
      printf("\nDeletion confirmed\n");
      global_retc = output_result(client_admin_command(in));
    } else {
      printf("\nDeletion aborted!\n");
      global_retc = -1;
    }
    return (0);
  }
  
  
  
 com_quota_usage:
  printf("usage: quota                                                                                       : show personal quota\n");
  printf("       quota ls [-n] [-m] -u <uid> [-p <path> ]                                                    : list configured quota and quota node(s)\n");
  printf("       quota ls [-n] [-m] --uid <uid> [--path <path>]                                              : list configured quota and quota node(s)\n");
  printf("       quota ls [-n] [-m] -g <gid> [-p <path> ]                                                    : list configured quota and quota node(s)\n");
  printf("       quota ls [-n] [-m] --gid <gid> [--path <path>]                                              : list configured quota and quota node(s)\n");
  printf("       quota set -u <uid>|-g <gid> -p <path>   [-v <bytes>] [-i <inodes>]                          : set volume and/or inode quota by uid or gid \n");
  printf("       quota set --uid <uid>|--gid <gid> -p|--path <path> [--volume <bytes>] [--inodes <inodes>]   : set volume and/or inode quota by uid or gid \n");
  printf("       quota rm  -u <uid>|-g <gid> -p|--path <path>                                                : remove configured quota for uid/gid in path\n");
  printf("       quota rm  --uid <uid>|--gid <gid> -p|--path <path>                                          : remove configured quota for uid/gid in path\n");
  printf("                                                 -m                  : print information in monitoring <key>=<value> format\n");
  printf("                                                 -n                  : don't translate ids, print uid+gid number\n");
  printf("                                                 -u/--uid <uid>      : print information only for uid <uid>\n");
  printf("                                                 -g/-gid <gid>       : print information only for gid <gid>\n");
  printf("                                                 -p/--path <path>    : print information only for path <path>\n");
  printf("                                                 -v/--volume <bytes> : set the volume limit to <bytes>\n");
  printf("                                                 -i/--inodes <inodes>: set the inodes limit to <inodes>\n");
  printf("     => you have to specify either the user or the group identified by the unix id or the user/group name\n");
  printf("     => the space argument is by default assumed as 'default'\n");
  printf("     => you have to specify at least a volume or an inode limit to set quota\n");
  printf("       quota rmnode -p <path>                                                                      : remove quota node and every defined quota on that node\n");
  

  return (0);
}
