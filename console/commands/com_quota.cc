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
            if ((arg == "--space") || (arg == "-s")) {
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
          if ((arg == "--space") || (arg == "-s")) {
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
          if ((arg == "--space") || (arg == "-s")) {
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
  
 com_quota_usage:
  printf("usage: quota                                                                                         : show personal quota\n");
  printf("       quota ls [-n] [-m] -u <uid> [-s <space> ]                                                     : list configured quota and used space\n");
  printf("       quota ls [-n] [-m] --uid <uid> [--space <space>]                                              : list configured quota and used space\n");
  printf("       quota ls [-n] [-m] -g <gid> [-s <space> ]                                                     : list configured quota and used space\n");
  printf("       quota ls [-n] [-m] --gid <gid> [--space <space>]                                              : list configured quota and used space\n");
  printf("       quota set -u <uid>|-g <gid> -s <space>   [-v <bytes>] [-i <inodes>]                           : set volume and/or inode quota by uid or gid \n");
  printf("       quota set --uid <uid>|--gid <gid> -s|--space <space> [--volume <bytes>] [--inodes <inodes>]   : set volume and/or inode quota by uid or gid \n");
  printf("       quota rm  --uid <uid>|--gid <gid> -s|--space <space>                                          : remove configured quota for uid/gid in space\n");
  printf("                                                 -m                  : print information in monitoring <key>=<value> format\n");
  printf("                                                 -n                  : don't translate ids, print uid+gid number\n");
  printf("                                                 -u/--uid <uid>      : print information only for uid <uid>\n");
  printf("                                                 -g/-gid <gid>       : print information only for gid <gid>\n");
  printf("                                                 -s/--space <space>  : print information only for space <space>\n");
  printf("                                                 -v/--volume <bytes> : set the volume limit to <bytes>\n");
  printf("                                                 -i/--inodes <inodes>: set the inodes limit to <inodes>\n");
  printf("     => you have to specify either the user or the group id\n");
  printf("     => the space argument is by default assumed as 'default'\n");
  printf("     => you have to specify at least a volume or an inode limit to set quota\n");

  return (0);
}
