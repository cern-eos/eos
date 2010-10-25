/*----------------------------------------------------------------------------*/
#include "ConsoleMain.hh"
/*----------------------------------------------------------------------------*/

/* Quota System listing, configuration, manipulation */
int
com_quota (char* arg1) {
  // split subcommands
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString subcommand = subtokenizer.GetToken();
  XrdOucString arg = subtokenizer.GetToken();
  
  if ( subcommand == "ls" ) {
    XrdOucString in ="mgm.cmd=quota&mgm.subcmd=ls";
    if (arg.length())
      do {
	if (arg == "-uid") {
	  XrdOucString uid = subtokenizer.GetToken();
	  if (!uid.length()) 
	    goto com_quota_usage;
	  in += "&mgm.quota.uid=";
	  in += uid;
	  arg = subtokenizer.GetToken();
	} else 
	  if (arg == "-gid") {
	    XrdOucString gid = subtokenizer.GetToken();
	    if (!gid.length()) 
	      goto com_quota_usage;
	    in += "&mgm.quota.gid=";
	    in += gid;
	    arg = subtokenizer.GetToken();
	  } else 
	    
	    if (arg.c_str()) {
	      in += "&mgm.quota.space=";
	      in += arg;
	    } else 
	      goto com_quota_usage;
      } while (arg.length());
    
    
    global_retc = output_result(client_admin_command(in));
    return (0);
  }
  
  if ( subcommand == "set" ) {
    XrdOucString in ="mgm.cmd=quota&mgm.subcmd=set";
    XrdOucString space ="default";
    do {
      if (arg == "-uid") {
	XrdOucString uid = subtokenizer.GetToken();
	if (!uid.length()) 
	  goto com_quota_usage;
	in += "&mgm.quota.uid=";
	in += uid;
	arg = subtokenizer.GetToken();
      } else
	if (arg == "-gid") {
	  XrdOucString gid = subtokenizer.GetToken();
	  if (!gid.length()) 
	    goto com_quota_usage;
	  in += "&mgm.quota.gid=";
	  in += gid;
	  arg = subtokenizer.GetToken();
	} else
	  if (arg == "-space") {
	     space = subtokenizer.GetToken();
	     if (!space.length()) 
	       goto com_quota_usage;
	     
	     in += "&mgm.quota.space=";
	     in += space;
	     arg = subtokenizer.GetToken();
	   } else
	     if (arg == "-size") {
	       XrdOucString bytes = subtokenizer.GetToken();
	       if (!bytes.length()) 
		 goto com_quota_usage;
	       in += "&mgm.quota.maxbytes=";
	       in += bytes;
	       arg = subtokenizer.GetToken();
	     } else
	       if (arg == "-inodes") {
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
      if (arg == "-uid") {
	XrdOucString uid = subtokenizer.GetToken();
	if (!uid.length()) 
	  goto com_quota_usage;
	in += "&mgm.quota.uid=";
	in += uid;
	arg = subtokenizer.GetToken();
      } else 
	if (arg == "-gid") {
	  XrdOucString gid = subtokenizer.GetToken();
	  if (!gid.length()) 
	    goto com_quota_usage;
	  in += "&mgm.quota.gid=";
	  in += gid;
	  arg = subtokenizer.GetToken();
	} else 
	  
	  if (arg.c_str()) {
	    in += "&mgm.quota.space=";
	    in += arg;
	  } else 
	    goto com_quota_usage;
    } while (arg.length());
    
    
    global_retc = output_result(client_admin_command(in));
    return (0);
  }
  
   com_quota_usage:
  printf("usage: quota ls [-uid <uid>] [ -gid <gid> ] [-space {<space>}                                          : list configured quota and used space\n");
  printf("usage: quota set [-uid <uid>] [ -gid <gid> ] -space {<space>} [-size <bytes>] [ -inodes <inodes>]      : set volume and/or inode quota by uid or gid \n");
  printf("usage: quota rm [-uid <uid>] [ -gid <gid> ] -space {<space>}                                           : remove configured quota for uid/gid in space\n");
  printf("                                                  -uid <uid>       : print information only for uid <uid>\n");
  printf("                                                  -gid <gid>       : print information only for gid <gid>\n");
  printf("                                                  -space {<space>} : print information only for space <space>\n");
  printf("                                                  -size <bytes>    : set the space quota to <bytes>\n");
  printf("                                                  -inodes <inodes> : limit the inodes quota to <inodes>\n");
  printf("     => you have to specify either the user or the group id\n");
  printf("     => the space argument is by default assumed as 'default'\n");
  printf("     => you have to sepecify at least a size or an inode limit to set quota\n");

  return (0);
}
