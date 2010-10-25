/*----------------------------------------------------------------------------*/
#include "ConsoleMain.hh"
/*----------------------------------------------------------------------------*/

/* Configuration System listing, configuration, manipulation */
int
com_config (char* arg1) {
  // split subcommands
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString subcommand = subtokenizer.GetToken();
  XrdOucString arg = subtokenizer.GetToken();
  
  if ( subcommand == "dump" ) {
    XrdOucString in ="mgm.cmd=config&mgm.subcmd=dump";
    if (arg.length()) { 
      do {
	if (arg == "-fs") {
	  in += "&mgm.config.fs=1";
	  arg = subtokenizer.GetToken();
	} else 
	  if (arg == "-vid") {
	    in += "&mgm.config.vid=1";
	    arg = subtokenizer.GetToken();
	  } else 
	    if (arg == "-quota") {
	      in += "&mgm.config.quota=1";
	      arg = subtokenizer.GetToken();
	    } else 
	      if (arg == "-comment") {
		in += "&mgm.config.comment=1";
		arg = subtokenizer.GetToken();
	      } else 
		if (arg == "-policy") {
		  in += "&mgm.config.policy=1";
		  arg = subtokenizer.GetToken();
		} else 
		  if (!arg.beginswith("-")) {
		    in += "&mgm.config.file=";
		    in += arg;
		    arg = subtokenizer.GetToken();
		  }
      } while (arg.length());
    }      
    
    global_retc = output_result(client_admin_command(in));
    return (0);
  }

  
  
  if ( subcommand == "ls" ) {
    XrdOucString in ="mgm.cmd=config&mgm.subcmd=ls";
    if (arg == "-backup") {
      in += "&mgm.config.showbackup=1";
    }
    global_retc = output_result(client_admin_command(in));
    return (0);
  }
  
  if ( subcommand == "load") {
    XrdOucString in ="mgm.cmd=config&mgm.subcmd=load&mgm.config.file=";
    if (!arg.length()) 
      goto com_config_usage;
    
    in += arg;
    global_retc = output_result(client_admin_command(in));
    return (0);
  }

  if ( subcommand == "reset") {
    XrdOucString in ="mgm.cmd=config&mgm.subcmd=reset";
    global_retc = output_result(client_admin_command(in));
    return (0);
  }

  if ( subcommand == "save") {
    XrdOucString in ="mgm.cmd=config&mgm.subcmd=save";
    bool hasfile =false;
    printf("arg is %s\n", arg.c_str());
    do {
      if (arg == "-f") {
	in += "&mgm.config.force=1";
	arg = subtokenizer.GetToken();
      } else 
	if (arg == "-comment") {
	  in += "&mgm.config.comment=";
	  arg = subtokenizer.GetToken();
	  if (arg.beginswith("\"")) {
	    in += arg;
	    arg = subtokenizer.GetToken();
	    if (arg.length()) {
	      do {
		in += " ";
		in += arg;
		arg = subtokenizer.GetToken();
	      } while (arg.length() && (!arg.endswith("\"")));
	      if (arg.endswith("\"")) {
		in += " ";
		in += arg;
		arg = subtokenizer.GetToken();
	      }
	    }
	  }
	} else {
	  if (!arg.beginswith("-")) {
	    in += "&mgm.config.file=";
	    in += arg;
	    hasfile = true;
	    arg = subtokenizer.GetToken();
	  } else {
	    goto com_config_usage;
	  }
	}
    } while (arg.length());
    
    if (!hasfile) goto com_config_usage;
    global_retc = output_result(client_admin_command(in));
    return (0);
  }

  if ( subcommand == "diff") {
    XrdOucString in ="mgm.cmd=config&mgm.subcmd=diff";
    arg = subtokenizer.GetToken();
    if (arg.length()) 
      goto com_config_usage;
    
    global_retc = output_result(client_admin_command(in));
    return (0);
  }


  if ( subcommand == "changelog") {
    XrdOucString in ="mgm.cmd=config&mgm.subcmd=changelog";
    if (arg.length()) {
      if (arg.beginswith("-")) {
	// allow -100 and 100 
	arg.erase(0,1);
      }
      in += "&mgm.config.lines="; in+= arg;
    }

    arg = subtokenizer.GetToken();
    if (arg.length()) 
      goto com_config_usage;

    global_retc = output_result(client_admin_command(in));
    return (0);
  }
  
 com_config_usage:
  printf("usage: config ls   [-backup]                                             :  list existing configurations\n");
  printf("usage: config dump [-fs] [-vid] [-quota] [-policy] [-comment] [<name>]   :  dump current configuration or configuration with name <name>\n");

  printf("usage: config save <name> [-comment \"<comment>\"] [-f] ]                :  save config (optionally under name)\n");
  printf("usage: config load <name>                                                :  load config (optionally with name)\n");
  printf("usage: config diff                                                       :  show changes since last load/save operation\n");
  printf("usage: config changelog [-#lines]                                        :  show the last <#> lines from the changelog - default is -10 \n");
  printf("usage: config reset                                                      :  reset all configuration to empty state\n");

  return (0);
}
