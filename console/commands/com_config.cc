/*----------------------------------------------------------------------------*/
#include "console/ConsoleMain.hh"
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
        if ((arg == "--fs") || (arg == "-f")) {
          in += "&mgm.config.fs=1";
          arg = subtokenizer.GetToken();
        } else 
          if ((arg == "--vid") || (arg == "-v")) {
            in += "&mgm.config.vid=1";
            arg = subtokenizer.GetToken();
          } else 
            if ((arg == "--quota") || (arg == "-q")) {
              in += "&mgm.config.quota=1";
              arg = subtokenizer.GetToken();
            } else 
              if ((arg == "--comment") || (arg == "-c")) {
                in += "&mgm.config.comment=1";
                arg = subtokenizer.GetToken();
              } else 
                if ((arg == "--policy") || (arg  == "-p")) {
                  in += "&mgm.config.policy=1";
                  arg = subtokenizer.GetToken();
                } else 
		  if ((arg == "--global") || (arg == "-g")) {
		    in += "&mgm.config.global=1";
		    arg = subtokenizer.GetToken();
		  } else 
		    if (!arg.beginswith("-")) {
		      in += "&mgm.config.file=";
		      in += arg;
		      arg = subtokenizer.GetToken();
		    } else {
		      goto com_config_usage;
		    }
      } while (arg.length());
    }      
    
    global_retc = output_result(client_admin_command(in));
    return (0);
  }

  
  
  if ( subcommand == "ls" ) {
    XrdOucString in ="mgm.cmd=config&mgm.subcmd=ls";
    if ( (arg == "--backup") || (arg == "-b")) {
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
    bool match=false;
    do {
      match=false;
      if (arg == "-f") {
        in += "&mgm.config.force=1";
        arg = subtokenizer.GetToken();
	match=true;
      } 
 
      if ((arg == "--comment")||(arg == "-c")) {
	in += "&mgm.config.comment=";
	arg = subtokenizer.GetToken();
	if ((arg.beginswith("\"") || (arg.beginswith("'") ))) {
	  arg.replace("'","\"");
	  if (arg.length()) {
	    do {
	      in += " ";
	      in += arg;
	      arg = subtokenizer.GetToken();
	    } while (arg.length() && (!arg.endswith("\"")) && (!arg.endswith("'")));
	    
	    if (arg.endswith("\"") || arg.endswith("'")) {
	      in += " ";
	      arg.replace("'","\"");
	      in += arg;
	    }
	    arg = subtokenizer.GetToken();
	  }
	}
	match=true;
      } 
      
      if (!arg.beginswith("-")) {
	in += "&mgm.config.file=";
	in += arg;
	hasfile = true;
	arg = subtokenizer.GetToken();
	match=true;
      } 
      if (!match)
	arg = subtokenizer.GetToken();

    } while (arg.length() && match);

    if (!match) goto com_config_usage;
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
  printf("usage: config ls   [--backup|-b]                                         :  list existing configurations\n");
  printf("usage: config dump [--fs|-f] [--vid|-v] [--quota|-q] [--policy|-p] [--comment|-c] [--global|-g] [<name>]\n");
  printf("                                                                         :  dump current configuration or configuration with name <name>\n");
  printf("                                                                            -f : dump only file system config\n");
  printf("                                                                            -v : dump only virtual id config\n");
  printf("                                                                            -q : dump only quota config\n");
  printf("                                                                            -p : dump only policy config\n");
  printf("                                                                            -g : dump only global config\n");

  printf("usage: config save <name> [--comment|-c \"<comment>\"] [-f] ]            :  save config (optionally under name)\n");
  printf("                                                                            -f : overwrite existing config name and create a timestamped backup\n");
  printf("usage: config load <name>                                                :  load config (optionally with name)\n");
  printf("usage: config diff                                                       :  show changes since last load/save operation\n");
  printf("usage: config changelog [-#lines]                                        :  show the last <#> lines from the changelog - default is -10 \n");
  printf("usage: config reset                                                      :  reset all configuration to empty state\n");

  return (0);
}
