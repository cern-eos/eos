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
                    if ((arg == "--map") || (arg == "-m")) {
                      in += "&mgm.config.map=1";
                      arg = subtokenizer.GetToken();
                    } else 
                      if ((arg == "--access") || (arg == "-a")) {
                        in += "mgm.config.access=1";
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

  if ( subcommand == "autosave") {
    XrdOucString in ="mgm.cmd=config&mgm.subcmd=autosave&mgm.config.state=";
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
  printf("Usage: config ls|dump|load|save|diff|changelog|reset|autosave [OPTIONS]\n");
  printf("'[eos] config' provides the configuration interface to EOS.\n\n");
  printf("Options:\n");
  printf("config ls   [--backup|-b] :\n");
  printf("                                                  list existing configurations\n");
  printf("            --backup|-b : show also backup & autosave files\n");

  printf("config dump [--fs|-f] [--vid|-v] [--quota|-q] [--policy|-p] [--comment|-c] [--global|-g] [--access|-a] [<name>] [--map|-m]] : \n");
  printf("                                                  dump current configuration or configuration with name <name>\n");
  printf("            -f : dump only file system config\n");
  printf("            -v : dump only virtual id config\n");
  printf("            -q : dump only quota config\n");
  printf("            -p : dump only policy config\n");
  printf("            -g : dump only global config\n");
  printf("            -a : dump only access config\n");
  printf("            -m : dump only mapping config\n");

  printf("config save [-f] [<name>] [--comment|-c \"<comment>\"] ] :\n");
  printf("                                                  save config (optionally under name)\n");
  printf("            -f : overwrite existing config name and create a timestamped backup\n");
  printf("=>   if no name is specified the current config file is overwritten\n\n");
  printf("config load <name> :\n");
  printf("                                                  load config (optionally with name)\n");
  printf("config diff :\n");
  printf("                                                  show changes since last load/save operation\n");
  printf("config changelog [-#lines] :\n");
  printf("                                                  show the last <#> lines from the changelog - default is -10 \n");
  printf("config reset :\n");
  printf("                                                  reset all configuration to empty state\n");
  printf("config autosave [on|off] :\n");
  printf("                                                  without on/off just prints the state otherwise set's autosave to on or off\n");

  return (0);
}
