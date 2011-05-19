/*----------------------------------------------------------------------------*/
#include "console/ConsoleMain.hh"
/*----------------------------------------------------------------------------*/

using namespace eos::common;

/* Space listing, configuration, manipulation */
int
com_space (char* arg1) {
  XrdOucString in = "";
  bool silent=false;
  bool printusage=false;
  bool highlighting=true;
  XrdOucString option="";
  XrdOucEnv* result=0;
  bool ok=false;
  bool sel=false;
  // split subcommands
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString subcommand = subtokenizer.GetToken();
  if ( subcommand == "ls" ) {
    in ="mgm.cmd=space&mgm.subcmd=ls";
    option="";

    do {
      ok=false;
      subtokenizer.GetLine();
      option = subtokenizer.GetToken();
      if (option.length()) {
	if (option == "-m") {
	  in += "&mgm.outformat=m";
	  ok=true;
	  highlighting=false;
	} 
	if (option == "-l") {
	  in += "&mgm.outformat=l";
	  ok=true;
	}
        if (option == "--io") {
          in += "&mgm.outformat=io";
          ok=true;
        }
	if (option == "-s") {
	  silent=true;
	  ok=true;
	}
        if (!option.beginswith("-")) {
          in += "&mgm.selection=";
          in += option;
          if (!sel)
            ok=true;
          sel=true;
        }
          
	if (!ok) 
	  printusage=true;
      } else {
	ok=true;
      }
    } while(option.length());
  }

  if ( subcommand == "define" ) {
    in ="mgm.cmd=space&mgm.subcmd=define";
    XrdOucString nodename = subtokenizer.GetToken();
    XrdOucString groupsize= subtokenizer.GetToken();
    XrdOucString groupmod = subtokenizer.GetToken();

    if (groupsize == "") {
      groupsize = "0";
    }

    if (groupmod == "") {
      groupmod = 24;
    }

    if (!nodename.length())
      printusage=true;

    in += "&mgm.space=";
    in += nodename;
    in += "&mgm.space.groupsize=";
    in += groupsize;
    in += "&mgm.space.groupmod=";
    in += groupmod;
    ok = true;
  }

  if ( subcommand == "set" ) {
    in ="mgm.cmd=space&mgm.subcmd=set";
    XrdOucString nodename = subtokenizer.GetToken();
    XrdOucString active   = subtokenizer.GetToken();

    if ( (!nodename.length()) || (!active.length()) ) 
      printusage=true;

    if ( (active != "on") && (active != "off") ) {
      printusage=true;
    }
    
    in += "&mgm.space=";
    in += nodename;
    in += "&mgm.space.state=";
    in += active;
    ok = true;
  }

  if ( subcommand == "rm" ) {
    in ="mgm.cmd=space&mgm.subcmd=rm";
    XrdOucString spacename = subtokenizer.GetToken();

    if (!spacename.length())
      printusage=true;
    in += "&mgm.space=";
    in += spacename;
    ok = true;
  }
  
  if ( subcommand == "quota" ) {
    in ="mgm.cmd=space&mgm.subcmd=quota";
    XrdOucString spacename = subtokenizer.GetToken();
    XrdOucString onoff = subtokenizer.GetToken();
    if ((!spacename.length()) || (!onoff.length()) ) {
      goto com_space_usage;
    }

    in += "&mgm.space=";in += spacename;
    in += "&mgm.space.quota="; in += onoff;
    ok = true;
  }
    
  if ( subcommand == "config" ) {
    XrdOucString spacename = subtokenizer.GetToken();
    XrdOucString keyval   = subtokenizer.GetToken();
    
    if ( (!spacename.length()) || (!keyval.length()) ) {
      goto com_space_usage;
    }
    
    if ( (keyval.find("=")) == STR_NPOS) {
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
    in += "&mgm.space.key="; in += token[0].c_str();
    in += "&mgm.space.value="; in += token[1].c_str();
    
    global_retc = output_result(client_admin_command(in));
    return (0);
  }

  if (printusage ||  (!ok))
    goto com_space_usage;

  result = client_admin_command(in);
  
  if (!silent) {
    global_retc = output_result(result, highlighting);
  } else {
    if (result) {
      global_retc = 0;
    } else {
      global_retc = EINVAL;
    }
  }
  
  return (0);

 com_space_usage:

  printf("usage: space ls                                                  : list spaces\n");
  printf("usage: space ls [-s] [-m|-l|--io] [<space>]                          : list in all spaces or select only <space>\n");
  printf("                                                                  -s : silent mode\n");
  printf("                                                                  -m : monitoring key=value output format\n");
  printf("                                                                  -l : long output - list also file systems after each space\n");
  printf("                                                                --io : print IO satistics\n");
  printf("       space config <space-name> space.nominalsize=<value>           : configure the nominal size for this space\n");
  printf("       space config <space-name> fs.<key>=<value>                    : configure file system parameters for each filesystem in this space (see help of 'fs config' for details)\n");
  printf("\n");
  printf("       space define <space-name> [<groupsize> [<groupmod>]]             : define how many filesystems can end up in one scheduling group <groupsize> [default=0]\n");
  printf("\n");
  printf("                                                                       => <groupsize>=0 means, that no groups are built within a space, otherwise it should be the maximum number of nodes in a scheduling group\n");
  printf("                                                                       => <groupmod> defines the maximun number of filesystems per node\n");
  printf("\n");
  printf("       space set <space-name> on|off                                 : enables/disabels all groups under that space ( not the nodes !) \n");
  printf("       space rm <space-name>                                         : remove space\n");
  printf("\n");
  printf("       space quota <space-name> on|off                               : enable/disable quota\n");
  return (0);
}
