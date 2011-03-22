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

  // split subcommands
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString subcommand = subtokenizer.GetToken();
  if ( subcommand == "ls" ) {
    in ="mgm.cmd=space&mgm.subcmd=ls";
    option="";

    do {
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
	if (option == "-s") {
	  silent=true;
	  ok=true;
	}
	if (!ok) 
	  printusage=true;
      } else {
	ok=true;
      }
    } while(option.length());
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
  printf("usage: space ls [-s] [-m|-l]                                        : list spaces\n");
  printf("                                                                  -s : silent mode\n");
  printf("                                                                  -m : monitoring key=value output format\n");
  printf("                                                                  -l : long output - list also file systems after each space\n");
  printf("       space rm <space-name>                                         : remove space\n");
  return (0);
}
