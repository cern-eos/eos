/*----------------------------------------------------------------------------*/
#include "console/ConsoleMain.hh"
/*----------------------------------------------------------------------------*/

using namespace eos::common;

/* Node listing, configuration, manipulation */
int
com_node (char* arg1) {
  XrdOucString in = "";
  // split subcommands
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString subcommand = subtokenizer.GetToken();
  if ( subcommand == "ls" ) {
    XrdOucString in ="mgm.cmd=node&mgm.subcmd=ls";
    bool silent = false;
    bool printusage=false;
    bool highlighting=true;
    XrdOucString option="";

    do {
      bool ok=false;
      subtokenizer.GetLine();
      option = subtokenizer.GetToken();
      if (option.length()) {
	if (option == "-m") {
	  in += "&mgm.outformat=mon";
	  ok=true;
	  highlighting=false;
	} 
	if (option == "-s") {
	  silent=true;
	  ok=true;
	}
	if (!ok) 
	  printusage=true;
      }
    } while(option.length());
    
    if (printusage)
      goto com_node_usage;

    XrdOucEnv* result = client_admin_command(in);

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
  }

  if ( subcommand == "set" ) {
    return (0);
  }

  if ( subcommand == "rm" ) {
    return (0);

  }

 com_node_usage:

  printf("usage: node ls                                                  : list nodes\n");
  //  printf("       fs set   <fs-name> <fs-id> [-sched <group> ] [-force]    : configure filesystem with name and id\n");
  //  printf("       fs rm    <fs-name>|<fs-id>                               : remove filesystem configuration by name or id\n");
  return (0);
}
