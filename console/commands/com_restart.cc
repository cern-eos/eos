/*----------------------------------------------------------------------------*/
#include "console/ConsoleMain.hh"
/*----------------------------------------------------------------------------*/

/* Restart System */
int
com_restart (char* arg1) {
  // split subcommands
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString nodes = subtokenizer.GetToken();
  XrdOucString selection = subtokenizer.GetToken();

  XrdOucString in = "mgm.cmd=restart&mgm.subcmd="; 
  if (nodes.length()) {
    in += nodes;
    if (selection.length()) {
      in += "&mgm.nodename=";
      in += selection;
    }
    
    global_retc = output_result(client_admin_command(in));
    return (0);
  }
  
  printf("       restart fst [*]                         : restart all services on fst nodes !\n");
  return (0);
}
