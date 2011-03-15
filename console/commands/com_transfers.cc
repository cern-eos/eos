/*----------------------------------------------------------------------------*/
#include "console/ConsoleMain.hh"
/*----------------------------------------------------------------------------*/

/* Transfer Interface */
int
com_transfers (char* arg1) {
  // split subcommands
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString subcmd = subtokenizer.GetToken();
  XrdOucString nodes = subtokenizer.GetToken();
  XrdOucString selection = subtokenizer.GetToken();

  XrdOucString in = "mgm.cmd=";

  if ( (subcmd != "drop") && (subcmd != "ls") ) 
    goto com_usage_transfers;

  if (subcmd == "drop")
    in += "droptransfers";
  if (subcmd == "ls") 
    in += "listtransfers";
  
  in += "&mgm.subcmd=";

  if (nodes.length()) {
    in += nodes;
    if (selection.length()) {
      in += "&mgm.nodename=";
      in += selection;
    }
    
    global_retc = output_result(client_admin_command(in));
    return (0);
  }
  
 com_usage_transfers:
  printf("       transfers drop fst *                 : drop transfers on all fst nodes !\n");
  printf("       transfers ls fst *                   : list transfers on all fst nodes !\n");
  return (0);
}
