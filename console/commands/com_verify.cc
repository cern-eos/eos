/*----------------------------------------------------------------------------*/
#include "console/ConsoleMain.hh"
/*----------------------------------------------------------------------------*/

/* Verify Interface */
int
com_verify (char* arg1) {
  // split subcommands
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString subcmd = subtokenizer.GetToken();
  XrdOucString nodes = subtokenizer.GetToken();
  XrdOucString selection = subtokenizer.GetToken();

  XrdOucString in = "mgm.cmd=";

  if ( (subcmd != "drop") && (subcmd != "ls") ) 
    goto com_usage_verify;

  if (subcmd == "drop")
    in += "dropverifications";
  if (subcmd == "ls") 
    in += "listverifications";
  
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
  
 com_usage_verify:
  printf("       verify drop fst *                   : drop transfers on all fst nodes !\n");
  printf("       verify ls fst *                     : list transfers on all fst nodes !\n");
  return (0);
}
