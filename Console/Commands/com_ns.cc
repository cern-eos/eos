/*----------------------------------------------------------------------------*/
#include "ConsoleMain.hh"
/*----------------------------------------------------------------------------*/

/* Namespace Interface */
int 
com_ns (char* arg1) {
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString cmd = subtokenizer.GetToken();
  XrdOucString option = subtokenizer.GetToken();

  XrdOucString in ="";
  if ( ( cmd != "stat") ) {
    goto com_ns_usage;
  }
  
  in = "mgm.cmd=ns&";
  if (cmd == "stat") {
    in += "mgm.subcmd=stat";
  }

  if (option == "-a") {
    in += "&mgm.option=a";
  }
  
  global_retc = output_result(client_admin_command(in));
  return (0);

 com_ns_usage:
  printf("usage: ns stat [-a]                                                  :  print namespace statistics\n");
  printf("                -a                                                   -  break down by uid/gid\n");
  return (0);
}
