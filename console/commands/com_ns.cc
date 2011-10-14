/*----------------------------------------------------------------------------*/
#include "console/ConsoleMain.hh"
/*----------------------------------------------------------------------------*/

/* Namespace Interface */
int 
com_ns (char* arg1) {
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString cmd = subtokenizer.GetToken();
  XrdOucString option="";
  XrdOucString options="";

  XrdOucString in ="";
  if ( ( cmd != "stat")  && ( cmd != "" ) && ( cmd != "compact" ) ) {
    goto com_ns_usage;
  }
  
  in = "mgm.cmd=ns&";
  if (cmd == "stat") {
    in += "mgm.subcmd=stat";
  }

  if (cmd == "compact") {
    in += "mgm.subcmd=compact";
  }

  do {
    option = subtokenizer.GetToken();
    if (!option.length())
      break;
    if (option == "-a") {
      options += "a";
    } else {
      if (option == "-m") {
        options += "m";
      } else {
        if (option == "-n") {
          options += "n";
        } else {
          goto com_ns_usage;
        }
      }
    }
  } while(1);
       
  if (options.length()) {
    in += "&mgm.option="; in += options;
  }  
  
  global_retc = output_result(client_admin_command(in));
  return (0);

 com_ns_usage:
  printf("Usage: ns                                                         :  print basic namespace parameters\n");
  printf("       ns stat [-a] [-m] [-n]                                     :  print namespace statistics\n");
  printf("                -a                                                   -  break down by uid/gid\n");
  printf("                -m                                                   -  print in <key>=<val> monitoring format\n");
  printf("                -n                                                   -  print numerical uid/gids\n");
  printf("       ns compact                                                    -  compact the current changelogfile and reload the namespace\n");
  return (0);
}
