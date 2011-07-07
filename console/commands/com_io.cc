/*----------------------------------------------------------------------------*/
#include "console/ConsoleMain.hh"
/*----------------------------------------------------------------------------*/

/* Namespace Interface */
int 
com_io (char* arg1) {
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString cmd = subtokenizer.GetToken();
  XrdOucString option="";
  XrdOucString options="";
  XrdOucString path="";
  XrdOucString in ="";
  if ( ( cmd != "stat") && ( cmd != "enable" ) && ( cmd != "disable") && ( cmd != "report" ) ) {
    goto com_io_usage;
  }
  
  in = "mgm.cmd=io&";

  if (cmd == "enable") {
    in += "mgm.subcmd=enable";
  }
  if (cmd == "disable") {
    in += "mgm.subcmd=disable";
  }

  if (cmd == "stat") {
    in += "mgm.subcmd=stat";
  }

  if (cmd == "report") {
    in += "mgm.subcmd=report";
    path = subtokenizer.GetToken();

    if (!path.length()) 
      goto com_io_usage;
    in += "&mgm.io.path=";
    in += path;
  }  else {
    do {
      option = subtokenizer.GetToken();
      if (!option.length())
        break;
      if (option == "-a") {
        options += "a";
      } else {
        if (option == "-m") {
          options += "m";
        }  else {
          if (option == "-n") {
            options += "n";
          } else {
            if ( option == "-t") {
              options += "t";
            } else {
              if ( option == "-r") {
                options += "r";
              } else {
                if ( option == "-n") {
                  options += "n";
                } else {
                  goto com_io_usage;
                }
              }
            }
          }
        }
      }
    } while(1);
  }
       
  if (options.length()) {
    in += "&mgm.option="; in += options;
  }  
  
  global_retc = output_result(client_admin_command(in));
  return (0);

 com_io_usage:
  printf("usage: io stat [-a] [-m] [-n]                                     :  print io statistics\n");
  printf("                -a                                                   -  break down by uid/gid\n");
  printf("                -m                                                   -  print in <key>=<val> monitoring format\n");
  printf("                -n                                                   -  print numerical uid/gids\n");
  printf("                -t                                                   -  print top user stats\n");
  printf("       io enable [-r] [-n]                                        :  enable collection of io statistics\n");
  printf("                                                               -r    enable collection of io reports\n");
  printf("                                                               -n    enable report namespace\n");
  printf("       io disable [-r] [-n]                                       :  disable collection of io statistics\n");
  printf("                                                               -r    disable collection of io reports\n");
  printf("                                                               -n    disable report namespace\n");
  printf("       io report <path>                                           :  show contents of report namespace for <path>\n");
  return (0);
}
