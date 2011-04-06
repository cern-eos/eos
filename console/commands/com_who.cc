/*----------------------------------------------------------------------------*/
#include "console/ConsoleMain.hh"
/*----------------------------------------------------------------------------*/

/* Who is connected -  Interface */
int 
com_who (char* arg1) {
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString option="";
  XrdOucString options="";

  XrdOucString in ="";
  
  in = "mgm.cmd=who";
  do {
    option = subtokenizer.GetToken();
    if (!option.length())
      break;
    if (option == "-c") {
      options += "c";
    }  else {
      if (option == "-n") {
        options += "n";
      } else { 
        if (option == "-a") {
          options += "a";
        } else {
          if (option == "-z") {
            options += "z";
          } else {
            if (option == "-m") {
              options += "m";
            } else {
              goto com_who_usage;
            }
          }
        }
      }
    }
  } while(1);
       
  if (options.length()) {
    in += "&mgm.option="; in += options;
  }  
  
  global_retc = output_result(client_user_command(in));
  return (0);

 com_who_usage:
  printf("usage: who [-c] [-n] [-z] [-a] [-m]                               :  print statistics about active users (idle<5min)\n");
  printf("                -c                                                   -  break down by client host\n");
  printf("                -n                                                   -  print id's instead of names\n");
  printf("                -z                                                   -  print auth protocols\n");
  printf("                -a                                                   -  print all\n");
  printf("                -m                                                   -  print in monitoring format <key>=<value>\n");
  return (0);
}
