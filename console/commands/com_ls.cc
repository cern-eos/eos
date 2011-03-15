/*----------------------------------------------------------------------------*/
#include "console/ConsoleMain.hh"
/*----------------------------------------------------------------------------*/

/* List a directory */
int
com_ls (char* arg1) {
  // split subcommands
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString param="";
  XrdOucString option="";
  XrdOucString path="";
  XrdOucString in = "mgm.cmd=ls"; 

  do {
    param = subtokenizer.GetToken();
    if (!param.length())
      break;
    if (param.beginswith("-")) {
      option+= param;
      if ( (option.find("&")) != STR_NPOS) {
        goto com_ls_usage;
      }
    } else {
      path = param;
      break;
    }
  } while(1);

  if (!path.length()) {
    path = pwd;
  } 

  path = abspath(path.c_str());

  in += "&mgm.path=";
  in += path;
  in += "&mgm.option=";
  in += option;
  global_retc = output_result(client_user_command(in));
  return (0);

 com_ls_usage:
  printf("usage: ls <path>                                                       :  list directory <path>\n");
  return (0);
}
