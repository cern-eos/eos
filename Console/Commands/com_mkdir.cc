/*----------------------------------------------------------------------------*/
#include "ConsoleMain.hh"
/*----------------------------------------------------------------------------*/


/* Create a directory */
int
com_mkdir (char* arg1) {
  // split subcommands
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString path = subtokenizer.GetToken();
  XrdOucString in = "mgm.cmd=mkdir"; 

  if (path == "-p") {
    path = subtokenizer.GetToken();
    in += "&mgm.option=p";
  }
  if (!path.length()) {
    goto com_mkdir_usage;
    
  } else {
    path = abspath(path.c_str());
    in += "&mgm.path=";
    in += path;
    
    global_retc = output_result(client_user_command(in));
    return (0);
  }

 com_mkdir_usage:
  printf("usage: mkdir -p <path>                                                :  create directory <path>\n");
  return (0);

}
