/*----------------------------------------------------------------------------*/
#include "ConsoleMain.hh"
/*----------------------------------------------------------------------------*/

/* Remove a directory */
int
com_rmdir (char* arg1) {
  // split subcommands
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString path = subtokenizer.GetToken();
  XrdOucString selection = subtokenizer.GetToken();

  XrdOucString in = "mgm.cmd=rmdir&"; 
  if (!path.length()) {
    goto com_rmdir_usage;
    
  } else {
    path = abspath(path.c_str());
    in += "mgm.path=";
    in += path;
    
    global_retc = output_result(client_user_command(in));
    return (0);
  }

 com_rmdir_usage:
  printf("usage: rmdir <path>                                                   :  remote directory <path>\n");
  return (0);

}
