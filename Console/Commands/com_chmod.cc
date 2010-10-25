/*----------------------------------------------------------------------------*/
#include "ConsoleMain.hh"
/*----------------------------------------------------------------------------*/

/* Mode Interface */
int 
com_chmod (char* arg1) {
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString mode = subtokenizer.GetToken();
  XrdOucString option="";
  XrdOucString in = "mgm.cmd=chmod";
  XrdOucString arg = "";

  if (mode.beginswith("-")) {
    option = mode;
    option.erase(0,1);
    mode = subtokenizer.GetToken();
    in += "&mgm.option=";
    in += option;
  }

  XrdOucString path = subtokenizer.GetToken();
  if ( !path.length() || !mode.length() ) 
    goto com_chmod_usage;
  in += "&mgm.path="; in += path;
  in += "&mgm.chmod.mode="; in += mode;

  global_retc = output_result(client_user_command(in));
  return (0);

 com_chmod_usage:
  printf("usage: chmod [-r] <mode> <path>                             : set mode for <path> (-r recursive)\n");  
  printf("                 <mode> can only numerical like 755, 644, 700\n");
  printf("                 <mode> to switch on attribute inheritance use 2755, 2644, 2700 ...\n");
  return (0);
}
