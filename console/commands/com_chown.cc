/*----------------------------------------------------------------------------*/
#include "console/ConsoleMain.hh"
/*----------------------------------------------------------------------------*/

/* Owner Interface */
int 
com_chown (char* arg1) {
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString owner = subtokenizer.GetToken();
  XrdOucString option="";
  XrdOucString in = "mgm.cmd=chown";
  XrdOucString arg = "";

  if (owner.beginswith("-")) {
    option = owner;
    option.erase(0,1);
    owner = subtokenizer.GetToken();
    in += "&mgm.chown.option=";
    in += option;
  }

  XrdOucString path = subtokenizer.GetToken();

  if ( !path.length() || !owner.length() ) 
    goto com_chown_usage;

  path = abspath(path.c_str());

  in += "&mgm.path="; in += path;
  in += "&mgm.chown.owner="; in += owner;

  global_retc = output_result(client_admin_command(in));
  return (0);

 com_chown_usage:
  printf("Usage: chown [-r] <owner>[:<group>] <dir-path>\n");
  printf("'[eos] chown ..' provides the change owner interface of EOS.\n");
  printf("<dir-path> is the directory to modify, <owner> has to be a user id or user name. <group> is optional and has to be a group id or group name. Remark: EOS has no ownership on file level!\n\n");
  printf("Options:\n");
  printf("                  -r : recursive\n");
  return (0);
}
