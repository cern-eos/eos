/*----------------------------------------------------------------------------*/
#include "console/ConsoleMain.hh"
/*----------------------------------------------------------------------------*/

/* Get the server version*/
int
com_version (char *arg) {
  XrdOucString in = "mgm.cmd=version"; 
  global_retc = output_result(client_user_command(in));
  fprintf(stdout,"EOS_CLIENT_VERSION=%s EOS_CLIENT_RELEASE=%s\n", VERSION, RELEASE);
  return (0);
}
 
