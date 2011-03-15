/*----------------------------------------------------------------------------*/
#include "ConsoleMain.hh"
/*----------------------------------------------------------------------------*/

/* Determine the mapping on server side */
int
com_whoami (char *arg) {
  XrdOucString in = "mgm.cmd=whoami"; 
  global_retc = output_result(client_user_command(in));
  return (0);
}
