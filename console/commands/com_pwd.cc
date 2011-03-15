/*----------------------------------------------------------------------------*/
#include "console/ConsoleMain.hh"
/*----------------------------------------------------------------------------*/

/* Print working directory */
int
com_pwd (char *arg) {
  fprintf(stdout,"%s\n",pwd.c_str());
  return (0);
}
