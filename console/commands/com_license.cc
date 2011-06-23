/*----------------------------------------------------------------------------*/
#include "console/ConsoleMain.hh"
/*----------------------------------------------------------------------------*/

extern char* license;

/* Display License File*/
int
com_license (char *arg) {
  fprintf(stderr,"%s",license);
  global_retc = 0;
  return (0);
}
