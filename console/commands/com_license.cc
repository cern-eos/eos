/*----------------------------------------------------------------------------*/
#include "console/ConsoleMain.hh"
/*----------------------------------------------------------------------------*/

#include "../../License"

/* Display License File*/
int
com_license (char *arg) {
  fprintf(stderr,license);
  global_retc = 0;
  return (0);
}
