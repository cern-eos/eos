/*----------------------------------------------------------------------------*/
#include "console/ConsoleMain.hh"
/*----------------------------------------------------------------------------*/

/* Clear the terminal screen */
int
com_clear (char *arg) {
  int rc = system("clear");
  return (rc);
}
