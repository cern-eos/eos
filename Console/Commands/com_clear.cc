/*----------------------------------------------------------------------------*/
#include "ConsoleMain.hh"
/*----------------------------------------------------------------------------*/

/* Clear the terminal screen */
int
com_clear (char *arg) {
  int src = system("clear");
  if (src) {
    src = 0;
  }
  return (0);
}
