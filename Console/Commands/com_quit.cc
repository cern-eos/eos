/*----------------------------------------------------------------------------*/
#include "ConsoleMain.hh"
/*----------------------------------------------------------------------------*/

/* The user wishes to quit using this program.  Just set DONE non-zero. */
int
com_quit (char *arg) {
  done = 1;
  return (0);
}
