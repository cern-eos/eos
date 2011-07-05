/*----------------------------------------------------------------------------*/
#include "console/ConsoleMain.hh"
/*----------------------------------------------------------------------------*/

/* Clear the terminal screen */
int
com_clear (char *arg) {
  if (!strcmp(arg,"-h") || (!strcmp(arg,"--help"))) {
    printf("Usage: clear\n");
    printf("'[eos] clear' is equivalent to the interactive shell command to clear the screen.\n");
    return (0);
  }
  
  int rc = system("clear");
  return (rc); 
}
