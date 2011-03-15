/*----------------------------------------------------------------------------*/
#include "console/ConsoleMain.hh"
/*----------------------------------------------------------------------------*/

/* Change working directory &*/
int
com_cd (char *arg) {
  XrdOucString newpath=abspath(arg);
  XrdOucString oldpwd = pwd;

  pwd = newpath;

  if (!pwd.endswith("/")) 
    pwd += "/";

  // filter "/./";
  while (pwd.replace("/./","/")) {}
  // filter "..";
  int dppos=0;
  while ( (dppos=pwd.find("/../")) != STR_NPOS) {
    if (dppos==0) {
      pwd = oldpwd;
      break;
    }
    int rpos = pwd.rfind("/",dppos-1);
    //    printf("%s %d %d\n", pwd.c_str(), dppos, rpos);
    if (rpos != STR_NPOS) {
      //      printf("erasing %d %d", rpos, dppos-rpos+3);
      pwd.erase(rpos, dppos-rpos+3);
    } else {
      pwd = oldpwd;
      break;
    }
  }

  if (!pwd.endswith("/")) 
    pwd += "/";

  return (0);
}
