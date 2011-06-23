/*----------------------------------------------------------------------------*/
#include "console/ConsoleMain.hh"
/*----------------------------------------------------------------------------*/

/* Change working directory &*/
int
com_cd (char *arg) {
  static XrdOucString opwd="/";
  // cd -
  if (!strcmp(arg,"-")) {
    arg = (char*) opwd.c_str();
    fprintf(stderr,"setting arg to %s\n", arg);
  }
  opwd=pwd;

  XrdOucString newpath=abspath(arg);
  XrdOucString oldpwd = pwd;

  // cd ~ (home)
  if (!arg || (!strlen(arg)) || (!strcmp(arg,"~"))) {
    if (getenv("EOS_HOME")) {
      newpath = abspath(getenv("EOS_HOME"));
    }
  }

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

  // check if this exists, otherwise go back to oldpwd

  XrdOucString lsminuss = "mgm.cmd=ls&mgm.path="; lsminuss += pwd;lsminuss+= "&mgm.option=s";
  global_retc = output_result(client_user_command(lsminuss));
  if (global_retc) { 
    pwd = oldpwd;
  }
  return (0);
}
