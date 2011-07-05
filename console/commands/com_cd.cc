/*----------------------------------------------------------------------------*/
#include "console/ConsoleMain.hh"
/*----------------------------------------------------------------------------*/

/* Change working directory &*/
int
com_cd (char *arg) {
  static XrdOucString opwd="/";
  static XrdOucString oopwd="/";
  XrdOucString lsminuss;
  XrdOucString newpath;
  XrdOucString oldpwd;

  if (!strcmp(arg,"--help") || !strcmp(arg,"-h"))
    goto com_cd_usage;

    
  // cd -
  if (!strcmp(arg,"-")) {
    oopwd = opwd;
    arg = (char*) opwd.c_str();
  }

  opwd=pwd;

  newpath =abspath(arg);
  oldpwd = pwd;

  // cd ~ (home)
  if (!arg || (!strlen(arg)) || (!strcmp(arg,"~"))) {
    if (getenv("EOS_HOME")) {
      newpath = abspath(getenv("EOS_HOME"));
    } else {
      fprintf(stderr,"warning: there is no home directory defined via EOS_HOME\n");
      newpath = opwd;
    }
  }

  pwd = newpath;

  if (!pwd.endswith("/")) 
    pwd += "/";

  // filter "/./";
  while (pwd.replace("/./","/")) {}
  // filter "..";
  int dppos;
  dppos=0;
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

  lsminuss = "mgm.cmd=cd&mgm.path="; lsminuss += pwd;lsminuss+= "&mgm.option=s";
  global_retc = output_result(client_user_command(lsminuss));
  if (global_retc) { 
    pwd = oldpwd;
  }
  return (0);

 com_cd_usage:
  printf("'[eos] cd ...' provides the namespace change directory command in EOS.\n");
  printf("Usage: cd <dir>|-|..|~\n");
  printf("Options:\n");
  printf("cd <dir> :\n");
  printf("                                                  change into direcotry <dir>. If it does not exist, the current directory will stay as before!\n");
  printf("cd - :\n");
  printf("                                                  change into the previous directory\n");
  printf("cd .. :\n");
  printf("                                                  change into the directory one level up\n");
  printf("cd ~ :\n");
  printf("                                                  change into the directory defined via the environment variable EOS_HOME\n");
  return (0);
}
