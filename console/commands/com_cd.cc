// ----------------------------------------------------------------------
// File: com_cd.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

/*----------------------------------------------------------------------------*/
#include "console/ConsoleMain.hh"
/*----------------------------------------------------------------------------*/

/* Change working directory &*/
int
com_cd (char *arg1)
{
  static XrdOucString opwd = "/";
  static XrdOucString oopwd = "/";
  XrdOucString lsminuss;
  XrdOucString newpath;
  XrdOucString oldpwd;

  eos::common::StringTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString arg = subtokenizer.GetToken();

  if ( (arg.find("--help")!=STR_NPOS) || (arg.find("-h")!=STR_NPOS) )
    goto com_cd_usage;


  // cd -
  if (arg == "-")
  {
    oopwd = opwd;
    arg = (char*) opwd.c_str();
  }

  opwd = pwd;

  newpath = abspath(arg.c_str());
  oldpwd = pwd;

  // cd ~ (home)
  if ( (arg=="") || (arg== "~") )
  {
    if (getenv("EOS_HOME"))
    {
      newpath = abspath(getenv("EOS_HOME"));
    }
    else
    {
      fprintf(stderr, "warning: there is no home directory defined via EOS_HOME\n");
      newpath = opwd;
    }
  }

  pwd = newpath;

  if ((!pwd.endswith("/")) && (!pwd.endswith("/\"")))
    pwd += "/";

  // filter "/./";
  while (pwd.replace("/./", "/"))
  {
  }
  // filter "..";
  int dppos;
  dppos = 0;
  while ((dppos = pwd.find("/../")) != STR_NPOS)
  {
    if (dppos == 0)
    {
      pwd = oldpwd;
      break;
    }
    int rpos = pwd.rfind("/", dppos - 1);
    //    fprintf(stdout,"%s %d %d\n", pwd.c_str(), dppos, rpos);
    if (rpos != STR_NPOS)
    {
      //      fprintf(stdout,"erasing %d %d", rpos, dppos-rpos+3);
      pwd.erase(rpos, dppos - rpos + 3);
    }
    else
    {
      pwd = oldpwd;
      break;
    }
  }

  if ((!pwd.endswith("/")) && (!pwd.endswith("/\"")))
    pwd += "/";

  // check if this exists, otherwise go back to oldpwd

  lsminuss = "mgm.cmd=cd&mgm.path=";
  lsminuss += pwd;
  lsminuss += "&mgm.option=s";
  global_retc = output_result(client_user_command(lsminuss));
  if (global_retc)
  {
    pwd = oldpwd;
  }
  else
  {
    if (pwdfile.length()) 
    {
    // store the last used directory
    int cfd = open(pwdfile.c_str(), O_CREAT | O_TRUNC | O_RDWR, S_IRWXU);
    if (cfd >= 0)
    {
      if ((::write(cfd, pwd.c_str(), pwd.length())) != pwd.length())
      {
        fprintf(stderr, "warning: unable to store CWD to %s [errno=%d]\n", pwdfile.c_str(), errno);
      }
      close(cfd);
    }
    else
    {
      fprintf(stderr, "warning: unable to store CWD to %s\n", pwdfile.c_str());
      }
    }
  }
  return (0);

com_cd_usage:
  fprintf(stdout, "'[eos] cd ...' provides the namespace change directory command in EOS.\n");
  fprintf(stdout, "Usage: cd <dir>|-|..|~\n");
  fprintf(stdout, "Options:\n");
  fprintf(stdout, "cd <dir> :\n");
  fprintf(stdout, "                                                  change into direcotry <dir>. If it does not exist, the current directory will stay as before!\n");
  fprintf(stdout, "cd - :\n");
  fprintf(stdout, "                                                  change into the previous directory\n");
  fprintf(stdout, "cd .. :\n");
  fprintf(stdout, "                                                  change into the directory one level up\n");
  fprintf(stdout, "cd ~ :\n");
  fprintf(stdout, "                                                  change into the directory defined via the environment variable EOS_HOME\n");
  return (0);
}
