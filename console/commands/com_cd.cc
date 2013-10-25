// ----------------------------------------------------------------------
// File: com_cd.cc
// Author: Andreas-Joachim Peters - CERN
// Author: Joaquim Rocha - CERN
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
#include "console/ConsoleCliCommand.hh"
/*----------------------------------------------------------------------------*/

/* Change working directory &*/
int
com_cd (char *arg)
{
  static XrdOucString opwd = "/";
  static XrdOucString oopwd = "/";
  XrdOucString lsminuss;
  XrdOucString newpath;
  XrdOucString oldpwd;

  ConsoleCliCommand cdCmd("cd", "provides the namespace change directory "
                          "command in EOS.");
  CliPositionalOption dirOption("dir",
                                "can be a directory path or symbol, e.g.:\n"
                                "cd -  : change to the previous directory\n"
                                "cd .. : change to the directory one level up\n"
                                "cd ~  : change to the directory pointed by "
                                "EOS_HOME; the same happens if no argument is "
                                "provided", 1, 1, "<dir>", false);
  cdCmd.addOption(dirOption);

  addHelpOptionRecursively(&cdCmd);

  cdCmd.parse(arg);

  if (checkHelpAndErrors(&cdCmd))
    return 0;

  // cd ~ (home)
  if (!cdCmd.hasValue("dir") || cdCmd.getValue("dir") == "~")
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
  // cd -
  else if (cdCmd.getValue("dir") == "-")
  {
    oopwd = opwd;
    newpath = abspath(opwd.c_str());
  }
  // cd <dir>
  else
  {
    newpath = abspath(cdCmd.getValue("dir").c_str());
  }

  opwd = pwd;
  oldpwd = pwd;
  pwd = newpath;

  if (!pwd.endswith("/"))
    pwd += "/";

  // filter "/./";
  while (pwd.replace("/./", "/")) {}

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

  if (!pwd.endswith("/"))
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
  return (0);
}
