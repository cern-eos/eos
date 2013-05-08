// ----------------------------------------------------------------------
// File: com_stat.cc
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

/* Stat a directory or a file */
int
com_stat (char* arg1)
{
  // split subcommands
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString param = "";
  XrdOucString option = "";
  XrdOucString path = "";
  XrdOucString sizestring;
  struct stat buf;
  XrdOucString url = serveruri.c_str();

  do
  {
    param = subtokenizer.GetToken();
    if (!param.length())
      break;
    if (param == "--help")
      goto com_stat_usage;
    if (param == "-h")
      goto com_stat_usage;

    if (param.beginswith("-"))
    {
      while (param.replace("-", ""))
      {
      }
      option += param;
      if ((option.find("&")) != STR_NPOS)
      {
        goto com_stat_usage;
      }
    }
    else
    {
      path = param;
      break;
    }
  }
  while (1);

  if (!path.length())
  {
    path = pwd;
  }
  if ((option.length()) && ((option != "f") && (option != "d")))
  {
    goto com_stat_usage;
  }

  path = abspath(path.c_str());

  url += "/";
  url += path;
  if (!XrdPosixXrootd::Stat(url.c_str(), &buf))
  {
    if ((option.find("f") != STR_NPOS))
    {
      if (S_ISREG(buf.st_mode))
      {
        global_retc = 0;
        return (0);
      }
      else
      {
        global_retc = 1;
        return (0);
      }
    }
    if ((option.find("d") != STR_NPOS))
    {
      if (S_ISDIR(buf.st_mode))
      {
        global_retc = 0;
        return (0);
      }
      else
      {
        global_retc = 1;
        return (0);
      }
    }
    fprintf(stdout, "  File: `%s'", path.c_str());
    if (S_ISDIR(buf.st_mode))
    {
      fprintf(stdout, " directory\n");
    }
    if (S_ISREG(buf.st_mode))
    {
      fprintf(stdout, "  Size: %llu            %s", (unsigned long long) buf.st_size, eos::common::StringConversion::GetReadableSizeString(sizestring, (unsigned long long) buf.st_size, "B"));
      fprintf(stdout, " regular file\n");
    }
    global_retc = 0;
  }
  else
  {
    fprintf(stderr, "error: failed to stat %s\n", path.c_str());
    global_retc = EFAULT;
    return (0);
  }

  return (0);

com_stat_usage:
  fprintf(stdout, "usage: stat [-f|-d]    <path>                                                  :  stat <path>\n");
  fprintf(stdout, "                    -f : checks if <path> is a file\n");
  fprintf(stdout, "                    -d : checks if <path> is a directory\n");
  return (0);
}
