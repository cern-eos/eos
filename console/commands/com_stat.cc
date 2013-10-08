// ----------------------------------------------------------------------
// File: com_stat.cc
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
/*----------------------------------------------------------------------------*/

/* Stat a directory or a file */
int
com_stat (char* arg1)
{
  XrdOucString path = "";
  XrdOucString sizestring;
  struct stat buf;
  XrdOucString url = serveruri.c_str();
  ConsoleCliCommand statCmd("stat", "stat <path> or current directory, "
                            "if <path> is not given");
  statCmd.addGroupedOptions({{"check-file", "checks if <path> is a file", "-f"},
                             {"check-dir", "checks if <path> is a directory",
                              "-d"}
                            });
  statCmd.addOption({"path", "", 1, 1, "<path>", false});

  addHelpOptionRecursively(&statCmd);

  statCmd.parse(arg1);

  if (checkHelpAndErrors(&statCmd))
    return 0;

  if (statCmd.hasValue("path"))
    path = statCmd.getValue("path").c_str();
  else
    path = pwd;

  path = cleanPath(path.c_str());

  url += "/";
  url += path;
  if (!XrdPosixXrootd::Stat(url.c_str(), &buf))
  {
    if (statCmd.hasValue("check-file"))
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
    if (statCmd.hasValue("check-dir"))
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
}
