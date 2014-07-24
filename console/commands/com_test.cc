// ----------------------------------------------------------------------
// File: com_test.cc
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

extern int com_mkdir (char*);
extern int com_rmdir (char*);
extern int com_ls (char*);

/* Test Interface */
int
com_test (char* arg1)
{
  // split subcommands
  eos::common::StringTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();

  do
  {
    XrdOucString tag = subtokenizer.GetToken();
    if (!tag.length())
      break;

    XrdOucString sn = subtokenizer.GetToken();
    if (!sn.length())
    {
      goto com_test_usage;
    }

    int n = atoi(sn.c_str());
    fprintf(stdout, "info: doing directory test with loop <n>=%d", n);

    if (tag == "mkdir")
    {
      XrdMqTiming timing("mkdir");

      TIMING("start", &timing);

      for (int i = 0; i < 10; i++)
      {
        char dname[1024];
        sprintf(dname, "/test/%02d", i);
        XrdOucString cmd = "";
        cmd += dname;
        //      fprintf(stdout,"===> %s\n", cmd.c_str());
        com_mkdir((char*) cmd.c_str());

        for (int j = 0; j < n / 10; j++)
        {
          sprintf(dname, "/test/%02d/%05d", i, j);
          XrdOucString cmd = "";
          cmd += dname;
          //      fprintf(stdout,"===> %s\n", cmd.c_str());
          com_mkdir((char*) cmd.c_str());
        }
      }
      TIMING("stop", &timing);
      timing.Print();
    }

    if (tag == "rmdir")
    {
      XrdMqTiming timing("rmdir");
      TIMING("start", &timing);

      for (int i = 0; i < 10; i++)
      {
        char dname[1024];
        sprintf(dname, "/test/%02d", i);
        XrdOucString cmd = "";
        cmd += dname;
        //fprintf(stdout,"===> %s\n", cmd.c_str());

        for (int j = 0; j < n / 10; j++)
        {
          sprintf(dname, "/test/%02d/%05d", i, j);
          XrdOucString cmd = "";
          cmd += dname;
          //fprintf(stdout,"===> %s\n", cmd.c_str());
          com_rmdir((char*) cmd.c_str());
        }
        com_rmdir((char*) cmd.c_str());
      }
      TIMING("stop", &timing);
      timing.Print();
    }

    if (tag == "ls")
    {
      XrdMqTiming timing("ls");
      TIMING("start", &timing);

      for (int i = 0; i < 10; i++)
      {
        char dname[1024];
        sprintf(dname, "/test/%02d", i);
        XrdOucString cmd = "";
        cmd += dname;
        com_ls((char*) cmd.c_str());
      }
      TIMING("stop", &timing);
      timing.Print();
    }

    if (tag == "lsla")
    {
      XrdMqTiming timing("lsla");
      TIMING("start", &timing);

      for (int i = 0; i < 10; i++)
      {
        char dname[1024];
        sprintf(dname, "/test/%02d", i);
        XrdOucString cmd = "-la ";
        cmd += dname;
        com_ls((char*) cmd.c_str());
      }
      TIMING("stop", &timing);
      timing.Print();
    }
  }
  while (1);

  return (0);
com_test_usage:
  fprintf(stdout, "usage: test [mkdir|rmdir|ls|lsla <N> ]                                             :  run performance test\n");
  return (0);

}
