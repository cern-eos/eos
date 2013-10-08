// ----------------------------------------------------------------------
// File: com_test.cc
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

extern int com_mkdir (char*);
extern int com_rmdir (char*);
extern int com_ls (char*);

/* Test Interface */
int
com_test (char* arg1)
{
  ConsoleCliCommand testCmd("test", "set mode for <path>");
  testCmd.addGroupedOptions({{"mkdir", "", "mkdir"},
                             {"rmdir", "", "rmdir"},
                             {"ls", "", "ls"},
                             {"lsla", "", "lsla"}
                            })->setRequired(true);
  CliPositionalOption loopOption("loop", "loop number", 1, 1, "<loop-number>",
                                 true);
  loopOption.addEvalFunction(optionIsPositiveNumberEvalFunc, 0);
  testCmd.addOption(loopOption);

  addHelpOptionRecursively(&testCmd);

  testCmd.parse(arg1);

  if (checkHelpAndErrors(&testCmd))
    return 0;

  int n = atoi(testCmd.getValue("loop").c_str());
  fprintf(stdout, "info: doing directory test with loop <n>=%d", n);

  if (testCmd.hasValue("mkdir"))
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
  else if (testCmd.hasValue("rmdir"))
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
  else if (testCmd.hasValue("ls"))
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
  else if (testCmd.hasValue("lsla"))
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

  return (0);
}
