// ----------------------------------------------------------------------
// File: com_io.cc
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

/* Namespace Interface */
int
com_io (char* arg1)
{
  eos::common::StringTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString cmd = subtokenizer.GetToken();
  XrdOucString option = "";
  XrdOucString options = "";
  XrdOucString path = "";
  XrdOucString in = "";
  XrdOucString target = "";

  if (wants_help(arg1))
    goto com_io_usage;

  if ((cmd != "stat") && (cmd != "enable") && (cmd != "disable") && (cmd != "report") && (cmd != "ns"))
  {
    goto com_io_usage;
  }

  in = "mgm.cmd=io&";

  if (cmd == "enable")
  {
    in += "mgm.subcmd=enable";
  }
  if (cmd == "disable")
  {
    in += "mgm.subcmd=disable";
  }

  if (cmd == "stat")
  {
    in += "mgm.subcmd=stat";
  }

  if (cmd == "report")
  {
    in += "mgm.subcmd=report";
    path = subtokenizer.GetToken();

    if (!path.length())
      goto com_io_usage;
    in += "&mgm.io.path=";
    in += path;
  }
  else
  {
    if (cmd == "ns")
    {
      in += "mgm.subcmd=ns";
      do
      {
        option = subtokenizer.GetToken();
        if (!option.length())
          break;
        if (option == "-m")
        {
          options += "-m";
        }
        else
        {
          if (option == "-100")
          {
            options += "-100";
          }
          else
          {
            if (option == "-1000")
            {
              options += "-1000";
            }
            else
            {
              if (option == "-10000")
              {
                options += "-10000";
              }
              else
              {
                if (option == "-a")
                {
                  options += "-a";
                }
                else
                {
                  if (option == "-b")
                  {
                    options += "-b";
                  }
                  else
                  {
                    if (option == "-n")
                    {
                      options += "-n";
                    }
                    else
                    {
                      if (option == "-w")
                      {
                        options += "-w";
                      } 
		      else
		      {
			if (option == "-f") 
			{
			  options += "-f";
			}
                        else
                        {
                          goto com_io_usage;
                        }
		      }
                    }
                  }
                }
              }
            }
          }
        }
      }
      while (1);
    }
    else
    {
      do
      {
        option = subtokenizer.GetToken();
        if (!option.length())
          break;
        if (option == "-a")
        {
          options += "a";
        }
        else
        {
          if (option == "-m")
          {
            options += "m";
          }
          else
          {
            if (option == "-n")
            {
              options += "n";
            }
            else
            {
              if (option == "-t")
              {
                options += "t";
              }
              else
              {
                if (option == "-r")
                {
                  options += "r";
                }
                else
                {
                  if (option == "-n")
                  {
                    options += "n";
                  }
                  else
                  {
                    if (option == "-d")
                    {
                      options += "d";
                    }
                    else
                    {
                      if (option == "-x")
                      {
                        options += "x";
                      }
                      else
                      {
                        if (option == "-l")
                        {
                          options += "l";
                        }
                        else
                        {
                          if (option == "-p")
                          {
                            options += "p";
                          }
                          else
                          {
                            if (option == "--udp")
                            {
                              target = subtokenizer.GetToken();
                              if ((!target.length()) || (target.beginswith("-")))
                              {
                                goto com_io_usage;
                              }
                            }
                            else
                            {
                              goto com_io_usage;
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
      while (1);
    }
  }


  if (options.length())
  {
    in += "&mgm.option=";
    in += options;
  }
  if (target.length())
  {
    in += "&mgm.udptarget=";
    in += target;
  }
  global_retc = output_result(client_admin_command(in));
  return (0);

com_io_usage:
  fprintf(stdout, "usage: io stat [-l] [-a] [-m] [-n] [-t] [-d] [-x]               :  print io statistics\n");
  fprintf(stdout, "                -l                                                   -  show summary information (this is the default if -t,-d,-x is not selected)\n");
  fprintf(stdout, "                -a                                                   -  break down by uid/gid\n");
  fprintf(stdout, "                -m                                                   -  print in <key>=<val> monitoring format\n");
  fprintf(stdout, "                -n                                                   -  print numerical uid/gids\n");
  fprintf(stdout, "                -t                                                   -  print top user stats\n");
  fprintf(stdout, "                -d                                                   -  break down by domains\n");
  fprintf(stdout, "                -x                                                   -  break down by application\n");
  fprintf(stdout, "       io enable [-r] [-p] [-n] [--udp <address>]                 :  enable collection of io statistics\n");
  fprintf(stdout, "                                                               -r    enable collection of io reports\n");
  fprintf(stdout, "                                                               -p    enable popularity accounting\n");
  fprintf(stdout, "                                                               -n    enable report namespace\n");
  fprintf(stdout, "                                                               --udp <address> add a UDP message target for io UDP packtes (the configured targets are shown by 'io stat -l'\n");
  fprintf(stdout, "       io disable [-r] [-p] [-n]                                       :  disable collection of io statistics\n");
  fprintf(stdout, "                                                               -r    disable collection of io reports\n");
  fprintf(stdout, "                                                               -p    disable popularity accounting\n");
  fprintf(stdout, "                                                               --udp <address> remove a UDP message target for io UDP packtes\n");
  fprintf(stdout, "                                                               -n    disable report namespace\n");
  fprintf(stdout, "       io report <path>                                           :  show contents of report namespace for <path>\n");
  fprintf(stdout, "       io ns [-a] [-n] [-b] [-100|-1000|-10000] [-w] [-f]         :  show namespace IO ranking (popularity)\n");
  fprintf(stdout, "                                                               -a    don't limit the output list\n");
  fprintf(stdout, "                                                               -n :  show ranking by number of accesses \n");
  fprintf(stdout, "                                                               -b :  show ranking by number of bytes\n");
  fprintf(stdout, "                                                             -100 :  show the first 100 in the ranking\n");
  fprintf(stdout, "                                                            -1000 :  show the first 1000 in the ranking\n");
  fprintf(stdout, "                                                           -10000 :  show the first 10000 in the ranking\n");
  fprintf(stdout, "                                                               -w :  show history for the last 7 days\n");
  fprintf(stdout, "                                                               -f :  show the 'hotfiles' which are the files with highest number of present file opens\n");
  return (0);
}
