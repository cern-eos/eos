// ----------------------------------------------------------------------
// File: com_ns.cc
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
#include "common/RWMutex.hh"
/*----------------------------------------------------------------------------*/

/* Namespace Interface */
int
com_ns (char* arg1)
{
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString cmd = subtokenizer.GetToken();
  XrdOucString option = "";
  XrdOucString options = "";

  XrdOucString in = "";
  XrdOucString state;
  XrdOucString delay;
  XrdOucString interval;

  if ((cmd != "stat") && (cmd != "") && (cmd != "compact") && (cmd != "master") && (cmd != "mutex"))
  {
    goto com_ns_usage;
  }

  in = "mgm.cmd=ns&";
  if (cmd == "stat")
  {
    in += "mgm.subcmd=stat";
  }

  if (cmd == "compact")
  {
    in += "mgm.subcmd=compact";
    state = subtokenizer.GetToken();
    if ((state != "on") && (state != "off"))
      goto com_ns_usage;
    in += "&mgm.ns.compact=";
    in += state;
    delay = subtokenizer.GetToken();
    interval = subtokenizer.GetToken();
    if (delay.length())
    {
      int idelay = atoi(delay.c_str());
      if (!idelay)
        goto com_ns_usage;

      in += "&mgm.ns.compact.delay=";
      in += delay;

      if (!interval.length())
      {
        interval = "0";
      }
      int iinterval = atoi(interval.c_str());

      if (!iinterval)
      {
        if (interval != "0")
          goto com_ns_usage;
      }
      in += "&mgm.ns.compact.interval=";
      in += interval;
    }
  }

  if (cmd == "master")
  {
    in += "mgm.subcmd=master";
    XrdOucString master = subtokenizer.GetToken();
    in += "&mgm.master=";
    in += master;
  }

#ifdef EOS_INSTRUMENTED_RWMUTEX
  if (cmd == "mutex")
  {
    in += "mgm.subcmd=mutex";
  }
#endif
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
          if (option == "--reset")
          {
            options += "r";
          }
          else
          {
#ifdef EOS_INSTRUMENTED_RWMUTEX
            if (option == "--toggletiming")
            {
              options += "t";
            }
            else
            {
              if (option == "--toggleorder")
              {
                options += "o";
              }
              else
              {
                if (option == "--smplrate1")
                {
                  options += "1";
                }
                else
                {
                  if (option == "--smplrate10")
                  {
                    options += "s";
                  }
                  else
                  {
                    if (option == "--smplrate100")
                    {
                      options += "f";
                    }
                    else
                    {
                      goto com_ns_usage;
                    }
                  }
                }
              }
            }
#else
            goto com_ns_usage;
#endif
          }
        }
      }
    }
  }
  while (1);

  if (options.length())
  {
    in += "&mgm.option=";
    in += options;
  }

  global_retc = output_result(client_admin_command(in));
  return (0);

com_ns_usage:
  fprintf(stdout, "Usage: ns                                                         :  print basic namespace parameters\n");
  fprintf(stdout, "       ns stat [-a] [-m] [-n]                                     :  print namespace statistics\n");
  fprintf(stdout, "                -a                                                   -  break down by uid/gid\n");
  fprintf(stdout, "                -m                                                   -  print in <key>=<val> monitoring format\n");
  fprintf(stdout, "                -n                                                   -  print numerical uid/gids\n");
  fprintf(stdout, "                --reset                                              -  reset namespace counter\n");
#ifdef EOS_INSTRUMENTED_RWMUTEX
  fprintf(stdout, "       ns mutex                                                   :  manage mutex monitoring\n");
  fprintf(stdout, "                --toggletiming                                       -  toggle the timing\n");
  fprintf(stdout, "                --toggleorder                                        -  toggle the order checking\n");
  fprintf(stdout, "                --smplrate1                                          -  set the timing sample rate at 1%%   (default, almost no slow-down)\n");
  fprintf(stdout, "                --smplrate10                                         -  set the timing sample rate at 10%%  (medium slow-down)\n");
  fprintf(stdout, "                --smplrate100                                        -  set the timing sample rate at 100%% (severe slow-down)\n");
#endif
  fprintf(stdout, "       ns compact on <delay> [<interval>]                            -  enable online compactification after <delay> seconds\n");
  fprintf(stdout, "                                                                     -  if <interval> is >0 the compactifcation is repeated automatically after <interval> seconds!\n");
  fprintf(stdout, "       ns compact off                                                -  disable online compactification\n");
  fprintf(stdout, "       ns master <master-hostname>|[--log]|--log-clear            :  master/slave operation\n");
  fprintf(stdout, "       ns master <master-hostname>                                   -  set the host name of the MGM RW master daemon\n");
  fprintf(stdout, "       ns master                                                     -  show the master log\n");
  fprintf(stdout, "       ns master --log                                               -  show the master log\n");
  fprintf(stdout, "       ns master --log-clear                                         -  clean the master log\n");
  fprintf(stdout, "       ns master --disable                                           -  disable the slave/master supervisor thread modifying stall/redirection variables\n");
  fprintf(stdout, "       ns master --enable                                            -  enable  the slave/master supervisor thread modifying stall/redirectino variables\n");
  return (0);
}
