// ----------------------------------------------------------------------
// File: com_who.cc
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

/* Who is connected -  Interface */
int
com_who (char* arg1)
{
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString option = "";
  XrdOucString options = "";

  XrdOucString in = "";

  if (wants_help(arg1))
    goto com_who_usage;

  in = "mgm.cmd=who";
  do
  {
    option = subtokenizer.GetToken();
    if (!option.length())
      break;
    if (option == "-c")
    {
      options += "c";
    }
    else
    {
      if (option == "-n")
      {
        options += "n";
      }
      else
      {
        if (option == "-a")
        {
          options += "a";
        }
        else
        {
          if (option == "-z")
          {
            options += "z";
          }
          else
          {
            if (option == "-m")
            {
              options += "m";
            }
            else
            {
              if (option == "-s")
              {
                options += "s";
              }
              else
              {
                goto com_who_usage;
              }
            }
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

  global_retc = output_result(client_user_command(in));
  return (0);

com_who_usage:
  fprintf(stdout, "usage: who [-c] [-n] [-z] [-a] [-m] [-s]                             :  print statistics about active users (idle<5min)\n");
  fprintf(stdout, "                -c                                                   -  break down by client host\n");
  fprintf(stdout, "                -n                                                   -  print id's instead of names\n");
  fprintf(stdout, "                -z                                                   -  print auth protocols\n");
  fprintf(stdout, "                -a                                                   -  print all\n");
  fprintf(stdout, "                -s                                                   -  print summary for clients\n");
  fprintf(stdout, "                -m                                                   -  print in monitoring format <key>=<value>\n");
  return (0);
}
