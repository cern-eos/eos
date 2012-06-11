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
com_io (char* arg1) {
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString cmd = subtokenizer.GetToken();
  XrdOucString option="";
  XrdOucString options="";
  XrdOucString path="";
  XrdOucString in ="";
  if ( ( cmd != "stat") && ( cmd != "enable" ) && ( cmd != "disable") && ( cmd != "report" ) ) {
    goto com_io_usage;
  }
  
  in = "mgm.cmd=io&";

  if (cmd == "enable") {
    in += "mgm.subcmd=enable";
  }
  if (cmd == "disable") {
    in += "mgm.subcmd=disable";
  }

  if (cmd == "stat") {
    in += "mgm.subcmd=stat";
  }

  if (cmd == "report") {
    in += "mgm.subcmd=report";
    path = subtokenizer.GetToken();

    if (!path.length()) 
      goto com_io_usage;
    in += "&mgm.io.path=";
    in += path;
  }  else {
    do {
      option = subtokenizer.GetToken();
      if (!option.length())
        break;
      if (option == "-a") {
        options += "a";
      } else {
        if (option == "-m") {
          options += "m";
        }  else {
          if (option == "-n") {
            options += "n";
          } else {
            if ( option == "-t") {
              options += "t";
            } else {
              if ( option == "-r") {
                options += "r";
              } else {
                if ( option == "-n") {
                  options += "n";
                } else {
		  if ( option == "-d") {
		    options += "d";
		  } else {
		    goto com_io_usage;
		  }
                }
              }
            }
          }
        }
      }
    } while(1);
  }
       
  if (options.length()) {
    in += "&mgm.option="; in += options;
  }  
  
  global_retc = output_result(client_admin_command(in));
  return (0);

 com_io_usage:
  fprintf(stdout,"usage: io stat [-a] [-m] [-n] [-t] [-d]                           :  print io statistics\n");
  fprintf(stdout,"                -a                                                   -  break down by uid/gid\n");
  fprintf(stdout,"                -m                                                   -  print in <key>=<val> monitoring format\n");
  fprintf(stdout,"                -n                                                   -  print numerical uid/gids\n");
  fprintf(stdout,"                -t                                                   -  print top user stats\n");
  fprintf(stdout,"                -d                                                   -  break down by domains\n");
  fprintf(stdout,"       io enable [-r] [-n]                                        :  enable collection of io statistics\n");
  fprintf(stdout,"                                                               -r    enable collection of io reports\n");
  fprintf(stdout,"                                                               -n    enable report namespace\n");
  fprintf(stdout,"       io disable [-r] [-n]                                       :  disable collection of io statistics\n");
  fprintf(stdout,"                                                               -r    disable collection of io reports\n");
  fprintf(stdout,"                                                               -n    disable report namespace\n");
  fprintf(stdout,"       io report <path>                                           :  show contents of report namespace for <path>\n");
  return (0);
}
