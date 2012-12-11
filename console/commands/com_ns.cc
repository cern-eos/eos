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
/*----------------------------------------------------------------------------*/

/* Namespace Interface */
int 
com_ns (char* arg1) {
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString cmd = subtokenizer.GetToken();
  XrdOucString option="";
  XrdOucString options="";

  XrdOucString in ="";
  if ( ( cmd != "stat")  && ( cmd != "" ) && ( cmd != "compact" ) && ( cmd != "master" ) ) {
    goto com_ns_usage;
  }
  
  in = "mgm.cmd=ns&";
  if (cmd == "stat") {
    in += "mgm.subcmd=stat";
  }

  if (cmd == "compact") {
    in += "mgm.subcmd=compact";
  }

  if (cmd == "master") {
    in += "mgm.subcmd=master";
    XrdOucString master = subtokenizer.GetToken();
    in += "&mgm.master="; in += master;
  }

  do {
    option = subtokenizer.GetToken();
    if (!option.length())
      break;
    if (option == "-a") {
      options += "a";
    } else {
      if (option == "-m") {
        options += "m";
      } else {
        if (option == "-n") {
          options += "n";
        } else {
	  if ( option == "--reset") {
	    options += "r";
	  } else {
	    goto com_ns_usage;
	  }
        }
      }
    }
  } while(1);
       
  if (options.length()) {
    in += "&mgm.option="; in += options;
  }  
  
  global_retc = output_result(client_admin_command(in));
  return (0);

 com_ns_usage:
  fprintf(stdout,"Usage: ns                                                         :  print basic namespace parameters\n");
  fprintf(stdout,"       ns stat [-a] [-m] [-n]                                     :  print namespace statistics\n");
  fprintf(stdout,"                -a                                                   -  break down by uid/gid\n");
  fprintf(stdout,"                -m                                                   -  print in <key>=<val> monitoring format\n");
  fprintf(stdout,"                -n                                                   -  print numerical uid/gids\n");
  fprintf(stdout,"                --reset                                              -  reset namespace counter\n");
  fprintf(stdout,"       ns compact                                                    -  compact the current changelogfile and reload the namespace\n");
  fprintf(stdout,"       ns master <master-hostname>|[--log]|--log-clear            :  master/slave operation\n");
  fprintf(stdout,"       ns master <master-hostname>                                   -  set the host name of the MGM RW master daemon\n");
  fprintf(stdout,"       ns master                                                     -  show the master log\n");
  fprintf(stdout,"       ns master --log                                               -  show the master log\n");
  fprintf(stdout,"       ns master --log-clear                                         -  clean the master log\n");
  fprintf(stdout,"       ns master --disable                                           -  disable the slave/master supervisor thread modifying stall/redirection variables\n");
  fprintf(stdout,"       ns master --enable                                            -  enable  the slave/master supervisor thread modifying stall/redirectino variables\n");
  return (0);
}
