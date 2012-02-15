// ----------------------------------------------------------------------
// File: com_dropbox.cc
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

/* Dropbox interface */
int
com_dropbox (char *arg) {
  XrdOucTokenizer subtokenizer(arg);
  subtokenizer.GetLine();
  XrdOucString subcommand = subtokenizer.GetToken();
  
  if ( (subcommand.find("--help")!=STR_NPOS) || (subcommand.find("-h")!=STR_NPOS))
    goto com_dropbox_usage;
  
  if ( (subcommand != "add") && (subcommand != "rm") && (subcommand != "start") && (subcommand != "stop") && (subcommand != "ls") ) {
    goto com_dropbox_usage;
  }
  
  if (subcommand == "add") {

    global_retc = 0;
    return (0);
  }

  if (subcommand == "start") {

    global_retc = 0;
    return (0);
  }

  if (subcommand == "rm") {

    global_retc = 0;
    return (0);
  }

  if (subcommand == "stop") {
    
    global_retc = 0;
    return (0);
  }

  if (subcommand == "ls") {
    
    global_retc = 0;
    return (0);
  }

 com_dropbox_usage:
  fprintf(stdout,"Usage: dropbox add|rm|start|stop|add|rm|ls ...\n");
  fprintf(stdout,"'[eos] dropbox ...' provides dropbox functionality for eos.\n");

  fprintf(stdout,"Options:\n");
  fprintf(stdout,"dropbox add <eos-dir> <local-dir>   :\n");
  fprintf(stdout,"                                                  add drop box configuration to synchronize from <eos-dir> to <local-dir>!\n");
  fprintf(stdout,"dropbox rm <eos-dir>                :\n");
  fprintf(stdout,"                                                  remove drop box configuration to synchronize from <eos-dir>!\n");
  fprintf(stdout,"dropbox start [<eos-dir>]           :\n");
  fprintf(stdout,"                                                  start the drop box daemon for <eos-dir>. If no directory is specified all configured directories will be started!\n");
  fprintf(stdout,"dropbox stop [<eos-dir>]            :\n");
  fprintf(stdout,"                                                  stop the drop box daemon for <eos-dir>. If no directory is specified all configured directories will be stopped!\n");
  fprintf(stdout,"dropbox ls                          :\n");
  fprintf(stdout,"                                                  list configured drop box daemons and their status\n");
  return (0);
}
