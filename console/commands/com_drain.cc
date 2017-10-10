// ----------------------------------------------------------------------
// File: com_drain.cc
// Author: Andrea Manzi - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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
#include "common/StringConversion.hh"
/*----------------------------------------------------------------------------*/

extern int com_file(char*);
extern int com_fs(char*);
extern int com_find(char*);

using namespace eos::common;

/*----------------------------------------------------------------------------*/

/* Central Drain listing, configuration, manipulation */
int
com_drain(char* arg1)
{
  // split subcommands
  eos::common::StringTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString subcommand = subtokenizer.GetToken();

  if (wants_help(arg1)) {
    goto com_drain_usage;
  }

  if (subcommand == "start") {
    XrdOucString in = "mgm.cmd=drain&mgm.subcmd=start";
    XrdOucString fsid = subtokenizer.GetToken();

    if (fsid.length()) {
      int ifsid = atoi(fsid.c_str());

      if (ifsid == 0) {
        goto com_drain_usage;
      }
    } else {
      goto com_drain_usage;
    }

    in += "&mgm.drain.fsid=";
    in += fsid;
    XrdOucEnv* result = client_command(in,true);
    global_retc = output_result(result);
    return (0);
  }

  if (subcommand == "stop") {
    XrdOucString in = "mgm.cmd=drain&mgm.subcmd=stop";
    XrdOucString fsid = subtokenizer.GetToken();

    if (fsid.length()) {
      int ifsid = atoi(fsid.c_str());

      if (ifsid == 0) {
        goto com_drain_usage;
      }
    } else {
      goto com_drain_usage;
    }

    in += "&mgm.drain.fsid=";
    in += fsid;
    XrdOucEnv* result = client_command(in,true);
    global_retc = output_result(result);
    return (0);
  }

  if (subcommand == "clear") {
    XrdOucString in = "mgm.cmd=drain&mgm.subcmd=clear";
    XrdOucString fsid = subtokenizer.GetToken();

    if (fsid.length()) {
      int ifsid = atoi(fsid.c_str());

      if (ifsid == 0) {
        goto com_drain_usage;
      }
    } else {
      goto com_drain_usage;
    }

    in += "&mgm.drain.fsid=";
    in += fsid;
    XrdOucEnv* result = client_command(in,true);
    global_retc = output_result(result);
    return (0);
  }

  if (subcommand == "status") {
    XrdOucString in = "mgm.cmd=drain&mgm.subcmd=status";
    XrdOucString fsid = subtokenizer.GetToken();

   if (fsid.length()) {
      int ifsid = atoi(fsid.c_str());
      if (ifsid == 0) {
        goto com_drain_usage;
      }
      in += "&mgm.drain.fsid=";
      in += fsid;
   }

    XrdOucEnv* result = client_command(in,true);
    global_retc = output_result(result);
    return (0);
  }

com_drain_usage:
  fprintf(stdout, "'[eos] drain ..' provides the drain interface of EOS.\n");
  fprintf(stdout,
          "Usage: drain start|stop|status [OPTIONS]\n");
  fprintf(stdout, "Options:\n");
  fprintf(stdout, "drain start <fsid>: \n");
  fprintf(stdout,
          "                                                  start the draining of the given fsid.\n\n");
  fprintf(stdout, "drain stop <fsid> : \n");
  fprintf(stdout,
          "                                                  stop the draining of the given fsid.\n\n");
  fprintf(stdout, "drain clear <fsid> : \n");
  fprintf(stdout,
          "                                                  clear the draining info for the given fsid.\n\n");
  fprintf(stdout, "drain status [fsid] :\n");
  fprintf(stdout,
          "                                                  show the status of the drain activities on the system. If the fsid is specified shows detailed info about that fs drain\n");
  fprintf(stdout, "Report bugs to eos-dev@cern.ch.\n");
  global_retc = EINVAL;
  return (0);
}
