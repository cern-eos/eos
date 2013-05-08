// ----------------------------------------------------------------------
// File: com_debug.cc
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

/* Debug Level Setting */
int
com_debug (char* arg1)
{
  // split subcommands
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString level = subtokenizer.GetToken();
  XrdOucString nodequeue = subtokenizer.GetToken();
  XrdOucString filterlist = "";

  if ((level != "-h") && (level != "--help"))
  {

    if (level == "this")
    {
      fprintf(stdout, "info: toggling shell debugmode to debug=%d\n", debug);
      debug = !debug;
      if (debug)
      {
        eos::common::Logging::SetLogPriority(LOG_DEBUG);
      }
      else
      {
        eos::common::Logging::SetLogPriority(LOG_NOTICE);
      }
      return (0);
    }
    if (level.length())
    {
      XrdOucString in = "mgm.cmd=debug&mgm.debuglevel=";
      in += level;

      if (nodequeue.length())
      {
        if (nodequeue == "-filter")
        {
          filterlist = subtokenizer.GetToken();
          in += "&mgm.filter=";
          in += filterlist;
        }
        else
        {
          in += "&mgm.nodename=";
          in += nodequeue;
          nodequeue = subtokenizer.GetToken();
          if (nodequeue == "-filter")
          {
            filterlist = subtokenizer.GetToken();
            in += "&mgm.filter=";
            in += filterlist;
          }
        }
      }



      global_retc = output_result(client_admin_command(in));
      return (0);
    }
  }

  fprintf(stdout, "Usage: debug [node-queue] this|<level> [-filter <unitlist>]\n");
  fprintf(stdout, "'[eos] debug ...' allows to modify the verbosity of the EOS log files in MGM and FST services.\n\n");
  fprintf(stdout, "Options:\n");
  fprintf(stdout, "debug  <level> [-filter <unitlist>] :\n");
  fprintf(stdout, "                                                  set the MGM where the console is connected to into debug level <level>\n");
  fprintf(stdout, "debug  <node-queue> <level> [-filter <unitlist>] :\n");
  fprintf(stdout, "                                                  set the <node-queue> into debug level <level>. <node-queue> are internal EOS names e.g. '/eos/<hostname>:<port>/fst'\n");
  fprintf(stdout, "     <unitlist> : a comma seperated list of strings of software units which should be filtered out in the message log ! The default filter list is 'Process,AddQuota,UpdateHint,UpdateQuotaStatus,SetConfigValue,Deletion,GetQuota,PrintOut,RegisterNode,SharedHash'.\n\n");
  fprintf(stdout, "The allowed debug levels are: debug info warning notice err crit alert emerg\n\n");
  fprintf(stdout, "Examples:\n");
  fprintf(stdout, "  debug info *                        set MGM & all FSTs into debug mode 'info'\n\n");
  fprintf(stdout, "  debug err /eos/*/fst                set all FSTs into debug mode 'info'\n\n");
  fprintf(stdout, "  debug crit /eos/*/mgm               set MGM into debug mode 'crit'\n\n");
  fprintf(stdout, "  debug debug -filter MgmOfsMessage   set MGM into debug mode 'debug' and filter only messages comming from unit 'MgmOfsMessage'.\n\n");
  return (0);
}
