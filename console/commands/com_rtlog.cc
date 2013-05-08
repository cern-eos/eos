// ----------------------------------------------------------------------
// File: com_rtlog.cc
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

/* Retrieve realtime log output */

int
com_rtlog (char* arg1)
{
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString queue = subtokenizer.GetToken();
  XrdOucString lines = subtokenizer.GetToken();
  XrdOucString tag = subtokenizer.GetToken();
  XrdOucString filter = subtokenizer.GetToken();
  XrdOucString in = "mgm.cmd=rtlog&mgm.rtlog.queue=";

  if (!queue.length())
    goto com_rtlog_usage;

  if ((queue != ".") && (queue != "*") && (!queue.beginswith("/eos/")))
  {
    // there is no queue argument and means to talk with the mgm directly
    filter = tag;
    tag = lines;
    lines = queue;
    queue = ".";
  }

  if (queue.length())
  {
    in += queue;
    if (!lines.length())
      in += "&mgm.rtlog.lines=10";
    else
      in += "&mgm.rtlog.lines=";
    in += lines;
    if (!tag.length())
      in += "&mgm.rtlog.tag=err";
    else
      in += "&mgm.rtlog.tag=";
    in += tag;

    if (filter.length())
      in += "&mgm.rtlog.filter=";
    in += filter;

    global_retc = output_result(client_admin_command(in));
    return (0);
  }

com_rtlog_usage:
  fprintf(stdout, "usage: rtlog [<queue>|*|.] [<sec in the past>=3600] [<debug>=err] [filter-word]\n");
  fprintf(stdout, "                     - '*' means to query all nodes\n");
  fprintf(stdout, "                     - '.' means to query only the connected mgm\n");
  fprintf(stdout, "                     - if the first argument is ommitted '.' is assumed\n");
  return (0);
}
