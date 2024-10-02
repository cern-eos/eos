//------------------------------------------------------------------------------
// File: com_archive.cc
// Author: Elvin-Alin Sindrilaru <esindril@cern.ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2014 CERN/Switzerland                                  *
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

#include <sstream>
#include "common/StringTokenizer.hh"
#include "console/ConsoleMain.hh"
#include <XrdCl/XrdClURL.hh>

int
com_archive(char* arg1)
{
  XrdOucString in = "";
  eos::common::StringTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString subcmd = subtokenizer.GetToken();
  std::ostringstream in_cmd;
  XrdOucString token;
  in_cmd << "mgm.cmd=archive&mgm.subcmd=" << subcmd;

  if (subcmd == "create") {
    XrdOucString path = subtokenizer.GetToken();

    if (!path.length()) {
      path = gPwd;
    }

    path = abspath(path.c_str());
    in_cmd << "&mgm.archive.path=" << path;
  } else if ((subcmd == "put") ||
             (subcmd == "get") ||
             (subcmd == "purge") ||
             (subcmd == "delete")) {
    token = subtokenizer.GetToken();

    if (!token.length()) {
      goto com_archive_usage;
    } else if (token.beginswith("--")) {
      token.erase(0, 2);

      if (token != "retry") {
        fprintf(stdout, "Unknown option: %s", token.c_str());
        goto com_archive_usage;
      } else {
        in_cmd << "&mgm.archive.option=r";
      }

      token = subtokenizer.GetToken();
    }

    // The last token is the path
    if (!token.length()) {
      in_cmd << "&mgm.archive.path=" << gPwd;
    } else {
      token = abspath(token.c_str());
      in_cmd << "&mgm.archive.path=" << token;
    }
  } else if (subcmd == "transfers") {
    // type: all, stage, migrate, job_uuid
    token = subtokenizer.GetToken();

    if (!token.length()) {
      in_cmd << "&mgm.archive.option=all";
    } else {
      in_cmd << "&mgm.archive.option=" << token;
    }
  } else if (subcmd == "list") {
    token = subtokenizer.GetToken();

    if (!token.length()) {
      in_cmd << "&mgm.archive.path=/";
    } else if (token == "./" || token == ".") {
      in_cmd << "&mgm.archive.path=" << abspath(gPwd.c_str());
    } else {
      in_cmd << "&mgm.archive.path=" << token;
    }
  } else if (subcmd == "kill") {
    // Token is the job_uuid
    token = subtokenizer.GetToken();

    if (token.length()) {
      in_cmd  << "&mgm.archive.option=" << token;
    } else {
      goto com_archive_usage;
    }
  } else {
    goto com_archive_usage;
  }

  in = in_cmd.str().c_str();
  global_retc = output_result(client_command(in));
  return (0);
com_archive_usage:
  std::ostringstream oss;
  oss << "usage: archive <subcmd> " << std::endl
      << "               create <path>                          "
      << ": create archive file" << std::endl
      << "               put [--retry] <path>                   "
      << ": copy files from EOS to archive location" << std::endl
      << "               get [--retry] <path>                   "
      << ": recall archive back to EOS" << std::endl
      << "               purge[--retry] <path>                  "
      << ": purge files on disk" << std::endl
      << "               transfers [all|put|get|purge|job_uuid] "
      << ": show status of running jobs" << std::endl
      << "               list [<path>]                          "
      << ": show status of archived directories in subtree" << std::endl
      << "               kill <job_uuid>                        "
      << ": kill transfer" << std::endl
      << "               delete <path>                          "
      << ": delete files from tape, keeping the ones on disk" << std::endl
      << "               help [--help|-h]                       "
      << ": display help message" << std::endl;
  fprintf(stdout, "%s", oss.str().c_str());
  global_retc = EINVAL;
  return 0;
}
