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

/*----------------------------------------------------------------------------*/
#include <sstream>
#include "console/ConsoleMain.hh"
#include "XrdCl/XrdClURL.hh"
/*----------------------------------------------------------------------------*/

int
com_archive(char* arg1)
{
  XrdOucString in = "";
  XrdOucString savearg = arg1;
  XrdOucTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString subcmd = subtokenizer.GetToken();
  std::ostringstream in_cmd;
  XrdOucString token;
  in_cmd << "mgm.cmd=archive&mgm.subcmd=" << subcmd;

  if (subcmd == "create")
  {
    XrdOucString path = subtokenizer.GetToken();
    XrdOucString dst = subtokenizer.GetToken();

    if (!path.length() || !dst.length())
    {
      fprintf(stdout, "Empty path or destination\n");
      goto com_archive_usage;
    }
                       
    XrdCl::URL dst_url = XrdCl::URL(dst.c_str());

    if (!dst_url.IsValid())
    {
      fprintf(stdout, "Invalid destination URL\n");
      goto com_archive_usage;
    }

    in_cmd << "&mgm.archive.path=" << path
           << "&mgm.archive.dst=" << dst;
  }
  else if ((subcmd == "migrate") || (subcmd == "stage"))
  {
    token = subtokenizer.GetToken();

    if (!token.length())
    {
      goto com_archive_usage;
    }
    else if (token.beginswith("--"))
    {
      token.erase(0, 2);

      if (token != "retry")
      {
        fprintf(stdout, "Unknown option: %s", token.c_str());
        goto com_archive_usage;
      }
      else
      {
        in_cmd << "&mgm.archive.option=r";
      }

      token = subtokenizer.GetToken();
    }

    if (!token.length())
      goto com_archive_usage;
    else
      in_cmd << "&mgm.archive.path=" << token;
  }
  else if (subcmd == "list")
  {
    // type: all, stage, migrate, job_uuid
    token = subtokenizer.GetToken(); 

    if (!token.length())
      in_cmd << "&mgm.archive.option=all";
    else      
      in_cmd << "&mgm.archive.option=" << token;
  }
  else if (subcmd == "purge")
  {
    token = subtokenizer.GetToken();

    if (!token.length())
      goto com_archive_usage;
    else
      in_cmd << "&mgm.archive.path=" << token;
  }
  else
    goto com_archive_usage;

  in = in_cmd.str().c_str();
  global_retc = output_result(client_admin_command(in));
  return (0);

com_archive_usage:
  std::ostringstream oss;
  oss << "usage: archive <subcmd> " << std::endl
      << "               create <path> <destination_url>         "
      << ": create archive file" << std::endl
      << "               migrate [--retry] <path>                "
      << ": submit migration job" << std::endl
      << "               stage [--retry] <path>                  "
      << ": submit stage job" << std::endl
      << "               purge[--retry] <path>                   "
      << ": purge files on disk" << std::endl
      << "               list [all|migrate|stage|purge|job_uuid] "
      << ": list status of jobs" << std::endl
      << "               help [--help|-h]                        "
      << ": display help message" << std::endl;

  fprintf(stdout, "%s", oss.str().c_str());
  return 0;
}
