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
  XrdOucString path;
  in_cmd << "mgm.cmd=archive&";

  if (subcmd == "create")
  {
    path = subtokenizer.GetToken();
    XrdOucString dst = subtokenizer.GetToken();
    XrdCl::URL dst_url = XrdCl::URL(dst.c_str());

    if (!path.length() || !dst_url.IsValid())
    {
      fprintf(stdout, "Invalid destination URL");
      goto com_archive_usage;
    }

    in_cmd << "mgm.subcmd=create&" << "mgm.archive.path=" << path << "&"
           << "mgm.archive.dst=" << dst;
  }
  else if (subcmd == "migrate")
  {
    path = subtokenizer.GetToken();

    if (!path.length())
      goto com_archive_usage;

    in_cmd << "mgm.subcmd=migrate&" << "mgm.archive.path=" << path;
  }
  else if (subcmd == "stage")
  {
    XrdOucString fpath = subtokenizer.GetToken();

    if (!path.length())
      goto com_archive_usage;

    in_cmd << "mgm.subcmd=stage&" << "mgm.archive.path=" << path;
  }
  else if (subcmd == "list")
  {
    in_cmd << "mgm.subcmd=list&";
    XrdOucString type = subtokenizer.GetToken();

    if (!type.length())
      in_cmd << "mgm.archive.type=all";
    else if (type == "migrate")
      in_cmd << "mgm.archive.type=migrate";
    else if (type == "stage")
      in_cmd << "mgm.archive.type=stage";
    else
      goto com_archive_usage;
  }

  in = in_cmd.str().c_str();
  global_retc = output_result(client_admin_command(in));
  return (0);

com_archive_usage:
  std::ostringstream oss;
  oss << "usage: archive <subcmd> " << std::endl
      << "               create <source> <destination>  : create archive file" << std::endl
      << "               migrate <archive.json>         : submit migration task" << std::endl
      << "               stage <archive.json>           : submit stage task" << std::endl
      << "               list [migration|stage|all]     : list status of tasks" << std::endl
      << "               help [--help|-h]               : display help message" << std::endl;

  fprintf(stdout, "%s", oss.str().c_str());
  return 0;
}
