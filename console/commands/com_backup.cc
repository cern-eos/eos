//------------------------------------------------------------------------------
// File: com_backup.cc
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
#include <memory>
/*----------------------------------------------------------------------------*/
#include "console/ConsoleMain.hh"
#include "XrdCl/XrdClFile.hh"
#include "XrdCl/XrdClFileSystem.hh"
/*----------------------------------------------------------------------------*/

int com_backup(char* arg1)
{
  XrdOucString in = "";
  std::ostringstream in_cmd;
  eos::common::StringTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdOucString src_surl = subtokenizer.GetToken();
  XrdOucString dst_surl = subtokenizer.GetToken();

  if (!src_surl.length() || !dst_surl.length())
    goto com_backup_usage;

  in_cmd << "mgm.cmd=backup&mgm.backup.src=" << src_surl
         << "&mgm.backup.dst=" << dst_surl;

  in = in_cmd.str().c_str();
  global_retc = output_result(client_user_command(in));
  return 0;

com_backup_usage:
  std::ostringstream oss;
  oss << "usage: backup <src_url> <dst_url>" << std::endl
      << "              create a backup of the subtree rooted in " << std::endl
      << "              <src_url> and save it at location <dst_url>" << std::endl;

  fprintf(stdout, "%s", oss.str().c_str());
  return 0;
}
