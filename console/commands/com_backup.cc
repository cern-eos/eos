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
#include <sys/time.h>
/*----------------------------------------------------------------------------*/
#include "console/ConsoleMain.hh"
#include "XrdCl/XrdClFile.hh"
#include "XrdCl/XrdClFileSystem.hh"
/*----------------------------------------------------------------------------*/

int com_backup(char* arg1)
{
  XrdOucString in, token;
  std::ostringstream in_cmd;
  eos::common::StringTokenizer subtokenizer(arg1);
  subtokenizer.GetLine();
  XrdCl::URL src_url;
  XrdCl::URL dst_url;
  XrdOucString src_surl = subtokenizer.GetToken();
  XrdOucString dst_surl = subtokenizer.GetToken();

  // Check if minimal input is present
  if (!src_surl.length() || !dst_surl.length())
    goto com_backup_usage;

  // Check that these are valid XRootD URLs
  if (!src_url.FromString(src_surl.c_str()) ||
      !dst_url.FromString(dst_surl.c_str()))
    goto com_backup_usage;

  in_cmd << "mgm.cmd=backup&mgm.backup.src=" << src_surl
         << "&mgm.backup.dst=" << dst_surl;
  token = subtokenizer.GetToken();

  while (token.length())
  {
    if (!token.beginswith("--"))
      goto com_backup_usage;

    // Get the type of incremental backup either by mtime or by ctime
    if (token == "--ctime" || token == "--mtime")
    {
      if (token == "--ctime")
        in_cmd << "&mgm.backup.ttime=ctime";
      else if (token == "--mtime")
        in_cmd << "&mgm.backup.ttime=mtime";

      // Get the interval time
      token = subtokenizer.GetToken();

      if (!token.length())
        goto com_backup_usage;

      char last = token[token.length() - 1];
      long int seconds = 0;
      if (last == 's')
        seconds = 1; // seconds
      else if (last == 'm')
        seconds = 60; //minutes
      else if (last == 'h')
        seconds = 3600; // hours
      else if (last == 'd')
        seconds = 24 * 3600; // days
      else
        goto com_backup_usage;

      // Try to convert the time window to integer value
      char* p_end = (char*)(token.c_str() + token.length());
      long int value = strtol(token.c_str(), &p_end, 10);

      if (value == 0L)
        goto com_backup_usage;

      value *= seconds;
      struct timeval tv;

      if (gettimeofday(&tv, NULL))
      {
        fprintf(stderr, "Error getting current timestamp\n");
        goto com_backup_usage;
      }

      in_cmd << "&mgm.backup.vtime=" << (tv.tv_sec - value);
    }
    else if (token == "--excl_xattr")
    {
      // Exclude certain directory extended attributes from being enforced
      // and checked
      token = subtokenizer.GetToken();

      if (!token.length())
        goto com_backup_usage;

      in_cmd << "&mgm.backup.excl_xattr=" << token.c_str();
    }
    else
      goto com_backup_usage;

    token = subtokenizer.GetToken();
  }

  in = in_cmd.str().c_str();
  global_retc = output_result(client_admin_command(in));
  return 0;

com_backup_usage:
  std::ostringstream oss;
  oss << "usage: backup <src_url> <dst_url> [options] " << std::endl
      << " " << std:: endl
      << " optional arguments: " << std::endl
      << " --ctime|mtime <val>s|m|h|d use the specified timewindow to select entries for backup" << std::endl
      << " --excl_xattr val_1[,val_2]...[,val_n] extended attributes which are not enforced and" << std::endl
      << "              also not checked during the verification step" << std::endl;

  fprintf(stdout, "%s", oss.str().c_str());
  return 0;
}
