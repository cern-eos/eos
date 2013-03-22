// ----------------------------------------------------------------------
// File: proc/user/Motd.cc
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
 * You should have received a copy of the AGNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

/*----------------------------------------------------------------------------*/
#include "mgm/ProcInterface.hh"
#include "mgm/XrdMgmOfs.hh"

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

int
ProcCommand::Motd ()
{
  XrdOucString motdupload = pOpaque->Get("mgm.motd") ? pOpaque->Get("mgm.motd") : "";
  gOFS->MgmStats.Add("Motd", pVid->uid, pVid->gid, 1);
  eos_info("motd");
  XrdOucString motdfile = gOFS->MgmConfigDir;
  motdfile += "/motd";

  if (motdupload.length() &&
      ((!pVid->uid) ||
       eos::common::Mapping::HasUid(3, vid.uid_list) ||
       eos::common::Mapping::HasGid(4, vid.gid_list)))
  {
    // root + admins can set the MOTD
    unsigned int motdlen = 0;
    char* motdout = 0;
    eos_info("decoding motd\n");
    if (eos::common::SymKey::Base64Decode(motdupload, motdout, motdlen))
    {
      if (motdlen)
      {
        int fd = ::open(motdfile.c_str(), O_WRONLY);
        if (fd)
        {
          size_t nwrite = ::write(fd, motdout, motdlen);
          if (!nwrite)
          {
            stdErr += "error: error writing motd file\n";
          }
          ::close(fd);
        }
        free(motdout);
      }
    }
    else
    {
      stdErr += "error: unabile to decode motd message\n";
    }
  }


  int fd = ::open(motdfile.c_str(), O_RDONLY);
  if (fd > 0)
  {
    size_t nread;
    char buffer[65536];
    nread = ::read(fd, buffer, sizeof (buffer));
    if (nread > 0)
    {
      buffer[65535] = 0;
      stdOut += buffer;
    }
    ::close(fd);
  }
  return SFS_OK;
}


EOSMGMNAMESPACE_END
