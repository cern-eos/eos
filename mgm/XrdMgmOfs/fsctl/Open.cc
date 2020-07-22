// ----------------------------------------------------------------------
// File: Open.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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

#include "common/Logging.hh"
#include "mgm/Stat.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/XrdMgmOfsFile.hh"
#include "mgm/Macros.hh"

#include <XrdOuc/XrdOucEnv.hh>

//----------------------------------------------------------------------------
// Parallel IO mode open
//----------------------------------------------------------------------------
int
XrdMgmOfs::Open(const char* path,
                const char* ininfo,
                XrdOucEnv& env,
                XrdOucErrInfo& error,
                eos::common::VirtualIdentity& vid,
                const XrdSecEntity* client)
{
  ACCESSMODE_R;
  MAYSTALL;
  MAYREDIRECT;
  gOFS->MgmStats.Add("OpenLayout", vid.uid, vid.gid, 1);
  XrdMgmOfsFile* file = new XrdMgmOfsFile(const_cast<char*>(client->tident));
  XrdOucString opaque = ininfo;
  int retc = SFS_ERROR;

  if (file) {
    opaque += "&eos.cli.access=pio";
    int rc = file->open(path, SFS_O_RDONLY, 0, client, opaque.c_str());
    error = file->error;

    if (rc == SFS_REDIRECT) {
      // When returning SFS_DATA the ecode represents the length of the data
      // to be sent to the client.
      error.setErrCode(strlen(error.getErrText()));
      retc = SFS_DATA;
    }

    delete file;
  } else {
    const char* emsg = "allocate file object";
    error.setErrInfo(strlen(emsg) + 1, emsg);
    error.setErrCode(ENOMEM);
  }

  return retc;
}
