// ----------------------------------------------------------------------
// File: OpenPio.cc
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
#include "mgm/stat/Stat.hh"
#include "mgm/ofs/XrdMgmOfs.hh"
#include "mgm/ofs/XrdMgmOfsFile.hh"
#include "mgm/macros/Macros.hh"

#include <XrdOuc/XrdOucEnv.hh>

//----------------------------------------------------------------------------
// Parallel IO (PIO) mode open of a RAIN file, handles mgm.pcmd=open. The
// file is opened as usual but, instead of a redirection to a gateway FST, the
// client gets back the locations of all the stripes and the capability.
//
// The open flags and mode are given as for mgm.pcmd=redirect, see
// XrdMgmOfsFile::GetClientOpenFlags. Without "eos.client.openflags" the file
// is opened for reading. For writing, only creation ("cr", exclusive) or
// overwrite ("tr") of a file is supported. The client is then in charge of
// writing all the stripes, computing the parity and committing the file to
// the MGM. Normal permission checks apply and any client, trusted or not, can
// commit or drop only what the PIO write capability issued to it covers.
//----------------------------------------------------------------------------
int
XrdMgmOfs::OpenPio(const char* path, const char* ininfo, XrdOucEnv& env,
                   XrdOucErrInfo& error, eos::common::VirtualIdentity& vid,
                   const XrdSecEntity* client)
{
  static const char* epname = "OpenPio";
  XrdSfsFileOpenMode open_mode = SFS_O_RDONLY;
  mode_t create_mode = 0;
  const bool valid_flags = XrdMgmOfsFile::GetClientOpenFlags(env, open_mode, create_mode);
  const bool is_write =
      (open_mode & (SFS_O_WRONLY | SFS_O_RDWR | SFS_O_CREAT | SFS_O_TRUNC));
  ACCESSMODE_R;

  if (is_write) {
    __AccessMode__ = 1;
  }

  MAYSTALL;
  const char* inpath = path;
  MAYREDIRECT;

  if (!valid_flags) {
    return Emsg(epname, error, EINVAL, "open - invalid eos.client.openmode", path);
  }

  if (is_write) {
    if (!(open_mode & (SFS_O_CREAT | SFS_O_TRUNC))) {
      return Emsg(epname, error, ENOTSUP,
                  "open - PIO write supports only "
                  "creation or overwrite of a file",
                  path);
    }

    // Note: SFS_O_CREAT means exclusive creation, SFS_O_TRUNC creates or
    // truncates the file (see XrdMgmOfsFile::GetPosixOpenFlags)
    open_mode |= SFS_O_RDWR;

    if (env.Get("eos.client.openmode") == nullptr) {
      create_mode |= S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
    }
  }

  gOFS->MgmStats.Add("OpenLayout", vid.uid, vid.gid, 1);
  XrdMgmOfsFile* file = new XrdMgmOfsFile(const_cast<char*>(client->tident));
  XrdOucString opaque = ininfo;
  int retc = SFS_ERROR;

  if (file) {
    opaque += "&eos.cli.access=pio";
    int rc = file->open(path, open_mode, create_mode, client, opaque.c_str());
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
