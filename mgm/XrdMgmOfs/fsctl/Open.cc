// ----------------------------------------------------------------------
// File: Open.cc
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


// -----------------------------------------------------------------------
// This file is included source code in XrdMgmOfs.cc to make the code more
// transparent without slowing down the compilation time.
// -----------------------------------------------------------------------

{
  ACCESSMODE_R;
  MAYSTALL;
  MAYREDIRECT;

  gOFS->MgmStats.Add("OpenLayout", vid.uid, vid.gid, 1);
  XrdMgmOfsFile* file = new XrdMgmOfsFile(client->tident);

  if (file)
  {
    opaque += "&eos.cli.access=pio";
    int rc = file->open(spath.c_str(), SFS_O_RDONLY, 0, client, opaque.c_str());
    error.setErrInfo(strlen(file->error.getErrText()) + 1, file->error.getErrText());
    if (rc == SFS_REDIRECT)
    {
      delete file;
      return SFS_DATA;
    }
    else
    {
      error.setErrCode(file->error.getErrInfo());
      delete file;
      return SFS_ERROR;
    }
  }
  else
  {
    error.setErrInfo(ENOMEM, "allocate file object");
    return SFS_ERROR;
  }
}
