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
  XrdMgmOfsFile* file = new XrdMgmOfsFile(const_cast<char*>(client->tident));

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


if (execmd == "utimes")
{
  // -----------------------------------------------------------------------
  // set modification times
  // -----------------------------------------------------------------------
  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;

  gOFS->MgmStats.Add("Fuse-Utimes", vid.uid, vid.gid, 1);

  char* tv1_sec;
  char* tv1_nsec;
  char* tv2_sec;
  char* tv2_nsec;

  tv1_sec = env.Get("tv1_sec");
  tv1_nsec = env.Get("tv1_nsec");
  tv2_sec = env.Get("tv2_sec");
  tv2_nsec = env.Get("tv2_nsec");

  struct timespec tvp[2];
  if (tv1_sec && tv1_nsec && tv2_sec && tv2_nsec)
  {
    tvp[0].tv_sec = strtol(tv1_sec, 0, 10);
    tvp[0].tv_nsec = strtol(tv1_nsec, 0, 10);
    tvp[1].tv_sec = strtol(tv2_sec, 0, 10);
    tvp[1].tv_nsec = strtol(tv2_nsec, 0, 10);

    int retc = utimes(spath.c_str(),
                      tvp,
                      error,
                      client,
                      0);

    XrdOucString response = "utimes: retc=";
    response += retc;
    error.setErrInfo(response.length() + 1, response.c_str());
    return SFS_DATA;
  }
  else
  {
    XrdOucString response = "utimes: retc=";
    response += EINVAL;
    error.setErrInfo(response.length() + 1, response.c_str());
    return SFS_DATA;
  }
}

if (execmd == "checksum")
{
  // -----------------------------------------------------------------------
  // Return a file checksum
  // -----------------------------------------------------------------------
  ACCESSMODE_R;
  MAYSTALL;
  MAYREDIRECT;

  gOFS->MgmStats.Add("Fuse-Checksum", vid.uid, vid.gid, 1);

  // get the checksum
  XrdOucString checksum = "";
  eos::IFileMD* fmd = 0;
  int retc = 0;

  eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
  try
  {
    fmd = gOFS->eosView->getFile(spath.c_str());
    size_t cxlen = eos::common::LayoutId::GetChecksumLen(fmd->getLayoutId());
    for (unsigned int i = 0; i < SHA_DIGEST_LENGTH; i++)
    {
      char hb[3];
      sprintf(hb, "%02x", (i < cxlen) ? (unsigned char) (fmd->getChecksum().getDataPadded(i)) : 0);
      checksum += hb;
    }
  }
  catch (eos::MDException &e)
  {
    errno = e.getErrno();
    eos_thread_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(), e.getMessage().str().c_str());
  }

  if (!fmd)
  {
    retc = errno;
  }
  else
  {
    retc = 0;
  }

  XrdOucString response = "checksum: ";
  response += checksum;
  response += " retc=";
  response += retc;
  error.setErrInfo(response.length() + 1, response.c_str());
  return SFS_DATA;
}
