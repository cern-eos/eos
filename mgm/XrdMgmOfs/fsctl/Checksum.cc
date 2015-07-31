// ----------------------------------------------------------------------
// File: Checksum.cc
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
