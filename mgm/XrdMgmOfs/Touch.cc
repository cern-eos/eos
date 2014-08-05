// ----------------------------------------------------------------------
// File: Touch.cc
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

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_touch (const char *path,
                   XrdOucErrInfo &error,
                   eos::common::Mapping::VirtualIdentity &vid,
                   const char *ininfo)
/*----------------------------------------------------------------------------*/
/*
 * @brief create(touch) a no-replica file in the namespace
 *
 * @param path file to touch
 * @param error error object
 * @param vid virtual identity of the client
 * @param ininfo CGI
 *
 * Access control is not fully done here, just the POSIX write flag is checked,
 * no ACLs ...
 */
/*----------------------------------------------------------------------------*/
{
  EXEC_TIMING_BEGIN("Touch");

  eos_info("path=%s vid.uid=%u vid.gid=%u", path, vid.uid, vid.gid);


  gOFS->MgmStats.Add("Touch", vid.uid, vid.gid, 1);

  // Perform the actual deletion
  //
  errno = 0;
  eos::FileMD* fmd = 0;

  if (_access(path, W_OK, error, vid, ininfo))
  {
    return SFS_ERROR;
  }

  eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);
  try
  {
    fmd = gOFS->eosView->getFile(path);
    errno = 0;
  }
  catch (eos::MDException &e)
  {
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
              e.getErrno(), e.getMessage().str().c_str());
  }

  try
  {
    if (!fmd)
    {
      fmd = gOFS->eosView->createFile(path, vid.uid, vid.gid);
      fmd->setCUid(vid.uid);
      fmd->setCGid(vid.gid);
      fmd->setCTimeNow();
      fmd->setSize(0);
    }
    fmd->setMTimeNow();
    gOFS->eosView->updateFileStore(fmd);
    errno = 0;
  }
  catch (eos::MDException &e)
  {
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
              e.getErrno(), e.getMessage().str().c_str());
  }
  if (errno)
  {
    return Emsg("utimes", error, errno, "touch", path);
  }
  EXEC_TIMING_END("Touch");
  return SFS_OK;
}
