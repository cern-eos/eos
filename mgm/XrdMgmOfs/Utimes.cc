// ----------------------------------------------------------------------
// File: Utimes.cc
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
XrdMgmOfs::utimes (const char *inpath,
                   struct timespec *tvp,
                   XrdOucErrInfo &error,
                   const XrdSecEntity *client,
                   const char *ininfo)
/*----------------------------------------------------------------------------*/
/*
 * @brief set change time for a given file/directory
 *
 * @param inpath path to set
 * @param tvp timespec structure
 * @param error error object
 * @client XRootD authentication object
 * @ininfo CGI
 *
 * @return SFS_OK if success otherwise SFS_ERROR
 */
/*----------------------------------------------------------------------------*/
{

  static const char *epname = "utimes";
  const char *tident = error.getErrUser();

  // use a thread private vid
  eos::common::Mapping::VirtualIdentity vid;

  NAMESPACEMAP;
  BOUNCE_ILLEGAL_NAMES;

  XrdOucEnv utimes_Env(info);

  AUTHORIZE(client, &utimes_Env, AOP_Update, "set utimes", inpath, error);

  EXEC_TIMING_BEGIN("IdMap");
  eos::common::Mapping::IdMap(client, info, tident, vid);
  EXEC_TIMING_END("IdMap");

  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);

  BOUNCE_NOT_ALLOWED;
  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;

  return _utimes(path, tvp, error, vid, info);
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_utimes (const char *path,
                    struct timespec *tvp,
                    XrdOucErrInfo &error,
                    eos::common::Mapping::VirtualIdentity &vid,
                    const char *info)
/*----------------------------------------------------------------------------*/
/*
 * @brief set change time for a given file/directory
 *
 * @param path path to set
 * @param tvp timespec structure
 * @param error error object
 * @param vid virtual identity of the client
 * @param info CGI
 *
 * @return SFS_OK if success otherwise SFS_ERROR
 *
 * For directories this routine set's the creation time and the in-memory
 * modification time to the specified modificationt time. For files it
 * set's the modification time.
 */
/*----------------------------------------------------------------------------*/
{
  bool done = false;
  eos::IContainerMD* cmd = 0;

  EXEC_TIMING_BEGIN("Utimes");

  gOFS->MgmStats.Add("Utimes", vid.uid, vid.gid, 1);

  // ---------------------------------------------------------------------------
  eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);
  try
  {
    cmd = gOFS->eosView->getContainer(path);
    UpdateInmemoryDirectoryModificationTime(cmd->getId(), tvp[1]);
    eosView->updateContainerStore(cmd);
    done = true;
  }
  catch (eos::MDException &e)
  {
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
              e.getErrno(), e.getMessage().str().c_str());
  }

  if (!cmd)
  {
    eos::FileMD* fmd = 0;
    // try as a file
    try
    {
      fmd = gOFS->eosView->getFile(path);
      fmd->setMTime(tvp[1]);
      eosView->updateFileStore(fmd);
      done = true;
    }
    catch (eos::MDException &e)
    {
      errno = e.getErrno();
      eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                e.getErrno(), e.getMessage().str().c_str());
    }
  }

  EXEC_TIMING_END("Utimes");

  if (!done)
  {


  }

  return SFS_OK;
}

