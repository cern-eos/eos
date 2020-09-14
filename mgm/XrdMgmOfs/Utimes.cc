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
XrdMgmOfs::utimes(const char* inpath,
                  struct timespec* tvp,
                  XrdOucErrInfo& error,
                  const XrdSecEntity* client,
                  const char* ininfo)
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
  static const char* epname = "utimes";
  const char* tident = error.getErrUser();
  // use a thread private vid
  eos::common::VirtualIdentity vid;
  EXEC_TIMING_BEGIN("IdMap");
  eos::common::Mapping::IdMap(client, ininfo, tident, vid);
  EXEC_TIMING_END("IdMap");
  NAMESPACEMAP;
  BOUNCE_ILLEGAL_NAMES;
  XrdOucEnv utimes_Env(ininfo);
  AUTHORIZE(client, &utimes_Env, AOP_Update, "set utimes", inpath, error);
  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);
  BOUNCE_NOT_ALLOWED;
  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;
  return _utimes(path, tvp, error, vid, ininfo);
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_utimes(const char* path,
                   struct timespec* tvp,
                   XrdOucErrInfo& error,
                   eos::common::VirtualIdentity& vid,
                   const char* info)
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
 * For directories this routine set's the modification
 * time to the specified modification time. For files it
 * set's the modification time.
 */
/*----------------------------------------------------------------------------*/
{
  std::shared_ptr<eos::IContainerMD> cmd;
  EXEC_TIMING_BEGIN("Utimes");
  gOFS->MgmStats.Add("Utimes", vid.uid, vid.gid, 1);
  eos_info("calling utimes for path=%s, uid=%i, gid=%i", path, vid.uid, vid.gid);
  // ---------------------------------------------------------------------------
  eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__, __FILE__);

  if (gOFS->_access(path,
		    W_OK,
		    error,
		    vid,
		    info,
		    false))
  {
    return SFS_ERROR;
  }

  try {
    cmd = gOFS->eosView->getContainer(path, false);

    cmd->setMTime(tvp[1]);
    cmd->notifyMTimeChange(gOFS->eosDirectoryService);
    eosView->updateContainerStore(cmd.get());
  } catch (eos::MDException& e) {
    errno = e.getErrno();
    eos_err("msg=\"exception\" ec=%d emsg=\"%s\"\n",
            e.getErrno(), e.getMessage().str().c_str());
  }

  if (!cmd) {
    std::shared_ptr<eos::IFileMD> fmd;

    try {
      fmd = gOFS->eosView->getFile(path, false);
      // Check permissions on the directory
      eos::common::Path cont_path(path);
      cmd = gOFS->eosView->getContainer(cont_path.GetParentPath(), false);

      // Set the ctime only if different from 0.0
      if (tvp[0].tv_sec != 0 || tvp[0].tv_nsec != 0) {
        fmd->setCTime(tvp[0]);
      }

      fmd->setMTime(tvp[1]);
      eosView->updateFileStore(fmd.get());
    } catch (eos::MDException& e) {
      errno = e.getErrno();
      eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                e.getErrno(), e.getMessage().str().c_str());
    }
  }

  EXEC_TIMING_END("Utimes");
  return SFS_OK;
}
