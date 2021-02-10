// ----------------------------------------------------------------------
// File: Chmod.cc
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
XrdMgmOfs::chmod(const char* inpath,
                 XrdSfsMode Mode,
                 XrdOucErrInfo& error,
                 const XrdSecEntity* client,
                 const char* ininfo)
/*----------------------------------------------------------------------------*/
/*
 * @brief change the mode of a directory
 *
 * @param inpath path to chmod
 * @param Mode mode to set
 * @param error error object
 * @param client XRootD authentication object
 * @param ininfo CGI
 *
 * Function calls the internal _chmod function. See info there for details.
 */
/*----------------------------------------------------------------------------*/
{
  static const char* epname = "chmod";
  const char* tident = error.getErrUser();
  //  mode_t acc_mode = Mode & S_IAMB;
  // use a thread private vid
  eos::common::VirtualIdentity vid;
  EXEC_TIMING_BEGIN("IdMap");
  eos::common::Mapping::IdMap(client, ininfo, tident, vid);
  EXEC_TIMING_END("IdMap");
  NAMESPACEMAP;
  BOUNCE_ILLEGAL_NAMES;
  XrdOucEnv chmod_Env(ininfo);
  AUTHORIZE(client, &chmod_Env, AOP_Chmod, "chmod", inpath, error);
  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);
  BOUNCE_NOT_ALLOWED;
  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;
  return _chmod(path, Mode, error, vid, ininfo);
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_chmod(const char* path,
                  XrdSfsMode& Mode,
                  XrdOucErrInfo& error,
                  eos::common::VirtualIdentity& vid,
                  const char* ininfo)
/*----------------------------------------------------------------------------*/
/*
 * @brief change mode of a directory or file
 *
 * @param path where to chmod
 * @param Mode mode to set (and effective mode returned)
 * @param error error object
 * @param vid virtual identity of the client
 * @param ininfo CGI
 * @return SFS_OK on success otherwise SFS_ERR
 *
 * EOS supports mode bits only on directories, file inherit them from the parent.
 * Only the owner, the admin user, the admin group, root and an ACL chmod granted
 * user are allowed to run this operation on a directory.
 */
/*----------------------------------------------------------------------------*/
{
  static const char* epname = "chmod";
  EXEC_TIMING_BEGIN("Chmod");
  // ---------------------------------------------------------------------------
  eos::Prefetcher::prefetchContainerMDAndWait(gOFS->eosView, path);
  eos::Prefetcher::prefetchFileMDAndWait(gOFS->eosView, path);
  eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__, __FILE__);
  std::shared_ptr<eos::IContainerMD> cmd;
  std::shared_ptr<eos::IContainerMD> pcmd;
  std::shared_ptr<eos::IFileMD> fmd;
  eos::IContainerMD::XAttrMap attrmap;
  errno = 0;
  gOFS->MgmStats.Add("Chmod", vid.uid, vid.gid, 1);
  eos_info("path=%s mode=%o", path, Mode);
  eos::common::Path cPath(path);

  try {
    cmd = gOFS->eosView->getContainer(path);
  } catch (eos::MDException& e) {
    errno = e.getErrno();
  }

  if (!cmd) {
    errno = 0;

    // Check if this is a file
    try {
      fmd = gOFS->eosView->getFile(path);
    } catch (eos::MDException& e) {
      errno = e.getErrno();
    }
  }

  if (cmd || fmd)
    try {
      std::string uri;

      if (cmd) {
        uri = gOFS->eosView->getUri(cmd.get());
      } else {
        uri = gOFS->eosView->getUri(fmd.get());
      }

      eos::common::Path pPath(uri.c_str());
      pcmd = gOFS->eosView->getContainer(pPath.GetParentPath());
      // ACL and permission check
      Acl acl(pPath.GetParentPath(), error, vid, attrmap, false);

      if (vid.uid && !acl.IsMutable()) {
        // immutable directory
        errno = EPERM;
      } else {
        // If owner without revoked chmod permissions
        if (((fmd && (fmd->getCUid() == vid.uid)) && (!acl.CanNotChmod())) ||
            ((cmd && (cmd->getCUid() == vid.uid)) && (!acl.CanNotChmod())) ||
            (!vid.uid) || // the root user
	    (vid.sudoer) ||
	    (vid.hasUid(3)) ||
	    (vid.hasGid(4)) ||
            (acl.CanChmod())
           ) { // a pre-defined mask to apply to the desired modbits
          // the chmod ACL entry
          // change the permission mask, but make sure it is set to a directory
          long mask = 07777777;

          if (Mode & S_IFREG) {
            Mode ^= S_IFREG;
          }

          if ((Mode & S_ISUID)) {
            Mode ^= S_ISUID;
          }

          eosView->updateContainerStore(pcmd.get());
          eos::ContainerIdentifier pcmd_id = pcmd->getIdentifier();
          eos::ContainerIdentifier pcmd_pid = pcmd->getParentIdentifier();
          eos::ContainerIdentifier cmd_id;
          eos::ContainerIdentifier cmd_pid;
          eos::FileIdentifier f_id;

          if (cmd) {
            Mode &= mask;
            cmd->setMode(Mode | S_IFDIR);
            cmd->setCTimeNow();
            // store the in-memory modification time for this directory
            eosView->updateContainerStore(cmd.get());
            cmd_id = cmd->getIdentifier();
            cmd_pid = cmd->getParentIdentifier();
          }

          if (fmd) {
            // we just store 9 bits in flags
            Mode &= (S_IRWXU | S_IRWXG | S_IRWXO);
            fmd->setFlags(Mode);
            eosView->updateFileStore(fmd.get());
            f_id = fmd->getIdentifier();
          }

          lock.Release();
          gOFS->FuseXCastContainer(pcmd_id);
          gOFS->FuseXCastRefresh(pcmd_id, pcmd_pid);

          if (cmd) {
            gOFS->FuseXCastContainer(cmd_id);
            gOFS->FuseXCastRefresh(cmd_id, cmd_pid);
          }

          if (fmd) {
            gOFS->FuseXCastFile(f_id);
          }

          errno = 0;
        } else {
          errno = EPERM;
        }
      }
    } catch (eos::MDException& e) {
      errno = e.getErrno();
    }

  if (cmd && (!errno)) {
    EXEC_TIMING_END("Chmod");
    return SFS_OK;
  }

  if (fmd && (!errno)) {
    EXEC_TIMING_END("Chmod");
    return SFS_OK;
  }

  return Emsg(epname, error, errno, "chmod", path);
}
