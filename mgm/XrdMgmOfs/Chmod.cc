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
XrdMgmOfs::chmod (const char *inpath,
                  XrdSfsMode Mode,
                  XrdOucErrInfo &error,
                  const XrdSecEntity *client,
                  const char *ininfo)
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

  static const char *epname = "chmod";
  const char *tident = error.getErrUser();
  //  mode_t acc_mode = Mode & S_IAMB;

  // use a thread private vid
  eos::common::Mapping::VirtualIdentity vid;


  NAMESPACEMAP;
  BOUNCE_ILLEGAL_NAMES;

  XrdOucEnv chmod_Env(info);

  AUTHORIZE(client, &chmod_Env, AOP_Chmod, "chmod", inpath, error);

  EXEC_TIMING_BEGIN("IdMap");
  eos::common::Mapping::IdMap(client, info, tident, vid);
  EXEC_TIMING_END("IdMap");
  
  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);

  BOUNCE_NOT_ALLOWED;
  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;

  return _chmod(path, Mode, error, vid, info);
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_chmod (const char *path,
                   XrdSfsMode Mode,
                   XrdOucErrInfo &error,
                   eos::common::Mapping::VirtualIdentity &vid,
                   const char *ininfo)
/*----------------------------------------------------------------------------*/
/*
 * @brief change mode of a directory or file
 *
 * @param path where to chmod
 * @param Mode mode to set
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
  static const char *epname = "chmod";

  EXEC_TIMING_BEGIN("Chmod");

  // ---------------------------------------------------------------------------
  eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);
  eos::IContainerMD* cmd = 0;
  eos::IContainerMD* pcmd = 0;
  eos::IFileMD* fmd = 0;
  eos::IContainerMD::XAttrMap attrmap;

  errno = 0;
  gOFS->MgmStats.Add("Chmod", vid.uid, vid.gid, 1);
  eos_info("path=%s mode=%o", path, Mode);
  eos::common::Path cPath(path);

  try
  {
    cmd = gOFS->eosView->getContainer(path);
  }
  catch (eos::MDException &e)
  {
    errno = e.getErrno();
  }

  if (!cmd)
  {
    errno = 0;
    // ---------------------------------------------------------------------------
    // try if this is a file
    // ---------------------------------------------------------------------------
    try
    {
      fmd = gOFS->eosView->getFile(path);

    }
    catch (eos::MDException &e)
    {
      errno = e.getErrno();
    }
  }

  if (cmd || fmd)
    try
    {
      pcmd = gOFS->eosView->getContainer(cPath.GetParentPath());

      eos::IContainerMD::XAttrMap::const_iterator it;
      for (it = pcmd->attributesBegin(); it != pcmd->attributesEnd(); ++it)
      {
        attrmap[it->first] = it->second;
      }
      // acl of the parent!
      Acl acl(attrmap.count("sys.acl") ? attrmap["sys.acl"] : std::string(""),
              attrmap.count("user.acl") ? attrmap["user.acl"] : std::string(""), vid, attrmap.count("sys.eval.useracl"));


      if (vid.uid && !acl.IsMutable())
      {
        // immutable directory
        errno = EPERM;
      }
      else
      {
        if (((fmd && (fmd->getCUid() == vid.uid)) && (!acl.CanNotChmod())) || // the owner without revoked chmod permissions
            ((cmd && (cmd->getCUid() == vid.uid)) && (!acl.CanNotChmod())) || // the owner without revoked chmod permissions
            (!vid.uid) || // the root user
            (vid.uid == 3) || // the admin user
            (vid.gid == 4) || // the admin group
            (acl.CanChmod()))
        { // the chmod ACL entry
          // change the permission mask, but make sure it is set to a directory
          if (Mode & S_IFREG)
            Mode ^= S_IFREG;
          if ((Mode & S_ISUID))
          {
            Mode ^= S_ISUID;
          }
          else
          {
            if (!(Mode & S_ISGID))
            {
              Mode |= S_ISGID;
            }
          }

          // store the in-memory modification time for parent
          UpdateNowInmemoryDirectoryModificationTime(pcmd->getId());
          if (cmd)
          {
            cmd->setMode(Mode | S_IFDIR);
            // store the in-memory modification time for this directory
            UpdateNowInmemoryDirectoryModificationTime(cmd->getId());
            eosView->updateContainerStore(cmd);
          }
          if (fmd)
          {
            // we just store 9 bits in flags
            Mode &= (S_IRWXU | S_IRWXG | S_IRWXO);
            fmd->setFlags(Mode);
            eosView->updateFileStore(fmd);
          }
          errno = 0;
        }
        else
        {
          errno = EPERM;
        }
      }
    }
    catch (eos::MDException &e)
    {
      errno = e.getErrno();
    }

  if (cmd && (!errno))
  {

    EXEC_TIMING_END("Chmod");
    return SFS_OK;
  }

  if (fmd && (!errno))
  {

    EXEC_TIMING_END("Chmod");
    return SFS_OK;
  }

  return Emsg(epname, error, errno, "chmod", path);
}

