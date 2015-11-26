// ----------------------------------------------------------------------
// File: Access.cc
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
XrdMgmOfs::access (const char *inpath,
                   int mode,
                   XrdOucErrInfo &error,
                   const XrdSecEntity *client,
                   const char *ininfo)
/*----------------------------------------------------------------------------*/
/*
 * @brief check access permissions for file/directories
 *
 * @param inpath path to access
 * @param mode access mode can be R_OK |& W_OK |& X_OK or F_OK
 * @param client XRootD authentication object
 * @param ininfo CGI
 * @return SFS_OK if possible otherwise SFS_ERROR
 *
 * See the internal implementation _access for details
 */
/*----------------------------------------------------------------------------*/
{

  static const char *epname = "access";
  const char *tident = error.getErrUser();


  NAMESPACEMAP;
  BOUNCE_ILLEGAL_NAMES;

  XrdOucEnv access_Env(info);

  AUTHORIZE(client, &access_Env, AOP_Stat, "access", inpath, error);

  // use a thread private vid
  eos::common::Mapping::VirtualIdentity vid;

  EXEC_TIMING_BEGIN("IdMap");
  eos::common::Mapping::IdMap(client, info, tident, vid);
  EXEC_TIMING_END("IdMap");

  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);

  BOUNCE_NOT_ALLOWED;
  ACCESSMODE_R;
  MAYSTALL;
  MAYREDIRECT;

  return _access(path, mode, error, vid, info);
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_access (const char *path,
                    int mode,
                    XrdOucErrInfo &error,
                    eos::common::Mapping::VirtualIdentity &vid,
                    const char *info)
/*----------------------------------------------------------------------------*/
/*
 * @brief check access permissions for file/directories
 *
 * @param inpath path to access
 * @param mode access mode can be R_OK |& W_OK |& X_OK or F_OK
 * @param client XRootD authentication object
 * @param ininfo CGI
 * @return SFS_OK if possible otherwise SFS_ERROR
 *
 * If F_OK is specified we just check for the existance of the path, which can
 * be a file or directory. We don't support X_OK since it cannot be mapped
 * in case of files (we don't have explicit execution permissions).
 */
/*----------------------------------------------------------------------------*/
{
  static const char *epname = "_access";

  eos_debug("path=%s mode=%x uid=%u gid=%u", path, mode, vid.uid, vid.gid);
  gOFS->MgmStats.Add("Access", vid.uid, vid.gid, 1);

  eos::common::Path cPath(path);

  eos::IContainerMD* dh = 0;
  eos::IFileMD* fh = 0;
  bool permok = false;
  uint16_t flags = 0;
  uid_t fuid = 99;
  gid_t fgid = 99;
  std::string attr_path = cPath.GetPath();

  // ---------------------------------------------------------------------------
  eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);

  // check for existing file
  try
  {
    fh = gOFS->eosView->getFile(cPath.GetPath());
    flags = fh->getFlags();
    fuid = fh->getCUid();
    fgid = fh->getCGid();
  }
  catch (eos::MDException &e)
  {
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(), e.getMessage().str().c_str());
  }

  try
  {
    dh = gOFS->eosView->getContainer(cPath.GetPath());
  }
  catch (eos::MDException &e)
  {
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(), e.getMessage().str().c_str());
  }

  errno = 0;
  try
  {
    eos::IContainerMD::XAttrMap attrmap;

    if (fh || (!dh))
    {
      std::string uri;
      // if this is a file or a not existing directory we check the access on the parent directory
      if (fh)
      {
        uri = gOFS->eosView->getUri(fh);
      }
      else
      {
        uri = cPath.GetPath();
      }

      eos::common::Path pPath(uri.c_str());

      dh = gOFS->eosView->getContainer(pPath.GetParentPath());
      attr_path = pPath.GetParentPath();
    }

    permok = dh->access(vid.uid, vid.gid, mode);

    // ACL and permission check
    Acl acl(attr_path.c_str(), error, vid, attrmap, false);

    eos_info("acl=%d r=%d w=%d wo=%d x=%d egroup=%d mutable=%d",
             acl.HasAcl(), acl.CanRead(), acl.CanWrite(), acl.CanWriteOnce(),
             acl.CanBrowse(), acl.HasEgroup(), acl.IsMutable());

    if (vid.uid && !acl.IsMutable() && (mode & W_OK))
    {
      eos_debug("msg=\"access\" errno=EPERM reason=\"immutable\"");
      errno = EPERM;
      return Emsg(epname, error, EPERM, "access", path);
    }

    if (!permok)
    {
      // browse permission by ACL
      if (acl.HasAcl())
      {
        permok = true;
        if ((mode & W_OK) && (!acl.CanWrite()))
        {
          permok = false;
        }

        if (mode & R_OK)
        {
          if (!acl.CanRead() &&
              (!dh->access(vid.uid, vid.gid, R_OK)))
          {
            permok = false;
          }
        }

        if (mode & X_OK)
        {
          if ((!acl.CanBrowse()) &&
              (!dh->access(vid.uid, vid.gid, X_OK)))

          {
            permok = false;
          }
        }
      }
    }
    if (fh && (mode & X_OK) && flags)
    {
      // the execution permission is taken from the flags definition of a file
      permok = false;
      if (vid.uid == fuid)
      {
        // user check
        if (flags & S_IXUSR)
          permok = true;
        else
          if (vid.gid == fgid)
        {
          // group check
          if (flags & S_IXGRP)
            permok = true;
          else
          {
            // other check
            if (flags & S_IXOTH)
              permok = true;
          }
        }
      }
    }
  }
  catch (eos::MDException &e)
  {
    dh = 0;
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
              e.getErrno(), e.getMessage().str().c_str());
  }

  // check permissions

  if (!dh)
  {
    eos_debug("msg=\"access\" errno=ENOENT");
    errno = ENOENT;
    return Emsg(epname, error, ENOENT, "access", path);
  }

  // root/daemon can always access, daemon only for reading!
  if ((vid.uid == 0) || ((vid.uid == 2) && (!(mode & W_OK))))
    permok = true;

  if (dh)
  {
    eos_debug("msg=\"access\" uid=%d gid=%d retc=%d mode=%o",
              vid.uid, vid.gid, permok, dh->getMode());
  }

  if (dh && (mode & F_OK))
  {
    return SFS_OK;
  }

  if (dh && permok)
  {
    return SFS_OK;
  }

  if (dh && (!permok))
  {

    errno = EACCES;
    return Emsg(epname, error, EACCES, "access", path);
  }

  errno = EOPNOTSUPP;
  return Emsg(epname, error, EOPNOTSUPP, "access", path);
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::acc_access (const char* path,
                       XrdOucErrInfo &error,
                       eos::common::Mapping::VirtualIdentity &vid,
                       std::string& accperm)
/*----------------------------------------------------------------------------*/
/*
 * @brief define access permissions for files/directories
 *
 * @param path path to access
 * @param error object
 * @param virtual ID of the client
 * @param accperm - return string defining access permission
 * @return SFS_OK if found, otherwise SFS_ERR
 *
 * Definition of accperm see here:
 * Code  Resource         Description
 * S       File or Folder       is shared
 * R       File or Folder       can share (includes reshare)
 * M       File or Folder       is mounted (like on DropBox, Samba, etc.)
 * W       File             can write file
 * C       Folder           can create file in folder
 * K       Folder           can create folder (mkdir)
 * D       File or Folder   can delete file or folder
 * N     File or Folder   can rename file or folder
 * V     File or Folder   can move file or folder
 */
/*----------------------------------------------------------------------------*/
{
  eos_debug("path=%s mode=%x uid=%u gid=%u", path, vid.uid, vid.gid);
  gOFS->MgmStats.Add("Access", vid.uid, vid.gid, 1);

  eos::common::Path cPath(path);

  eos::IContainerMD* dh = 0;
  eos::IFileMD* fh = 0;

  std::string attr_path = cPath.GetPath();
  bool r_ok = false;
  bool w_ok = false;
  bool x_ok = false;
  bool d_ok = false;

  // ---------------------------------------------------------------------------
  eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);

  // check for existing file
  try
  {
    fh = gOFS->eosView->getFile(cPath.GetPath());
  }
  catch (eos::MDException &e)
  {
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(), e.getMessage().str().c_str());
  }

  try
  {
    dh = gOFS->eosView->getContainer(cPath.GetPath());
  }
  catch (eos::MDException &e)
  {
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(), e.getMessage().str().c_str());
  }
  try
  {
    eos::IContainerMD::XAttrMap attrmap;

    if (fh || (!dh))
    {
      std::string uri;
      // if this is a file or a not existing directory we check the access on the parent directory
      if (fh)
      {
        uri = gOFS->eosView->getUri(fh);
      }
      else
      {
        uri = cPath.GetPath();
      }

      eos::common::Path pPath(uri.c_str());

      dh = gOFS->eosView->getContainer(pPath.GetParentPath());
      attr_path = pPath.GetParentPath();
    }


    if (dh->access(vid.uid, vid.gid, R_OK))
      r_ok = true;

    if (dh->access(vid.uid, vid.gid, W_OK))
    {
      w_ok = true;
      d_ok = true;
    }

    if (dh->access(vid.uid, vid.gid, X_OK))
      x_ok = true;

    // ACL and permission check
    Acl acl(attr_path.c_str(),
            error,
            vid,
            attrmap,
            false);

    eos_info("acl=%d r=%d w=%d wo=%d x=%d egroup=%d mutable=%d",
             acl.HasAcl(), acl.CanRead(), acl.CanWrite(), acl.CanWriteOnce(),
             acl.HasAcl(), acl.CanRead(), acl.CanWrite(), acl.CanWriteOnce(),
             acl.CanBrowse(), acl.HasEgroup(), acl.IsMutable());

    // browse permission by ACL
    if (acl.HasAcl())
    {

      // write-once or write is fine for OC write permission
      if (!(acl.CanWrite() || acl.CanWriteOnce()))
        w_ok = false;
      // deletion might be overwritten/forbidden
      if (acl.CanNotDelete())
        d_ok = false;

      // the r/x are added to the posix permissions already set
      if (acl.CanRead())
        r_ok |= true;
      if (acl.CanBrowse())
        x_ok |= true;
      if (!acl.IsMutable())
      {
        w_ok = d_ok = false;
      }
    }
  }
  catch (eos::MDException &e)
  {
    dh = 0;
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
              e.getErrno(), e.getMessage().str().c_str());
  }

  // check permissions
  if (!dh)
  {
    accperm = "";
    return SFS_ERROR;
  }

  // return the OC string;
  accperm == "R";
  if (r_ok)
    accperm += "R";
  if (w_ok)
  {
    accperm += "WCKNV";
  }
  if (d_ok)
  {
    accperm += "D";
  }
  return SFS_OK;
}
/*----------------------------------------------------------------------------*/
