// ----------------------------------------------------------------------
// File: Mkdir.cc
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
XrdMgmOfs::mkdir (const char *inpath,
                  XrdSfsMode Mode,
                  XrdOucErrInfo &error,
                  const XrdSecEntity *client,
                  const char *ininfo,
                  ino_t* outino)
/*----------------------------------------------------------------------------*/
/*
 * @brief create a directory with the given mode
 *
 * @param inpath directory path to create
 * @param Mode mode to set
 * @param error error object
 * @param client XRootD authentication object
 * @param ininfo CGI
 * @param outino return inode number
 * @return SFS_OK if success otherwise SFS_ERROR
 *
 * If mode contains SFS_O_MKPTH the full path is (possibly) created.
 */
/*----------------------------------------------------------------------------*/
{

  static const char *epname = "mkdir";
  const char *tident = error.getErrUser();

  // use a thread private vid
  eos::common::Mapping::VirtualIdentity vid;

  NAMESPACEMAP;
  BOUNCE_ILLEGAL_NAMES;

  XrdOucEnv mkdir_Env(info);

  EXEC_TIMING_BEGIN("IdMap");
  eos::common::Mapping::IdMap(client, info, tident, vid);
  EXEC_TIMING_END("IdMap");

  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);

  eos_info("path=%s ininfo=%s info=%s", path, ininfo, info);

  BOUNCE_NOT_ALLOWED;
  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;

  return _mkdir(path, Mode, error, vid, info, outino);
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_mkdir (const char *path,
                   XrdSfsMode Mode,
                   XrdOucErrInfo &error,
                   eos::common::Mapping::VirtualIdentity &vid,
                   const char *ininfo,
                   ino_t* outino)
/*----------------------------------------------------------------------------*/
/*
 * @brief create a directory with the given mode
 *
 * @param inpath directory path to create
 * @param Mode mode to set
 * @param error error object
 * @param client XRootD authentication object
 * @param ininfo CGI
 * @param outino return inode number
 * @return SFS_OK on success otherwise SFS_ERROR
 *
 * If mode contains SFS_O_MKPTH the full path is (possibly) created.
 *
 */
/*----------------------------------------------------------------------------*/
{
  static const char *epname = "_mkdir";
  mode_t acc_mode = (Mode & S_IAMB) | S_IFDIR;
  errno = 0;

  EXEC_TIMING_BEGIN("Mkdir");

  gOFS->MgmStats.Add("Mkdir", vid.uid, vid.gid, 1);

  //  const char *tident = error.getErrUser();

  XrdOucString spath = path;

  eos_info("path=%s", spath.c_str());

  if (!spath.beginswith("/"))
  {
    errno = EINVAL;
    return Emsg(epname, error, EINVAL,
                "create directory - you have to specifiy an absolute pathname",
                path);
  }

  bool recurse = false;

  eos::common::Path cPath(path);
  bool noParent = false;

  eos::IContainerMD* dir = 0;
  eos::IContainerMD::XAttrMap attrmap;
  // TODO: use std::unique_ptr to simplify the memory mgm of copydir
  eos::IContainerMD* copydir = 0;

  {
    // -------------------------------------------------------------------------
    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);

    // check for the parent directory
    if (spath != "/")
    {
      try
      {
        dir = eosView->getContainer(cPath.GetParentPath());
        copydir = dir->clone();
        dir = copydir;
      }
      catch (eos::MDException &e)
      {
        dir = 0;
        eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                  e.getErrno(), e.getMessage().str().c_str());
        noParent = true;
      }
    }

    // check permission
    if (dir)
    {
      uid_t d_uid = dir->getCUid();
      gid_t d_gid = dir->getCGid();

      // ACL and permission check
      Acl acl(cPath.GetParentPath(), error, vid, attrmap, false);

      eos_info("acl=%d r=%d w=%d wo=%d egroup=%d mutable=%d",
               acl.HasAcl(), acl.CanRead(), acl.CanWrite(), acl.CanWriteOnce(),
               acl.HasEgroup(),
               acl.IsMutable());

      if (vid.uid && !acl.IsMutable())
      {
        // immutable directory
        errno = EPERM;
        return Emsg(epname, error, EPERM, "create directory - immutable", cPath.GetParentPath());
      }
      bool sticky_owner = false;

      // Check for sys.owner.auth entries, which let people operate as the owner of the directory
      if (attrmap.count("sys.owner.auth"))
      {
        if (attrmap["sys.owner.auth"] == "*")
        {
          sticky_owner = true;
        }
        else
        {
          attrmap["sys.owner.auth"] += ",";
          std::string ownerkey = vid.prot.c_str();
          ownerkey += ":";
          if (vid.prot == "gsi")
          {
            ownerkey += vid.dn.c_str();
          }
          else
          {
            ownerkey += vid.uid_string.c_str();
          }
          if ((attrmap["sys.owner.auth"].find(ownerkey)) != std::string::npos)
          {
            eos_info("msg=\"client authenticated as directory owner\" path=\"%s\"uid=\"%u=>%u\" gid=\"%u=>%u\"",
                     path, vid.uid, vid.gid, d_uid, d_gid);
            // yes the client can operate as the owner, we rewrite the virtual identity to the directory uid/gid pair
            vid.uid = d_uid;
            vid.gid = d_gid;
          }
        }
      }
      bool stdpermcheck = false;

      if (acl.HasAcl())
      {
        if ((!acl.CanWrite()) && (!acl.CanWriteOnce()))
        {
          // we have to check the standard permissions
          stdpermcheck = true;
        }
      }
      else
      {
        stdpermcheck = true;
      }


      // admin's can always create a directory
      if (stdpermcheck && (!dir->access(vid.uid, vid.gid, X_OK | W_OK)))
      {
        if (copydir) delete copydir;

        errno = EPERM;
        return Emsg(epname, error, EPERM, "create parent directory", cPath.GetParentPath());
      }
      if (sticky_owner)
      {
        eos_info("msg=\"client acting as directory owner\" path=\"%s\"uid=\"%u=>%u\" gid=\"%u=>%u\"",
                 path, vid.uid, vid.gid, d_uid, d_gid);
        // The client can operate as the owner, we rewrite the virtual identity
        // to the directory uid/gid pair
        vid.uid = d_uid;
        vid.gid = d_gid;
      }
    }
  }

  // check if the path exists anyway
  if (Mode & SFS_O_MKPTH)
  {
    recurse = true;
    eos_debug("SFS_O_MKPATH set", path);
    // short cut if it exists already
    eos::IContainerMD* fulldir = 0;

    if (dir)
    {
      eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
      // only if the parent exists, the full path can exist!
      try
      {
        fulldir = eosView->getContainer(path);
      }
      catch (eos::MDException &e)
      {
        fulldir = 0;
        eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                  e.getErrno(), e.getMessage().str().c_str());
      }
      if (fulldir)
      {
        if (copydir) delete copydir;
        EXEC_TIMING_END("Exists");
        return SFS_OK;
      }
    }
  }

  eos_debug("mkdir path=%s deepness=%d dirname=%s basename=%s",
            path, cPath.GetSubPathSize(), cPath.GetParentPath(), cPath.GetName());
  eos::IContainerMD* newdir = 0;

  if (noParent)
  {
    if (recurse)
    {
      int i, j;
      std::string existingdir;

      uid_t d_uid = 99;
      gid_t d_gid = 99;

      // go the paths up until one exists!
      for (i = cPath.GetSubPathSize() - 1; i >= 0; i--)
      {
        eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
        attrmap.clear();
        eos_debug("testing path %s", cPath.GetSubPath(i));
        try
        {
          if (copydir) delete copydir;
          dir = eosView->getContainer(cPath.GetSubPath(i));
          copydir = dir->clone();
	  existingdir = cPath.GetSubPath(i);
	  d_uid = dir->getCUid();
	  d_gid = dir->getCGid();
        }
        catch (eos::MDException &e)
        {
          dir = 0;
        }
        if (dir)
          break;
      }
      // that is really a serious problem!
      if (!dir)
      {
        if (copydir) delete copydir;
        eos_crit("didn't find any parent path traversing the namespace");
        errno = ENODATA;
        // ---------------------------------------------------------------------
        return Emsg(epname, error, ENODATA, "create directory", cPath.GetSubPath(i));
      }

      // ACL and permission check
      Acl acl(existingdir.c_str(),
	      error,
	      vid,
	      attrmap,
	      true);

      eos_info("acl=%d r=%d w=%d wo=%d egroup=%d mutable=%d",
               acl.HasAcl(), acl.CanRead(), acl.CanWrite(), acl.CanWriteOnce(),
               acl.HasEgroup(),
               acl.IsMutable());

      // Check for sys.owner.auth entries, which let people operate as the owner of the directory
      if (attrmap.count("sys.owner.auth"))
      {
        if (attrmap["sys.owner.auth"] == "*")
	{
	  eos_info("msg=\"client acting as directory owner\" path=\"%s\"uid=\"%u=>%u\" gid=\"%u=>%u\"",
		   existingdir.c_str(), vid.uid, vid.gid, d_uid, d_gid);
	  // yes the client can operate as the owner, we rewrite the virtual identity to the directory uid/gid pair
	  vid.uid = d_uid;
	  vid.gid = d_gid;
        }
        else
        {
          attrmap["sys.owner.auth"] += ",";
          std::string ownerkey = vid.prot.c_str();
          ownerkey += ":";
          if (vid.prot == "gsi")
          {
            ownerkey += vid.dn.c_str();
          }
          else
          {
            ownerkey += vid.uid_string.c_str();
          }
          if ((attrmap["sys.owner.auth"].find(ownerkey)) != std::string::npos)
          {
            eos_info("msg=\"client authenticated as directory owner\" path=\"%s\"uid=\"%u=>%u\" gid=\"%u=>%u\"",
                     path, vid.uid, vid.gid, d_uid, d_gid);
            // yes the client can operate as the owner, we rewrite the virtual identity to the directory uid/gid pair
            vid.uid = d_uid;
            vid.gid = d_gid;
          }
        }
      }

      if (vid.uid && !acl.IsMutable())
      {
        errno = EPERM;
        return Emsg(epname, error, EPERM, "create parent directory - immutable",
                    cPath.GetParentPath());
      }

      bool stdpermcheck = false;
      if (acl.HasAcl())
      {
        if ((!acl.CanWrite()) && (!acl.CanWriteOnce()))
        {
          // we have to check the standard permissions
          stdpermcheck = true;
        }
      }
      else
      {
        stdpermcheck = true;
      }

      if (stdpermcheck && (!dir->access(vid.uid, vid.gid, X_OK | W_OK)))
      {
        if (copydir) delete copydir;
        errno = EPERM;

        return Emsg(epname, error, EPERM, "create parent directory",
                    cPath.GetParentPath());
      }


      for (j = i + 1; j < (int) cPath.GetSubPathSize(); j++)
      {
        eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);
        try
        {
          eos_debug("creating path %s", cPath.GetSubPath(j));
          newdir = eosView->createContainer(cPath.GetSubPath(j), recurse);
          newdir->setCUid(vid.uid);
          newdir->setCGid(vid.gid);
          newdir->setMode(dir->getMode());
          if (dir->getMode() & S_ISGID)
          {
            // inherit the attributes
            eos::IContainerMD::XAttrMap::const_iterator it;
            for (it = dir->attributesBegin(); it != dir->attributesEnd(); ++it)
            {
              newdir->setAttribute(it->first, it->second);
            }
          }

	  // store the in-memory modification time into the parent
	  eos::IContainerMD::ctime_t ctime;
	  newdir->getCTime(ctime);
	  UpdateInmemoryDirectoryModificationTime(dir->getId(), ctime);

          // commit
          eosView->updateContainerStore(newdir);
        }
        catch (eos::MDException &e)
        {
          errno = e.getErrno();
          eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                    e.getErrno(), e.getMessage().str().c_str());
        }

        dir = newdir;

        if (dir)
        {
          if (copydir) delete copydir;
          copydir = dir->clone();
          dir = copydir;
        }

        if (!newdir)
        {
          if (copydir) delete copydir;
          return Emsg(epname, error, errno, "mkdir", path);
        }
      }
    }
    else
    {
      if (copydir) delete copydir;
      errno = ENOENT;
      return Emsg(epname, error, errno, "mkdir", path);
    }
  }

  // this might not be needed, but it is detected by coverty
  if (!dir)
  {
    return Emsg(epname, error, errno, "mkdir", path);
  }

  eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);
  try
  {
    newdir = eosView->createContainer(path);
    newdir->setCUid(vid.uid);
    newdir->setCGid(vid.gid);
    newdir->setMode(acc_mode);
    newdir->setMode(dir->getMode());

    // store the in-memory modification time
    eos::IContainerMD::ctime_t ctime;
    newdir->getCTime(ctime);
    UpdateInmemoryDirectoryModificationTime(dir->getId(), ctime);

    if ((dir->getMode() & S_ISGID) &&
        (cPath.GetFullPath().find(EOS_COMMON_PATH_VERSION_PREFIX) == STR_NPOS))
    {
      // inherit the attributes - not for version directories
      eos::IContainerMD::XAttrMap::const_iterator it;
      for (it = dir->attributesBegin(); it != dir->attributesEnd(); ++it)
      {
        newdir->setAttribute(it->first, it->second);
      }
    }
    if (outino)
    {
      *outino = newdir->getId();
    }
    // commit on disk
    eosView->updateContainerStore(newdir);
  }
  catch (eos::MDException &e)
  {
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
              e.getErrno(), e.getMessage().str().c_str());
  }

  if (copydir) delete copydir;

  if (!newdir)
  {

    return Emsg(epname, error, errno, "mkdir", path);
  }

  EXEC_TIMING_END("Mkdir");
  return SFS_OK;
}
