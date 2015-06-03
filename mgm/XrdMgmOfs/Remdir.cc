// ----------------------------------------------------------------------
// File: Remdir.cc
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
XrdMgmOfs::remdir (const char *inpath,
                   XrdOucErrInfo &error,
                   const XrdSecEntity *client,
                   const char *ininfo)
/*----------------------------------------------------------------------------*/
/*
 * @brief delete a directory from the namespace
 *
 * @param inpath directory to delete
 * @param error error object
 * @param client XRootD authentication object
 * @param ininfo CGI
 * @return SFS_OK on success otherwise SFS_ERROR
 *
 */
/*----------------------------------------------------------------------------*/
{

  static const char *epname = "remdir";
  const char *tident = error.getErrUser();

  // use a thread private vid
  eos::common::Mapping::VirtualIdentity vid;

  XrdSecEntity mappedclient;

  NAMESPACEMAP;
  BOUNCE_ILLEGAL_NAMES;

  XrdOucEnv remdir_Env(info);

  AUTHORIZE(client, &remdir_Env, AOP_Delete, "remove", inpath, error);

  EXEC_TIMING_BEGIN("IdMap");
  eos::common::Mapping::IdMap(client, info, tident, vid);
  EXEC_TIMING_END("IdMap");

  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);

  BOUNCE_NOT_ALLOWED;
  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;

  return _remdir(path, error, vid, info);
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_remdir (const char *path,
                    XrdOucErrInfo &error,
                    eos::common::Mapping::VirtualIdentity &vid,
                    const char *ininfo,
                    bool simulate, 
		    bool lock_quota)
/*----------------------------------------------------------------------------*/
/*
 * @brief delete a directory from the namespace
 *
 * @param inpath directory to delete
 * @param error error object
 * @param vid virtual identity of the client
 * @param ininfo CGI
 * @return SFS_OK on success otherwise SFS_ERROR
 *
 * We support a special ACL to forbid deletion if it would be allowed by the
 * normal POSIX settings (ACL !d flag).
 */
/*----------------------------------------------------------------------------*/

{
  static const char *epname = "remdir";
  errno = 0;

  eos_info("path=%s", path);

  EXEC_TIMING_BEGIN("RmDir");

  gOFS->MgmStats.Add("RmDir", vid.uid, vid.gid, 1);

  eos::ContainerMD* dhpar = 0;
  eos::ContainerMD* dh = 0;

  eos::ContainerMD::id_t dh_id = 0;
  eos::ContainerMD::id_t dhpar_id = 0;

  eos::common::Path cPath(path);
  eos::ContainerMD::XAttrMap attrmap;

  // ---------------------------------------------------------------------------
  // make sure this is not a quota node
  // ---------------------------------------------------------------------------
  {
    if (lock_quota)
	Quota::gQuotaMutex.LockRead();
    SpaceQuota* quota = Quota::GetSpaceQuota(path, true);

    if (lock_quota)
	Quota::gQuotaMutex.UnLockRead();
    if (quota)
    {
      errno = EBUSY;
      return Emsg(epname, error, errno, "rmdir - this is a quota node", path);
    }
  }

  // ---------------------------------------------------------------------------
  eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);

  try
  {
    dh = gOFS->eosView->getContainer(path);
    eos::common::Path pPath(gOFS->eosView->getUri(dh).c_str());
    dhpar = gOFS->eosView->getContainer(pPath.GetParentPath());
    dhpar_id = dhpar->getId();
    eos::ContainerMD::XAttrMap::const_iterator it;
    for (it = dhpar->attributesBegin(); it != dhpar->attributesEnd(); ++it)
    {
      attrmap[it->first] = it->second;
    }

    dh_id = dh->getId();
  }
  catch (eos::MDException &e)
  {
    dhpar = 0;
    dh = 0;
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
              e.getErrno(), e.getMessage().str().c_str());
  }

  // check existence
  if (!dh)
  {
    errno = ENOENT;
    return Emsg(epname, error, errno, "rmdir", path);
  }

  Acl acl(attrmap.count("sys.acl") ? attrmap["sys.acl"] : std::string(""), attrmap.count("user.acl") ? attrmap["user.acl"] : std::string(""), vid,
          attrmap.count("sys.eval.useracl"));

  if (vid.uid && !acl.IsMutable())
  {
    errno = EPERM;
    return Emsg(epname, error, EPERM, "rmdir - immutable", path);
  }

  bool stdpermcheck = false;
  bool aclok = false;
  if (acl.HasAcl())
  {
    if ((dh->getCUid() != vid.uid) &&
        (vid.uid) && // not the root user
        (vid.uid != 3) && // not the admin user
        (vid.gid != 4) && // not the admin group
        (acl.CanNotDelete()))
    {
      // deletion is explicitly forbidden
      errno = EPERM;
      return Emsg(epname, error, EPERM, "rmdir by ACL", path);
    }

    if ((!acl.CanWrite()))
    {
      // we have to check the standard permissions
      stdpermcheck = true;
    }
    else
    {
      aclok = true;
    }
  }
  else
  {
    stdpermcheck = true;
  }


  // check permissions
  bool permok = stdpermcheck ? (dhpar ? (dhpar->access(vid.uid, vid.gid, X_OK | W_OK)) : false) : aclok;

  if (!permok)
  {
    errno = EPERM;
    return Emsg(epname, error, errno, "rmdir", path);
  }

  if ((dh->getFlags() && eos::QUOTA_NODE_FLAG) && (vid.uid))
  {
    errno = EADDRINUSE;
    eos_err("%s is a quota node - deletion canceled", path);
    return Emsg(epname, error, errno, "rmdir", path);
  }

  if (!simulate)
  {
    try
    {
      // remove the in-memory modification time of the deleted directory
      gOFS->MgmDirectoryModificationTimeMutex.Lock();
      gOFS->MgmDirectoryModificationTime.erase(dh_id);
      gOFS->MgmDirectoryModificationTimeMutex.UnLock();
      // update the in-memory modification time of the parent directory
      UpdateNowInmemoryDirectoryModificationTime(dhpar_id);

      eosView->removeContainer(path);
    }
    catch (eos::MDException &e)
    {
      errno = e.getErrno();
      eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                e.getErrno(), e.getMessage().str().c_str());
    }
  }

  EXEC_TIMING_END("RmDir");

  if (errno)
  {
    return Emsg(epname, error, errno, "rmdir", path);
  }
  else
  {

    return SFS_OK;
  }
}

