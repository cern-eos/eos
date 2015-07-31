// ----------------------------------------------------------------------
// File: Attr.cc
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
XrdMgmOfs::attr_ls (const char *inpath,
                    XrdOucErrInfo &error,
                    const XrdSecEntity *client,
                    const char *ininfo,
                    eos::IContainerMD::XAttrMap & map)
/*----------------------------------------------------------------------------*/
/*
 * @brief list extended attributes for a given directory
 *
 * @param inpath directory name to list attributes
 * @param error error object
 * @param client XRootD authentication object
 * @param ininfo CGI
 * @param map return object with the extended attribute key-value map
 *
 * @return SFS_OK if success otherwise SFS_ERROR
 *
 * See _attr_ls for details on the internals.
 */
/*----------------------------------------------------------------------------*/
{

  static const char *epname = "attr_ls";
  const char *tident = error.getErrUser();

  // use a thread private vid
  eos::common::Mapping::VirtualIdentity vid;

  NAMESPACEMAP;
  BOUNCE_ILLEGAL_NAMES;

  XrdOucEnv access_Env(info);

  AUTHORIZE(client, &access_Env, AOP_Stat, "access", inpath, error);

  EXEC_TIMING_BEGIN("IdMap");
  eos::common::Mapping::IdMap(client, info, tident, vid);
  EXEC_TIMING_END("IdMap");

  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);

  BOUNCE_NOT_ALLOWED;

  return _attr_ls(path, error, vid, info, map);
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::attr_set (const char *inpath,
                     XrdOucErrInfo &error,
                     const XrdSecEntity *client,
                     const char *ininfo,
                     const char *key,
                     const char *value)
/*----------------------------------------------------------------------------*/
/*
 * @brief set an extended attribute for a given directory to key=value
 *
 * @param inpath directory name to set attribute
 * @param error error object
 * @param client XRootD authentication object
 * @param ininfo CGI
 * @param key key to set
 * @param value value to set for key
 *
 * @return SFS_OK if success otherwise SFS_ERROR
 *
 * See _attr_set for details on the internals.
 */
/*----------------------------------------------------------------------------*/
{

  static const char *epname = "attr_set";
  const char *tident = error.getErrUser();
  // use a thread private vid
  eos::common::Mapping::VirtualIdentity vid;

  NAMESPACEMAP;
  BOUNCE_ILLEGAL_NAMES;

  XrdOucEnv access_Env(info);

  AUTHORIZE(client, &access_Env, AOP_Update, "update", inpath, error);

  EXEC_TIMING_BEGIN("IdMap");
  eos::common::Mapping::IdMap(client, info, tident, vid);
  EXEC_TIMING_END("IdMap");

  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);

  BOUNCE_NOT_ALLOWED;

  return _attr_set(path, error, vid, info, key, value);
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::attr_get (const char *inpath,
                     XrdOucErrInfo &error,
                     const XrdSecEntity *client,
                     const char *ininfo,
                     const char *key,
                     XrdOucString & value)
/*----------------------------------------------------------------------------*/
/*
 * @brief get an extended attribute for a given directory by key
 *
 * @param inpath directory name to get attribute
 * @param error error object
 * @param client XRootD authentication object
 * @param ininfo CGI
 * @param key key to retrieve
 * @param value variable to store the value
 *
 * @return SFS_OK if success otherwise SFS_ERROR
 *
 * See _attr_get for details on the internals.
 */
/*----------------------------------------------------------------------------*/
{

  static const char *epname = "attr_get";
  const char *tident = error.getErrUser();
  // use a thread private vid
  eos::common::Mapping::VirtualIdentity vid;


  NAMESPACEMAP;
  BOUNCE_ILLEGAL_NAMES;

  XrdOucEnv access_Env(info);

  AUTHORIZE(client, &access_Env, AOP_Stat, "access", inpath, error);

  EXEC_TIMING_BEGIN("IdMap");
  eos::common::Mapping::IdMap(client, info, tident, vid);
  EXEC_TIMING_END("IdMap");

  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);

  BOUNCE_NOT_ALLOWED;

  return _attr_get(path, error, vid, info, key, value);
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::attr_rem (const char *inpath,
                     XrdOucErrInfo &error,
                     const XrdSecEntity *client,
                     const char *ininfo,
                     const char *key)
/*----------------------------------------------------------------------------*/
/*
 * @brief delete an extended attribute for a given directory by key
 *
 * @param inpath directory name to delete attribute
 * @param error error object
 * @param client XRootD authentication object
 * @param ininfo CGI
 * @param key key to delete
 *
 * @return SFS_OK if success otherwise SFS_ERROR
 *
 * See _attr_rem for details on the internals.
 */
/*----------------------------------------------------------------------------*/
{

  static const char *epname = "attr_rm";
  const char *tident = error.getErrUser();
  // use a thread private vid
  eos::common::Mapping::VirtualIdentity vid;

  NAMESPACEMAP;

  BOUNCE_ILLEGAL_NAMES;

  XrdOucEnv access_Env(info);

  AUTHORIZE(client, &access_Env, AOP_Delete, "delete", inpath, error);

  EXEC_TIMING_BEGIN("IdMap");
  eos::common::Mapping::IdMap(client, info, tident, vid);
  EXEC_TIMING_END("IdMap");

  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);

  BOUNCE_NOT_ALLOWED;

  return _attr_rem(path, error, vid, info, key);
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_attr_ls (const char *path,
                     XrdOucErrInfo &error,
                     eos::common::Mapping::VirtualIdentity &vid,
                     const char *info,
                     eos::IContainerMD::XAttrMap & map,
                     bool lock,
                     bool links)
/*----------------------------------------------------------------------------*/
/*
 * @brief list extended attributes for a given directory
 *
 * @param path directory name to list attributes
 * @param error error object
 * @param vid virtual identity of the client
 * @param info CGI
 * @param map return object with the extended attribute key-value map
 *
 * @return SFS_OK if success otherwise SFS_ERROR
 *
 * Normal unix permissions R_OK & X_OK are needed to list attributes.
 */
/*----------------------------------------------------------------------------*/
{
  static const char *epname = "attr_ls";
  eos::IContainerMD *dh = 0;
  errno = 0;

  EXEC_TIMING_BEGIN("AttrLs");

  gOFS->MgmStats.Add("AttrLs", vid.uid, vid.gid, 1);

  // ---------------------------------------------------------------------------
  if (lock)
    gOFS->eosViewRWMutex.LockRead();

  try
  {
    dh = gOFS->eosView->getContainer(path);
    eos::IContainerMD::XAttrMap::const_iterator it;
    for (it = dh->attributesBegin(); it != dh->attributesEnd(); ++it)
    {
      map[it->first] = it->second;
    }
  }
  catch (eos::MDException &e)
  {
    dh = 0;
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(), e.getMessage().str().c_str());
  }

  // check for attribute references
  if (map.count("sys.attr.link"))
  {
    try
    {
      dh = gOFS->eosView->getContainer(map["sys.attr.link"]);
      eos::IContainerMD::XAttrMap::const_iterator it;
      for (it = dh->attributesBegin(); it != dh->attributesEnd(); ++it)
      {
        XrdOucString key = it->first.c_str();
        if (links)
          key.replace("sys.", "sys.link.");

        if (!map.count(it->first))
          map[key.c_str()] = it->second;
      }
    }
    catch (eos::MDException &e)
    {
      dh = 0;
      errno = e.getErrno();
      eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(), e.getMessage().str().c_str());
    }
  }

  if (lock)
    gOFS->eosViewRWMutex.UnLockRead();

  EXEC_TIMING_END("AttrLs");

  if (errno)
    return Emsg(epname, error, errno, "list attributes", path);

  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_attr_set (const char *path,
                      XrdOucErrInfo &error,
                      eos::common::Mapping::VirtualIdentity &vid,
                      const char *info,
                      const char *key,
                      const char *value)
/*----------------------------------------------------------------------------*/
/*
 * @brief set an extended attribute for a given directory with key=value
 *
 * @param path directory name to set attribute
 * @param error error object
 * @param vid virtual identity of the client
 * @param info CGI
 * @param key key to set
 * @param value value for key
 *
 * @return SFS_OK if success otherwise SFS_ERROR
 *
 * Only the owner of a directory can set extended attributes with user prefix.
 * sys prefix attributes can be set only by sudo'ers or root.
 */
/*----------------------------------------------------------------------------*/
{
  static const char *epname = "attr_set";
  eos::IContainerMD *dh = 0;
  errno = 0;

  EXEC_TIMING_BEGIN("AttrSet");

  gOFS->MgmStats.Add("AttrSet", vid.uid, vid.gid, 1);

  if (!key || !value)
    return Emsg(epname, error, EINVAL, "set attribute", path);

  std::string vpath = path;
  if (vpath.find(EOS_COMMON_PATH_VERSION_PREFIX) != std::string::npos)
  {
    // if never put any attribute on version directories
    errno = 0;
    return SFS_OK;
  }

  // ---------------------------------------------------------------------------
  eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);
  try
  {
    dh = gOFS->eosView->getContainer(path);
    XrdOucString Key = key;
    if (Key.beginswith("sys.") && ((!vid.sudoer) && (vid.uid)))
      errno = EPERM;
    else
    {
      // check permissions in case of user attributes
      if (dh && Key.beginswith("user.") && (vid.uid != dh->getCUid())
          && (!vid.sudoer))
      {
        errno = EPERM;
      }
      else
      {
        // check format of acl
        if (Key.beginswith("user.acl") || Key.beginswith("sys.acl"))
        {
          if (!Acl::IsValid(value, error, Key.beginswith("sys.acl")))
          {
            errno = EINVAL;
            return SFS_ERROR;
          }
        }
        dh->setAttribute(key, value);
        eosView->updateContainerStore(dh);
        UpdateNowInmemoryDirectoryModificationTime(dh->getId());
        errno = 0;
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

  EXEC_TIMING_END("AttrSet");

  if (errno)
    return Emsg(epname, error, errno, "set attributes", path);

  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_attr_get (const char *path,
                      XrdOucErrInfo &error,
                      eos::common::Mapping::VirtualIdentity &vid,
                      const char *info,
                      const char *key,
                      XrdOucString &value,
                      bool islocked)
/*----------------------------------------------------------------------------*/
/*
 * @brief get an extended attribute for a given directory by key
 *
 * @param path directory name to get attribute
 * @param error error object
 * @param vid virtual identity of the client
 * @param info CGI
 * @param key key to get
 * @param value value returned
 *
 * @return SFS_OK if success otherwise SFS_ERROR
 *
 * Normal POSIX R_OK & X_OK permissions are required to retrieve a key.
 */
/*----------------------------------------------------------------------------*/
{
  static const char *epname = "attr_get";
  eos::IContainerMD *dh = 0;
  errno = 0;

  EXEC_TIMING_BEGIN("AttrGet");

  gOFS->MgmStats.Add("AttrGet", vid.uid, vid.gid, 1);

  if (!key)
    return Emsg(epname, error, EINVAL, "get attribute", path);

  value = "";
  XrdOucString link;

  // ---------------------------------------------------------------------------
  if (!islocked) gOFS->eosViewRWMutex.LockRead();
  try
  {
    dh = gOFS->eosView->getContainer(path);
    value = (dh->getAttribute(key)).c_str();
  }
  catch (eos::MDException &e)
  {
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
              e.getErrno(), e.getMessage().str().c_str());
  }

  if (dh && errno)
  {
    // try linked attributes
    try
    {
      std::string lkey = "sys.attr.link";
      link = (dh->getAttribute(lkey)).c_str();
      dh = gOFS->eosView->getContainer(link.c_str());
      value = (dh->getAttribute(key)).c_str();
      errno = 0;
    }
    catch (eos::MDException &e)
    {
      dh = 0;
      errno = e.getErrno();
      eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                e.getErrno(), e.getMessage().str().c_str());
    }
  }

  if (!islocked) gOFS->eosViewRWMutex.UnLockRead();

  EXEC_TIMING_END("AttrGet");

  if (errno)
    return Emsg(epname, error, errno, "get attributes", path);
  ;

  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_attr_rem (const char *path,
                      XrdOucErrInfo &error,
                      eos::common::Mapping::VirtualIdentity &vid,
                      const char *info,
                      const char *key)
/*----------------------------------------------------------------------------*/
/*
 * @brief delete an extended attribute for a given directory by key
 *
 * @param path directory name to set attribute
 * @param error error object
 * @param vid virtual identity of the client
 * @param info CGI
 * @param key key to delete
 *
 * @return SFS_OK if success otherwise SFS_ERROR
 *
 * Only the owner of a directory can delete an extended attributes with user prefix.
 * sys prefix attributes can be deleted only by sudo'ers or root.
 */
/*----------------------------------------------------------------------------*/
{
  static const char *epname = "attr_rm";
  eos::IContainerMD *dh = 0;
  errno = 0;

  EXEC_TIMING_BEGIN("AttrRm");

  gOFS->MgmStats.Add("AttrRm", vid.uid, vid.gid, 1);

  if (!key)
    return Emsg(epname, error, EINVAL, "delete attribute", path);

  // ---------------------------------------------------------------------------
  eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);
  try
  {
    dh = gOFS->eosView->getContainer(path);
    XrdOucString Key = key;
    if (Key.beginswith("sys.") && ((!vid.sudoer) && (vid.uid)))
      errno = EPERM;
    else
    {
      if (dh->hasAttribute(key))
      {
        dh->removeAttribute(key);
        eosView->updateContainerStore(dh);
      }
      else
      {
        errno = ENODATA;
      }
    }
  }
  catch (eos::MDException &e)
  {
    dh = 0;
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(), e.getMessage().str().c_str());
  }
  // check permissions
  if (dh && (!dh->access(vid.uid, vid.gid, X_OK | R_OK)))
    if (!errno) errno = EPERM;

  EXEC_TIMING_END("AttrRm");

  if (errno)
    return Emsg(epname, error, errno, "remove attribute", path);

  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_attr_clear (const char *path,
                        XrdOucErrInfo &error,
                        eos::common::Mapping::VirtualIdentity &vid,
                        const char *info)
/*----------------------------------------------------------------------------*/
/*
 * @brief clear all  extended attribute for a given directory
 *
 * @param path directory name to set attribute
 * @param error error object
 * @param vid virtual identity of the client
 * @param info CGI
 *
 * @return SFS_OK if success otherwise SFS_ERROR
 *
 * Only the owner of a directory can delete extended attributes with user prefix.
 * sys prefix attributes can be deleted only by sudo'ers or root.
 */
/*----------------------------------------------------------------------------*/
{
  eos::IContainerMD::XAttrMap map;

  if (_attr_ls(path, error, vid, info, map))
  {
    return SFS_ERROR;
  }

  int success = SFS_OK;
  for (auto it = map.begin(); it != map.end(); ++it)
  {

    success |= _attr_rem(path, error, vid, info, it->first.c_str());
  }
  return success;
}
