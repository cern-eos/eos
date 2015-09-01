// ----------------------------------------------------------------------
// File: Link.cc
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
XrdMgmOfs::symlink (const char *source_name,
		    const char *target_name,
		    XrdOucErrInfo &error,
		    const XrdSecEntity *client,
		    const char *infoO,
		    const char *infoN)
/*----------------------------------------------------------------------------*/
/*
 * @brief symlink a file or directory
 *
 * @param source_name source name
 * @param target_name target name
 * @param error error object
 * @param client XRootD authentication object
 * @param infoO CGI of the old name
 * @param infoN CGI of the new name
 * @return SFS_OK on success otherwise SFS_ERROR
 *
 */
/*----------------------------------------------------------------------------*/
{
  static const char *epname = "symlink";
  const char *tident = error.getErrUser();

  // use a thread private vid
  eos::common::Mapping::VirtualIdentity vid;

  XrdSecEntity mappedclient;

  EXEC_TIMING_BEGIN("IdMap");
  eos::common::Mapping::IdMap(client, infoO, tident, vid);
  EXEC_TIMING_END("IdMap");

  eos_info("old-name=%s new-name=%s", source_name, target_name);

  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);

  errno = 0;

  XrdOucString source, destination;
  XrdOucString sourcen, targetn;
  XrdOucEnv symlinko_Env(infoO);
  XrdOucEnv symlinkn_Env(infoN);

  sourcen = source_name;
  targetn = target_name;

  sourcen.replace("#space#"," ");
  targetn.replace("#space#"," ");

  {
    const char* inpath = sourcen.c_str();
    const char* ininfo = infoO;
    AUTHORIZE(client, &symlinko_Env, AOP_Create, "link", inpath, error);
    NAMESPACEMAP;
    BOUNCE_ILLEGAL_NAMES;
    sourcen = path;
    if (info)info = 0;
  }

  {
    const char* inpath = targetn.c_str();
    const char* ininfo = infoN;
    NAMESPACEMAP;
    BOUNCE_ILLEGAL_NAMES;
    targetn = path;

    if (info)info = 0;
  }

  BOUNCE_NOT_ALLOWED;
  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;

  return symlink(sourcen.c_str(), targetn.c_str(), error, vid, infoO, infoN, true);
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::symlink (const char *source_name,
		    const char *target_name,
		    XrdOucErrInfo &error,
		    eos::common::Mapping::VirtualIdentity& vid,
		    const char *infoO,
		    const char *infoN,
		    bool overwrite)
/*----------------------------------------------------------------------------*/
/*
 * @brief symlink a file or directory
 *
 * @param source_name source name
 * @param target_name target name
 * @param error error object
 * @param vid virtual identity of the client
 * @param infoO CGI of the source name
 * @param infoN CGI of the target name
 * @return SFS_OK on success otherwise SFS_ERROR
 *
 */
/*----------------------------------------------------------------------------*/
{
  static const char *epname = "symlink";
  errno = 0;

  eos_info("source=%s target=%s", source_name, target_name);

  XrdOucString source, destination;
  XrdOucString sourcen, targetn;
  XrdOucEnv symlinko_Env(infoO);
  XrdOucEnv symlinkn_Env(infoN);


  sourcen = source_name;
  targetn = target_name;

  {
    const char* inpath = source_name;
    const char* ininfo = infoO;
    NAMESPACEMAP;
    BOUNCE_ILLEGAL_NAMES;
    sourcen = path;
    if (info)info = 0;
  }

  BOUNCE_NOT_ALLOWED;
  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;

  // check access permissions on source
  if ((_access(sourcen.c_str(), W_OK, error, vid, infoO) != SFS_OK))
  {
    return SFS_ERROR;
  }

  return _symlink(sourcen.c_str(), targetn.c_str(), error, vid, infoO, infoN);
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_symlink (const char *source_name,
		     const char *target_name,
		     XrdOucErrInfo &error,
		     eos::common::Mapping::VirtualIdentity& vid,
		     const char *infoO,
		     const char *infoN
                    )
/*----------------------------------------------------------------------------*/
/*
 * @brief symlink a file or directory
 *
 * @param source_name source name
 * @param target_name target name
 * @param error error object
 * @param vid virtual identity of the client
 * @param infoO CGI of the source name
 * @param infoN CGI of the target name
 * @return SFS_OK on success otherwise SFS_ERROR
 *
 */
/*----------------------------------------------------------------------------*/

{
  static const char *epname = "_symlink";
  errno = 0;

  eos_info("source=%s target=%s", source_name, target_name);

  EXEC_TIMING_BEGIN("SymLink");

  eos::common::Path oPath(source_name);
  std::string oP = oPath.GetParentPath();

  if ((!source_name) || (!target_name))
  {
    errno = EINVAL;
    return Emsg(epname, error, EINVAL, "symlink - 0 source or target name");
  }

  if (!strcmp(source_name, target_name))
  {
    errno = EINVAL;
    return Emsg(epname, error, EINVAL, "symlink - source and target are identical");
  }

  gOFS->MgmStats.Add("Symlink", vid.uid, vid.gid, 1);

  eos::ContainerMD* dir = 0;

  XrdSfsFileExistence file_exists = XrdSfsFileExistNo;

  _exists(oP.c_str(), file_exists, error, vid, infoN);
  
  if (file_exists != XrdSfsFileExistIsDirectory)
  {
    errno = ENOENT;
    return Emsg(epname, error, ENOENT, "symlink - parent source dir does not exist");
  }

  file_exists = XrdSfsFileExistNo;

  _exists(source_name, file_exists, error, vid, infoN);
  
  if (file_exists != XrdSfsFileExistNo)
  {
    errno = EEXIST;
    return Emsg(epname, error, ENOENT, "symlink - source exists");
  }

  {
    eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);
    try
    {
      dir = eosView->getContainer(oPath.GetParentPath());
      eosView->createLink(oPath.GetPath(), target_name,
			  vid.uid, vid.gid);
      UpdateNowInmemoryDirectoryModificationTime(dir->getId());
    }
    catch (eos::MDException &e)
    {
      eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
		e.getErrno(), e.getMessage().str().c_str());
      errno = e.getErrno();
      return Emsg(epname, error, errno, e.getMessage().str().c_str());
    }
  }

  EXEC_TIMING_END("SymLink");

  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::readlink (const char *inpath,
		     XrdOucErrInfo &error,
		     XrdOucString &link,
		     const XrdSecEntity *client,
		     const char *ininfo)
/*----------------------------------------------------------------------------*/
/*
 * @brief read symbolic link target
 *
 * @param name of the link to read
 * @param error error object
 * @param client XRootD authentication object
 * @param link target string if symbolic link otherwise empty
 * @return SFS_OK on success otherwise SFS_ERROR
 *
 */
/*----------------------------------------------------------------------------*/
{
  static const char *epname = "readlink";
  const char *tident = error.getErrUser();

  // use a thread private vid
  eos::common::Mapping::VirtualIdentity vid;

  XrdSecEntity mappedclient;

  EXEC_TIMING_BEGIN("IdMap");
  eos::common::Mapping::IdMap(client, ininfo, tident, vid);
  EXEC_TIMING_END("IdMap");

  eos_info("path=%s",inpath);

  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);

  errno = 0;

  XrdOucEnv readlink_Env(ininfo);

  AUTHORIZE(client, &readlink_Env, AOP_Read, "link", inpath, error);
  NAMESPACEMAP;
  BOUNCE_ILLEGAL_NAMES;
  BOUNCE_NOT_ALLOWED;
  ACCESSMODE_R;
  MAYSTALL;
  MAYREDIRECT;
  
  if (info) 
    info = 0;
  return _readlink(path, error, vid, link);
}

// ---------------------------------------------------------------------------                                                                                                                
// read symbolic link                                                                                                                                                                         
// ---------------------------------------------------------------------------                                                                                                                
int
XrdMgmOfs::_readlink (const char *name,
		      XrdOucErrInfo &error,
		      eos::common::Mapping::VirtualIdentity &vid,
		      XrdOucString &link)
{
  static const char *epname = "_readlink";
  errno = 0;

  eos_info("name=%s", name);
  std::string linktarget;

  gOFS->MgmStats.Add("Symlink", vid.uid, vid.gid, 1);

  EXEC_TIMING_BEGIN("ReadLink");

  {
    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
    eos::FileMD* file=0;
    try
    {
      file = eosView->getFile(name,false);
      std::string slink = file->getLink();
      link = slink.c_str();
    }
    catch (eos::MDException &e)
    {
      eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
		e.getErrno(), e.getMessage().str().c_str());
      errno = e.getErrno();
      return Emsg(epname, error, errno, e.getMessage().str().c_str());
    }
  }

  EXEC_TIMING_END("ReadLink");

  return SFS_OK;
}
  
