// ----------------------------------------------------------------------
// File: Rename.cc
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
XrdMgmOfs::rename (const char *old_name,
                   const char *new_name,
                   XrdOucErrInfo &error,
                   const XrdSecEntity *client,
                   const char *infoO,
                   const char *infoN)
/*----------------------------------------------------------------------------*/
/*
 * @brief rename a file or directory
 *
 * @param old_name old name
 * @param new_name new name
 * @param error error object
 * @param client XRootD authentication object
 * @param infoO CGI of the old name
 * @param infoN CGI of the new name
 * @return SFS_OK on success otherwise SFS_ERROR
 *
 * There are three flavours of rename function, two external and one internal
 * implementation. See the internal implementation _rename for details.
 */
/*----------------------------------------------------------------------------*/
{
  static const char *epname = "rename";
  const char *tident = error.getErrUser();

  // use a thread private vid
  eos::common::Mapping::VirtualIdentity vid;

  XrdSecEntity mappedclient;

  EXEC_TIMING_BEGIN("IdMap");
  eos::common::Mapping::IdMap(client, infoO, tident, vid);
  EXEC_TIMING_END("IdMap");

  eos_info("old-name=%s new-name=%s", old_name, new_name);

  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);

  errno = 0;

  XrdOucString source, destination;
  XrdOucString oldn, newn;
  XrdOucEnv renameo_Env(infoO);
  XrdOucEnv renamen_Env(infoN);

  oldn = old_name;
  newn = new_name;

  oldn.replace("#space#"," ");
  newn.replace("#space#"," ");

  if ((oldn.find(EOS_COMMON_PATH_VERSION_PREFIX) != STR_NPOS) ||
      (newn.find(EOS_COMMON_PATH_VERSION_PREFIX) != STR_NPOS))
  {
    errno = EINVAL;
    return Emsg(epname, error, EINVAL, "rename version files - use 'file versions' !");
  }

  {
    const char* inpath = oldn.c_str();
    const char* ininfo = infoO;
    AUTHORIZE(client, &renameo_Env, AOP_Delete, "rename", inpath, error);
    NAMESPACEMAP;
    BOUNCE_ILLEGAL_NAMES;
    oldn = path;
    if (info)info = 0;
  }

  {
    const char* inpath = newn.c_str();
    const char* ininfo = infoN;
    AUTHORIZE(client, &renamen_Env, AOP_Update, "rename", inpath, error);
    NAMESPACEMAP;
    BOUNCE_ILLEGAL_NAMES;
    newn = path;

    if (info)info = 0;
  }

  BOUNCE_NOT_ALLOWED;
  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;

  return rename(oldn.c_str(), newn.c_str(), error, vid, infoO, infoN, true);
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::rename (const char *old_name,
                   const char *new_name,
                   XrdOucErrInfo &error,
                   eos::common::Mapping::VirtualIdentity& vid,
                   const char *infoO,
                   const char *infoN,
                   bool overwrite)
/*----------------------------------------------------------------------------*/
/*
 * @brief rename a file or directory
 *
 * @param old_name old name
 * @param new_name new name
 * @param error error object
 * @param vid virtual identity of the client
 * @param infoO CGI of the old name
 * @param infoN CGI of the new name
 * @return SFS_OK on success otherwise SFS_ERROR
 *
 * There are three flavours of rename function, two external and one internal
 * implementation. See the internal implementation _rename for details.
 */
/*----------------------------------------------------------------------------*/
{
  static const char *epname = "rename";
  errno = 0;

  XrdOucString source, destination;
  XrdOucString oldn, newn;
  XrdOucEnv renameo_Env(infoO);
  XrdOucEnv renamen_Env(infoN);


  oldn = old_name;
  newn = new_name;

  {
    const char* inpath = old_name;
    const char* ininfo = infoO;
    NAMESPACEMAP;
    BOUNCE_ILLEGAL_NAMES;
    oldn = path;
    if (info)info = 0;
  }

  {
    const char* inpath = new_name;
    const char* ininfo = infoN;
    NAMESPACEMAP;
    BOUNCE_ILLEGAL_NAMES;
    newn = path;

    if (info)info = 0;
  }

  BOUNCE_NOT_ALLOWED;
  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;

  // check access permissions on source
  if ((_access(oldn.c_str(), W_OK, error, vid, infoO) != SFS_OK))
  {
    return SFS_ERROR;
  }

  // check access permissions on target
  if ((_access(newn.c_str(), W_OK, error, vid, infoN) != SFS_OK))
  {

    return SFS_ERROR;
  }
  return _rename(oldn.c_str(), newn.c_str(), error, vid, infoO, infoN, false, false, overwrite);
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_rename (const char *old_name,
                    const char *new_name,
                    XrdOucErrInfo &error,
                    eos::common::Mapping::VirtualIdentity& vid,
                    const char *infoO,
                    const char *infoN,
                    bool updateCTime,
                    bool checkQuota,
                    bool overwrite,
		    bool lock_quota
                    )
/*----------------------------------------------------------------------------*/
/*
 * @brief rename a file or directory
 *
 * @param old_name old name
 * @param new_name new name
 * @param error error object
 * @param vid virtual identity of the client
 * @param infoO CGI of the old name
 * @param infoN CGI of the new name
 * @param updateCTime indicates to update the change time of a directory
 * @param checkQuota indicates to check the quota during a rename operation
 * @param overwrite indicates if the target name can be overwritten
 * @param lock_quota tells if we have to lock the quota mutex internally
 * @return SFS_OK on success otherwise SFS_ERROR
 *
 * There are three flavours of rename function, two external and one internal
 * implementation.
 * Rename within a directory is simple since the quota accounting has not to
 * be modified. Rename of directories between quota nodes need to recompute
 * all the quota of the subtree which is moving and in case reject the operation
 * if there is not enough quota left. Overall it is a quite complex function.
 */
/*----------------------------------------------------------------------------*/

{
  static const char *epname = "_rename";
  errno = 0;

  eos_info("source=%s target=%s", old_name, new_name);

  EXEC_TIMING_BEGIN("Rename");

  eos::common::Path oPath(old_name);
  eos::common::Path nPath(new_name);
  std::string oP = oPath.GetParentPath();
  std::string nP = nPath.GetParentPath();

  if ((!old_name) || (!new_name))
  {
    errno = EINVAL;
    return Emsg(epname, error, EINVAL, "rename - 0 source or target name");
  }

  if (!strcmp(old_name, new_name))
  {
    // source and target are the same, nothing to do
    return SFS_OK;
  }

  gOFS->MgmStats.Add("Rename", vid.uid, vid.gid, 1);

  eos::IContainerMD* dir = 0;
  eos::IContainerMD* newdir = 0;
  eos::IContainerMD* rdir = 0;
  eos::IFileMD* file = 0;
  bool renameFile = false;
  bool renameDir = false;
  bool renameVersion = false;
  bool findOk = false;

  XrdSfsFileExistence file_exists;

  if (_exists(old_name, file_exists, error, vid, infoN))
  {
    errno = ENOENT;
    return Emsg(epname, error, ENOENT, "rename - source does not exist");
  }
  else
  {
    if (file_exists == XrdSfsFileExistIsFile)
    {
      XrdSfsFileExistence version_exists;
      renameFile = true;
      XrdOucString vpath = nPath.GetPath();

      if ((!_exists(oPath.GetVersionDirectory(), version_exists, error, vid, infoN)) &&
          (version_exists == XrdSfsFileExistIsDirectory) &&
          (!vpath.beginswith(oPath.GetVersionDirectory())) &&
          (!vpath.beginswith(Recycle::gRecyclingPrefix.c_str())))
      {
        renameVersion = true;
      }
    }
    if (file_exists == XrdSfsFileExistIsDirectory)
    {
      std::string n_path = nPath.GetPath();
      std::string o_path = oPath.GetPath();
      if ( (n_path.at( n_path.length()-1 ) != '/') )
	n_path += "/";
      if ( (o_path.at( o_path.length()-1 ) != '/') )
	o_path += "/";

      renameDir = true;
      // check if old path is a subpath of new path
      if ( (n_path.length() > o_path.length()) && (!n_path.compare(0,o_path.length(),o_path)))
      {
	errno = EINVAL;
	return Emsg(epname, error, EINVAL, "rename - old path is subpath of new path");
      }
    }
  }

  if (!_exists(new_name, file_exists, error, vid, infoN))
  {
    if (file_exists == XrdSfsFileExistIsFile)
    {
      if (overwrite && renameFile)
      {
        // check if we are renaming a version to the primary copy
        bool keepversion = false;
        {
          XrdOucString op = oPath.GetParentPath();
          XrdOucString vp = nPath.GetVersionDirectory();
          if (op == vp)
            keepversion = true;
        }
        // delete the existing target
        if (gOFS->_rem(new_name, error, vid, infoN, false, keepversion))
          return SFS_ERROR;
      }
      else
      {
        errno = EEXIST;
        return Emsg(epname, error, EEXIST, "rename - target file name exists");
      }
    }
    if (file_exists == XrdSfsFileExistIsDirectory)
    {
      errno = EEXIST;
      return Emsg(epname, error, EEXIST,
                  "rename - target directory name exists");
    }
  }

  std::map<std::string, std::set<std::string> > found; //< list of source files if a directory is renamed
  if (renameDir)
  {
    // for directory renaming which move into a different directory
    // we build the list of files which we are moving
    if (oP != nP)
    {
      XrdOucString stdErr;

      if (!gOFS->_find(oPath.GetFullPath().c_str(), error, stdErr, vid, found))
      {
        findOk = true;
      }
      else
      {
        return Emsg(epname, error, errno, "rename - cannot do 'find' inside the source tree");
      }
    }
  }
  // ---------------------------------------------------------------------------
  {
    if (lock_quota)
      Quota::gQuotaMutex.LockRead();

    {
      eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);
      try
      {
	dir = eosView->getContainer(oPath.GetParentPath());
	newdir = eosView->getContainer(nPath.GetParentPath());
	if (renameFile)
	{
	  if (oP == nP)
	  {
	    file = dir->findFile(oPath.GetName());
	    if (file)
	    {
	      eosView->renameFile(file, nPath.GetName());
	      UpdateNowInmemoryDirectoryModificationTime(dir->getId());
	    }
	  }
	  else
	  {
	    file = dir->findFile(oPath.GetName());
	    if (file)
	    {
	      // move to a new directory
	      dir->removeFile(oPath.GetName());
	      UpdateNowInmemoryDirectoryModificationTime(dir->getId());
	      UpdateNowInmemoryDirectoryModificationTime(newdir->getId());
	      file->setName(nPath.GetName());
	      file->setContainerId(newdir->getId());
	      if (updateCTime)
	      {
		file->setCTimeNow();
	      }
	      newdir->addFile(file);
	      eosView->updateFileStore(file);
	      // adjust the quota
	      SpaceQuota* oldspace = Quota::GetResponsibleSpaceQuota(oP.c_str());
	      SpaceQuota* newspace = Quota::GetResponsibleSpaceQuota(nP.c_str());
	      eos::QuotaNode* oldquotanode = 0;
	      eos::QuotaNode* newquotanode = 0;
	      if (oldspace)
	      {
		oldquotanode = oldspace->GetQuotaNode();
		// remove quota
		if (oldquotanode)
		{
		  oldquotanode->removeFile(file);
		}
	      }
	      if (newspace)
	      {
		newquotanode = newspace->GetQuotaNode();
		// add quota
		if (newquotanode)
		{
		  newquotanode->addFile(file);
		}
	      }
	    }
	  }
	}
	if (renameDir)
	{
	  rdir = dir->findContainer(oPath.GetName());
	  if (rdir)
	  {
	    // remove all the quota from the source node and add to the target node
	    
	    std::map<std::string, std::set<std::string> >::const_reverse_iterator rfoundit;
	    std::set<std::string>::const_iterator fileit;
	    // loop over all the files and subtract them from their quota node
	    if (findOk)
	    {
	      if (checkQuota)
	      {
		std::map<uid_t, unsigned long long> user_deletion_size;
		std::map<gid_t, unsigned long long> group_deletion_size;
		// -----------------------------------------------------------------
		// compute the total quota we need to rename by uid/gid
		// -----------------------------------------------------------------s
		for (rfoundit = found.rbegin(); rfoundit != found.rend(); rfoundit++)
		{
		  for (fileit = rfoundit->second.begin(); fileit != rfoundit->second.end(); fileit++)
		  {
		    std::string fspath = rfoundit->first;
		    fspath += *fileit;
		    eos::IFileMD* fmd = 0;
		    // stat this file and add to the deletion maps
		    
		    try
		    {
		      fmd = gOFS->eosView->getFile(fspath.c_str());
		    }
		    catch (eos::MDException &e)
		    {
		      errno = e.getErrno();
		      eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
				e.getErrno(), e.getMessage().str().c_str());
		    }
		    
		    if (!fmd)
		    {
		      if (lock_quota)
			Quota::gQuotaMutex.UnLockRead();
		      return Emsg(epname, error, errno, "rename - cannot stat file in subtree", fspath.c_str());
		    }
		    user_deletion_size[fmd->getCUid()] += (fmd->getSize() * fmd->getNumLocation());
		    group_deletion_size[fmd->getCGid()] += (fmd->getSize() * fmd->getNumLocation());
		  }
              }
		// -----------------------------------------------------------------
		// verify for each uid/gid that there is enough quota to rename
		// -----------------------------------------------------------------
		bool userok = true;
		bool groupok = true;

		// either all have user quota then userok is true
		for (auto it = user_deletion_size.begin(); it != user_deletion_size.end(); it++)
		{
		  SpaceQuota* namespacequota = Quota::GetResponsibleSpaceQuota(nP.c_str());
		  
		  if (namespacequota)
		  {
		    // there is quota defined on that recycle path
		    if (!namespacequota->CheckWriteQuota(it->first,
							 Quota::gProjectId,
							 it->second,
							 1))
		    {
		      userok = false;
		    }
		  }
		}
		
		// or all have group quota then groupok is true
		for (auto it = group_deletion_size.begin(); it != group_deletion_size.end(); it++)
		{
		  SpaceQuota* namespacequota = Quota::GetResponsibleSpaceQuota(nP.c_str());
		  
		  if (namespacequota)
		    {
		      // there is quota defined on that recycle path
		      if (!namespacequota->CheckWriteQuota(Quota::gProjectId,
							   it->first,
							   it->second,
							   1))
		      {
			groupok = false;
		      }
		    }
		}
		
		if ((!userok) && (!groupok))
		{
		  // deletion has to fail there is not enough quota on the target
		  if (lock_quota)
		    Quota::gQuotaMutex.UnLockRead();
		  return Emsg(epname, error, ENOSPC, "rename - cannot get all the needed quota for the target directory");
		}
	      } // if (checkQuota)
	      
	      for (rfoundit = found.rbegin(); rfoundit != found.rend(); rfoundit++)
		{
		  for (fileit = rfoundit->second.begin(); fileit != rfoundit->second.end(); fileit++)
		  {
		    std::string fspath = rfoundit->first;
		    fspath += *fileit;
		    file = gOFS->eosView->getFile(fspath.c_str());
		    if (file)
		    {
		      SpaceQuota* oldspace = Quota::GetResponsibleSpaceQuota(fspath.c_str()); // quota node from file path
		      SpaceQuota* newspace = Quota::GetResponsibleSpaceQuota(nP.c_str()); // quota node of target directory
		      eos::QuotaNode* oldquotanode = 0;
		      eos::QuotaNode* newquotanode = 0;
		      if (oldspace)
		      {
			oldquotanode = oldspace->GetQuotaNode();
			// remove quota
			if (oldquotanode)
			{
			  oldquotanode->removeFile(file);
			}
		      }
		      if (newspace)
		      {
			newquotanode = newspace->GetQuotaNode();
			// add quota
			if (newquotanode)
			{
			  newquotanode->addFile(file);
			}
		      }
		    }
		  }
		}
	    }
	    if (nP == oP)
	    {
	      // -------------------------------------------------------------------
	      // rename within a container
	      // -------------------------------------------------------------------
	      eosView->renameContainer(rdir, nPath.GetName());
	      UpdateNowInmemoryDirectoryModificationTime(rdir->getId());
	    }
	    else
	    {
	      // -------------------------------------------------------------------
	      // move from one container to another one
	      // -------------------------------------------------------------------
	      dir->removeContainer(oPath.GetName());
	      UpdateNowInmemoryDirectoryModificationTime(dir->getId());
	      rdir->setName(nPath.GetName());
	      if (updateCTime)
	      {
		rdir->setCTimeNow();
	      }
	      newdir->addContainer(rdir);
	      UpdateNowInmemoryDirectoryModificationTime(newdir->getId());
	      eosView->updateContainerStore(rdir);
	    }
	  }
	  file = 0;
	}
      }
      catch (eos::MDException &e)
      {
	dir = 0;
	file = 0;
	errno = e.getErrno();
	eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
		  e.getErrno(), e.getMessage().str().c_str());
      }
    }

    if (lock_quota)
      Quota::gQuotaMutex.UnLockRead();

    if ((!dir) || ((!file) && (!rdir)))
    {

      errno = ENOENT;
      return Emsg(epname, error, ENOENT, "rename", old_name);
    }
  }

  // check if this was a versioned file
  if (renameVersion)
  {
    // rename also the version directory

    if (_rename(oPath.GetVersionDirectory(), nPath.GetVersionDirectory(),
                error, vid, infoO, infoN, false, false, false))
      return SFS_ERROR;
  }

  EXEC_TIMING_END("Rename");

  return SFS_OK;
}

