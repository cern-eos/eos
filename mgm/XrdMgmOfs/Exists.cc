// ----------------------------------------------------------------------
// File: Exists.cc
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
XrdMgmOfs::exists(const char* inpath,
                  XrdSfsFileExistence& file_exists,
                  XrdOucErrInfo& error,
                  const XrdSecEntity* client,
                  const char* ininfo)
/*----------------------------------------------------------------------------*/
/*
 * @brief Check for the existence of a file or directory
 *
 * @param inpath path to check existence
 * @param file_exists return parameter specifying the type (see _exists for details)
 * @param error error object
 * @param client XRootD authentication object
 * @param ininfo CGI
 * @result SFS_OK on success otherwise SFS_ERROR
 *
 * The function calls the internal implementation _exists. See there for details.
 */
/*----------------------------------------------------------------------------*/
{
  static const char* epname = "exists";
  const char* tident = error.getErrUser();
  eos::common::VirtualIdentity vid;
  EXEC_TIMING_BEGIN("IdMap");
  eos::common::Mapping::IdMap(client, ininfo, tident, vid, gOFS->mTokenAuthz,
                              AOP_Stat, inpath);
  EXEC_TIMING_END("IdMap");
  NAMESPACEMAP;
  BOUNCE_ILLEGAL_NAMES;
  XrdOucEnv exists_Env(ininfo);
  AUTHORIZE(client, &exists_Env, AOP_Stat, "execute exists", inpath, error);
  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);
  BOUNCE_NOT_ALLOWED;
  ACCESSMODE_R;
  MAYSTALL;
  MAYREDIRECT;
  return _exists(path, file_exists, error, vid, ininfo);
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_exists(const char* path,
                   XrdSfsFileExistence& file_exists,
                   XrdOucErrInfo& error,
                   const XrdSecEntity* client,
                   const char* ininfo, bool files_first)
/*----------------------------------------------------------------------------*/
/*
 * @brief check for the existence of a file or directory
 *
 * @param path path to check
 * @param file_exists return the type of the checked path
 * @param client XRootD authentication object
 * @param ininfo CGI
 * @param files_first check first for files
 * @return SFS_OK if found otherwise SFS_ERROR
 *
 * The values of file_exists are:
 * XrdSfsFileExistIsDirectory - this is a directory
 * XrdSfsFileExistIsFile - this is a file
 * XrdSfsFileExistNo - this is neither a file nor a directory
 *
 * This function may send a redirect response and should not be used as an
 * internal function. The internal function has as a parameter the virtual
 * identity and not the XRootD authentication object.
 */
/*----------------------------------------------------------------------------*/
{
  if ((path == nullptr) || (strlen(path) == 0)) {
    eos_err("%s", "msg=\"null or empty path\"");
    return SFS_ERROR;
  }
  
  // try if that is directory
  EXEC_TIMING_BEGIN("Exists");
  gOFS->MgmStats.Add("Exists", vid.uid, vid.gid, 1);
  std::shared_ptr<eos::IContainerMD> cmd;

  std::shared_ptr<eos::IFileMD> fmd;
  
  if (files_first) {
    // -------------------------------------------------------------------------
    // try if that is a file
    // -------------------------------------------------------------------------
    eos::Prefetcher::prefetchFileMDAndWait(gOFS->eosView, path, false);
    
    try {
      fmd = gOFS->eosView->getFile(path, false);
    } catch (eos::MDException& e) {
      eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),
		e.getMessage().str().c_str());
    }
    if (fmd) {
      file_exists = XrdSfsFileExistIsFile;
      return SFS_OK;
    }
    // continue with container check
  }
  
  {
    // -------------------------------------------------------------------------
    eos::Prefetcher::prefetchContainerMDAndWait(gOFS->eosView, path, false);
    
    try {
      cmd = gOFS->eosView->getContainer(path, false);
    } catch (eos::MDException& e) {
      eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
		e.getErrno(), e.getMessage().str().c_str());
    };
    
    // -------------------------------------------------------------------------
  }
  
  if (!cmd) {
    // -------------------------------------------------------------------------
    // try if that is a file
    // -------------------------------------------------------------------------

    if (!files_first) {
      eos::Prefetcher::prefetchFileMDAndWait(gOFS->eosView, path, false);
      try {
	fmd = gOFS->eosView->getFile(path, false);
      } catch (eos::MDException& e) {
	eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),
		  e.getMessage().str().c_str());
      }
    }
    
    // -------------------------------------------------------------------------
    if (!fmd) {
      file_exists = XrdSfsFileExistNo;
    } else {
      file_exists = XrdSfsFileExistIsFile;
    }
  } else {
    file_exists = XrdSfsFileExistIsDirectory;
  }
  
  if (file_exists == XrdSfsFileExistNo) {
    // get the parent directory
    eos::common::Path cPath(path);
    std::shared_ptr<eos::IContainerMD> dir;
    eos::IContainerMD::XAttrMap attrmap;
    // -------------------------------------------------------------------------
    eos::Prefetcher::prefetchContainerMDAndWait(gOFS->eosView,
						cPath.GetParentPath(), false);
    
    try {
      auto dirLock = eosView->getContainerReadLocked(cPath.GetParentPath(), false);
      if(dirLock) {
	dir = dirLock->getUnderlyingPtr();
      }
      eos::IContainerMD::XAttrMap::const_iterator it;
      // get attributes
      gOFS->_attr_ls(cPath.GetParentPath(), error, vid, 0, attrmap);
    } catch (eos::MDException& e) {
      dir.reset();
    }
    
    // -------------------------------------------------------------------------
    
    if (dir) {
      MAYREDIRECT_ENOENT;
      MAYSTALL_ENOENT;
      XrdOucString redirectionhost = "invalid?";
      int ecode = 0;
      int rcode = SFS_OK;
      
      if (attrmap.count("sys.redirect.enoent")) {
	// there is a redirection setting here
	redirectionhost = "";
	redirectionhost = attrmap["sys.redirect.enoent"].c_str();
	int portpos = 0;
	
	if ((portpos = redirectionhost.find(":")) != STR_NPOS) {
	  XrdOucString port = redirectionhost;
	  port.erase(0, portpos + 1);
	  ecode = atoi(port.c_str());
	  redirectionhost.erase(portpos);
	} else {
	  ecode = 1094;
	}
	
	rcode = SFS_REDIRECT;
	error.setErrInfo(ecode, redirectionhost.c_str());
	gOFS->MgmStats.Add("RedirectENOENT", vid.uid, vid.gid, 1);
	return rcode;
      }
    }
  }
  EXEC_TIMING_END("Exists");
  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_exists(const char* path,
                   XrdSfsFileExistence& file_exists,
                   XrdOucErrInfo& error,
                   eos::common::VirtualIdentity& vid,
                   std::shared_ptr<eos::IContainerMD>& cmd,
                   std::shared_ptr<eos::IFileMD>& fmd,
                   const char* ininfo,
		   bool first_files)
/*----------------------------------------------------------------------------*/
/*
 * @brief check for the existence of a file or directory
 *
 * @param path path to check
 * @param file_exists return the type of the checked path
 * @param vid virtual identity of the client
 * @param cmd Container MD (out param)
 * @param fmd File MD (out param)
 * @param ininfo CGI
 * @param files_first check first for files
 * @return SFS_OK if found otherwise SFS_ERROR
 *
 * The values of file_exists are:
 * XrdSfsFileExistIsDirectory - this is a directory
 * XrdSfsFileExistIsFile - this is a file
 * XrdSfsFileExistNo - this is neither a file nor a directory
 *
 */
/*----------------------------------------------------------------------------*/
{
  EXEC_TIMING_BEGIN("Exists");
  gOFS->MgmStats.Add("Exists", vid.uid, vid.gid, 1);
  // try if that is directory
  {
    // -------------------------------------------------------------------------
    eos::Prefetcher::prefetchContainerMDAndWait(gOFS->eosView, path, false);

    try {
      cmd = gOFS->eosView->getContainer(path, false);
    } catch (eos::MDException& e) {
      cmd.reset();
      eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"", e.getErrno(),
                e.getMessage().str().c_str());
    };

    // -------------------------------------------------------------------------
  }

  if (!cmd) {
    // try if that is a file
    // -------------------------------------------------------------------------
    eos::Prefetcher::prefetchFileMDAndWait(gOFS->eosView, path, false);

    try {
      fmd = gOFS->eosView->getFile(path, false);
    } catch (eos::MDException& e) {
      eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                e.getErrno(), e.getMessage().str().c_str());
    }

    // -------------------------------------------------------------------------

    if (!fmd) {
      file_exists = XrdSfsFileExistNo;
    } else {
      file_exists = XrdSfsFileExistIsFile;
    }
  } else {
    file_exists = XrdSfsFileExistIsDirectory;
  }

  EXEC_TIMING_END("Exists");
  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
/*
 * @brief check for the existence of a file or directory by vid
 *
 * @param path path to check
 * @param file_exists return the type of the checked path
 * @param vid virtual identity of the client
 * @param ininfo CGI
 * @return SFS_OK if found otherwise SFS_ERROR
 *
 * The values of file_exists are:
 * XrdSfsFileExistIsDirectory - this is a directory
 * XrdSfsFileExistIsFile - this is a file
 * XrdSfsFileExistNo - this is neither a file nor a directory
 *
 */
/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_exists(const char* fileName,
                   XrdSfsFileExistence& exists_flag,
                   XrdOucErrInfo& out_error,
                   eos::common::VirtualIdentity& vid,
                   const char* opaque, bool take_lock, bool files_first)
{
  std::shared_ptr<eos::IContainerMD> cmd;
  std::shared_ptr<eos::IFileMD> fmd;
  return _exists(fileName, exists_flag, out_error, vid, cmd, fmd, opaque, files_first);
}
