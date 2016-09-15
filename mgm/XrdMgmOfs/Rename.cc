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
XrdMgmOfs::rename(const char* old_name,
                  const char* new_name,
                  XrdOucErrInfo& error,
                  const XrdSecEntity* client,
                  const char* infoO,
                  const char* infoN)
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
  static const char* epname = "rename";
  const char* tident = error.getErrUser();
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
  XrdOucEnv renameo_Env(infoO);
  XrdOucEnv renamen_Env(infoN);
  XrdOucString oldn = old_name;
  XrdOucString newn = new_name;

  if (!renameo_Env.Get("eos.encodepath")) {
    oldn.replace("#space#", " ");
  }

  if (!renamen_Env.Get("eos.encodepath")) {
    newn.replace("#space#", " ");
  }

  if ((oldn.find(EOS_COMMON_PATH_VERSION_PREFIX) != STR_NPOS) ||
      (newn.find(EOS_COMMON_PATH_VERSION_PREFIX) != STR_NPOS)) {
    errno = EINVAL;
    return Emsg(epname, error, EINVAL,
                "rename version files - use 'file versions' !");
  }

  const char* inpath = 0;
  const char* ininfo = 0;
  {
    inpath = oldn.c_str();
    ininfo = infoO;
    AUTHORIZE(client, &renameo_Env, AOP_Delete, "rename", inpath, error);
    NAMESPACEMAP;
    BOUNCE_ILLEGAL_NAMES;
    oldn = path;
    info = 0;
  }
  {
    inpath = newn.c_str();
    ininfo = infoN;
    AUTHORIZE(client, &renamen_Env, AOP_Update, "rename", inpath, error);
    NAMESPACEMAP;
    BOUNCE_ILLEGAL_NAMES;
    newn = path;
    info = 0;
  }
  BOUNCE_NOT_ALLOWED;
  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;
  return rename(oldn.c_str(), newn.c_str(), error, vid, infoO, infoN, true);
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::rename(const char* old_name,
                  const char* new_name,
                  XrdOucErrInfo& error,
                  eos::common::Mapping::VirtualIdentity& vid,
                  const char* infoO,
                  const char* infoN,
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
  static const char* epname = "rename";
  errno = 0;
  XrdOucString source, destination;
  XrdOucEnv renameo_Env(infoO);
  XrdOucEnv renamen_Env(infoN);
  XrdOucString oldn = old_name;
  XrdOucString newn = new_name;
  const char* inpath = 0;
  const char* ininfo = 0;
  {
    inpath = old_name;
    ininfo = infoO;
    NAMESPACEMAP;
    BOUNCE_ILLEGAL_NAMES;
    oldn = path;
    info = 0;
  }
  {
    inpath = new_name;
    ininfo = infoN;
    NAMESPACEMAP;
    BOUNCE_ILLEGAL_NAMES;
    newn = path;
    info = 0;
  }
  BOUNCE_NOT_ALLOWED;
  ACCESSMODE_W;
  MAYSTALL;
  MAYREDIRECT;

  // check access permissions on source
  if ((_access(oldn.c_str(), W_OK, error, vid, infoO) != SFS_OK)) {
    return SFS_ERROR;
  }

  // check access permissions on target
  if ((_access(newn.c_str(), W_OK, error, vid, infoN) != SFS_OK)) {
    return SFS_ERROR;
  }

  return _rename(oldn.c_str(), newn.c_str(), error, vid, infoO, infoN, false,
                 false, overwrite);
}

/*----------------------------------------------------------------------------*/
/* Rename a file or directory
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
int
XrdMgmOfs::_rename(const char* old_name,
                   const char* new_name,
                   XrdOucErrInfo& error,
                   eos::common::Mapping::VirtualIdentity& vid,
                   const char* infoO,
                   const char* infoN,
                   bool updateCTime,
                   bool checkQuota,
                   bool overwrite)
{
  static const char* epname = "_rename";
  eos_info("source=%s target=%s overwrite=%d", old_name, new_name, overwrite);
  errno = 0;
  EXEC_TIMING_BEGIN("Rename");
  eos::common::Path oPath(old_name);
  eos::common::Path nPath(new_name);
  std::string oP = oPath.GetParentPath();
  std::string nP = nPath.GetParentPath();

  if ((!old_name) || (!new_name)) {
    errno = EINVAL;
    return Emsg(epname, error, EINVAL, "rename - 0 source or target name");
  }

  // If source and target are the same return success
  if (!strcmp(old_name, new_name)) {
    return SFS_OK;
  }

  gOFS->MgmStats.Add("Rename", vid.uid, vid.gid, 1);
  std::shared_ptr<eos::IContainerMD> dir = std::shared_ptr<eos::IContainerMD>((
        eos::IContainerMD*)0);
  std::shared_ptr<eos::IContainerMD> newdir = std::shared_ptr<eos::IContainerMD>((
        eos::IContainerMD*)0);
  std::shared_ptr<eos::IContainerMD> rdir = std::shared_ptr<eos::IContainerMD>((
        eos::IContainerMD*)0);
  std::shared_ptr<eos::IFileMD> file = std::shared_ptr<eos::IFileMD>((
                                         eos::IFileMD*)0);
  bool renameFile = false;
  bool renameDir = false;
  bool renameVersion = false;
  bool findOk = false;
  XrdSfsFileExistence file_exists;

  if (_exists(old_name, file_exists, error, vid, infoN)) {
    errno = ENOENT;
    return Emsg(epname, error, ENOENT, "rename - source does not exist");
  } else {
    if (file_exists == XrdSfsFileExistIsFile) {
      XrdSfsFileExistence version_exists;
      renameFile = true;
      XrdOucString vpath = nPath.GetPath();

      if ((!_exists(oPath.GetVersionDirectory(), version_exists, error, vid,
                    infoN)) &&
          (version_exists == XrdSfsFileExistIsDirectory) &&
          (!vpath.beginswith(oPath.GetVersionDirectory())) &&
          (!vpath.beginswith(Recycle::gRecyclingPrefix.c_str()))) {
        renameVersion = true;
      }
    }

    if (file_exists == XrdSfsFileExistIsDirectory) {
      std::string n_path = nPath.GetPath();
      std::string o_path = oPath.GetPath();

      if ((n_path.at(n_path.length() - 1) != '/')) {
        n_path += "/";
      }

      if ((o_path.at(o_path.length() - 1) != '/')) {
        o_path += "/";
      }

      renameDir = true;

      // Check if old path is a subpath of new path
      if ((n_path.length() > o_path.length()) &&
          (!n_path.compare(0, o_path.length(), o_path))) {
        errno = EINVAL;
        return Emsg(epname, error, EINVAL, "rename - old path is subpath of new path");
      }
    }
  }

  if (!_exists(new_name, file_exists, error, vid, infoN)) {
    if (file_exists == XrdSfsFileExistIsFile) {
      if (overwrite && renameFile) {
        // Check if we are renaming a version to the primary copy
        bool keepversion = false;
        {
          XrdOucString op = oPath.GetParentPath();
          XrdOucString vp = nPath.GetVersionDirectory();

          if (op == vp) {
            keepversion = true;
          }
        }

        // Delete the existing target
        if (gOFS->_rem(new_name, error, vid, infoN, false, keepversion)) {
          return SFS_ERROR;
        }
      } else {
        errno = EEXIST;
        return Emsg(epname, error, EEXIST, "rename - target file name exists");
      }
    }

    if (file_exists == XrdSfsFileExistIsDirectory) {
      errno = EEXIST;
      return Emsg(epname, error, EEXIST, "rename - target directory name exists");
    }
  }

  // List of source files if a directory is renamed
  std::map<std::string, std::set<std::string> > found;

  if (renameDir) {
    // For directory renaming which move into a different directory, we build
    // the list of files which we are moving
    if (oP != nP) {
      XrdOucString stdErr;

      if (!gOFS->_find(oPath.GetFullPath().c_str(), error, stdErr, vid, found)) {
        findOk = true;
      } else {
        return Emsg(epname, error, errno,
                    "rename - cannot do 'find' inside the source tree");
      }
    }
  }

  {
    eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);

    try {
      dir = eosView->getContainer(oPath.GetParentPath());
      newdir = eosView->getContainer(nPath.GetParentPath());
      // Translate to paths without symlinks
      std::string duri = eosView->getUri(dir.get());
      std::string newduri = eosView->getUri(newdir.get());
      // Get symlink-free dir's
      dir = eosView->getContainer(duri);
      newdir = eosView->getContainer(newduri);

      if (renameFile) {
        if (oP == nP) {
          file = dir->findFile(oPath.GetName());

          if (file) {
            eosView->renameFile(file.get(), nPath.GetName());
            dir->setMTimeNow();
            dir->notifyMTimeChange(gOFS->eosDirectoryService);
            eosView->updateContainerStore(dir.get());
          }
        } else {
          file = dir->findFile(oPath.GetName());

          if (file) {
            // Move to a new directory
            // TODO: deal with conflicts and proper roll-back in case a file
            // with the same name already exists in the destination directory
            dir->removeFile(oPath.GetName());
            dir->setMTimeNow();
            dir->notifyMTimeChange(gOFS->eosDirectoryService);
            newdir->setMTimeNow();
            newdir->notifyMTimeChange(gOFS->eosDirectoryService);
            eosView->updateContainerStore(dir.get());
            eosView->updateContainerStore(newdir.get());
            file->setName(nPath.GetName());
            file->setContainerId(newdir->getId());

            if (updateCTime) {
              file->setCTimeNow();
            }

            newdir->addFile(file.get());
            eosView->updateFileStore(file.get());
            // Adjust the ns quota
            eos::IQuotaNode* old_qnode = eosView->getQuotaNode(dir.get());
            eos::IQuotaNode* new_qnode = eosView->getQuotaNode(newdir.get());

            if (old_qnode) {
              old_qnode->removeFile(file.get());
            }

            if (new_qnode) {
              new_qnode->addFile(file.get());
            }
          }
        }
      }

      if (renameDir) {
        rdir = dir->findContainer(oPath.GetName());

        if (rdir) {
          // Remove all the quota from the source node and add to the target node
          std::map<std::string, std::set<std::string> >::const_reverse_iterator rfoundit;
          std::set<std::string>::const_iterator fileit;

          // Loop over all the files and subtract them from their quota node
          if (findOk) {
            if (checkQuota) {
              std::map<uid_t, unsigned long long> user_del_size;
              std::map<gid_t, unsigned long long> group_del_size;

              // Compute the total quota we need to rename by uid/gid
              for (rfoundit = found.rbegin(); rfoundit != found.rend(); rfoundit++) {
                for (fileit = rfoundit->second.begin(); fileit != rfoundit->second.end();
                     fileit++) {
                  std::string fspath = rfoundit->first;
                  fspath += *fileit;
                  std::shared_ptr<eos::IFileMD> fmd = std::shared_ptr<eos::IFileMD>((
                                                        eos::IFileMD*)0);

                  // Stat this file and add to the deletion maps
                  try {
                    fmd = gOFS->eosView->getFile(fspath.c_str());
                  } catch (eos::MDException& e) {
                    errno = e.getErrno();
                    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                              e.getErrno(), e.getMessage().str().c_str());
                  }

                  if (!fmd) {
                    return Emsg(epname, error, errno, "rename - cannot stat file in subtree",
                                fspath.c_str());
                  }

                  user_del_size[fmd->getCUid()] += (fmd->getSize() * fmd->getNumLocation());
                  group_del_size[fmd->getCGid()] += (fmd->getSize() * fmd->getNumLocation());
                }
              }

              // Verify for each uid/gid that there is enough quota to rename
              bool userok = true;
              bool groupok = true;

              // Either all have user quota therefore userok is true
              for (auto it = user_del_size.begin(); it != user_del_size.end(); ++it) {
                if (!Quota::Check(nP, it->first, Quota::gProjectId, it->second, 1)) {
                  userok = false;
                  break;
                }
              }

              // or all have group quota therefore groupok is true
              for (auto it = group_del_size.begin(); it != group_del_size.end(); it++) {
                if (!Quota::Check(nP, Quota::gProjectId, it->first, it->second, 1)) {
                  groupok = false;
                  break;
                }
              }

              if ((!userok) && (!groupok)) {
                // Deletion will fail as there is not enough quota on the target
                return Emsg(epname, error, ENOSPC, "rename - cannot get all "
                            "the needed quota for the target directory");
              }
            } // if (checkQuota)

            for (rfoundit = found.rbegin(); rfoundit != found.rend(); rfoundit++) {
              for (fileit = rfoundit->second.begin(); fileit != rfoundit->second.end();
                   fileit++) {
                std::string fspath = rfoundit->first;
                fspath += *fileit;
                file = gOFS->eosView->getFile(fspath.c_str());

                if (file) {
                  // Get quota nodes from file path and target directory
                  eos::IQuotaNode* old_qnode = eosView->getQuotaNode(rdir.get());
                  eos::IQuotaNode* new_qnode = eosView->getQuotaNode(newdir.get());

                  if (old_qnode) {
                    old_qnode->removeFile(file.get());
                  }

                  if (new_qnode) {
                    new_qnode->addFile(file.get());
                  }
                }
              }
            }
          }

          if (nP == oP) {
            // Rename within a container
            eosView->renameContainer(rdir.get(), nPath.GetName());
            rdir->setMTimeNow();
            rdir->notifyMTimeChange(gOFS->eosDirectoryService);
            eosView->updateContainerStore(rdir.get());
          } else {
            // Remove from one container to another one
            unsigned long long tree_size = rdir->getTreeSize();
            {
              // update the source directory - remove the directory
              dir->removeContainer(oPath.GetName());
              dir->setMTimeNow();
              dir->notifyMTimeChange(gOFS->eosDirectoryService);

              if (gOFS->eosContainerAccounting) {
                gOFS->eosContainerAccounting->RemoveTree(dir.get(), tree_size);
              }

              eosView->updateContainerStore(dir.get());
            }
            {
              // rename the moved directory and udpate it's parent ID
              rdir->setName(nPath.GetName());
              rdir->setParentId(newdir->getId());

              if (updateCTime) {
                rdir->setCTimeNow();
              }

              eosView->updateContainerStore(rdir.get());
            }
            {
              // update the target directory - add the directory
              newdir->addContainer(rdir.get());
              newdir->setMTimeNow();

              if (gOFS->eosContainerAccounting) {
                gOFS->eosContainerAccounting->AddTree(newdir.get(), tree_size);
              }

              newdir->notifyMTimeChange(gOFS->eosDirectoryService);
              eosView->updateContainerStore(newdir.get());
            }
          }
        }

        file.reset();
      }
    } catch (eos::MDException& e) {
      dir.reset();
      file.reset();
      errno = e.getErrno();
      eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                e.getErrno(), e.getMessage().str().c_str());
    }
  }

  if ((!dir) || ((!file) && (!rdir))) {
    errno = ENOENT;
    return Emsg(epname, error, ENOENT, "rename", old_name);
  }

  // check if this was a versioned file
  if (renameVersion) {
    // rename also the version directory
    if (_rename(oPath.GetVersionDirectory(), nPath.GetVersionDirectory(),
                error, vid, infoO, infoN, false, false, false)) {
      return SFS_ERROR;
    }
  }

  EXEC_TIMING_END("Rename");
  return SFS_OK;
}
