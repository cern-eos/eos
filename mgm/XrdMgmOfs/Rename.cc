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

//------------------------------------------------------------------------------
// This file is included source code in XrdMgmOfs.cc to make the code more
// transparent without slowing down the compilation time.
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
// Rename file or directory - part of the XRootD API
//------------------------------------------------------------------------------
int
XrdMgmOfs::rename(const char* old_name,
                  const char* new_name,
                  XrdOucErrInfo& error,
                  const XrdSecEntity* client,
                  const char* infoO,
                  const char* infoN)
{
  static const char* epname = "rename";
  const char* tident = error.getErrUser();
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

  // Use a thread private vid
  eos::common::VirtualIdentity vid;
  EXEC_TIMING_BEGIN("IdMap");
  eos::common::Mapping::IdMap(client, infoO, tident, vid, gOFS->mTokenAuthz,
                              AOP_Update, newn.c_str());
  EXEC_TIMING_END("IdMap");
  eos_info("old-name=%s new-name=%s", old_name, new_name);
  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);
  const char* inpath = 0;
  const char* ininfo = 0;
  {
    inpath = oldn.c_str();
    ininfo = infoO;
    AUTHORIZE(client, &renameo_Env, AOP_Delete, "rename", inpath, error);
    NAMESPACEMAP;
    BOUNCE_ILLEGAL_NAMES;
    oldn = path;
  }
  {
    inpath = newn.c_str();
    ininfo = infoN;
    AUTHORIZE(client, &renamen_Env, AOP_Update, "rename", inpath, error);
    NAMESPACEMAP;
    BOUNCE_ILLEGAL_NAMES;
    newn = path;
  }
  BOUNCE_NOT_ALLOWED;
  ACCESSMODE_W;
  MAYSTALL;
  {
    const char* path = inpath;
    MAYREDIRECT;
  }
  return rename(oldn.c_str(), newn.c_str(), error, vid, infoO, infoN, true);
}

//------------------------------------------------------------------------------
// Rename file or directory - EOS internal API that performs
// permission checks
//------------------------------------------------------------------------------
int
XrdMgmOfs::rename(const char* old_name,
                  const char* new_name,
                  XrdOucErrInfo& error,
                  eos::common::VirtualIdentity& vid,
                  const char* infoO,
                  const char* infoN,
                  bool overwrite)
{
  static const char* epname = "rename";
  XrdOucString source, destination;
  XrdOucEnv renameo_Env(infoO);
  XrdOucEnv renamen_Env(infoN);
  XrdOucString oldn = old_name;
  XrdOucString newn = new_name;
  const char* inpath = 0;
  const char* ininfo = 0;
  errno = 0;
  {
    inpath = old_name;
    ininfo = infoO;
    NAMESPACEMAP;
    BOUNCE_ILLEGAL_NAMES;
    oldn = path;
  }
  {
    inpath = new_name;
    ininfo = infoN;
    NAMESPACEMAP;
    BOUNCE_ILLEGAL_NAMES;
    newn = path;
  }
  BOUNCE_NOT_ALLOWED;
  ACCESSMODE_W;
  MAYSTALL;
  {
    const char* path = inpath;
    MAYREDIRECT;
  }

  // Check access permissions on source
  if (_access(oldn.c_str(), W_OK | D_OK, error, vid, infoO) != SFS_OK) {
    return Emsg(epname, error, errno, "rename - source access failure");
  }

  // Check access permissions on target
  if (_access(newn.c_str(), W_OK, error, vid, infoN) != SFS_OK) {
    return Emsg(epname, error, errno, "rename - destination access failure");
  }

  return _rename(oldn.c_str(), newn.c_str(), error, vid, infoO, infoN, true,
                 false, overwrite);
}

//------------------------------------------------------------------------------
// Rename file or directory - EOS internal low-level API
//------------------------------------------------------------------------------
int
XrdMgmOfs::_rename(const char* old_name,
                   const char* new_name,
                   XrdOucErrInfo& error,
                   eos::common::VirtualIdentity& vid,
                   const char* infoO,
                   const char* infoN,
                   bool updateCTime,
                   bool checkQuota,
                   bool overwrite,
                   bool fusexcast)
{
  static const char* epname = "_rename";
  eos_info("source=%s target=%s overwrite=%d", old_name, new_name, overwrite);
  errno = 0;
  EXEC_TIMING_BEGIN("Rename");
  eos::common::Timing tm("_rename");
  COMMONTIMING("begin", &tm);
  eos::common::Path nPath(new_name);
  eos::common::Path oPath(old_name);
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
  std::shared_ptr<eos::IContainerMD> dir;
  std::shared_ptr<eos::IContainerMD> newdir;
  std::shared_ptr<eos::IContainerMD> rdir;
  std::shared_ptr<eos::IFileMD> file;
  bool renameFile = false;
  bool renameDir = false;
  bool renameVersion = false;
  bool findOk = false;
  bool quotaMove = false;
  XrdSfsFileExistence file_exists;
  std::string new_path = new_name;
  eos::Prefetcher::prefetchContainerMDAndWait(gOFS->eosView,
      nPath.GetParentPath());
  eos::Prefetcher::prefetchContainerMDAndWait(gOFS->eosView,
      oPath.GetParentPath());
  eos::Prefetcher::prefetchItemAndWait(gOFS->eosView, oPath.GetPath());
  eos::Prefetcher::prefetchItemAndWait(gOFS->eosView, nPath.GetPath());
  COMMONTIMING("prefetchItems", &tm);

  if (_exists(old_name, file_exists, error, vid, infoN)) {
    errno = ENOENT;
    return Emsg(epname, error, ENOENT, "rename - source does not exist");
  } else {
    if (file_exists == XrdSfsFileExistNo) {
      errno = ENOENT;
      return Emsg(epname, error, ENOENT, "rename - source does not exist");
    }

    if (file_exists == XrdSfsFileExistIsFile) {
      renameFile = true;
      XrdSfsFileExistence version_exists;
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
      renameDir = true;
      std::string n_path = nPath.GetPath();
      std::string o_path = oPath.GetPath();

      if ((n_path.at(n_path.length() - 1) != '/')) {
        n_path += "/";
      }

      if ((o_path.at(o_path.length() - 1) != '/')) {
        o_path += "/";
      }

      // Check if old path is a subpath of new path
      if ((n_path.length() > o_path.length()) &&
          (!n_path.compare(0, o_path.length(), o_path))) {
        errno = EINVAL;
        return Emsg(epname, error, EINVAL, "rename - old path is subpath of new path");
      }

      // Check if old path is a quota node - this is forbidden
      try {
        auto rdir = eosView->getContainer(oPath.GetPath());
        eos::MDLocking::ContainerReadLock rdirLocked(rdir.get());

        if (rdir->getFlags() & eos::QUOTA_NODE_FLAG) {
          errno = EACCES;
          return Emsg(epname, error, EACCES, "rename - source is a quota node");
        }
      } catch (eos::MDException& e) {
        errno = ENOENT;
        return Emsg(epname, error, ENOENT, "rename - source does not exist");
      }
    }
  }

  if (!_exists(new_name, file_exists, error, vid, infoN)) {
    if (file_exists == XrdSfsFileExistIsFile) {
      if (new_path.back() == '/') {
        errno = ENOTDIR;
        return Emsg(epname, error, ENOTDIR, "rename - target is a not directory");
      }

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
      // append the previous last name to the target path
      if (new_path.back() != '/') {
        new_path += "/";
      }

      new_path += oPath.GetName();
      new_name = new_path.c_str();
      nPath = new_path;
      nP = nPath.GetParentPath();

      // check if this directory exists already
      if (!_exists(new_name, file_exists, error, vid, infoN)) {
        if (file_exists == XrdSfsFileExistIsFile) {
          errno = EEXIST;
          return Emsg(epname, error, EEXIST,
                      "rename - target directory is an existing file");
        }

        if (file_exists == XrdSfsFileExistIsDirectory) {
          // Delete the existing target, if it empty it will work, otherwise it will fail
          if (gOFS->_remdir(new_name, error, vid, infoN)) {
            return SFS_ERROR;
          }
        }
      }
    }
  } else {
    if (!renameDir) {
      if (new_path.back() == '/') {
        // append the previous last name to the target path - nevertheless the parent won't exist
        new_path += oPath.GetName();
        new_name = new_path.c_str();
        nPath = new_path;
        nP = nPath.GetParentPath();
      }
    }
  }

  COMMONTIMING("exists", &tm);
  // List of source files if a directory is renamed
  std::map<std::string, std::set<std::string> > found;

  if (renameDir) {
    {
      // figure out if this is a move within the same quota node
      eos::IContainerMD::id_t q1 {0ull};
      eos::IContainerMD::id_t q2 {0ull};
      long long avail_files, avail_bytes;
      Quota::QuotaByPath(oPath.GetParentPath(), 0, 0, avail_files, avail_bytes, q1);
      Quota::QuotaByPath(nPath.GetParentPath(), 0, 0, avail_files, avail_bytes, q2);

      if (q1 != q2) {
        quotaMove = true;
      }
    }

    if (EOS_LOGS_DEBUG) {
      eos_debug("quotaMove = %d", quotaMove);
    }

    // For directory renaming which move into a different directory, we build
    // the list of files which we are moving if they move between quota nodes
    if ((oP != nP) && quotaMove) {
      XrdOucString stdErr;

      if (!gOFS->_find(oPath.GetFullPath().c_str(), error, stdErr, vid, found)) {
        findOk = true;
      } else {
        return Emsg(epname, error, errno,
                    "rename - cannot do 'find' inside the source tree");
      }

      COMMONTIMING("rename::dir_find_files_for_quota_move", &tm);
    }
  }

  {
    eos::mgm::FusexCastBatch fuse_batch;

    try {
      dir = eosView->getContainer(oPath.GetParentPath());
      newdir = eosView->getContainer(nPath.GetParentPath());
      // Translate to paths without symlinks
      std::string duri = eosView->getUri(dir.get());
      std::string newduri = eosView->getUri(newdir.get());
      // Get symlink-free dir's
      dir = eosView->getContainer(duri);
      newdir = eosView->getContainer(newduri);
      const eos::ContainerIdentifier did = dir->getIdentifier();
      const eos::ContainerIdentifier pdid = dir->getParentIdentifier();
      const eos::ContainerIdentifier ndid = newdir->getIdentifier();
      const eos::ContainerIdentifier pndid = newdir->getParentIdentifier();
      COMMONTIMING("rename::get_old_and_new_containers", &tm);

      if (renameFile) {
        if (oP == nP) {
          file = dir->findFile(oPath.GetName());
          COMMONTIMING("rename::rename_file_within_same_container_find_file", &tm);

          if (file) {
            eos::FileIdentifier fid;
            {
              eos::MDLocking::BulkMDWriteLock dirFileLocker;
              dirFileLocker.add(dir.get());
              dirFileLocker.add(file.get());
              auto locks = dirFileLocker.lockAll();
              COMMONTIMING("rename::rename_file_within_same_container_dir_file_write_lock", &tm);
              eosView->renameFile(file.get(), nPath.GetName());
              dir->setMTimeNow();
              dir->notifyMTimeChange(gOFS->eosDirectoryService);
              eosView->updateContainerStore(dir.get());
              COMMONTIMING("rename::rename_file_within_same_container_file_rename", &tm);
              const std::string old_name = oPath.GetName();
              fid = file->getIdentifier();
            }
            if (fusexcast) {
              fuse_batch.Register([&, did, pdid, fid, old_name]() {
                gOFS->FuseXCastRefresh(did, pdid);
                gOFS->FuseXCastDeletion(did, old_name);
                gOFS->FuseXCastRefresh(fid, did);
              });
            }
          }
        } else {
          file = dir->findFile(oPath.GetName());
          COMMONTIMING("rename::move_file_to_different_container_find_file", &tm);

          if (file) {
            // Get the quota nodes before locking the directories. Indeed, getting the quota node requires the tree
            // to be browsed from the directory to all its parent until reaching the quota node (taking read locks on each directory), which
            // could break the locking order (by directory ID)...
            eos::IQuotaNode* old_qnode = eosView->getQuotaNode(dir.get());
            eos::IQuotaNode* new_qnode = eosView->getQuotaNode(newdir.get());
            // Move to a new directory
            // TODO: deal with conflicts and proper roll-back in case a file
            // with the same name already exists in the destination directory
            eos::MDLocking::BulkMDWriteLock helper;
            helper.add(dir.get());
            helper.add(newdir.get());
            helper.add(file.get());
            auto locks = helper.lockAll();
            COMMONTIMING("rename::move_file_to_different_container_dirs_file_write_lock", &tm);
            dir->removeFile(oPath.GetName());
            dir->setMTimeNow();
            dir->notifyMTimeChange(gOFS->eosDirectoryService);
            newdir->setMTimeNow();
            newdir->notifyMTimeChange(gOFS->eosDirectoryService);
            eosView->updateContainerStore(dir.get());
            eosView->updateContainerStore(newdir.get());

            if (fusexcast) {
              const eos::FileIdentifier fid = file->getIdentifier();
              const std::string old_name = oPath.GetName();
              fuse_batch.Register([&, did, pdid, ndid, pndid, fid, old_name]() {
                gOFS->FuseXCastRefresh(did, pdid);
                gOFS->FuseXCastRefresh(ndid, pndid);
                gOFS->FuseXCastDeletion(did, old_name);
                gOFS->FuseXCastRefresh(fid, ndid);
              });
            }

            file->setName(nPath.GetName());
            file->setContainerId(newdir->getId());

            if (updateCTime) {
              file->setCTimeNow();
            }

            newdir->addFile(file.get());
            eosView->updateFileStore(file.get());
            COMMONTIMING("rename::move_file_to_different_container_rename", &tm);
            // Adjust the ns quota

            if (old_qnode) {
              old_qnode->removeFile(file.get());
            }

            if (new_qnode) {
              new_qnode->addFile(file.get());
            }

            COMMONTIMING("rename::move_file_to_different_container_adjust_ns_quota", &tm);
          }
        }
      }

      if (renameDir) {
        rdir = dir->findContainer(oPath.GetName());
        COMMONTIMING("rename::rename_dir_find_container", &tm);

        if (rdir) {
          {
            eos::MDLocking::BulkMDReadLock containerBulkLocker;
            containerBulkLocker.add(rdir.get());
            containerBulkLocker.add(newdir.get());
            auto containerLocks = containerBulkLocker.lockAll();
            COMMONTIMING("rename::rename_dir_first_is_safe_to_rename_all_dirs_read_lock",
                         &tm);

            if (!eos::isSafeToRename(gOFS->eosView, rdir.get(), newdir.get())) {
              errno = EINVAL;
              return Emsg(epname, error, EINVAL,
                          "rename - old path is subpath of new path");
            }

            COMMONTIMING("rename::rename_dir_first_is_safe_to_rename", &tm);
          }
          // Remove all the quota from the source node and add to the target node
          std::map<std::string, std::set<std::string> >::const_reverse_iterator rfoundit;
          std::set<std::string>::const_iterator fileit;

          // Loop over all the files and subtract them from their quota node
          if (findOk) {
            if (checkQuota) {
              {
                std::map<uid_t, unsigned long long> user_del_size;
                std::map<gid_t, unsigned long long> group_del_size;
                // Compute the total quota we need to rename by uid/gid
                for (rfoundit = found.rbegin(); rfoundit != found.rend();
                     rfoundit++) {
                  // To compute the quota, we don't need to read-lock the entire tree as
                  // it will anyway not be an atomic operation without the big namespace lock taken.
                  for (fileit = rfoundit->second.begin();
                       fileit != rfoundit->second.end(); fileit++) {
                    std::string fspath = rfoundit->first;
                    fspath += *fileit;
                    std::shared_ptr<eos::IFileMD> fmd =
                      std::shared_ptr<eos::IFileMD>((eos::IFileMD*)0);

                    // Stat this file and add to the deletion maps
                    try {
                      fmd = gOFS->eosView->getFile(fspath.c_str(), false);
                    } catch (eos::MDException& e) {
                      // Check if this is a symbolic link
                      std::string fname = *fileit;
                      size_t link_pos = fname.find(" -> ");

                      if (link_pos != std::string::npos) {
                        fname.erase(link_pos);
                        fspath = rfoundit->first;
                        fspath += fname;

                        try {
                          fmd = gOFS->eosView->getFile(fspath.c_str(), false);
                        } catch (eos::MDException& e) {
                          errno = e.getErrno();
                          eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"",
                                    e.getErrno(), e.getMessage().str().c_str());
                        }
                      } else {
                        errno = e.getErrno();
                        eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"",
                                  e.getErrno(), e.getMessage().str().c_str());
                      }
                    }

                    if (fmd) {
                      eos::MDLocking::FileReadLock locker(fmd.get());

                      if (!fmd->isLink()) {
                        // compute quotas to check
                        user_del_size[fmd->getCUid()] += fmd->getSize();
                        group_del_size[fmd->getCGid()] += fmd->getSize();
                      }
                    } else {
                      return Emsg(epname, error, errno,
                                  "rename - cannot stat file in subtree",
                                  fspath.c_str());
                    }
                  }
                }

                COMMONTIMING("rename::rename_dir_compute_quotas_to_check",
                             &tm);
                // Verify for each uid/gid that there is enough quota to rename
                bool userok = true;
                bool groupok = true;

                // Either all have user quota therefore userok is true
                for (const auto& [uid, size] : user_del_size) {
                  if (!Quota::Check(nP, uid, Quota::gProjectId, size, 1)) {
                    userok = false;
                    break;
                  }
                }

                // or all have group quota therefore groupok is true
                for (const auto& [gid, size] : group_del_size) {
                  if (!Quota::Check(nP, Quota::gProjectId, gid, size, 1)) {
                    groupok = false;
                    break;
                  }
                }

                if ((!userok) || (!groupok)) {
                  // Deletion will fail as there is not enough quota on the target
                  return Emsg(epname, error, ENOSPC,
                              "rename - cannot get all "
                              "the needed quota for the target directory");
                }
              }
              COMMONTIMING("rename::rename_dir_check_quotas", &tm);
            } // if (checkQuota)

            for (rfoundit = found.rbegin(); rfoundit != found.rend();
                 rfoundit++) {
              // Loop through every files
              // To compute the quota, we don't need to read-lock the entire tree as
              // it will anyway not be an atomic operation without the big namespace lock taken.
              for (fileit = rfoundit->second.begin();
                   fileit != rfoundit->second.end(); fileit++) {
                std::string fspath = rfoundit->first;
                fspath += *fileit;
                std::string fname = *fileit;

                if (fname.find(" -> ") != std::string::npos) {
                  // Skip symlinks
                  continue;
                }

                try {
                  file = gOFS->eosView->getFile(fspath.c_str());
                } catch (eos::MDException& e) {
                  // Check if this is a symbolic link
                  std::string fname = *fileit;
                  size_t link_pos = fname.find(" -> ");

                  if (link_pos != std::string::npos) {
                    fname.erase(link_pos);
                    fspath = rfoundit->first;
                    fspath += fname;

                    try {
                      file = gOFS->eosView->getFile(fspath.c_str(), false);
                    } catch (eos::MDException& e) {
                      errno = e.getErrno();
                      eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"",
                                e.getErrno(), e.getMessage().str().c_str());
                    }
                  } else {
                    errno = e.getErrno();
                    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"",
                              e.getErrno(), e.getMessage().str().c_str());
                  }
                }

                if (file) {
                  eos::MDLocking::FileReadLock locker(file.get());

                  if (!file->isLink()) {
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

            COMMONTIMING("rename::rename_dir_apply_quotas", &tm);
          }

          if (nP == oP) {
            // Rename within a container
            // Lock the containers
            eos::MDLocking::BulkMDWriteLock bulkContainerLocker;
            bulkContainerLocker.add(rdir.get());
            bulkContainerLocker.add(dir.get());
            auto containerLocks = bulkContainerLocker.lockAll();
            COMMONTIMING("rename::rename_dir_within_same_container_dirs_lock_write", &tm);
            eosView->renameContainer(rdir.get(), nPath.GetName());

            if (updateCTime) {
              rdir->setCTimeNow();
            }

            const std::string old_name = oPath.GetName();
            dir->setMTimeNow();
            dir->notifyMTimeChange(gOFS->eosDirectoryService);
            eosView->updateContainerStore(rdir.get());
            eosView->updateContainerStore(dir.get());
            const eos::ContainerIdentifier rdid = rdir->getIdentifier();
            fuse_batch.Register([&, rdid, did, pdid, old_name]() {
              gOFS->FuseXCastRefresh(rdid, did);
              gOFS->FuseXCastRefresh(did, pdid);
              gOFS->FuseXCastDeletion(did, old_name);
            });
            COMMONTIMING("rename::rename_dir_within_same_container", &tm);
          } else {
            {
              eos::MDLocking::BulkMDReadLock bulkDirLocker;
              bulkDirLocker.add(rdir.get());
              bulkDirLocker.add(newdir.get());
              auto dirLocks = bulkDirLocker.lockAll();
              COMMONTIMING("rename::rename_dir_second_is_safe_to_rename_all_dirs_read_lock",
                           &tm);

              // Do the check once again, because we're paranoid
              if (!eos::isSafeToRename(gOFS->eosView, rdir.get(),
                                       newdir.get())) {
                eos_static_crit(
                  "%s", SSTR("Unsafe rename of container "
                             << rdir->getId() << " -> " << newdir->getId()
                             << " was prevented at the last resort check")
                  .c_str());
                errno = EINVAL;
                return Emsg(
                         epname, error, EINVAL,
                         "rename - old path is subpath "
                         "of new path - caught by last resort check, quotanodes "
                         "may have become inconsistent");
              }

              COMMONTIMING("rename::rename_dir_second_is_safe_to_rename", &tm);
            }
            // Remove from one container to another one
            eos::MDLocking::BulkMDWriteLock bulkContainerLocker;
            bulkContainerLocker.add(dir.get());
            bulkContainerLocker.add(rdir.get());
            bulkContainerLocker.add(newdir.get());
            auto containerLocks = bulkContainerLocker.lockAll();
            COMMONTIMING("rename::move_dir_all_dirs_write_lock", &tm);
            int64_t tree_size = static_cast<int64_t>(rdir->getTreeSize());
            int64_t tree_files = static_cast<int64_t>(rdir->getTreeFiles());
            int64_t tree_cont = static_cast<int64_t>(rdir->getTreeContainers());
            {
              // update the source directory - remove the directory
              dir->removeContainer(oPath.GetName());
              dir->setMTimeNow();
              dir->notifyMTimeChange(gOFS->eosDirectoryService);

              if (gOFS->eosContainerAccounting) {
                gOFS->eosContainerAccounting->RemoveTree(dir.get(), {tree_size,tree_files,tree_cont});
              }

              eosView->updateContainerStore(dir.get());
              COMMONTIMING("rename::move_dir_remove_source_tree", &tm);
              const std::string dir_name = oPath.GetName();
              fuse_batch.Register([&, did, pdid, dir_name]() {
                gOFS->FuseXCastDeletion(did, dir_name);
                gOFS->FuseXCastRefresh(did, pdid);
              });
            }
            {
              // rename the moved directory and udpate it's parent ID
              rdir->setName(nPath.GetName());
              rdir->setParentId(newdir->getId());

              if (updateCTime) {
                rdir->setCTimeNow();
              }

              eosView->updateContainerStore(rdir.get());
              const eos::ContainerIdentifier rdid = rdir->getIdentifier();
              const eos::ContainerIdentifier prdid = rdir->getParentIdentifier();
              fuse_batch.Register([&, rdid, prdid]() {
                gOFS->FuseXCastRefresh(rdid, prdid);
              });
              COMMONTIMING("rename::move_dir_rename_moved_dir", &tm);
            }
            {
              // update the target directory - add the directory
              newdir->addContainer(rdir.get());
              newdir->setMTimeNow();

              if (gOFS->eosContainerAccounting) {
                gOFS->eosContainerAccounting->AddTree(newdir.get(), {tree_size,tree_files,tree_cont});
              }

              const eos::ContainerIdentifier rdid = rdir->getIdentifier();
              newdir->notifyMTimeChange(gOFS->eosDirectoryService);
              eosView->updateContainerStore(newdir.get());
              fuse_batch.Register([&, ndid, pndid, rdid]() {
                gOFS->FuseXCastRefresh(ndid, pndid);
                gOFS->FuseXCastRefresh(rdid, ndid);
              });
              COMMONTIMING("rename::move_dir_update_target_directory_add_old_dir", &tm);
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

  std::ostringstream oss;
  oss << "renamed " << oPath.GetFullPath() << " to " << nPath.GetFullPath() <<
      " timing=" << tm.Dump();
  eos_static_debug(oss.str().c_str());

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

  COMMONTIMING("end", &tm);
  EXEC_TIMING_END("Rename");
  return SFS_OK;
}
