// ----------------------------------------------------------------------
// File: Stripes.cc
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
/*
 * @brief send a verification message to a file system for a given file
 *
 * @param path file name to verify
 * @param error error object
 * @param vid virtual identity of the client
 * @param fsid filesystem id where to run the verification
 * @param option pass-through string for the verification
 *
 * @return SFS_OK if success otherwise SFS_ERROR
 *
 * The function requires POSIX W_OK & X_OK on the parent directory to succeed.
 */
/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_verifystripe(const char* path,
                         XrdOucErrInfo& error,
                         eos::common::VirtualIdentity& vid,
                         unsigned long fsid,
                         XrdOucString option)
{
  static const char* epname = "verifystripe";
  std::shared_ptr<eos::IContainerMD> dh;
  std::shared_ptr<eos::IFileMD> fmd;
  EXEC_TIMING_BEGIN("VerifyStripe");
  errno = 0;
  unsigned long long fid = 0;
  unsigned long long cid = 0;
  int lid = 0;
  eos::IContainerMD::XAttrMap attrmap;
  gOFS->MgmStats.Add("VerifyStripe", vid.uid, vid.gid, 1);
  eos_debug("verify");
  eos::common::Path cPath(path);
  std::string attr_path;
  {
    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);

    try {
      dh = gOFS->eosView->getContainer(cPath.GetParentPath());
      attr_path = gOFS->eosView->getUri(dh.get());
      dh = gOFS->eosView->getContainer(gOFS->eosView->getUri(dh.get()));
    } catch (eos::MDException& e) {
      dh.reset();
      eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),
                e.getMessage().str().c_str());
    }

    // Check permissions
    if (dh && (!dh->access(vid.uid, vid.gid, X_OK | W_OK))) {
      if (!errno) {
        errno = EPERM;
      }
    } else {
      // only root can delete a detached replica
      if (vid.uid) {
        errno = EPERM;
      }
    }

    if (errno) {
      return Emsg(epname, error, errno, "verify stripe", path);
    }

    // Get attributes
    gOFS->_attr_ls(attr_path.c_str(), error, vid, 0, attrmap);

    // Get the file
    try {
      fmd = gOFS->eosView->getFile(path);
      fid = fmd->getId();
      lid = fmd->getLayoutId();
      cid = fmd->getContainerId();
    } catch (eos::MDException& e) {
      fmd.reset();
      errno = e.getErrno();
      eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                e.getErrno(), e.getMessage().str().c_str());
      return Emsg(epname, error, errno,
                  "verify stripe - not file metadata", path);
    }
  }
  int fst_port;
  std::string fst_path, fst_queue, fst_host;
  {
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
    auto* verify_fs = FsView::gFsView.mIdView.lookupByID(fsid);

    if (!verify_fs) {
      errno = EINVAL;
      return Emsg(epname, error, ENOENT,
                  "verify stripe - filesystem does not exist", path);
    }

    // @todo(esindril) only issue verify for booted filesystems
    fst_path = verify_fs->GetPath();
    fst_queue = verify_fs->GetQueue();
    fst_host = verify_fs->GetHost();
    fst_port = verify_fs->getCoreParams().getLocator().getPort();
  }
  XrdOucString receiver = fst_queue.c_str();
  XrdOucString opaquestring = "";
  // build the opaquestring contents
  opaquestring += "&mgm.localprefix=";
  opaquestring += fst_path.c_str();
  opaquestring += "&mgm.fid=";
  const std::string hex_fid = eos::common::FileId::Fid2Hex(fid);
  opaquestring += hex_fid.c_str();
  opaquestring += "&mgm.manager=";
  opaquestring += gOFS->ManagerId.c_str();
  opaquestring += "&mgm.access=verify";
  opaquestring += "&mgm.fsid=";
  opaquestring += std::to_string(fsid).c_str();

  if (attrmap.count("user.tag")) {
    opaquestring += "&mgm.container=";
    opaquestring += attrmap["user.tag"].c_str();
  }

  XrdOucString sizestring = "";
  opaquestring += "&mgm.cid=";
  opaquestring += eos::common::StringConversion::GetSizeString(sizestring, cid);
  opaquestring += "&mgm.path=";
  XrdOucString safepath = path;
  eos::common::StringConversion::SealXrdPath(safepath);
  opaquestring += safepath;
  opaquestring += "&mgm.lid=";
  opaquestring += lid;

  if (option.length()) {
    opaquestring += option;
  }

  std::string qreq = "/?fst.pcmd=verify";
  qreq += opaquestring.c_str();
  std::string qresp;

  if (SendQuery(fst_host, fst_port, qreq, qresp)) {
    eos_static_err("msg=\"unable to send verification message\" target=%s",
                   fst_queue.c_str());
    errno = ECOMM;
  } else {
    errno = 0;
  }

  EXEC_TIMING_END("VerifyStripe");

  if (errno) {
    return Emsg(epname, error, errno, "verify stripe", path);
  } else {
    return SFS_OK;
  }
}

/*----------------------------------------------------------------------------*/
/*
 * @brief send a drop message to a file system for a given file
 *
 * @param path file name to drop stripe
 * #param file id of the file to drop stripe
 * @param error error object
 * @param vid virtual identity of the client
 * @param fsid filesystem id where to run the drop
 * @param forceRemove if true the stripe is immediately dropped
 *
 * @return SFS_OK if success otherwise SFS_ERROR
 *
 * The function requires POSIX W_OK & X_OK on the parent directory to succeed.
 */
/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_dropstripe(const char* path,
                       eos::common::FileId::fileid_t fid,
                       XrdOucErrInfo& error,
                       eos::common::VirtualIdentity& vid,
                       unsigned long fsid,
                       bool forceRemove)
{
  static const char* epname = "dropstripe";
  std::shared_ptr<eos::IContainerMD> dh;
  std::shared_ptr<eos::IFileMD> fmd;
  int errc = 0;
  EXEC_TIMING_BEGIN("DropStripe");
  gOFS->MgmStats.Add("DropStripe", vid.uid, vid.gid, 1);
  eos_debug("drop");
  eos::common::Path cPath(path);
  // ---------------------------------------------------------------------------
  eos::common::RWMutexWriteLock ns_wr_lock(gOFS->eosViewRWMutex);

  try {
    dh = gOFS->eosView->getContainer(cPath.GetParentPath());
    dh = gOFS->eosView->getContainer(gOFS->eosView->getUri(dh.get()));
  } catch (eos::MDException& e) {
    dh.reset();
    errc = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),
              e.getMessage().str().c_str());
  }

  // Check permissions
  if (dh && (!dh->access(vid.uid, vid.gid, X_OK | W_OK))) {
    if (!errc) {
      errc = EPERM;
    }
  } else {
    // only root can drop by file id
    if (vid.uid) {
      errc = EPERM;
    }
  }

  if (errc) {
    return Emsg(epname, error, errc, "drop stripe", path);
  }

  // get the file
  try {
    if (fid) {
      fmd = gOFS->eosFileService->getFileMD(fid);
    } else {
      fmd = gOFS->eosView->getFile(path);
    }

    std::string locations;

    try {
      locations = fmd->getAttribute("sys.fs.tracking");
    } catch (...) {}

    if (!forceRemove) {
      // we only unlink a location
      if (fmd->hasLocation(fsid)) {
        fmd->unlinkLocation(fsid);
        locations += "-";
        locations += std::to_string(fsid);
        fmd->setAttribute("sys.fs.tracking",
                          eos::common::StringConversion::ReduceString(locations).c_str());
        gOFS->eosView->updateFileStore(fmd.get());
        eos_debug("unlinking location %u", fsid);
      } else {
        errc = ENOENT;
      }
    } else {
      // we unlink and remove a location by force
      if (fmd->hasLocation(fsid)) {
        fmd->unlinkLocation(fsid);
        locations += "-";
        locations += std::to_string(fsid);
        fmd->setAttribute("sys.fs.tracking",
                          eos::common::StringConversion::ReduceString(locations).c_str());
      }

      fmd->removeLocation(fsid);
      gOFS->eosView->updateFileStore(fmd.get());
      ns_wr_lock.Release();
      eos_debug("msg=\"removing/unlinking location\" fxid=%08llx fsid=%u",
                fid, fsid);
      // eraseEntry is only needed if the fsview is inconsistent with the
      // FileMD: It exists on the selected fsview, but not in the fmd locations.
      // Very rare case but needs to be done outside the namespace lock as it
      // might needs to load the FileSystem view in memory.
      gOFS->eosFsView->eraseEntry(fsid, fid);
    }
  } catch (eos::MDException& e) {
    fmd.reset();
    errc = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
              e.getErrno(), e.getMessage().str().c_str());
  }

  EXEC_TIMING_END("DropStripe");

  if (errc) {
    return Emsg(epname, error, errc, "drop stripe", path);
  }

  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
/*
 * @brief send a drop message to all filesystems where given file is located
 *
 * @param path file name to drop stripe
 * @param error error object
 * @param vid virtual identity of the client
 * @param forceRemove if true the stripe is immediately dropped
 *
 * @return SFS_OK if success otherwise SFS_ERROR
 *
 * The function requires POSIX W_OK & X_OK on the parent directory to succeed.
 */
/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_dropallstripes(const char* path,
                           XrdOucErrInfo& error,
                           eos::common::VirtualIdentity& vid,
                           bool forceRemove)
{
  static const char* epname = "dropallstripes";
  std::shared_ptr<eos::IContainerMD> dh;
  std::shared_ptr<eos::IFileMD> fmd;
  errno = 0;
  EXEC_TIMING_BEGIN("DropAllStripes");
  gOFS->MgmStats.Add("DropAllStripes", vid.uid, vid.gid, 1);
  eos_debug("dropall");
  eos::common::Path cPath(path);
  auto parentPath = cPath.GetParentPath();
  // ---------------------------------------------------------------------------
  {
    eos::common::RWMutexReadLock rlock(gOFS->eosViewRWMutex);
    if(errno == EAGAIN) errno = 0;

    try {
      dh = gOFS->eosView->getContainer(parentPath);
      if(errno) eos_static_crit("[1.a] ERRNO-DEBUG , path=%s, errno=%d",path,errno);
      dh = gOFS->eosView->getContainer(gOFS->eosView->getUri(dh.get()));
      if(errno) eos_static_crit("[1.b] ERRNO-DEBUG , path=%s, errno=%d",path,errno);
    } catch (eos::MDException& e) {
      dh.reset();
      errno = e.getErrno();
      eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),
                e.getMessage().str().c_str());
    }

    // Check permissions
    if (dh && (!dh->access(vid.uid, vid.gid, X_OK | W_OK)))
      if (!errno) {
        errno = EPERM;
      }
    if(errno) eos_static_crit("[1.c] ERRNO-DEBUG , path=%s, errno=%d",path,errno);
    if (errno) {
      return Emsg(epname, error, errno, "drop all stripes", path);
    }

    try {
      fmd = gOFS->eosView->getFile(path);
      if(errno) eos_static_crit("[1.d] ERRNO-DEBUG , path=%s, errno=%d",path,errno);
      // only on tape, we don't touch this file here
      if (fmd && fmd->getLocations().size() == 1 &&
          fmd->hasLocation(eos::common::TAPE_FS_ID)) {
        return SFS_OK;
      }
    } catch (eos::MDException& e) {
      errno = e.getErrno();
      eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                e.getErrno(), e.getMessage().str().c_str());
      // return error if we don't have the file metadata
      return e.getErrno();
    }
  }

  if(errno) eos_static_crit("[1.e] ERRNO-DEBUG , path=%s, errno=%d",path,errno);

  try {
    // only write lock at this point
    eos::common::RWMutexWriteLock wlock(gOFS->eosViewRWMutex, EOS_FUNCTION, EOS_FILE, EOS_LINE, true);
    if(errno == EAGAIN) errno = 0;
    if(errno) eos_static_crit("[1.f] ERRNO-DEBUG , path=%s, errno=%d",path,errno);
    for (auto location : fmd->getLocations()) {
      if(errno) eos_static_crit("[1.g] ERRNO-DEBUG , path=%s, errno=%d",path,errno);
      if (location == eos::common::TAPE_FS_ID) {
        continue;
      }

      if (!forceRemove) {
        // we only unlink a location
        fmd->unlinkLocation(location, true);
        eos_debug("unlinking location %u", location);
        if(errno) eos_static_crit("[1.h] ERRNO-DEBUG , path=%s, errno=%d",path,errno);
      } else {
        // we unlink and remove a location by force
        if (fmd->hasLocation(location)) {
          if(errno) eos_static_crit("[1.i] ERRNO-DEBUG , path=%s, errno=%d",path,errno);
          fmd->unlinkLocation(location, true);
          if(errno) eos_static_crit("[1.j] ERRNO-DEBUG , path=%s, errno=%d",path,errno);
        }

        fmd->removeLocation(location);
        eos_debug("removing/unlinking location %u", location);
        if(errno) eos_static_crit("[1.k] ERRNO-DEBUG , path=%s, errno=%d",path,errno);
      }
    }

    // update the file store only once at the end
    gOFS->eosView->updateFileStore(fmd.get());
    if(errno) eos_static_crit("[1.l] ERRNO-DEBUG , path=%s, errno=%d",path,errno);
  } catch (eos::MDException& e) {
    fmd.reset();
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
              e.getErrno(), e.getMessage().str().c_str());
  }

  EXEC_TIMING_END("DropAllStripes");

  if (errno) {
    return Emsg(epname, error, errno, "drop all stripes", path);
  }

  return SFS_OK;
}

//------------------------------------------------------------------------------
// Move file replica/stripe from source to target file system
//------------------------------------------------------------------------------
int
XrdMgmOfs::_movestripe(const char* path,
                       XrdOucErrInfo& error,
                       eos::common::VirtualIdentity& vid,
                       unsigned long sourcefsid,
                       unsigned long targetfsid)
{
  EXEC_TIMING_BEGIN("MoveStripe");
  int retc = _replicatestripe(path, error, vid, sourcefsid, targetfsid, true);
  EXEC_TIMING_END("MoveStripe");
  return retc;
}

//------------------------------------------------------------------------------
// Copy file replica/stripe from source to target file system
//------------------------------------------------------------------------------
int
XrdMgmOfs::_copystripe(const char* path,
                       XrdOucErrInfo& error,
                       eos::common::VirtualIdentity& vid,
                       unsigned long sourcefsid,
                       unsigned long targetfsid)
{
  EXEC_TIMING_BEGIN("CopyStripe");
  int retc = _replicatestripe(path, error, vid, sourcefsid, targetfsid, false);
  EXEC_TIMING_END("CopyStripe");
  return retc;
}

//------------------------------------------------------------------------------
// Copy file replica/stripe from source to target file system - by path
//------------------------------------------------------------------------------
int
XrdMgmOfs::_replicatestripe(const char* path,
                            XrdOucErrInfo& error,
                            eos::common::VirtualIdentity& vid,
                            unsigned long sourcefsid,
                            unsigned long targetfsid,
                            bool dropsource)
{
  static const char* epname = "replicatestripe";
  std::shared_ptr<eos::IContainerMD> dh;
  std::shared_ptr<eos::IFileMD> fmd;
  errno = 0;
  EXEC_TIMING_BEGIN("ReplicateStripe");
  eos::common::Path cPath(path);
  eos_debug("msg=\"replicate file\" path=\"%s\" src_fsid=%u dst_fsid=%u drop=%d",
            path, sourcefsid, targetfsid, dropsource);
  {
    eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex);

    try {
      dh = gOFS->eosView->getContainer(cPath.GetParentPath());
      dh = gOFS->eosView->getContainer(gOFS->eosView->getUri(dh.get()));
    } catch (eos::MDException& e) {
      dh.reset();
      errno = e.getErrno();
      eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),
                e.getMessage().str().c_str());
    }

    // check permissions
    if (dh && (!dh->access(vid.uid, vid.gid, X_OK | W_OK)))
      if (!errno) {
        errno = EPERM;
      }

    // get the file
    try {
      fmd = gOFS->eosView->getFile(path);

      if (fmd->hasLocation(sourcefsid)) {
        if (fmd->hasLocation(targetfsid)) {
          errno = EEXIST;
        }
      } else {
        // this replica does not exist!
        errno = ENODATA;
      }
    } catch (eos::MDException& e) {
      fmd.reset();
      errno = e.getErrno();
      eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),
                e.getMessage().str().c_str());
    }
  }

  if (errno) {
    return Emsg(epname, error, errno, "replicate stripe", path);
  }

  int retc = _replicatestripe(fmd.get(), path, error, vid, sourcefsid,
                              targetfsid, dropsource);
  EXEC_TIMING_END("ReplicateStripe");
  return retc;
}

//------------------------------------------------------------------------------
// Copy file replica/stripe from source to target file system - by FileMD
//------------------------------------------------------------------------------
int
XrdMgmOfs::_replicatestripe(eos::IFileMD* fmd,
                            const char* path,
                            XrdOucErrInfo& error,
                            eos::common::VirtualIdentity& vid,
                            unsigned long sourcefsid,
                            unsigned long targetfsid,
                            bool dropsource)
{
  static const char* epname = "replicatestripe";
  uint64_t fid = fmd->getId();
  std::string app_tag = (dropsource ? "MoveStripe" : "CopyStripe");;
  std::shared_ptr<DrainTransferJob> job {
    new DrainTransferJob(fid, sourcefsid, targetfsid, {}, {}, dropsource, app_tag,
    false, vid)};

  if (!gOFS->mFidTracker.AddEntry(fid, TrackerType::Drain)) {
    eos_err("msg=\"file already tracked\" fxid=%08llx", fid);
    return Emsg(epname, error, ENOENT, "replicate stripe - file already "
                "tracked ", std::to_string(fid).c_str());
  } else {
    gOFS->mDrainEngine.GetThreadPool().PushTask<void>([ = ] {
      job->UpdateMgmStats();
      job->DoIt();
      job->UpdateMgmStats();
    });
  }

  return SFS_OK;
}
