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

//------------------------------------------------------------------------------
// Send verify stripe request to a certain file system for file path
//------------------------------------------------------------------------------
int
XrdMgmOfs::_verifystripe(const char* path,
                         XrdOucErrInfo& error,
                         eos::common::VirtualIdentity& vid,
                         unsigned long fsid,
                         const std::string& options)
{
  static const char* epname = "verifystripe";
  eos::IFileMD::id_t fid = 0ull;

  try {
    auto fmd = gOFS->eosView->getFile(path);
    fid = fmd->getId();
  } catch (eos::MDException& e) {
    int errc = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
              e.getErrno(), e.getMessage().str().c_str());
    return Emsg(epname, error, errc,
                "verify stripe - not file metadata", path);
  }

  return _verifystripe(fid, error, vid, fsid, options, path);
}

//----------------------------------------------------------------------------
// Send verify stripe request to a certain file system for file identifier
//----------------------------------------------------------------------------
int
XrdMgmOfs::_verifystripe(const eos::IFileMD::id_t fid,
                         XrdOucErrInfo& error,
                         eos::common::VirtualIdentity& vid,
                         unsigned long fsid,
                         const std::string& options,
                         const std::string& ns_path)
{
  eos_debug("verify");
  static const char* epname = "verifystripe";
  EXEC_TIMING_BEGIN("VerifyStripe");
  int lid = 0;
  int errc = 0;
  unsigned long long cid = 0;
  eos::IContainerMD::XAttrMap attrmap;
  gOFS->MgmStats.Add("VerifyStripe", vid.uid, vid.gid, 1);

  try {
    auto fmd = gOFS->eosView->getFileMDSvc()->getFileMD(fid);
    auto fmdLock = eos::MDLocking::readLock(fmd.get());
    cid = fmd->getContainerId();
    lid = fmd->getLayoutId();
  } catch (eos::MDException& e) {
    errc = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),
              e.getMessage().str().c_str());
    return Emsg(epname, error, errc, "verify stripe - no file metadata fid=",
                std::to_string(fid).c_str());
  }

  {  // Check parent existance and permissions
    eos::MDLocking::ContainerReadLockPtr cmd_rlock;
    std::shared_ptr<eos::IContainerMD> cmd;

    try {
      cmd = gOFS->eosView->getContainerMDSvc()->getContainerMD(cid);
      cmd_rlock = eos::MDLocking::readLock(cmd.get());
    } catch (eos::MDException& e) {
      cmd.reset();
      eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),
                e.getMessage().str().c_str());
    }

    // Check permissions
    errno = 0;

    if (cmd && (vid.token || (!cmd->access(vid.uid, vid.gid, X_OK | W_OK)))) {
      if (!errno) {
        errc = EPERM;
      }
    } else { // Only root can delete a detached replica
      if (vid.uid) {
        errc = EPERM;
      }
    }

    if (errc) {
      return Emsg(epname, error, errc, "verify stripe fid=",
                  std::to_string(fid).c_str());
    }

    // Get extended attributes if parent container exists
    if (cmd) {
      eos::FileOrContainerMD item;
      item.container = cmd;
      gOFS->listAttributes(gOFS->eosView, item, attrmap, true);
    }
  }

  int fst_port;
  std::string fst_path, fst_queue, fst_host;
  {
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
    auto* verify_fs = FsView::gFsView.mIdView.lookupByID(fsid);

    if (!verify_fs) {
      errc = EINVAL;
      return Emsg(epname, error, ENOENT,
                  "verify stripe - filesystem does not exist fid=",
                  std::to_string(fid).c_str());
    }

    // @todo(esindril) only issue verify for booted filesystems
    fst_path = verify_fs->GetPath();
    fst_queue = verify_fs->GetQueue();
    fst_host = verify_fs->GetHost();
    fst_port = verify_fs->getCoreParams().getLocator().getPort();
  }

  // Build the opaquestring contents
  XrdOucString opaquestring = "";
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
  XrdOucString safepath = ns_path.c_str();
  eos::common::StringConversion::SealXrdPath(safepath);
  opaquestring += safepath;
  opaquestring += "&mgm.lid=";
  opaquestring += lid;

  if (!options.empty()) {
    opaquestring += options.c_str();
  }

  std::string qreq = "/?fst.pcmd=verify";
  qreq += opaquestring.c_str();
  std::string qresp;

  if (SendQuery(fst_host, fst_port, qreq, qresp)) {
    eos_static_err("msg=\"unable to send verification message\" target=%s",
                   fst_queue.c_str());
    errc = ECOMM;
  } else {
    errc = 0;
  }

  EXEC_TIMING_END("VerifyStripe");

  if (errc) {
    return Emsg(epname, error, errc, "verify stripe fid=",
                std::to_string(fid).c_str());
  }

  return SFS_OK;
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
  using eos::common::StringConversion;
  static const char* epname = "dropstripe";
  eos_debug("msg=\"drop stripe\" path=\"%s\" fxid=%08llx fsid=%lu",
            path, fid, fsid);
  gOFS->MgmStats.Add("DropStripe", vid.uid, vid.gid, 1);
  int errc = 0;
  eos::IContainerMD::id_t cid = 0ull;
  EXEC_TIMING_BEGIN("DropStripe");

  // Retrieve read locked file
  try {
    eos::IFileMDPtr fmd = nullptr;
    eos::MDLocking::FileReadLockPtr fmd_rlock;

    if (fid) {
      fmd = gOFS->eosView->getFileMDSvc()->getFileMD(fid);
    } else {
      fmd = gOFS->eosView->getFile(path);
      fmd_rlock = eos::MDLocking::readLock(fmd.get());
      fid = fmd->getId(); // set in case we were called by path
    }

    cid = fmd->getContainerId();
  } catch (eos::MDException& e) {
    errc = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),
              e.getMessage().str().c_str());
    return Emsg(epname, error, errc, "drop stripe", path);
  }

  // Retrieve parent container and check permissions
  try {
    auto cmd = gOFS->eosView->getContainerMDSvc()->getContainerMD(cid);
    errno = 0;
    // only one operation, no need to readlock!
    if (vid.token || (!cmd->access(vid.uid, vid.gid, X_OK | W_OK) && !errno)) {
      errc = EPERM;
      return Emsg(epname, error, errc, "drop stripe", path);
    }
  } catch (eos::MDException& e) {
    // Missing parent container - only root can drop stripes in this case
    if (vid.uid) {
      errc = EPERM;
      return Emsg(epname, error, errc, "drop detached stripe", path);
    }
  }

  // Retrieve write locked file and modify it
  try {
    std::string locations;
    eos::IFileMDPtr fmd = gOFS->eosView->getFileMDSvc()->getFileMD(fid);
    eos::MDLocking::FileWriteLockPtr fmd_wlock = eos::MDLocking::writeLock(fmd.get());

    try {
      locations = fmd->getAttribute("sys.fs.tracking");
    } catch (...) {
      // ignore missing attribute
    }

    if (!forceRemove) {
      // We only unlink the location
      if (fmd->hasLocation(fsid)) {
        fmd->unlinkLocation(fsid);
        locations += "-";
        locations += std::to_string(fsid);
        fmd->setAttribute("sys.fs.tracking",
                                   StringConversion::ReduceString(locations).c_str());
        gOFS->eosView->updateFileStore(fmd.get());
        eos_debug("msg=\"unlinking location\" fid=%08llx fsid=%lu", fid, fsid);
      } else {
        errc = ENOENT;
        return Emsg(epname, error, errc, "drop stripe", path);
      }
    } else {
      // Unlink and remove location by force
      if (fmd->hasLocation(fsid)) {
        fmd->unlinkLocation(fsid);
        locations += "-";
        locations += std::to_string(fsid);
        fmd->setAttribute("sys.fs.tracking",
                                   StringConversion::ReduceString(locations).c_str());
      }

      fmd->removeLocation(fsid);
      gOFS->eosView->updateFileStore(fmd.get());
      eos_debug("msg=\"unlinking and removing location\" fxid=%08llx fsid=%lu",
                fid, fsid);
      fmd_wlock.reset(nullptr);
      // eraseEntry is only needed if the fsview is inconsistent with the
      // FileMD. It exists on the selected fsview, but not in the fmd locations.
      // Very rare case but needs to be done outside the namespace lock as it
      // might need to load the FileSystem view in memory!
      gOFS->eosFsView->eraseEntry(fsid, fid);
    }
  } catch (eos::MDException& e) {
    errc = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),
              e.getMessage().str().c_str());
    return Emsg(epname, error, errc, "drop stripe", path);
  }

  EXEC_TIMING_END("DropStripe");
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
  eos_debug("msg=\"drop all stripes\" path=\"%s\" force=%d", path, forceRemove);
  gOFS->MgmStats.Add("DropAllStripes", vid.uid, vid.gid, 1);
  int errc = 0;
  EXEC_TIMING_BEGIN("DropAllStripes");

  // Retrieve parent container and check permissions
  try {
    eos::common::Path cpath(path);
    eos::IContainerMDPtr cont = gOFS->eosView->getContainer(cpath.GetParentPath());
    auto contLock = eos::MDLocking::readLock(cont.get());
    errno = 0;

    if (vid.token || (!cont->access(vid.uid, vid.gid, X_OK | W_OK) && !errno)) {
      errc = EPERM;
      return Emsg(epname, error, errc, "drop stripe", path);
    }
  } catch (eos::MDException& e) {
    // Missing parent container
    errc = EPERM;
    return Emsg(epname, error, errc, "drop detached stripe", path);
  }

  // Retrieve write locked file and modify it
  try {
    eos::IFileMDPtr fmd = gOFS->eosView->getFile(path);
    auto fmdLock = eos::MDLocking::writeLock(fmd.get());
    eos::IFileMD::id_t fid = fmd->getId();

    // If file only on tape then don't touch it
    if ((fmd->getLocations().size() == 1) &&
        fmd->hasLocation(eos::common::TAPE_FS_ID)) {
      return SFS_OK;
    }

    for (auto location : fmd->getLocations()) {
      if (location == eos::common::TAPE_FS_ID) {
        continue;
      }

      if (!forceRemove) {
        fmd->unlinkLocation(location);
        eos_debug("msg=\"unlinking location\" fid=%08llx fsid=%lu",
                  fid, location);
      } else {
        fmd->unlinkLocation(location);
        fmd->removeLocation(location);
        eos_debug("msg=\"unlinking and removing location\" fxid=%08llx fsid=%lu",
                  fid, location);
      }
    }

    gOFS->eosView->updateFileStore(fmd.get());
  } catch (eos::MDException& e) {
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
              e.getErrno(), e.getMessage().str().c_str());
    return Emsg(epname, error, errc, "drop all stripes", path);
  }

  EXEC_TIMING_END("DropAllStripes");
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
  int errc = 0;
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
      errc = e.getErrno();
      eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),
                e.getMessage().str().c_str());
    }

    // check permissions
    errno = 0;

    if (dh && (vid.token ||(!dh->access(vid.uid, vid.gid, X_OK | W_OK)))) {
      if (!errno) {
        errc = EPERM;
      }
    }

    if (errc) {
      return Emsg(epname, error, errc, "replicate stripe", path);
    }

    // get the file
    try {
      fmd = gOFS->eosView->getFile(path);

      if (fmd->hasLocation(sourcefsid)) {
        if (fmd->hasLocation(targetfsid)) {
          errc = EEXIST;
        }
      } else {
        // this replica does not exist!
        errc = ENODATA;
      }
    } catch (eos::MDException& e) {
      fmd.reset();
      errc = e.getErrno();
      eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),
                e.getMessage().str().c_str());
    }
  }

  if (errc) {
    return Emsg(epname, error, errc, "replicate stripe", path);
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
    return Emsg(epname, error, ETXTBSY, "replicate stripe - file already "
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
