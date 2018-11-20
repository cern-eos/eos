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

//----------------------------------------------------------------------------
// Utility functions to help with capability string generation
//----------------------------------------------------------------------------
namespace {
  // Build general transfer capability string
  XrdOucString constructCapability(unsigned long lid, unsigned long long cid,
                                   const char* path, unsigned long long fid,
                                   const char* localprefix, int fsid,
                                   bool dropsource, int drain_fsid,
                                   bool uselpath, const char* lpath) {
    XrdOucString capability = "";
    XrdOucString safepath = path;
    while (safepath.replace("&", "#AND#")) {}

    capability += "&mgm.lid=";
    capability += std::to_string(lid).c_str();
    capability += "&mgm.cid=";
    capability += std::to_string(cid).c_str();
    capability += "&mgm.ruid=1";
    capability += "&mgm.rgid=1";
    capability += "&mgm.uid=1";
    capability += "&mgm.gid=1";
    capability += "&mgm.path=";
    capability += safepath.c_str();
    capability += "&mgm.manager=";
    capability += gOFS->ManagerId.c_str();
    capability += "&mgm.fid=";
    capability += eos::common::FileId::Fid2Hex(fid).c_str();
    capability += "&mgm.sec=";
    capability += eos::common::SecEntity::ToKey(0, "eos/replication").c_str();
    capability += "&mgm.localprefix=";
    capability += localprefix;
    capability += "&mgm.fsid=";
    capability += fsid;

    if (dropsource) {
      capability += "&mgm.drainfsid=";
      capability += drain_fsid;
    }

    if (uselpath) {
      capability += "&mgm.lpath=";
      capability += lpath;
    }

    return capability;
  }

  // Build source specific capability string
  XrdOucString constructSourceCapability(unsigned long lid, unsigned long long cid,
                                         const char* path, unsigned long long fid,
                                         const char* localprefix, int fsid,
                                         const char* hostport, bool dropsource,
                                         int drain_fsid, bool uselpath,
                                         const char* lpath) {
   XrdOucString capability = "mgm.access=read";

   capability += constructCapability(lid, cid, path, fid, localprefix, fsid,
                                     dropsource, drain_fsid, uselpath, lpath);
   capability += "&mgm.sourcehostport=";
   capability += hostport;

    return capability;
  }

  // Build target specific capability string
  XrdOucString constructTargetCapability(unsigned long lid, unsigned long long cid,
                                         const char* path, unsigned long long fid,
                                         const char* localprefix, int fsid,
                                         const char* hostport, bool dropsource,
                                         int drain_fsid, bool uselpath,
                                         const char* lpath,
                                         unsigned long long size,
                                         unsigned long source_lid,
                                         uid_t source_uid,
                                         gid_t source_gid) {
    XrdOucString capability = "mgm.access=write";

    capability += constructCapability(lid, cid, path, fid, localprefix, fsid,
                                      dropsource, drain_fsid, uselpath, lpath);
    capability += "&mgm.targethostport=";
    capability += hostport;
    capability += "&mgm.bookingsize=";
    capability += std::to_string(size).c_str();
    capability += "&mgm.source.lid=";
    capability += std::to_string(source_lid).c_str();
    capability += "&mgm.source.ruid=";
    capability += std::to_string(source_uid).c_str();
    capability += "&mgm.source.rgid=";
    capability += std::to_string(source_gid).c_str();

    return capability;
  }

  int issueFullCapability(XrdOucString source_cap, XrdOucString target_cap,
                          unsigned long long capValidity,
                          const char* source_hostport,
                          const char* target_hostport,
                          unsigned long long fid,
                          XrdOucString& full_capability,
                          XrdOucErrInfo& error) {
    XrdOucEnv insourcecap_env(source_cap.c_str());
    XrdOucEnv intargetcap_env(target_cap.c_str());
    XrdOucEnv* sourcecap_env = 0;
    XrdOucEnv* targetcap_env = 0;
    eos::common::SymKey* symkey = eos::common::gSymKeyStore.GetCurrentKey();

    int rc = gCapabilityEngine.Create(&insourcecap_env, sourcecap_env,
                                      symkey, capValidity);
    if (rc) {
      error.setErrInfo(rc, "source");
      return rc;
    }

    rc = gCapabilityEngine.Create(&intargetcap_env, targetcap_env,
                                  symkey, capValidity);
    if (rc) {
      error.setErrInfo(rc, "target");
      return rc;
    }

    XrdOucString hexfid;
    eos::common::FileId::Fid2Hex(fid, hexfid);

    int caplen = 0;
    source_cap = sourcecap_env->Env(caplen);
    target_cap = targetcap_env->Env(caplen);

    source_cap.replace("cap.sym", "source.cap.sym");
    source_cap.replace("cap.msg", "source.cap.msg");
    source_cap += "&source.url=root://";
    source_cap += source_hostport;
    source_cap += "//replicate:";
    source_cap += hexfid.c_str();

    target_cap.replace("cap.sym", "target.cap.sym");
    target_cap.replace("cap.msg", "target.cap.msg");
    target_cap += "&target.url=root://";
    target_cap += target_hostport;
    target_cap += "//replicate:";
    target_cap += hexfid.c_str();

    full_capability = source_cap;
    full_capability += target_cap;

    if (sourcecap_env) { delete sourcecap_env; }
    if (targetcap_env) { delete targetcap_env; }

    return 0;
  }

  // Build Verify capability string
  XrdOucString constructVerifyCapability(unsigned long lid, unsigned long long cid,
                                         const char* path, unsigned long long fid,
                                         const char* localprefix, int fsid,
                                         bool has_usertag, const char* usertag,
                                         bool has_lpath, const char* lpath,
                                         XrdOucString option) {
    XrdOucString capability = "";
    XrdOucString safepath = path;
    while (safepath.replace("&", "#AND#")) {}

    capability += "&mgm.access=verify";
    capability += "&mgm.lid=";
    capability += std::to_string(lid).c_str();
    capability += "&mgm.cid=";
    capability += std::to_string(cid).c_str();
    capability += "&mgm.path=";
    capability += safepath.c_str();
    capability += "&mgm.manager=";
    capability += gOFS->ManagerId.c_str();
    capability += "&mgm.fid=";
    capability += eos::common::FileId::Fid2Hex(fid).c_str();
    capability += "&mgm.localprefix=";
    capability += localprefix;
    capability += "&mgm.fsid=";
    capability += fsid;

    if (has_usertag) {
      capability += "&mgm.container=";
      capability += usertag;
    }

    if (has_lpath) {
      capability += "&mgm.lpath=";
      capability += lpath;
    }

    if (option.length()) {
      capability += option;
    }

    return capability;
  }
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_verifystripe(const char* path,
                         XrdOucErrInfo& error,
                         eos::common::Mapping::VirtualIdentity& vid,
                         unsigned long fsid,
                         XrdOucString option)
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
{
  static const char* epname = "verifystripe";
  std::shared_ptr<eos::IContainerMD> dh;
  std::shared_ptr<eos::IFileMD> fmd;
  EXEC_TIMING_BEGIN("VerifyStripe");
  errno = 0;
  unsigned long long fid = 0;
  unsigned long long cid = 0;
  unsigned long lid = 0;
  eos::IContainerMD::XAttrMap attrmap;
  gOFS->MgmStats.Add("VerifyStripe", vid.uid, vid.gid, 1);
  eos_debug("verify for path=%s fsid=%lu", path, fsid);

  eos::common::Path cPath(path);
  XrdOucString lpath;
  std::string attr_path;
  // ---------------------------------------------------------------------------
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
  gOFS->_attr_ls(attr_path.c_str(), error, vid, 0, attrmap, false);

  // Get the file
  try {
    fmd = gOFS->eosView->getFile(path);
    fid = fmd->getId();
    lid = fmd->getLayoutId();
    cid = fmd->getContainerId();
    if (fmd->hasAttribute("sys.eos.lpath")) {
      eos::common::FileFsPath::GetPhysicalPath(fsid, fmd, lpath);
    }
  } catch (eos::MDException& e) {
    fmd.reset();
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
              e.getErrno(), e.getMessage().str().c_str());
  }

  if (!errno) {
    eos::common::RWMutexReadLock vlock(FsView::gFsView.ViewMutex);
    eos::mgm::FileSystem* verifyfilesystem = 0;

    if (FsView::gFsView.mIdView.count(fsid)) {
      verifyfilesystem = FsView::gFsView.mIdView[fsid];
    }

    if (!verifyfilesystem) {
      errno = EINVAL;
      return Emsg(epname, error, ENOENT,
                  "verify stripe - filesystem does not exist",
                  fmd->getName().c_str());
    }

    // Prepare user tag data
    const char* usertag = 0;
    bool has_usertag = attrmap.count("user.tag");
    if (has_usertag) {
      usertag = attrmap["user.tag"].c_str();
    }

    // Build capability string
    XrdOucString capability =
        constructVerifyCapability(lid, cid, path, fid,
                                  verifyfilesystem->GetPath().c_str(),
                                  verifyfilesystem->GetId(),
                                  has_usertag, usertag,
                                  (lpath.length() != 0), lpath.c_str(),
                                  option);

    XrdOucString receiver = verifyfilesystem->GetQueue().c_str();
    XrdMqMessage message("verification");
    XrdOucString msgbody = "mgm.cmd=verify";
    msgbody += capability;
    message.SetBody(msgbody.c_str());

    if (!Messaging::gMessageClient.SendMessage(message, receiver.c_str())) {
      eos_static_err("unable to send verification message to %s", receiver.c_str());
      errno = ECOMM;
    } else {
      errno = 0;
    }
  }

  EXEC_TIMING_END("VerifyStripe");

  if (errno) {
    return Emsg(epname, error, errno, "verify stripe", path);
  }

  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_dropstripe(const char* path,
                       eos::common::FileId::fileid_t fid,
                       XrdOucErrInfo& error,
                       eos::common::Mapping::VirtualIdentity& vid,
                       unsigned long fsid,
                       bool forceRemove)
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
{
  static const char* epname = "dropstripe";
  std::shared_ptr<eos::IContainerMD> dh;
  std::shared_ptr<eos::IFileMD> fmd;
  errno = 0;
  EXEC_TIMING_BEGIN("DropStripe");
  gOFS->MgmStats.Add("DropStripe", vid.uid, vid.gid, 1);
  eos_debug("drop");
  eos::common::Path cPath(path);
  // ---------------------------------------------------------------------------
  eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);

  try {
    dh = gOFS->eosView->getContainer(cPath.GetParentPath());
    dh = gOFS->eosView->getContainer(gOFS->eosView->getUri(dh.get()));
  } catch (eos::MDException& e) {
    dh.reset();
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),
              e.getMessage().str().c_str());
  }

  // Check permissions
  if (dh && (!dh->access(vid.uid, vid.gid, X_OK | W_OK))) {
    if (!errno) {
      errno = EPERM;
    }
  } else {
    // only root can drop by file id
    if (vid.uid) {
      errno = EPERM;
    }
  }

  if (errno) {
    return Emsg(epname, error, errno, "drop stripe", path);
  }

  // get the file
  try {
    if (fid) {
      fmd = gOFS->eosFileService->getFileMD(fid);
    } else {
      fmd = gOFS->eosView->getFile(path);
    }

    if (!forceRemove) {
      // we only unlink a location
      if (fmd->hasLocation(fsid)) {
        fmd->unlinkLocation(fsid);
        gOFS->eosView->updateFileStore(fmd.get());
        eos_debug("unlinking location %u", fsid);
      } else {
        errno = ENOENT;
      }
    } else {
      // we unlink and remove a location by force
      if (fmd->hasLocation(fsid)) {
        fmd->unlinkLocation(fsid);
      }

      fmd->removeLocation(fsid);
      eos::common::FileFsPath::RemovePhysicalPath(fsid, fmd);
      gOFS->eosView->updateFileStore(fmd.get());
      eos_debug("removing/unlinking location %u", fsid);
    }
  } catch (eos::MDException& e) {
    fmd.reset();
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
              e.getErrno(), e.getMessage().str().c_str());
  }

  EXEC_TIMING_END("DropStripe");

  if (errno) {
    return Emsg(epname, error, errno, "drop stripe", path);
  }

  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_dropallstripes(const char* path,
                           XrdOucErrInfo& error,
                           eos::common::Mapping::VirtualIdentity& vid,
                           bool forceRemove)
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

    try {
      dh = gOFS->eosView->getContainer(parentPath);
      dh = gOFS->eosView->getContainer(gOFS->eosView->getUri(dh.get()));
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

    if (errno) {
      return Emsg(epname, error, errno, "drop all stripes", path);
    }

    try {
      fmd = gOFS->eosView->getFile(path);

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

  try {
    // only write lock at this point
    eos::common::RWMutexWriteLock wlock(gOFS->eosViewRWMutex);

    for (auto location : fmd->getLocations()) {
      if (location == eos::common::TAPE_FS_ID) {
        continue;
      }

      if (!forceRemove) {
        // we only unlink a location
        fmd->unlinkLocation(location);
        eos_debug("unlinking location %u", location);
      } else {
        // we unlink and remove a location by force
        if (fmd->hasLocation(location)) {
          fmd->unlinkLocation(location);
        }

        fmd->removeLocation(location);
        eos::common::FileFsPath::RemovePhysicalPath(location, fmd);
        eos_debug("removing/unlinking location %u", location);
      }
    }

    // update the file store only once at the end
    gOFS->eosView->updateFileStore(fmd.get());
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

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_movestripe(const char* path,
                       XrdOucErrInfo& error,
                       eos::common::Mapping::VirtualIdentity& vid,
                       unsigned long sourcefsid,
                       unsigned long targetfsid,
                       bool expressflag)
/*----------------------------------------------------------------------------*/
/*
 * @brief send a move message for a given file from source to target file system
 *
 * @param path file name to move stripe
 * @param error error object
 * @param vid virtual identity of the client
 * @param sourcefsid filesystem id of the source
 * @param targetfsid filesystem id of the target
 * @param expressflag if true the move is put in front of the queue on the FST
 *
 * @return SFS_OK if success otherwise SFS_ERROR
 *
 * The function requires POSIX W_OK & X_OK on the parent directory to succeed.
 * It calls _replicatestripe internally.
 */
/*----------------------------------------------------------------------------*/
{
  EXEC_TIMING_BEGIN("MoveStripe");
  int retc = _replicatestripe(path, error, vid, sourcefsid, targetfsid, true,
                              expressflag);
  EXEC_TIMING_END("MoveStripe");
  return retc;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_copystripe(const char* path,
                       XrdOucErrInfo& error,
                       eos::common::Mapping::VirtualIdentity& vid,
                       unsigned long sourcefsid,
                       unsigned long targetfsid,
                       bool expressflag)
/*----------------------------------------------------------------------------*/
/*
 * @brief send a copy message for a given file from source to target file system
 *
 * @param path file name to copy stripe
 * @param error error object
 * @param vid virtual identity of the client
 * @param sourcefsid filesystem id of the source
 * @param targetfsid filesystem id of the target
 * @param expressflag if true the copy is put in front of the queue on the FST
 *
 * @return SFS_OK if success otherwise SFS_ERROR
 *
 * The function requires POSIX W_OK & X_OK on the parent directory to succeed.
 * It calls _replicatestripe internally.
 */
/*----------------------------------------------------------------------------*/
{
  EXEC_TIMING_BEGIN("CopyStripe");
  int retc = _replicatestripe(path, error, vid, sourcefsid, targetfsid, false,
                              expressflag);
  EXEC_TIMING_END("CopyStripe");
  return retc;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_replicatestripe(const char* path,
                            XrdOucErrInfo& error,
                            eos::common::Mapping::VirtualIdentity& vid,
                            unsigned long sourcefsid,
                            unsigned long targetfsid,
                            bool dropsource,
                            bool expressflag)
/*----------------------------------------------------------------------------*/
/*
 * @brief send a replication message for a given file from source to target file system
 *
 * @param path file name to copy stripe
 * @param error error object
 * @param vid virtual identity of the client
 * @param sourcefsid filesystem id of the source
 * @param targetfsid filesystem id of the target
 * @param dropsource indicates if the source is deleted(dropped) after successful replication
 * @param expressflag if true the copy is put in front of the queue on the FST
 *
 * @return SFS_OK if success otherwise SFS_ERROR
 *
 * The function requires POSIX W_OK & X_OK on the parent directory to succeed.
 * It calls _replicatestripe with a file meta data object.
 * The call needs to have   eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
 */
/*----------------------------------------------------------------------------*/
{
  static const char* epname = "replicatestripe";
  std::shared_ptr<eos::IContainerMD> dh;
  errno = 0;
  EXEC_TIMING_BEGIN("ReplicateStripe");
  eos::common::Path cPath(path);
  eos_debug("replicating %s from %u=>%u [drop=%d]", path, sourcefsid, targetfsid,
            dropsource);
  // ---------------------------------------------------------------------------
  eos::common::RWMutexReadLock viewReadLock(gOFS->eosViewRWMutex);

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

  std::shared_ptr<eos::IFileMD> fmd;

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

  if (errno) {
    // -------------------------------------------------------------------------
    return Emsg(epname, error, errno, "replicate stripe", path);
  }

  // ---------------------------------------------------------------------------
  viewReadLock.Release();
  int retc = _replicatestripe(fmd, path, error, vid, sourcefsid,
                              targetfsid, dropsource, expressflag);
  EXEC_TIMING_END("ReplicateStripe");
  return retc;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_replicatestripe(const std::shared_ptr<eos::IFileMD> &fmd,
                            const char* path,
                            XrdOucErrInfo& error,
                            eos::common::Mapping::VirtualIdentity& vid,
                            unsigned long sourcefsid,
                            unsigned long targetfsid,
                            bool dropsource,
                            bool expressflag)
/*----------------------------------------------------------------------------*/
/*
 * @brief send a replication message for a given file from source to target file system
 *
 * @param fmd namespace file meta data object
 * @param path file name
 * @param error error object
 * @param vid virtual identity of the client
 * @param sourcefsid filesystem id of the source
 * @param targetfsid filesystem id of the target
 * @param dropsource indicates if the source is deleted(dropped) after successful replication
 * @param expressflag if true the copy is put in front of the queue on the FST
 *
 * @return SFS_OK if success otherwise SFS_ERROR
 *
 * The function sends an appropriate message to the target FST.
 * The call needs to have   eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
 */
/*----------------------------------------------------------------------------*/
{
  using eos::common::LayoutId;
  static const char* epname = "replicatestripe";
  unsigned long long fid = fmd->getId();
  unsigned long long cid = fmd->getContainerId();
  long unsigned int lid = fmd->getLayoutId();
  unsigned long src_lid = LayoutId::SetLayoutType(lid, LayoutId::kPlain);
  unsigned long dst_lid = LayoutId::SetLayoutType(lid, LayoutId::kPlain);

  // Mask block checksum (set to kNone) for replica layouts
  if (LayoutId::GetLayoutType(lid) == LayoutId::kReplica) {
    dst_lid = LayoutId::SetBlockChecksum(dst_lid, LayoutId::kNone);
  }

  uid_t uid = fmd->getCUid();
  gid_t gid = fmd->getCGid();
  unsigned long long size = fmd->getSize();

  if (dropsource) {
    gOFS->MgmStats.Add("MoveStripe", vid.uid, vid.gid, 1);
  } else {
    gOFS->MgmStats.Add("CopyStripe", vid.uid, vid.gid, 1);
  }

  if ((!sourcefsid) || (!targetfsid)) {
    eos_err("illegal fsid sourcefsid=%u targetfsid=%u", sourcefsid, targetfsid);
    return Emsg(epname, error, EINVAL,
                "illegal source/target fsid", fmd->getName().c_str());
  }

  eos::mgm::FileSystem* sourcefilesystem = 0;
  eos::mgm::FileSystem* targetfilesystem = 0;

  if (FsView::gFsView.mIdView.count(sourcefsid)) {
    sourcefilesystem = FsView::gFsView.mIdView[sourcefsid];
  }

  if (FsView::gFsView.mIdView.count(targetfsid)) {
    targetfilesystem = FsView::gFsView.mIdView[targetfsid];
  }

  if (!sourcefilesystem) {
    errno = EINVAL;
    return Emsg(epname, error, ENOENT,
                "replicate stripe - source filesystem does not exist",
                fmd->getName().c_str());
  }

  if (!targetfilesystem) {
    errno = EINVAL;
    return Emsg(epname, error, ENOENT,
                "replicate stripe - target filesystem does not exist",
                fmd->getName().c_str());
  }

  // Snapshot the filesystems
  eos::common::FileSystem::fs_snapshot source_snapshot;
  eos::common::FileSystem::fs_snapshot target_snapshot;
  sourcefilesystem->SnapShotFileSystem(source_snapshot);
  targetfilesystem->SnapShotFileSystem(target_snapshot);

  // Check if source filesystem uses logical path
  XrdOucString source_lpath = "";
  bool source_uselpath = fmd->hasAttribute("sys.eos.lpath");

  if (source_uselpath) {
    std::shared_ptr<eos::IFileMD> fmdPtr(fmd);
    eos::common::FileFsPath::GetPhysicalPath(source_snapshot.mId, fmdPtr,
                                             source_lpath);
  }

  // Check if target filesystem uses logical path setting
  bool target_uselpath = (target_snapshot.mLogicalPath == "1");

  if (target_uselpath) {
    eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);
    eos::common::FileFsPath::StorePhysicalPath(target_snapshot.mId, fmd, path);
    gOFS->eosView->updateFileStore(fmd.get());
  }

  // Construct capability strings
  XrdOucString source_capability =
      constructSourceCapability(src_lid, cid, path, fid,
                                source_snapshot.mPath.c_str(),
                                source_snapshot.mId,
                                source_snapshot.mHostPort.c_str(),
                                dropsource, source_snapshot.mId,
                                source_uselpath, source_lpath.c_str());

  XrdOucString target_capability =
      constructTargetCapability(dst_lid, cid, path, fid,
                                target_snapshot.mPath.c_str(),
                                target_snapshot.mId,
                                target_snapshot.mHostPort.c_str(),
                                dropsource, source_snapshot.mId,
                                target_uselpath, path, size, lid, uid, gid);

  // Issue full capability string
  XrdOucErrInfo capError;
  XrdOucString full_capability = "";
  int rc = issueFullCapability(source_capability, target_capability,
                               mCapabilityValidity,
                               source_snapshot.mHostPort.c_str(),
                               target_snapshot.mHostPort.c_str(),
                               fid, full_capability, capError);

  if (rc) {
    std::ostringstream errstream;
    errstream << "create " << capError.getErrText() << " capability [EADV]";

    eos_err("unable to create %s capability - ec=%d",
            capError.getErrText(), capError.getErrInfo());
    return Emsg(epname, error, rc, errstream.str().c_str());
  }

  // Schedule file transfer
  std::unique_ptr<eos::common::TransferJob>
      txjob(new eos::common::TransferJob(full_capability.c_str()));

  if (targetfilesystem->GetExternQueue()->Add(txjob.get())) {
    eos_info("info=\"submitted transfer job\" fxid=%llx job=%s",
             fid, full_capability.c_str());
  } else {
    eos_err("msg=\"failed to submit transfer job\" fxid=%llx job=%s",
            fid, full_capability.c_str());
    return Emsg(epname, error, ENXIO, "replicate stripe - failed to submit job",
               fmd->getName().c_str());
  }

  return SFS_OK;
}
