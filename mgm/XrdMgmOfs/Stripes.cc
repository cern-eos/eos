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
int
XrdMgmOfs::_verifystripe (const char *path,
                          XrdOucErrInfo &error,
                          eos::common::Mapping::VirtualIdentity &vid,
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
  static const char *epname = "verifystripe";
  eos::ContainerMD *dh = 0;
  eos::FileMD *fmd = 0;

  EXEC_TIMING_BEGIN("VerifyStripe");

  errno = 0;
  unsigned long long fid = 0;
  unsigned long long cid = 0;
  int lid = 0;

  eos::ContainerMD::XAttrMap attrmap;

  gOFS->MgmStats.Add("VerifyStripe", vid.uid, vid.gid, 1);

  eos_debug("verify");
  eos::common::Path cPath(path);
  std::string attr_path;
  // ---------------------------------------------------------------------------
  eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
  try
  {
    dh = gOFS->eosView->getContainer(cPath.GetParentPath());
    attr_path = gOFS->eosView->getUri(dh);
    dh = gOFS->eosView->getContainer(gOFS->eosView->getUri(dh));

  }
  catch (eos::MDException &e)
  {
    dh = 0;
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(), e.getMessage().str().c_str());
  }

  // check permissions
  if (dh && (!dh->access(vid.uid, vid.gid, X_OK | W_OK)))
    if (!errno) errno = EPERM;


  if (errno)
  {
    return Emsg(epname, error, errno, "verify stripe", path);
  }

  // get attributes                                                                                                                                                                             
  gOFS->_attr_ls(attr_path.c_str(),
		 error,
		 vid,
		 0,
		 attrmap,
		 false);


  // get the file
  try
  {
    fmd = gOFS->eosView->getFile(path);
    fid = fmd->getId();
    lid = fmd->getLayoutId();
    cid = fmd->getContainerId();
  }
  catch (eos::MDException &e)
  {
    fmd = 0;
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
              e.getErrno(), e.getMessage().str().c_str());
  }

  if (!errno)
  {
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
    eos::mgm::FileSystem* verifyfilesystem = 0;
    if (FsView::gFsView.mIdView.count(fsid))
    {
      verifyfilesystem = FsView::gFsView.mIdView[fsid];
    }
    if (!verifyfilesystem)
    {
      errno = EINVAL;
      return Emsg(epname, error, ENOENT,
                  "verify stripe - filesystem does not exist",
                  fmd->getName().c_str());
    }

    XrdOucString receiver = verifyfilesystem->GetQueue().c_str();
    XrdOucString opaquestring = "";
    // build the opaquestring contents
    opaquestring += "&mgm.localprefix=";
    opaquestring += verifyfilesystem->GetPath().c_str();
    opaquestring += "&mgm.fid=";
    XrdOucString hexfid;
    eos::common::FileId::Fid2Hex(fid, hexfid);
    opaquestring += hexfid;
    opaquestring += "&mgm.manager=";
    opaquestring += gOFS->ManagerId.c_str();
    opaquestring += "&mgm.access=verify";
    opaquestring += "&mgm.fsid=";
    opaquestring += (int) verifyfilesystem->GetId();
    if (attrmap.count("user.tag"))
    {
      opaquestring += "&mgm.container=";
      opaquestring += attrmap["user.tag"].c_str();
    }
    XrdOucString sizestring = "";
    opaquestring += "&mgm.cid=";
    opaquestring += eos::common::StringConversion::GetSizeString(sizestring, cid);
    opaquestring += "&mgm.path=";
    opaquestring += path;
    opaquestring += "&mgm.lid=";
    opaquestring += lid;

    if (option.length())
    {
      opaquestring += option;
    }

    XrdMqMessage message("verifycation");
    XrdOucString msgbody = "mgm.cmd=verify";

    msgbody += opaquestring;

    message.SetBody(msgbody.c_str());

    if (!Messaging::gMessageClient.SendMessage(message, receiver.c_str()))
    {
      eos_static_err("unable to send verification message to %s", receiver.c_str());
      errno = ECOMM;
    }
    else
    {
      errno = 0;
    }
  }

  EXEC_TIMING_END("VerifyStripe");

  if (errno)
    return Emsg(epname, error, errno, "verify stripe", path);

  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_dropstripe (const char *path,
                        XrdOucErrInfo &error,
                        eos::common::Mapping::VirtualIdentity &vid,
                        unsigned long fsid,
                        bool forceRemove)
/*----------------------------------------------------------------------------*/
/*
 * @brief send a drop message to a file system for a given file
 *
 * @param path file name to drop stripe
 * @param error error object
 * @param vid virtual identity of the client
 * @param fsid filesystem id where to run the drop
 * @param forceRemove if true the stripe is immediatly dropped
 *
 * @return SFS_OK if success otherwise SFS_ERROR
 *
 * The function requires POSIX W_OK & X_OK on the parent directory to succeed.
 */
/*----------------------------------------------------------------------------*/
{
  static const char *epname = "dropstripe";
  eos::ContainerMD *dh = 0;
  eos::FileMD *fmd = 0;
  errno = 0;

  EXEC_TIMING_BEGIN("DropStripe");

  gOFS->MgmStats.Add("DropStripe", vid.uid, vid.gid, 1);

  eos_debug("drop");
  eos::common::Path cPath(path);
  // ---------------------------------------------------------------------------
  eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);
  try
  {
    dh = gOFS->eosView->getContainer(cPath.GetParentPath());
    dh = gOFS->eosView->getContainer(gOFS->eosView->getUri(dh));
  }
  catch (eos::MDException &e)
  {
    dh = 0;
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(), e.getMessage().str().c_str());
  }

  // check permissions
  if (dh && (!dh->access(vid.uid, vid.gid, X_OK | W_OK)))
    if (!errno) errno = EPERM;

  if (errno)
  {
    return Emsg(epname, error, errno, "drop stripe", path);
  }

  // get the file
  try
  {
    fmd = gOFS->eosView->getFile(path);
    if (!forceRemove)
    {
      // we only unlink a location
      if (fmd->hasLocation(fsid))
      {
        fmd->unlinkLocation(fsid);
        gOFS->eosView->updateFileStore(fmd);
        eos_debug("unlinking location %u", fsid);
      }
      else
      {
        errno = ENOENT;
      }
    }
    else
    {
      // we unlink and remove a location by force
      if (fmd->hasLocation(fsid))
      {
        fmd->unlinkLocation(fsid);
      }
      fmd->removeLocation(fsid);
      gOFS->eosView->updateFileStore(fmd);
      eos_debug("removing/unlinking location %u", fsid);
    }
  }
  catch (eos::MDException &e)
  {
    fmd = 0;
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
              e.getErrno(), e.getMessage().str().c_str());
  }

  EXEC_TIMING_END("DropStripe");

  if (errno)
    return Emsg(epname, error, errno, "drop stripe", path);

  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_movestripe (const char *path,
                        XrdOucErrInfo &error,
                        eos::common::Mapping::VirtualIdentity &vid,
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
  int retc = _replicatestripe(path, error, vid, sourcefsid, targetfsid, true, expressflag);
  EXEC_TIMING_END("MoveStripe");
  return retc;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_copystripe (const char *path,
                        XrdOucErrInfo &error,
                        eos::common::Mapping::VirtualIdentity &vid,
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
  int retc = _replicatestripe(path, error, vid, sourcefsid, targetfsid, false, expressflag);
  EXEC_TIMING_END("CopyStripe");
  return retc;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_replicatestripe (const char *path,
                             XrdOucErrInfo &error,
                             eos::common::Mapping::VirtualIdentity &vid,
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
 * @param dropsource indicates if the source is deleted(dropped) after successfull replication
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
  static const char *epname = "replicatestripe";
  eos::ContainerMD *dh = 0;
  errno = 0;

  EXEC_TIMING_BEGIN("ReplicateStripe");

  eos::common::Path cPath(path);

  eos_debug("replicating %s from %u=>%u [drop=%d]", path, sourcefsid, targetfsid, dropsource);

  // ---------------------------------------------------------------------------
  gOFS->eosViewRWMutex.LockRead();
  try
  {
    dh = gOFS->eosView->getContainer(cPath.GetParentPath());
    dh = gOFS->eosView->getContainer(gOFS->eosView->getUri(dh));
  }
  catch (eos::MDException &e)
  {
    dh = 0;
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(), e.getMessage().str().c_str());
  }

  // check permissions
  if (dh && (!dh->access(vid.uid, vid.gid, X_OK | W_OK)))
    if (!errno) errno = EPERM;

  eos::FileMD * fmd = 0;

  // get the file
  try
  {
    fmd = gOFS->eosView->getFile(path);
    if (fmd->hasLocation(sourcefsid))
    {
      if (fmd->hasLocation(targetfsid))
      {
        errno = EEXIST;
      }
    }
    else
    {
      // this replica does not exist!
      errno = ENODATA;
    }
  }
  catch (eos::MDException &e)
  {
    fmd = 0;
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(), e.getMessage().str().c_str());
  }

  if (errno)
  {
    // -------------------------------------------------------------------------
    gOFS->eosViewRWMutex.UnLockRead();
    return Emsg(epname, error, errno, "replicate stripe", path);
  }

  // make a copy of the file meta data to release the lock
  eos::FileMD fmdCopy(*fmd);
  fmd = &fmdCopy;

  // ------------------------------------------

  gOFS->eosViewRWMutex.UnLockRead();
  int retc = _replicatestripe(fmd, path, error, vid, sourcefsid, targetfsid, dropsource, expressflag);

  EXEC_TIMING_END("ReplicateStripe");

  return retc;

}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfs::_replicatestripe (eos::FileMD *fmd,
                             const char* path,
                             XrdOucErrInfo &error,
                             eos::common::Mapping::VirtualIdentity &vid,
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
 * @param dropsource indicates if the source is deleted(dropped) after successfull replication
 * @param expressflag if true the copy is put in front of the queue on the FST
 *
 * @return SFS_OK if success otherwise SFS_ERROR
 *
 * The function sends an appropriate message to the target FST.
 * The call needs to have   eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex); 
 */
/*----------------------------------------------------------------------------*/
{
  static const char *epname = "replicatestripe";
  unsigned long long fid = fmd->getId();
  unsigned long long cid = fmd->getContainerId();
  long unsigned int lid = fmd->getLayoutId();
  uid_t uid = fmd->getCUid();
  gid_t gid = fmd->getCGid();

  unsigned long long size = fmd->getSize();

  if (dropsource)
    gOFS->MgmStats.Add("MoveStripe", vid.uid, vid.gid, 1);
  else
    gOFS->MgmStats.Add("CopyStripe", vid.uid, vid.gid, 1);

  if ((!sourcefsid) || (!targetfsid))
  {
    eos_err("illegal fsid sourcefsid=%u targetfsid=%u", sourcefsid, targetfsid);
    return Emsg(epname, error, EINVAL,
                "illegal source/target fsid", fmd->getName().c_str());
  }

  eos::mgm::FileSystem* sourcefilesystem = 0;
  eos::mgm::FileSystem* targetfilesystem = 0;


  if (FsView::gFsView.mIdView.count(sourcefsid))
  {
    sourcefilesystem = FsView::gFsView.mIdView[sourcefsid];
  }

  if (FsView::gFsView.mIdView.count(targetfsid))
  {
    targetfilesystem = FsView::gFsView.mIdView[targetfsid];
  }

  if (!sourcefilesystem)
  {
    errno = EINVAL;
    return Emsg(epname, error, ENOENT,
                "replicate stripe - source filesystem does not exist",
                fmd->getName().c_str());
  }

  if (!targetfilesystem)
  {
    errno = EINVAL;
    return Emsg(epname, error, ENOENT,
                "replicate stripe - target filesystem does not exist",
                fmd->getName().c_str());
  }

  // snapshot the filesystems
  eos::common::FileSystem::fs_snapshot source_snapshot;
  eos::common::FileSystem::fs_snapshot target_snapshot;
  sourcefilesystem->SnapShotFileSystem(source_snapshot);
  targetfilesystem->SnapShotFileSystem(target_snapshot);

  // build a transfer capability
  XrdOucString source_capability = "";
  XrdOucString sizestring;
  source_capability += "mgm.access=read";
  source_capability += "&mgm.lid=";
  source_capability += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) lid & 0xffffff0f);
  // make's it a plain replica
  source_capability += "&mgm.cid=";
  source_capability += eos::common::StringConversion::GetSizeString(sizestring, cid);
  source_capability += "&mgm.ruid=";
  source_capability += (int) 1;
  source_capability += "&mgm.rgid=";
  source_capability += (int) 1;
  source_capability += "&mgm.uid=";
  source_capability += (int) 1;
  source_capability += "&mgm.gid=";
  source_capability += (int) 1;
  source_capability += "&mgm.path=";
  source_capability += path;
  source_capability += "&mgm.manager=";
  source_capability += gOFS->ManagerId.c_str();
  source_capability += "&mgm.fid=";

  XrdOucString hexfid;
  eos::common::FileId::Fid2Hex(fid, hexfid);
  source_capability += hexfid;

  source_capability += "&mgm.sec=";
  source_capability += eos::common::SecEntity::ToKey(0, "eos/replication").c_str();


  // this is a move of a replica
  if (dropsource)
  {
    source_capability += "&mgm.drainfsid=";
    source_capability += (int) source_snapshot.mId;
  }

  // build the source_capability contents
  source_capability += "&mgm.localprefix=";
  source_capability += source_snapshot.mPath.c_str();
  source_capability += "&mgm.fsid=";
  source_capability += (int) source_snapshot.mId;
  source_capability += "&mgm.sourcehostport=";
  source_capability += source_snapshot.mHostPort.c_str();

  XrdOucString target_capability = "";
  target_capability += "mgm.access=write";
  target_capability += "&mgm.lid=";
  target_capability += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) lid & 0xffffff0f);
  // make's it a plain replica
  target_capability += "&mgm.cid=";
  target_capability += eos::common::StringConversion::GetSizeString(sizestring, cid);
  target_capability += "&mgm.ruid=";
  target_capability += (int) 1;
  target_capability += "&mgm.rgid=";
  target_capability += (int) 1;
  target_capability += "&mgm.uid=";
  target_capability += (int) 1;
  target_capability += "&mgm.gid=";
  target_capability += (int) 1;
  target_capability += "&mgm.path=";
  target_capability += path;
  target_capability += "&mgm.manager=";
  target_capability += gOFS->ManagerId.c_str();
  target_capability += "&mgm.fid=";
  target_capability += hexfid;

  target_capability += "&mgm.sec=";
  target_capability += eos::common::SecEntity::ToKey(0, "eos/replication").c_str();
  if (dropsource)
  {
    target_capability += "&mgm.drainfsid=";
    target_capability += (int) source_snapshot.mId;
  }

  target_capability += "&mgm.source.lid=";
  target_capability += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) lid);
  target_capability += "&mgm.source.ruid=";
  target_capability += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) uid);
  target_capability += "&mgm.source.rgid=";
  target_capability += eos::common::StringConversion::GetSizeString(sizestring, (unsigned long long) gid);

  // build the target_capability contents
  target_capability += "&mgm.localprefix=";
  target_capability += target_snapshot.mPath.c_str();
  target_capability += "&mgm.fsid=";
  target_capability += (int) target_snapshot.mId;
  target_capability += "&mgm.targethostport=";
  target_capability += target_snapshot.mHostPort.c_str();
  target_capability += "&mgm.bookingsize=";
  target_capability += eos::common::StringConversion::GetSizeString(sizestring, size);
  // issue a source_capability
  XrdOucEnv insource_capability(source_capability.c_str());
  XrdOucEnv intarget_capability(target_capability.c_str());
  XrdOucEnv* source_capabilityenv = 0;
  XrdOucEnv* target_capabilityenv = 0;
  XrdOucString fullcapability = "";
  eos::common::SymKey* symkey = eos::common::gSymKeyStore.GetCurrentKey();

  int caprc = 0;
  if ((caprc = gCapabilityEngine.Create(&insource_capability, source_capabilityenv, symkey)) ||
      (caprc = gCapabilityEngine.Create(&intarget_capability, target_capabilityenv, symkey)))
  {
    eos_err("unable to create source/target capability - errno=%u", caprc);
    errno = caprc;
  }
  else
  {
    errno = 0;
    int caplen = 0;
    XrdOucString source_cap = source_capabilityenv->Env(caplen);
    XrdOucString target_cap = target_capabilityenv->Env(caplen);
    source_cap.replace("cap.sym", "source.cap.sym");
    target_cap.replace("cap.sym", "target.cap.sym");
    source_cap.replace("cap.msg", "source.cap.msg");
    target_cap.replace("cap.msg", "target.cap.msg");
    source_cap += "&source.url=root://";
    source_cap += source_snapshot.mHostPort.c_str();
    source_cap += "//replicate:";
    source_cap += hexfid;
    target_cap += "&target.url=root://";
    target_cap += target_snapshot.mHostPort.c_str();
    target_cap += "//replicate:";
    target_cap += hexfid;
    fullcapability += source_cap;
    fullcapability += target_cap;

    eos::common::TransferJob* txjob = new eos::common::TransferJob(fullcapability.c_str());

    bool sub = targetfilesystem->GetExternQueue()->Add(txjob);
    eos_info("info=\"submitted transfer job\" subretc=%d fxid=%s fid=%llu cap=%s\n",
             sub, hexfid.c_str(), fid, fullcapability.c_str());

    if (!sub)
      errno = ENXIO;
    else
      errno = 0;

    if (txjob)
      delete txjob;
    else
    {
      eos_err("Couldn't create transfer job to replicate stripe of %s", path);
      errno = ENOMEM;
    }

    if (source_capabilityenv)
      delete source_capabilityenv;
    if (target_capabilityenv)
      delete target_capabilityenv;
  }

  if (errno)
    return Emsg(epname, error, errno, "replicate stripe", fmd->getName().c_str());

  return SFS_OK;
}

