// ----------------------------------------------------------------------
// File: XrdMgmOfs.cc
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

#include "common/Mapping.hh"
#include "common/FileId.hh"
#include "common/LayoutId.hh"
#include "common/Path.hh"
#include "common/SecEntity.hh"
#include "common/StackTrace.hh"
#include "common/ParseUtils.hh"
#include "common/StringTokenizer.hh"
#include "common/Strerror_r_wrapper.hh"
#include "common/BehaviourConfig.hh"
#include "mgm/Access.hh"
#include "mgm/FileSystem.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/XrdMgmOfsFile.hh"
#include "mgm/XrdMgmOfsSecurity.hh"
#include "mgm/Stat.hh"
#include "mgm/Policy.hh"
#include "mgm/Quota.hh"
#include "mgm/Acl.hh"
#include "mgm/Workflow.hh"
#include "mgm/proc/ProcInterface.hh"
#include "mgm/tracker/ReplicationTracker.hh"
#include "mgm/Recycle.hh"
#include "mgm/Macros.hh"
#include "mgm/ZMQ.hh"
#include "mgm/tgc/MultiSpaceTapeGc.hh"
#include "mgm/utils/AttrHelper.hh"
#include "mgm/XattrLock.hh"
#include "mgm/placement/FsScheduler.hh"
#include "namespace/utils/Attributes.hh"
#include "namespace/Prefetcher.hh"
#include "namespace/Resolver.hh"
#include "XrdOss/XrdOss.hh"
#include "XrdSec/XrdSecInterface.hh"
#include "XrdSec/XrdSecEntityAttr.hh"
#include "XrdSfs/XrdSfsAio.hh"
#include "common/Constants.hh"
#include "XrdOuc/XrdOucPgrwUtils.hh"

#ifdef __APPLE__
#define ECOMM 70
#endif

#ifndef S_IAMB
#define S_IAMB  0x1FF
#endif

namespace
{
//----------------------------------------------------------------------------
//! Thrown if a disk location could not be found
//----------------------------------------------------------------------------
struct DiskLocationNotFound: public std::runtime_error {
  using std::runtime_error::runtime_error;
};

//----------------------------------------------------------------------------
//! @param locations locations to be searched
//! @return first location that is a disk as opposed to tape
//! @throw DiskLocationNotFound if a disk location could not be found
//----------------------------------------------------------------------------
eos::IFileMD::location_t
getFirstDiskLocation(const eos::IFileMD::LocationVector& locations)
{
  if (locations.empty()) {
    throw DiskLocationNotFound("Failed to find disk location");
  }

  if (EOS_TAPE_FSID != locations.at(0)) {
    return locations.at(0);
  }

  if (2 > locations.size()) {
    throw DiskLocationNotFound("Failed to find disk location");
  }

  return locations.at(1);
}

//----------------------------------------------------------------------------
//! Enforce the RainMinFsidEntry behaviour by returning the index in the
//! given input vector corresponding to the smallest fsid.
//!
//! @param input_fsids vector for fsids
//!
//! @return index pointing to the smallest fsid
//----------------------------------------------------------------------------
size_t
EnforceRainMinFsidEntry(const std::vector<unsigned int>& input_fsids)
{
  unsigned int min_fsid = UINT_MAX;
  size_t index = 0;

  for (int i = 0; i < input_fsids.size(); ++i) {
    if (input_fsids[i] < min_fsid) {
      index = i;
      min_fsid = input_fsids[i];
    }
  }

  return index;
}
}


/******************************************************************************/
/* MGM File Interface                                                         */
/******************************************************************************/

/* copied for "eos_static_..." */
static int
emsg(XrdOucErrInfo& error, int ec, const char* txt, const char* txt2)
{
  // Get the reason for the error
  if (ec < 0) {
    ec = -ec;
  }

  char* etext = strerror(ec);
  char sbuff[1024];
  char ebuff[64];

  if (etext == NULL) {
    etext = ebuff;
    snprintf(ebuff, sizeof(ebuff), "error code %d", ec);
  }

  snprintf(sbuff, sizeof(sbuff), "create_cow: unable to %s %s: %s", txt, txt2,
           etext);
  eos_static_err(sbuff);
  error.setErrInfo(ec, sbuff);
  return SFS_ERROR;
}

/*
 * Auxiliary routine: creates the copy-on-write clone an intermediate directories
 * cow_type:
 *      0 = copy                (for file updates, two files exist)
 *      1 = rename              (for a "deletes", clone's contents survive under different name)
 *      2 = hardlink            (file untouched but a new name is created, e.g. for recycle)
 *
 * returns:
 *      - error code if the clone could not be created
 *      - -1 if the file is not to be cloned
 */

int
XrdMgmOfsFile::create_cow(int cowType,
                          std::shared_ptr<eos::IContainerMD> dmd, std::shared_ptr<eos::IFileMD> fmd,
                          eos::common::VirtualIdentity& vid, XrdOucErrInfo& error)
{
  char sbuff[1024];
  uint64_t cloneId = fmd->getCloneId();

  if (cloneId == 0 or not fmd->getCloneFST().empty()) {
    return -1;
  }

  eos_static_info("Creating cow clone (type %d) for %s fxid:%lx cloneId %lld",
                  cowType, fmd->getName().c_str(), fmd->getId(), cloneId);
  snprintf(sbuff, sizeof(sbuff), "%s/clone/%ld", gOFS->MgmProcPath.c_str(),
           cloneId);
  std::shared_ptr<eos::IContainerMD> cloneMd, dirMd;

  try {
    cloneMd = gOFS->eosView->getContainer(sbuff);
  } catch (eos::MDException& e) {
    eos_static_debug("caught exception %d %s path %s\n", e.getErrno(),
                     e.getMessage().str().c_str(), sbuff);
    return emsg(error, ENOENT /*EEXIST*/, "open file ()", sbuff);
  }

  if (!dmd) {
    return emsg(error, ENOENT, "determine parent", fmd->getName().c_str());
  }

  /* set up directory for clone */
  int tlen = strlen(sbuff);
  snprintf(sbuff + tlen, sizeof(sbuff) - tlen, "/%lx", dmd->getId());

  try {
    dirMd = gOFS->eosView->getContainer(sbuff);
  } catch (eos::MDException& e) {
    dirMd = gOFS->eosView->createContainer(sbuff, true);
    dirMd->setMode(dmd->getMode());
    eos::IFileMD::XAttrMap xattrs = dmd->getAttributes();

    for (const auto& a : xattrs) {
      if (a.first == "sys.acl" || a.first == "user.acl" ||
          a.first == "sys.eval.useracl") {
        dirMd->setAttribute(a.first, a.second);
      }
    }
  }

  /* create the clone */
  if (cowType ==
      XrdMgmOfsFile::cowDelete) {                   /* basically a "mv" */
    dmd->removeFile(fmd->getName());
    snprintf(sbuff, sizeof(sbuff), "%lx", fmd->getId());
    fmd->setName(sbuff);
    fmd->setCloneId(0); /* don't ever cow this again! */
    dirMd->addFile(fmd.get());
    gOFS->eosFileService->updateStore(fmd.get());
  } else {                              /* cowType == cowUpdate or cowType == cowUnlink */
    std::shared_ptr<eos::IFileMD> gmd;
    eos::IFileMD::ctime_t ttime;
    tlen = strlen(sbuff);
    snprintf(sbuff + tlen, sizeof(sbuff) - tlen, "/%lx", fmd->getId());
    gmd = gOFS->eosView->createFile(sbuff, vid.uid, vid.gid);
    gmd->setAttribute("sys.clone.targetFid", sbuff + tlen + 1);
    gmd->setSize(fmd->getSize());

    if (cowType ==
        XrdMgmOfsFile::cowUpdate) {  /* prepare a "cp --reflink" (to be performed on the FSTs) */
      fmd->getCTime(ttime);
      gmd->setCTime(ttime);
      fmd->getMTime(ttime);
      gmd->setMTime(ttime);
      gmd->setCUid(fmd->getCUid());
      gmd->setCGid(fmd->getCGid());
      gmd->setFlags(fmd->getFlags());
      gmd->setLayoutId(fmd->getLayoutId());
      gmd->setChecksum(fmd->getChecksum());
      gmd->setContainerId(dirMd->getId());

      for (unsigned int i = 0; i < fmd->getNumLocation(); i++) {
        gmd->addLocation(fmd->getLocation(i));
      }
    } else if (cowType == cowUnlink) {
      int nlink = (fmd->hasAttribute(XrdMgmOfsFile::k_nlink)) ?
                  std::stoi(fmd->getAttribute(XrdMgmOfsFile::k_nlink)) + 1 : 1;
      fmd->setAttribute(k_nlink, std::to_string(nlink));
      gOFS->eosFileService->updateStore(fmd.get());
      uint64_t hlTarget = eos::common::FileId::FidToInode(fmd->getId());
      gmd->setAttribute(XrdMgmOfsFile::k_mdino, std::to_string(hlTarget));
      eos_static_debug("create_cow Unlink %s (%ld) -> %s (%ld)",
                       gmd->getName().c_str(), gmd->getSize(),
                       fmd->getName().c_str(), fmd->getSize());
    }

    gOFS->eosFileService->updateStore(gmd.get());
    fmd->setCloneFST(eos::common::FileId::Fid2Hex(gmd->getId()));
    gOFS->eosFileService->updateStore(fmd.get());
  }

  gOFS->eosDirectoryService->updateStore(dirMd.get());
  gOFS->FuseXCastRefresh(dirMd->getIdentifier(), dirMd->getParentIdentifier());
  gOFS->FuseXCastRefresh(cloneMd->getIdentifier(),
                         cloneMd->getParentIdentifier());
  return 0;
}

/*----------------------------------------------------------------------------*
 * special handling of hard links
 * returns:
 *      0 = continue deleting fmd
 *      1 = do nothing
 *
 *----------------------------------------------------------------------------*/
int
XrdMgmOfsFile::handleHardlinkDelete(std::shared_ptr<eos::IContainerMD> cmd,
                                    std::shared_ptr<eos::IFileMD> fmd,
                                    eos::common::VirtualIdentity& vid)
{
  if (!cmd) {
    return 0;
  }

  long nlink =
    -2;                /* assume this has nothing to do with hard links */

  if (fmd->hasAttribute(XrdMgmOfsFile::k_mdino)) {
    /* this is a hard link, decrease reference count on underlying file */
    uint64_t hlTgt = std::stoull(fmd->getAttribute(XrdMgmOfsFile::k_mdino));
    uint64_t clock;
    /* gmd = the hard link target */
    std::shared_ptr<eos::IFileMD> gmd = gOFS->eosFileService->getFileMD(
                                          eos::common::FileId::InodeToFid(hlTgt), &clock);
    nlink = std::stol(gmd->getAttribute(XrdMgmOfsFile::k_nlink)) - 1;

    if (nlink > 0) {
      gmd->setAttribute(XrdMgmOfsFile::k_nlink, std::to_string(nlink));
    } else {
      gmd->removeAttribute(XrdMgmOfsFile::k_nlink);
    }

    gOFS->eosFileService->updateStore(gmd.get());
    eos_static_info("hlnk update target %s for %s nlink %ld",
                    gmd->getName().c_str(), fmd->getName().c_str(), nlink);

    if (nlink <= 0) {
      if (gmd->getName().substr(0, 13) == "...eos.ino...") {
        eos_static_info("hlnk unlink target %s for %s nlink %ld",
                        gmd->getName().c_str(), fmd->getName().c_str(), nlink);
        uint64_t cloneId = gmd->getCloneId();

        if (cloneId != 0 and
            gmd->getCloneFST().empty()) {     /* this file needs to be cloned */
          XrdOucErrInfo error;
          std::shared_ptr<eos::IContainerMD> dmd;

          try {
            dmd = gOFS->eosDirectoryService->getContainerMD(gmd->getContainerId());
          } catch (eos::MDException& e) {
          }

          XrdMgmOfsFile::create_cow(XrdMgmOfsFile::cowDelete, dmd, gmd, vid, error);
          return 1;
        } else {                /* delete hard link target */
          cmd->removeFile(gmd->getName());
          gmd->unlinkAllLocations();
          gmd->setContainerId(0);
        }

        gOFS->eosFileService->updateStore(gmd.get());
      }
    }
  } else if (fmd->hasAttribute(
               XrdMgmOfsFile::k_nlink)) {      /* a hard link target */
    nlink = std::stol(fmd->getAttribute(XrdMgmOfsFile::k_nlink));
    eos_static_info("hlnk rm target nlink %ld", nlink);

    if (nlink > 0) {
      // hard links exist, just rename the file so the inode does not disappear
      char nameBuf[1024];
      uint64_t ino = eos::common::FileId::FidToInode(fmd->getId());
      snprintf(nameBuf, sizeof(nameBuf), "...eos.ino...%lx", ino);
      std::string nameBufs(nameBuf);
      fmd->setAttribute(XrdMgmOfsFile::k_nlink, std::to_string(nlink));
      eos_static_info("hlnk unlink rename %s=>%s new nlink %d",
                      fmd->getName().c_str(), nameBufs.c_str(), nlink);
      cmd->removeFile(nameBufs);            // if the target exists, remove it!
      gOFS->eosView->renameFile(fmd.get(), nameBufs);
      return 1;
    }

    /* no other links exist, continue deleting the target like a simple file */
  }

  eos_static_debug("hard link nlink %ld, delete %s", nlink,
                   fmd->getName().c_str());
  return 0;
}

//------------------------------------------------------------------------------
// Get the application name if specified
//------------------------------------------------------------------------------
const std::string
XrdMgmOfsFile::GetApplicationName(XrdOucEnv* open_opaque,
                                  const XrdSecEntity* client)
{
  // Application name derived from the following in order of priority:
  // 1. eos.app=<tag>
  // 2. XRD_APPNAME=<tag> env variable or -DSAppName for xrdcp commands
  const std::string eos_tag = "eos.app";
  const std::string xrd_tag = "xrd.appname";
  std::string app_name;
  const char* val = nullptr;

  if (open_opaque && (val = open_opaque->Get(eos_tag.c_str()))) {
    app_name = val;
  } else {
    if (client) {
      if (!client->eaAPI->Get(xrd_tag, app_name)) {
        app_name.clear();
      }
    }
  }

  return app_name;
}

//------------------------------------------------------------------------------
// Get POSIX open flags from the given XRootD open mode
//------------------------------------------------------------------------------
int
XrdMgmOfsFile::GetPosixOpenFlags(XrdSfsFileOpenMode open_mode)
{
  int open_flags = 0;

  if (open_mode & SFS_O_CREAT) {
    open_mode = SFS_O_CREAT;
  } else if (open_mode & SFS_O_TRUNC) {
    open_mode = SFS_O_TRUNC;
  }

  switch (open_mode & (SFS_O_RDONLY | SFS_O_WRONLY | SFS_O_RDWR |
                       SFS_O_CREAT | SFS_O_TRUNC)) {
  case SFS_O_CREAT:
    open_flags = O_CREAT | O_EXCL | O_RDWR;
    break;

  case SFS_O_TRUNC:
    open_flags = O_CREAT | O_TRUNC | O_RDWR;
    break;

  case SFS_O_RDONLY:
    open_flags = O_RDONLY;
    break;

  case SFS_O_WRONLY:
    open_flags = O_WRONLY;
    break;

  case SFS_O_RDWR:
    open_flags = O_RDWR;
    break;

  default:
    open_flags = O_RDONLY;
    break;
  }

  return open_flags;
}

//------------------------------------------------------------------------------
// Get XRootD acceess operation bases on the given open flags
//------------------------------------------------------------------------------
Access_Operation
XrdMgmOfsFile::GetXrdAccessOperation(int open_flags)
{
  Access_Operation op;

  if (open_flags & O_CREAT) {
    op = AOP_Create;
  } else {
    if (open_flags == O_RDONLY) {
      op = AOP_Read;
    } else {
      op = AOP_Update;
    }
  }

  return op;
}

/*----------------------------------------------------------------------------*/
/*
 * @brief open a given file with the indicated mode
 *
 * @param inpath path to open
 * @param open_mode SFS_O_RDONLY,SFS_O_WRONLY,SFS_O_RDWR,SFS_O_CREAT,SFS_TRUNC
 * @param Mode posix access mode bits to be assigned
 * @param client XRootD authentication object
 * @param ininfo CGI
 * @return SFS_OK on succes, otherwise SFS_ERROR on error or redirection
 *
 * Mode may also contain SFS_O_MKPATH if one desires to automatically create
 * all missing directories for a file (if possible).
 *
 */
/*----------------------------------------------------------------------------*/
int
XrdMgmOfsFile::open(eos::common::VirtualIdentity* invid,
                    const char* inpath,
                    XrdSfsFileOpenMode open_mode,
                    mode_t Mode,
                    const XrdSecEntity* client,
                    const char* ininfo)
{
  using eos::common::LayoutId;
  static const char* epname = "open";
  const char* tident = error.getErrUser();
  eos::IFileMD::XAttrMap attrmapF;
  errno = 0;
  eos::common::Timing tm("Open");
  COMMONTIMING("begin", &tm);
  EXEC_TIMING_BEGIN("Open");
  XrdOucString spath = inpath;
  XrdOucString sinfo = ininfo;
  SetLogId(logId, tident);
  int open_flags = GetPosixOpenFlags(open_mode);
  bool isRW = ((open_flags == O_RDONLY) ? false : true);
  bool isRewrite = ((open_flags & O_CREAT) ? false : true);
  Access_Operation acc_op = GetXrdAccessOperation(open_flags);
  {
    EXEC_TIMING_BEGIN("IdMap");

    if (spath.beginswith("/zteos64:")) {
      sinfo += "&authz=";
      sinfo += spath.c_str() + 1;
      ininfo = sinfo.c_str();
    }

    if (!invid) {
      eos::common::Mapping::IdMap(client, ininfo, tident, vid,
                                  gOFS->mTokenAuthz, acc_op, spath.c_str());
    } else {
      vid = *invid;
    }

    EXEC_TIMING_END("IdMap");
  }
  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);
  COMMONTIMING("IdMap", &tm);
  SetLogId(logId, vid, tident);
  NAMESPACEMAP;
  BOUNCE_ILLEGAL_NAMES;
  BOUNCE_NOT_ALLOWED;
  spath = path;
  COMMONTIMING("Bounce", &tm);

  if (!spath.beginswith("/proc/") && spath.endswith("/")) {
    return Emsg(epname, error, EISDIR,
                "open - you specified a directory as target file name", path);
  }

  bool isCreation = false;
  // flag indicating parallel IO access
  bool isPio = false;
  // flag indicating access with reconstruction
  bool isPioReconstruct = false;
  // flag indicating FUSE file access
  bool isFuse = false;
  // flag indiciating an atomic upload where a file get's a hidden unique name and is renamed when it is closed
  bool isAtomicUpload = false;
  // flag indicating an atomic file name
  bool isAtomicName = false;
  // flag indicating a new injection - upload of a file into a stub without physical location
  bool isInjection = false;
  // flag indicating to drop the current disk replica in the policy space
  bool isRepair = false;
  // flag indicating a read for repair (meaningfull only on the FST)
  bool isRepairRead = false;
  // chunk upload ID
  XrdOucString ocUploadUuid = "";
  // Set of filesystem IDs to reconstruct
  std::set<unsigned int> pio_reconstruct_fs;
  // list of filesystem IDs usable for replacement of RAIN file
  std::vector<unsigned int> pio_replacement_fs;
  // tried hosts CGI
  std::string tried_cgi;
  // versioning CGI
  std::string versioning_cgi;
  // file size
  uint64_t fmdsize = 0;
  // io priority string
  std::string ioPriority;
  XrdOucString pinfo = (ininfo ? ininfo : "");
  eos::common::StringConversion::MaskTag(pinfo, "cap.msg");
  eos::common::StringConversion::MaskTag(pinfo, "cap.sym");
  eos::common::StringConversion::MaskTag(pinfo, "authz");

  if (isRW) {
    eos_info("op=write trunc=%d path=%s info=%s",
             open_mode & SFS_O_TRUNC, path, pinfo.c_str());
  } else {
    eos_info("op=read path=%s info=%s", path, pinfo.c_str());
  }

  ACCESSMODE_R;

  if (isRW) {
    SET_ACCESSMODE_W;
  }

  if (ProcInterface::IsProcAccess(path)) {
    if (ProcInterface::IsWriteAccess(path, pinfo.c_str())) {
      SET_ACCESSMODE_W;
    }
  } else {
    if (getenv("EOS_HA_REDIRECT_READS")) {
      SET_ACCESSMODE_R_MASTER;
    }
  }

  MAYSTALL;
  MAYREDIRECT;
  XrdOucString currentWorkflow = "default";
  unsigned long long byfid = 0;
  unsigned long long bypid = 0;
  COMMONTIMING("fid::fetch", &tm);

  /* check paths starting with fid: fxid: ino: ... */
  if (spath.beginswith("fid:") || spath.beginswith("fxid:") ||
      spath.beginswith("ino:")) {
    WAIT_BOOT;
    // reference by fid+fsid
    byfid = eos::Resolver::retrieveFileIdentifier(spath).getUnderlyingUInt64();

    try {
      eos::Prefetcher::prefetchFileMDAndWait(gOFS->eosView, byfid);
      eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
      fmd = gOFS->eosFileService->getFileMD(byfid);
      spath = gOFS->eosView->getUri(fmd.get()).c_str();
      bypid = fmd->getContainerId();
      eos_info("msg=\"access by inode\" ino=%s path=%s", path, spath.c_str());
      path = spath.c_str();
    } catch (eos::MDException& e) {
      eos_debug("caught exception %d %s\n", e.getErrno(),
                e.getMessage().str().c_str());
      MAYREDIRECT_ENOENT;
      MAYSTALL_ENOENT;
      return Emsg(epname, error, ENOENT,
                  "open - you specified a not existing inode number", path);
    }
  }

  COMMONTIMING("fid::fetched", &tm);
  openOpaque = new XrdOucEnv(ininfo);

  // Handle (delegated) tpc redirection for writes
  if (isRW && RedirectTpcAccess()) {
    return SFS_REDIRECT;
  }

  const std::string app_name = GetApplicationName(openOpaque, client);

  // Decide if this is a FUSE access
  if (!app_name.empty() &&
      ((app_name == "fuse") || (app_name == "xrootdfs") ||
       (app_name.find("fuse::") == 0))) {
    isFuse = true;
  }

  {
    // handle io priority
    const char* val = 0;

    if ((val = openOpaque->Get("eos.iopriority"))) {
      if (vid.hasUid(11)) {  // operator role
        // admin members can set iopriority
        ioPriority = val;
      } else {
        eos_info("msg=\"suppressing IO priority setting '%s', no operator role\"",
                 val);
      }
    }
  }

  {
    // Handle obfuscation and encryption
    const char* val = 0;

    if ((val = openOpaque->Get("eos.obfuscate"))) {
      try {
        mEosObfuscate = std::strtoul(val, 0, 10);
      } catch (...) {
        eos_warning("msg=\"ignore invalid eos.obfuscate\" value=\"%s\"", val);
      }
    }

    if ((val = openOpaque->Get("eos.key"))) {
      mEosKey = val;

      if (mEosObfuscate == 0) {
        mEosObfuscate = 1;
      }
    }
  }

  {
    // figure out if this is an OC upload
    const char* val = 0;

    if ((val = openOpaque->Get("oc-chunk-uuid"))) {
      ocUploadUuid = val;
    }
  }

  {
    // populate tried hosts from the CGI
    const char* val = 0;

    if ((val = openOpaque->Get("tried"))) {
      tried_cgi = val;
      tried_cgi += ",";
    }
  }

  {
    // extract the workflow name from the CGI
    const char* val = 0;

    if ((val = openOpaque->Get("eos.workflow"))) {
      currentWorkflow = val;
    }
  }

  {
    // populate versioning cgi from the CGI
    const char* val = 0;

    if ((val = openOpaque->Get("eos.versioning"))) {
      versioning_cgi = val;
    }
  }

  if (!isFuse && isRW) {
    // resolve symbolic links
    try {
      std::string s_path = path;
      spath = gOFS->eosView->getRealPath(s_path).c_str();
      eos_info("msg=\"rewrote symlinks\" sym-path=%s realpath=%s", s_path.c_str(),
               spath.c_str());
      path = spath.c_str();
    } catch (eos::MDException& e) {
      eos_debug("caught exception %d %s\n",
                e.getErrno(),
                e.getMessage().str().c_str());
      // will throw the error later
    }
  }

  // ---------------------------------------------------------------------------
  // PIO MODE CONFIGURATION
  // ---------------------------------------------------------------------------
  // PIO mode return's a vector of URLs to a client and the client contact's
  // directly these machines and run's the RAIN codec on client side.
  // The default mode return's one gateway machine and this machine run's the
  // RAIN codec.
  // On the fly reconstruction is done using PIO mode when the reconstruction
  // action is defined ('eos.pio.action=reconstruct'). The client can specify
  // a list of filesystem's which should be excluded. In case they are used
  // in the layout the stripes on the explicitly referenced filesystems and
  // all other unavailable filesystems get reconstructed into stripes on
  // new machines.
  // ---------------------------------------------------------------------------
  // ---------------------------------------------------------------------------
  // discover PIO mode
  // ---------------------------------------------------------------------------
  XrdOucString sPio = (openOpaque) ? openOpaque->Get("eos.cli.access") : "";

  if (sPio == "pio") {
    isPio = true;
  }

  // Discover PIO reconstruction mode
  XrdOucString sPioRecover = (openOpaque) ?
                             openOpaque->Get("eos.pio.action") : "";

  if (sPioRecover == "reconstruct") {
    isPioReconstruct = true;
  }

  {
    // Discover PIO reconstruction filesystems (stripes to be replaced)
    std::string sPioRecoverFs = (openOpaque) ?
                                (openOpaque->Get("eos.pio.recfs") ? openOpaque->Get("eos.pio.recfs") : "")
                                : "";
    std::vector<std::string> fsToken;
    eos::common::StringConversion::Tokenize(sPioRecoverFs, fsToken, ",");

    if (openOpaque->Get("eos.pio.recfs") && fsToken.empty()) {
      return Emsg(epname, error, EINVAL, "open - you specified a list of"
                  " reconstruction filesystems but the list is empty", path);
    }

    for (size_t i = 0; i < fsToken.size(); i++) {
      errno = 0;
      unsigned int rfs = (unsigned int) strtol(fsToken[i].c_str(), 0, 10);
      XrdOucString srfs = "";
      srfs += (int) rfs;

      if (errno || (srfs != fsToken[i].c_str())) {
        return Emsg(epname, error, EINVAL, "open - you specified a list of "
                    "reconstruction filesystems but the list contains non "
                    "numerical or illegal id's", path);
      }

      // store in the reconstruction filesystem list
      pio_reconstruct_fs.insert(rfs);
    }
  }

  int rcode = SFS_ERROR;
  XrdOucString redirectionhost = "invalid?";
  XrdOucString targethost = "";
  int targetport = atoi(gOFS->MgmOfsTargetPort.c_str());
  int targethttpport = gOFS->mHttpdPort;
  int ecode = 0;
  unsigned long fmdlid = 0;
  unsigned long long cid = 0;

  // Proc filter
  if (ProcInterface::IsProcAccess(path)) {
    if (gOFS->mExtAuthz &&
        (vid.prot != "sss") &&
        (vid.prot != "gsi") &&
        (vid.prot != "krb5") &&
        (vid.host != "localhost") &&
        (vid.host != "localhost.localdomain")) {
      return Emsg(epname, error, EPERM, "execute proc command - you don't have"
                  " the requested permissions for that operation (1)", path);
    }

    gOFS->MgmStats.Add("OpenProc", vid.uid, vid.gid, 1);

    if (!ProcInterface::Authorize(path, ininfo, vid, client)) {
      return Emsg(epname, error, EPERM, "execute proc command - you don't have "
                  "the requested permissions for that operation (2)", path);
    } else {
      mProcCmd = ProcInterface::GetProcCommand(tident, vid, path, ininfo, logId);

      if (mProcCmd) {
        eos_static_info("proccmd=%s", mProcCmd->GetCmd(ininfo).c_str());
        mProcCmd->SetLogId(logId, vid, tident);
        mProcCmd->SetError(&error);
        rcode = mProcCmd->open(path, ininfo, vid, &error);

        // If we need to stall the client then save the IProcCommand object and
        // add it to the map for when the client comes back.
        if (rcode > 0) {
          if (mProcCmd->GetCmd(ininfo) != "proto") {
            return rcode;
          }

          if (!ProcInterface::SaveSubmittedCmd(tident, std::move(mProcCmd))) {
            eos_err("failed to save submitted command object");
            return Emsg(epname, error, EINVAL, "save sumitted command object "
                        "for path ", path);
          }

          // Now the mProcCmd object is null and moved to the global map
        }

        return rcode;
      } else {
        return Emsg(epname, error, ENOMEM, "allocate proc command object for ",
                    path);
      }
    }
  }

  gOFS->MgmStats.Add("Open", vid.uid, vid.gid, 1);
  bool dotFxid = spath.beginswith("/.fxid:");

  if (dotFxid) {
    byfid = eos::Resolver::retrieveFileIdentifier(spath).getUnderlyingUInt64();

    try {
      eos::Prefetcher::prefetchFileMDAndWait(gOFS->eosView, byfid);
      eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
      fmd = gOFS->eosFileService->getFileMD(byfid);
      spath = gOFS->eosView->getUri(fmd.get()).c_str();
      bypid = fmd->getContainerId();
      eos_info("msg=\"access by inode\" ino=%s path=%s", path, spath.c_str());
      path = spath.c_str();
    } catch (eos::MDException& e) {
      eos_debug("caught exception %d %s\n", e.getErrno(),
                e.getMessage().str().c_str());
      return Emsg(epname, error, ENOENT,
                  "open - you specified a not existing fxid", path);
    }
  }

  COMMONTIMING("authorize", &tm);
  AUTHORIZE(client, openOpaque, acc_op,
            ((acc_op == AOP_Create) ? "create" : "open"), inpath, error);
  COMMONTIMING("authorized", &tm);
  eos::common::Path cPath(path);
  // indicate the scope for a possible token
  TOKEN_SCOPE;
  isAtomicName = cPath.isAtomicFile();

  // prevent any access to a recycling bin for writes
  if (isRW && cPath.GetFullPath().beginswith(Recycle::gRecyclingPrefix.c_str())) {
    return Emsg(epname, error, EPERM,
                "open file - nobody can write to a recycling bin",
                cPath.GetParentPath());
  }

  std::shared_ptr<eos::IContainerMD> dmd;

  // check if we have to create the full path
  if (Mode & SFS_O_MKPTH) {
    eos_debug("%s", "msg=\"SFS_O_MKPTH was requested\"");
    XrdSfsFileExistence file_exists;
    std::shared_ptr<eos::IFileMD> _fmd;
    int ec = gOFS->_exists(cPath.GetParentPath(), file_exists,
                           error, vid, dmd, _fmd, 0);

    // check if that is a file
    if ((!ec) && (file_exists != XrdSfsFileExistNo) &&
        (file_exists != XrdSfsFileExistIsDirectory)) {
      return Emsg(epname, error, ENOTDIR,
                  "open file - parent path is not a directory",
                  cPath.GetParentPath());
    }

    // if it does not exist try to create the path!
    if ((!ec) && (file_exists == XrdSfsFileExistNo)) {
      ec = gOFS->_mkdir(cPath.GetParentPath(), Mode, error, vid, ininfo);

      if (ec) {
        gOFS->MgmStats.Add("OpenFailedPermission", vid.uid, vid.gid, 1);
        return SFS_ERROR;
      }
    }
  }

  bool isSharedFile = gOFS->VerifySharePath(path, openOpaque);

  if (gOFS->is_squashfs_access(path, vid)) {
    isSharedFile = true;
  }

  COMMONTIMING("path-computed", &tm);
  // Get the directory meta data if it exists
  eos::IContainerMD::XAttrMap attrmap;
  Acl acl;
  Workflow workflow;
  bool stdpermcheck = false;
  int versioning = 0;
  uid_t d_uid = vid.uid;
  gid_t d_gid = vid.gid;
  std::string creation_path = path;
  {
    // This is probably one of the hottest code paths in the MGM, we definitely
    // want prefetching here.
    if (!byfid) {
      if (!(open_flags & O_EXCL)) {
        // if we want to create, why would we prefetch file md?
        eos::Prefetcher::prefetchFileMDAndWait(gOFS->eosView, cPath.GetPath());
      } else {
        eos::Prefetcher::prefetchContainerMDAndWait(gOFS->eosView,
            cPath.GetParentPath());
      }
    }

    eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex);

    try {
      if (byfid) {
        dmd = gOFS->eosDirectoryService->getContainerMD(bypid);
      } else if (!dmd) {
        dmd = gOFS->eosView->getContainer(cPath.GetParentPath());
      }

      // get the attributes out
      eos::listAttributes(gOFS->eosView, dmd.get(), attrmap, false);
      // extract workflows
      workflow.Init(&attrmap);

      if (dmd) {
        try {
          std::string filePath = cPath.GetPath();
          std::string fileName = cPath.GetName();

          if (ocUploadUuid.length()) {
            eos::common::Path aPath(cPath.GetAtomicPath(attrmap.count("sys.versioning"),
                                    ocUploadUuid));
            filePath = aPath.GetPath();
            fileName = aPath.GetName();
          }

          if ((fmd = dmd->findFile(fileName))) {
            /* in case of a hard link, may need to switch to target */
            /* A hard link to another file */
            if (fmd->hasAttribute(XrdMgmOfsFile::k_mdino)) {
              std::shared_ptr<eos::IFileMD> gmd;
              uint64_t mdino;

              if (eos::common::StringToNumeric(fmd->getAttribute(XrdMgmOfsFile::k_mdino),
                                               mdino)) {
                gmd = gOFS->eosFileService->getFileMD(
                        eos::common::FileId::InodeToFid(mdino));
                eos_info("hlnk switched from %s (%#lx) to file %s (%#lx)",
                         fmd->getName().c_str(), fmd->getId(),
                         gmd->getName().c_str(), gmd->getId());
                fmd = gmd;
              } else {
                //Conversion from string to inode number failed, log the error and return an error to the client
                return Emsg(epname, error, ENOENT,
                            "convert the inode extended attribute to a number", path);
              }
            }

            if (fmd->isLink()) {
              // we have to get it by path
              fmd = gOFS->eosView->getFile(filePath);
            }

            uint64_t dmd_id = fmd->getContainerId();

            // If fmd is resolved via a symbolic link, we have to find the
            // 'real' parent directory
            if (dmd_id != dmd->getId()) {
              // retrieve the 'real' parent
              try {
                dmd = gOFS->eosDirectoryService->getContainerMD(dmd_id);
              } catch (const eos::MDException& e) {
                // this looks like corruption, but will return in ENOENT for the parent
                dmd.reset();
                errno = ENOENT;
              }
            }

            // check for O_EXCL here to save some time
            if (open_flags & O_EXCL) {
              gOFS->MgmStats.Add("OpenFailedExists", vid.uid, vid.gid, 1);
              return Emsg(epname, error, EEXIST, "create file - (O_EXCL)", path);
            }
          }
        } catch (eos::MDException& e) {
          fmd.reset();
        }

        if (!fmd) {
          if (dmd && dmd->findContainer(cPath.GetName())) {
            errno = EISDIR;
          } else {
            errno = ENOENT;
          }
        } else {
          mFid = fmd->getId();
          fmdlid = fmd->getLayoutId();
          cid = fmd->getContainerId();
          fmdsize = fmd->getSize();
        }

        if (dmd) {
          d_uid = dmd->getCUid();
          d_gid = dmd->getCGid();
        }
      } else {
        fmd.reset();
      }
    } catch (eos::MDException& e) {
      dmd.reset();
      errno = e.getErrno();
      eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                e.getErrno(), e.getMessage().str().c_str());
    };

    COMMONTIMING("container::fetched", &tm);

    // Check permissions
    if (!dmd) {
      int save_errno = errno;
      MAYREDIRECT_ENOENT;

      if (cPath.GetSubPath(2)) {
        eos_info("info=\"checking l2 path\" path=%s", cPath.GetSubPath(2));

        // Check if we have a redirection setting at level 2 in the namespace
        try {
          dmd = gOFS->eosView->getContainer(cPath.GetSubPath(2));
          // get the attributes out
          eos::listAttributes(gOFS->eosView, dmd.get(), attrmap, false);
        } catch (eos::MDException& e) {
          dmd.reset();
          errno = e.getErrno();
          eos_debug("msg=\"exception\" ec=%d emsg=%s\n",
                    e.getErrno(), e.getMessage().str().c_str());
        }

        // ---------------------------------------------------------------------
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

          if (!gOFS->SetRedirectionInfo(error, redirectionhost.c_str(), ecode)) {
            eos_err("msg=\"failed setting redirection\" path=\"%s\"", path);
            return SFS_ERROR;
          }

          rcode = SFS_REDIRECT;
          gOFS->MgmStats.Add("RedirectENOENT", vid.uid, vid.gid, 1);
          XrdOucString predirectionhost = redirectionhost.c_str();
          eos::common::StringConversion::MaskTag(predirectionhost, "cap.msg");
          eos::common::StringConversion::MaskTag(predirectionhost, "cap.sym");
          eos::common::StringConversion::MaskTag(pinfo, "authz");
          eos_info("info=\"redirecting\" hostport=%s:%d", predirectionhost.c_str(),
                   ecode);
          return rcode;
        }
      }

      // put back original errno
      errno = save_errno;
      gOFS->MgmStats.Add("OpenFailedENOENT", vid.uid, vid.gid, 1);
      return Emsg(epname, error, errno, "open file", path);
    }

    bool sticky_owner;
    attr::checkDirOwner(attrmap, d_uid, d_gid, vid, sticky_owner, path);

    // -------------------------------------------------------------------------
    // ACL and permission check
    // -------------------------------------------------------------------------
    if (dotFxid and (not vid.sudoer) and (vid.uid != 0)) {
      /* restricted: this could allow access to a file hidden by the hierarchy */
      eos_debug(".fxid=%d uid %d sudoer %d", dotFxid, vid.uid, vid.sudoer);
      errno = EPERM;
      return Emsg(epname, error, errno, "open file - open by fxid denied", path);
    }

    if (fmd) {
      eos::listAttributes(gOFS->eosView, fmd.get(), attrmapF, false);
    }

    acl.SetFromAttrMap(attrmap, vid, &attrmapF);
    eos_info("acl=%d r=%d w=%d wo=%d egroup=%d shared=%d mutable=%d facl=%d",
             acl.HasAcl(), acl.CanRead(), acl.CanWrite(), acl.CanWriteOnce(),
             acl.HasEgroup(), isSharedFile, acl.IsMutable(),
             acl.EvalUserAttrFile());

    if (acl.HasAcl() && (vid.uid != 0)) {
      if ((vid.uid != 0) && (!vid.sudoer) &&
          (isRW ? (acl.CanNotWrite() && acl.CanNotUpdate()) : acl.CanNotRead())) {
        eos_debug("uid %d sudoer %d isRW %d CanNotRead %d CanNotWrite %d CanNotUpdate %d",
                  vid.uid, vid.sudoer, isRW, acl.CanNotRead(), acl.CanNotWrite(),
                  acl.CanNotUpdate());
        errno = EPERM;
        gOFS->MgmStats.Add("OpenFailedPermission", vid.uid, vid.gid, 1);
        return Emsg(epname, error, errno, "open file - forbidden by ACL", path);
      }

      if (isRW) {
        // Update case - unless SFS_O_TRUNC is specified then this is a normal write
        if (fmd && ((open_flags & O_TRUNC) == 0)) {
          eos_debug("CanUpdate %d CanNotUpdate %d stdpermcheck %d file uid/gid = %d/%d",
                    acl.CanUpdate(), acl.CanNotUpdate(), stdpermcheck, fmd->getCUid(),
                    fmd->getCGid());

          if (acl.CanNotUpdate() || (acl.CanNotWrite() && !acl.CanUpdate())) {
            // the ACL has !u set - we don't allow to do file updates
            gOFS->MgmStats.Add("OpenFailedNoUpdate", vid.uid, vid.gid, 1);
            return Emsg(epname, error, EPERM, "update file - fobidden by ACL",
                        path);
          }

          stdpermcheck = (!acl.CanUpdate());
        } else {
          // Write case
          if (!(acl.CanWrite() || acl.CanWriteOnce())) {
            // We have to check the standard permissions
            stdpermcheck = true;
          }
        }
      } else {
        // read case
        if (!acl.CanRead()) {
          // we have to check the standard permissions
          stdpermcheck = true;
        }
      }
    } else {
      stdpermcheck = true;
    }

    if (isRW && !acl.IsMutable() && vid.uid && !vid.sudoer) {
      // immutable directory
      errno = EPERM;
      gOFS->MgmStats.Add("OpenFailedPermission", vid.uid, vid.gid, 1);
      return Emsg(epname, error, errno, "open file - directory immutable", path);
    }

    // check publicaccess level
    if (!gOFS->allow_public_access(path, vid)) {
      return Emsg(epname, error, EACCES, "access - public access level restriction",
                  path);
    }

    int taccess = -1;

    if ((!isSharedFile || isRW) && stdpermcheck
        && (!(taccess = dmd->access(vid.uid, vid.gid,
                                    (isRW) ? W_OK | X_OK : R_OK | X_OK)))) {
      eos_debug("fCUid %d dCUid %d uid %d isSharedFile %d isRW %d stdpermcheck %d access %d",
                fmd ? fmd->getCUid() : 0, dmd->getCUid(), vid.uid, isSharedFile, isRW,
                stdpermcheck, taccess);

      if (!((vid.uid == DAEMONUID) && (isPioReconstruct))) {
        // we don't apply this permission check for reconstruction jobs issued via the daemon account
        errno = EPERM;
        gOFS->MgmStats.Add("OpenFailedPermission", vid.uid, vid.gid, 1);
        return Emsg(epname, error, errno, "open file", path);
      }
    }

    if (sticky_owner) {
      eos_info("msg=\"client acting as directory owner\" path=\"%s\" uid=\"%u=>%u\" gid=\"%u=>%u\"",
               path, vid.uid, vid.gid, d_uid, d_gid);
      vid.uid = d_uid;
      vid.gid = d_gid;
    }

    // If a file has the sys.proc attribute, it will be redirected as a command
    if (fmd != nullptr && fmd->hasAttribute("sys.proc")) {
      ns_rd_lock.Release();
      return open("/proc/user/", open_mode, Mode, client,
                  fmd->getAttribute("sys.proc").c_str());
    }
  }
  // check for versioning depth, cgi overrides sys & user attributes
  versioning = attr::getVersioning(attrmap, versioning_cgi);
  // check for atomic uploads only in non fuse clients
  isAtomicUpload = !isFuse &&
                   attr::checkAtomicUpload(attrmap, openOpaque->Get("eos.atomic"));
  // check for injection in non fuse clients with cgi
  isInjection = !isFuse && openOpaque->Get("eos.injection");

  if (openOpaque->Get("eos.repair")) {
    isRepair = true;
  }

  if (openOpaque->Get("eos.repairread")) {
    isRepairRead = true;
  }

  // Short-cut to block multi-source access to EC files
  if (IsRainRetryWithExclusion(isRW, fmdlid)) {
    return Emsg(epname, error, ENETUNREACH,  "open file - "
                "multi-source reading on EC file blocked for ", path);
  }

  // ---------------------------------------------------------------------------
  // attribute lock logic, don't allow file opens which have an attr lock
  // ---------------------------------------------------------------------------
  XattrLock alock(attrmapF);

  if (alock.foreignLock(vid, isRW)) {
    return Emsg(epname, error, EBUSY,
                "open file - file has a valid extended attribute lock ", path);
  }

  if (isRW) {
    // Allow updates of 0-size RAIN files so that we are able to write from the
    // FUSE mount with lazy-open mode enabled.
    if (!getenv("EOS_ALLOW_RAIN_RWM") && isRewrite && (vid.uid > 3) &&
        (fmdsize != 0) && (LayoutId::IsRain(fmdlid))) {
      // Unpriviledged users are not allowed to open RAIN files for update
      gOFS->MgmStats.Add("OpenFailedNoUpdate", vid.uid, vid.gid, 1);
      return Emsg(epname, error, EPERM, "update RAIN layout file - "
                  "you have to be a priviledged user for updates");
    }

    if (!isInjection && (open_flags & O_TRUNC) && fmd) {
      // check if this directory is write-once for the mapped user
      if (acl.HasAcl()) {
        if (acl.CanWriteOnce()) {
          gOFS->MgmStats.Add("OpenFailedNoUpdate", vid.uid, vid.gid, 1);
          // this is a write once user
          return Emsg(epname, error, EEXIST,
                      "overwrite existing file - you are write-once user");
        } else {
          if ((!stdpermcheck) && (!acl.CanWrite())) {
            return Emsg(epname, error, EPERM,
                        "overwrite existing file - you have no write permission");
          }
        }
      }

      if (versioning) {
        if (isAtomicUpload) {
          // atomic uploads need just to purge version to max-1, the version is created on commit
          // purge might return an error if the file was not yet existing/versioned
          gOFS->PurgeVersion(cPath.GetVersionDirectory(), error, versioning - 1);
          errno = 0;
        } else {
          // handle the versioning for a specific file ID
          if (gOFS->Version(mFid, error, vid, versioning)) {
            return Emsg(epname, error, errno, "version file", path);
          }
        }
      } else {
        // drop the old file (for non atomic uploads) and create a new truncated one
        if ((!isAtomicUpload) && gOFS->_rem(path, error, vid, ininfo, false, false)) {
          return Emsg(epname, error, errno, "remove file for truncation", path);
        }
      }

      if (!ocUploadUuid.length()) {
        fmd.reset();
      } else {
        eos_info("%s", "msg=\"keep attached to existing fmd in chunked upload\"");
      }

      gOFS->MgmStats.Add("OpenWriteTruncate", vid.uid, vid.gid, 1);
    } else {
      if (isInjection && !fmd) {
        errno = ENOENT;
        return Emsg(epname, error, errno, "inject into a non-existing file", path);
      }

      if (!(fmd) && ((open_flags & O_CREAT))) {
        gOFS->MgmStats.Add("OpenWriteCreate", vid.uid, vid.gid, 1);
      } else {
        if (acl.HasAcl()) {
          if (acl.CanWriteOnce()) {
            // this is a write once user
            return Emsg(epname, error, EEXIST,
                        "overwrite existing file - you are write-once user");
          } else {
            if ((!stdpermcheck) && (!acl.CanWrite()) && (!acl.CanUpdate())) {
              return Emsg(epname, error, EPERM,
                          "overwrite existing file - you have no write permission");
            }
          }
        }

        gOFS->MgmStats.Add("OpenWrite", vid.uid, vid.gid, 1);
      }
    }

    // -------------------------------------------------------------------------
    // write case
    // -------------------------------------------------------------------------
    if (!fmd) {
      if (!(open_flags & O_CREAT)) {
        // Open for write for non existing file without creation flag
        return Emsg(epname, error, ENOENT, "open file without creation flag", path);
      } else {
        // creation of a new file or isOcUpload
        COMMONTIMING("write::begin", &tm);
        {
          // -------------------------------------------------------------------
          std::shared_ptr<eos::IFileMD> ref_fmd;
          eos::common::RWMutexWriteLock ns_wr_lock(gOFS->eosViewRWMutex);

          try {
            // we create files with the uid/gid of the parent directory
            if (isAtomicUpload) {
              creation_path = cPath.GetAtomicPath(versioning, ocUploadUuid);
              eos_info("atomic-path=%s", creation_path.c_str());

              try {
                ref_fmd = gOFS->eosView->getFile(path);
              } catch (eos::MDException& e) {
                // empty
              }
            }

            // Avoid any race condition when opening for creation O_EXCL
            if (open_flags & O_EXCL) {
              if (isAtomicUpload) {
                try {
                  fmd = gOFS->eosView->getFile(creation_path);
                } catch (eos::MDException& e1) {
                  // empty
                }
              }

              if (fmd) {
                gOFS->MgmStats.Add("OpenFailedExists", vid.uid, vid.gid, 1);
                return Emsg(epname, error, EEXIST, "create file - (O_EXCL)", path);
              }
            }

            {
              // a faster replacement for createFile view view
              auto file = gOFS->eosFileService->createFile(0);

              if (!file) {
                eos_static_crit("File creation failed for %s", creation_path.c_str());
                throw_mdexception(EIO, "File creation failed");
              }

              eos::common::Path cPath(creation_path.c_str());
              std::string fileName = cPath.GetName();
              file->setName(fileName);
              file->setCUid(vid.uid);
              file->setCGid(vid.gid);
              file->setCTimeNow();
              file->setATimeNow(0);
              file->setMTimeNow();
              file->clearChecksum(0);
              dmd->addFile(file.get());
              fmd = file;
            }

            if ((mEosObfuscate > 0) ||
                (attrmap.count("sys.file.obfuscate") &&
                 (attrmap["sys.file.obfuscate"] == "1"))) {
              std::string skey = eos::common::SymKey::RandomCipher(mEosKey);
              // attach an obfucation key
              fmd->setAttribute("user.obfuscate.key", skey);

              if (mEosKey.length()) {
                fmd->setAttribute("user.encrypted", "1");
              }

              attrmapF["user.obfuscate.key"] = skey;
            }

            if (ocUploadUuid.length()) {
              fmd->setFlags(0);
            } else {
              fmd->setFlags(Mode & (S_IRWXU | S_IRWXG | S_IRWXO));
            }

            // For versions copy xattrs over from the original file
            if (versioning) {
              static std::set<std::string> skip_tag {"sys.eos.btime", "sys.fs.tracking", eos::common::EOS_DTRACE_ATTR, eos::common::EOS_VTRACE_ATTR, "sys.tmp.atomic"};

              for (const auto& xattr : attrmapF) {
                if (skip_tag.find(xattr.first) == skip_tag.end()) {
                  fmd->setAttribute(xattr.first, xattr.second);
                }
              }
            }

            fmd->setAttribute("sys.utrace", logId);
            fmd->setAttribute("sys.vtrace", vid.getTrace());

            if (ref_fmd) {
              // If we have a target file we tag the latest atomic upload name
              // on a temporary attribute
              ref_fmd->setAttribute("sys.tmp.atomic", fmd->getName());

              if (acl.EvalUserAttrFile()) {
                // we inherit existing ACLs during (atomic) versioning
                ref_fmd->setAttribute("user.acl", acl.UserAttrFile());
                ref_fmd->setAttribute("sys.eval.useracl", "1");
              }
            }

            mFid = fmd->getId();
            fmdlid = fmd->getLayoutId();
            // oc chunks start with flags=0
            cid = fmd->getContainerId();
            auto cmd = dmd; // we have this already
            cmd->setMTimeNow();
            eos::ContainerIdentifier cmd_id = cmd->getIdentifier();
            eos::ContainerIdentifier cmd_pid = cmd->getParentIdentifier();
            gOFS->mReplicationTracker->Create(fmd);
            ns_wr_lock.Release();
            cmd->notifyMTimeChange(gOFS->eosDirectoryService);
            gOFS->eosView->updateContainerStore(cmd.get());
            gOFS->eosView->updateFileStore(fmd.get());

            if (ref_fmd) {
              gOFS->eosView->updateFileStore(ref_fmd.get());
            }

            gOFS->FuseXCastRefresh(cmd_id, cmd_pid);
          } catch (eos::MDException& e) {
            fmd.reset();
            errno = e.getErrno();
            eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                      e.getErrno(), e.getMessage().str().c_str());
          };

          // -------------------------------------------------------------------
        } // end ns_lock
        COMMONTIMING("write::end", &tm);

        if (!fmd) {
          // creation failed
          gOFS->MgmStats.Add("OpenFailedCreate", vid.uid, vid.gid, 1);
          return Emsg(epname, error, errno, "create file", path);
        }

        isCreation = true;
        // -------------------------------------------------------------------------
      }
    } else {
      // we attached to an existing file
      if (open_flags & O_EXCL) {
        gOFS->MgmStats.Add("OpenFailedExists", vid.uid, vid.gid, 1);
        return Emsg(epname, error, EEXIST, "create file (O_EXCL)", path);
      }
    }
  } else {
    if (!fmd) {
      // check if there is a redirect or stall for missing entries
      MAYREDIRECT_ENOENT;
      MAYSTALL_ENOENT;

      if (auto redirect_kv = attrmap.find("sys.redirect.enoent");
          redirect_kv != attrmap.end()) {
        // there is a redirection setting here
        redirectionhost = "";
        redirectionhost = redirect_kv->second.c_str();
        int portpos = 0;

        if ((portpos = redirectionhost.find(":")) != STR_NPOS) {
          XrdOucString port = redirectionhost;
          port.erase(0, portpos + 1);
          ecode = atoi(port.c_str());
          redirectionhost.erase(portpos);
        } else {
          ecode = 1094;
        }

        if (!gOFS->SetRedirectionInfo(error, redirectionhost.c_str(), ecode)) {
          eos_err("msg=\"failed setting redirection\" path=\"%s\"", path);
          return SFS_ERROR;
        }

        rcode = SFS_REDIRECT;
        gOFS->MgmStats.Add("RedirectENOENT", vid.uid, vid.gid, 1);
        return rcode;
      }

      gOFS->MgmStats.Add("OpenFailedENOENT", vid.uid, vid.gid, 1);
      return Emsg(epname, error, errno, "open file", path);
    }

    if (isSharedFile) {
      gOFS->MgmStats.Add("OpenShared", vid.uid, vid.gid, 1);
    } else {
      gOFS->MgmStats.Add("OpenRead", vid.uid, vid.gid, 1);
    }
  }

  // ---------------------------------------------------------------------------
  // flush synchronization logic, don't open a file which is currently flushing
  // ---------------------------------------------------------------------------
  if (gOFS->zMQ->gFuseServer.Flushs().hasFlush(eos::common::FileId::FidToInode(
        mFid))) {
    // the first 255ms are covered inside hasFlush, otherwise we stall clients for a sec
    return gOFS->Stall(error, 1, "file is currently being flushed");
  }

  // ---------------------------------------------------------------------------
  // construct capability
  // ---------------------------------------------------------------------------
  XrdOucString capability = "";

  if (gOFS->mTapeEnabled) {
    capability += "&tapeenabled=1";
  }

  if (isPioReconstruct) {
    capability += "&mgm.access=update";
  } else {
    if (isRW) {
      if (isRewrite) {
        capability += "&mgm.access=update";
      } else {
        capability += "&mgm.access=create";
      }

      uint64_t cloneId;

      if (fmd && (cloneId = fmd->getCloneId()) != 0) {
        char sbuff[1024];
        std::string cloneFST = fmd->getCloneFST();

        if (cloneFST == "") {      /* This triggers the copy-on-write */
          if (int rc = create_cow(cowUpdate, dmd, fmd, vid, error)) {
            return rc;
          }
        }

        eos_debug("file %s cloneid %ld cloneFST %s trunc %d", path, fmd->getCloneId(),
                  fmd->getCloneFST().c_str(), open_flags & O_TRUNC);
        snprintf(sbuff, sizeof(sbuff), "&mgm.cloneid=%ld&mgm.cloneFST=%s", cloneId,
                 fmd->getCloneFST().c_str());
        capability += sbuff;
      }
    } else {
      capability += "&mgm.access=read";
    }
  }

  if (mEosObfuscate && !isFuse) {
    // add obfuscation key to redirection capability
    if (attrmapF.count("user.obfuscate.key")) {
      capability += "&mgm.obfuscate.key=";
      capability += attrmapF["user.obfuscate.key"].c_str();
    }

    // add encryption key to redirection capability
    if (mEosKey.length()) {
      capability += "&mgm.encryption.key=";
      capability += mEosKey.c_str();
    }
  }

  // ---------------------------------------------------------------------------
  // forward some allowed user opaque tags
  // ---------------------------------------------------------------------------
  unsigned long layoutId = (isCreation) ? LayoutId::kPlain : fmdlid;
  // the client can force to read a file on a defined file system
  unsigned long forcedFsId = 0;
  // the client can force to place a file in a specified group of a space
  long forced_group = -1;
  // this is the filesystem defining the client access point in the selection
  // vector - for writes it is always 0, for reads it comes out of the
  // FileAccess function
  unsigned long fsIndex = 0;
  XrdOucString space = "default";
  unsigned long new_lid = 0;
  eos::mgm::Scheduler::tPlctPolicy plctplcy;
  std::string targetgeotag;
  std::string bandwidth;
  std::string ioprio;
  std::string iotype;
  bool schedule = false;
  uint64_t atimeage = 0;
  // select space and layout according to policies
  COMMONTIMING("Policy::begin", &tm);
  Policy::GetLayoutAndSpace(path, attrmap, vid, new_lid, space, *openOpaque,
                            forcedFsId, forced_group, bandwidth, schedule,
                            ioprio, iotype, isRW, true, &atimeage);
  COMMONTIMING("Policy::end", &tm);

  // do a local redirect here if there is only one replica attached
  if (!isRW && !isPio && (fmd->getNumLocation() == 1) &&
      Policy::RedirectLocal(path, attrmap, vid, layoutId, space, *openOpaque)) {
    XrdCl::URL url(std::string("root://localhost//") + std::string(
                     path ? path : "/dummy/") + std::string("?") + std::string(
                     ininfo ? ininfo : ""));
    std::string localhost = "localhost";

    if (gOFS->Tried(url, localhost, "*")) {
      gOFS->MgmStats.Add("OpenFailedRedirectLocal", vid.uid, vid.gid, 1);
      eos_info("msg=\"local-redirect disabled - forwarding to FST\" path=\"%s\" info=\"%s\"",
               path, ininfo);
    } else {
      eos::common::FileSystem::fs_snapshot_t local_snapshot;
      unsigned int local_id = fmd->getLocation(0);
      {
        eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
        eos::mgm::FileSystem* local_fs = FsView::gFsView.mIdView.lookupByID(local_id);
        local_fs->SnapShotFileSystem(local_snapshot);
      }
      // compute the local path
      std::string local_path = eos::common::FileId::FidPrefix2FullPath(
                                 eos::common::FileId::Fid2Hex(fmd->getId()).c_str(),
                                 local_snapshot.mPath.c_str());
      eos_info("msg=\"local-redirect screening - forwarding to local\" local-path=\"%s\" path=\"%s\" info=\"%s\"",
               local_path.c_str(), path, ininfo);
      redirectionhost = "file://localhost";
      redirectionhost += local_path.c_str();
      ecode = -1;

      if (!gOFS->SetRedirectionInfo(error, redirectionhost.c_str(), ecode)) {
        eos_err("msg=\"failed setting redirection\" path=\"%s\"", path);
        return SFS_ERROR;
      }

      rcode = SFS_REDIRECT;
      gOFS->MgmStats.Add("OpenRedirectLocal", vid.uid, vid.gid, 1);
      eos_info("local-redirect=\"%s\"", redirectionhost.c_str());
      return rcode;
    }
  }

  if (ioPriority.length()) {
    ioprio = ioPriority;
    capability += "&mgm.iopriority=";
    capability += ioPriority.c_str();
  } else {
    if (ioprio.length()) {
      capability += "&mgm.iopriority=";
      capability += ioprio.c_str();
    }
  }

  if (schedule) {
    capability += "&mgm.schedule=1";
  }

  if (iotype.length()) {
    capability += "&mgm.iotype=";
    capability += iotype.c_str();
  }

  if (fmd && atimeage) {
    static std::set<std::string> skip_tag {"balancer", "groupdrainer", "groupbalancer", "geobalancer", "drainer", "converter", "fsck"};

    if (app_name.empty() || (skip_tag.find(app_name) == skip_tag.end())) {
      // do a potential atime update, we don't need a name
      try {
        if (fmd->setATimeNow(atimeage)) {
          gOFS->eosView->updateFileStore(fmd.get());
        }
      } catch (eos::MDException& e) {
        errno = e.getErrno();
        std::string errmsg = e.getMessage().str();
        eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                  e.getErrno(), e.getMessage().str().c_str());
        gOFS->MgmStats.Add("OpenFailedQuota", vid.uid, vid.gid, 1);
        return Emsg(epname, error, errno, "open file and update atime for reading",
                    errmsg.c_str());
      }
    }
  }

  // get placement policy
  Policy::GetPlctPolicy(path, attrmap, vid, *openOpaque, plctplcy, targetgeotag);
  unsigned long long ext_mtime_sec = 0;
  unsigned long long ext_mtime_nsec = 0;
  unsigned long long ext_ctime_sec = 0;
  unsigned long long ext_ctime_nsec = 0;
  std::string ext_etag;
  std::map<std::string, std::string> ext_xattr_map;

  if (openOpaque->Get("eos.ctime")) {
    std::string str_ctime = openOpaque->Get("eos.ctime");
    size_t pos = str_ctime.find('.');

    if (pos == std::string::npos) {
      ext_ctime_sec = strtoull(str_ctime.c_str(), 0, 10);
      ext_ctime_nsec = 0;
    } else {
      ext_ctime_sec = strtoull(str_ctime.substr(0, pos).c_str(), 0, 10);
      ext_ctime_nsec = strtoull(str_ctime.substr(pos + 1).c_str(), 0, 10);
    }
  }

  if (openOpaque->Get("eos.mtime")) {
    std::string str_mtime = openOpaque->Get("eos.mtime");
    size_t pos = str_mtime.find('.');

    if (pos == std::string::npos) {
      ext_mtime_sec = strtoull(str_mtime.c_str(), 0, 10);
      ext_mtime_nsec = 0;
    } else {
      ext_mtime_sec = strtoull(str_mtime.substr(0, pos).c_str(), 0, 10);
      ext_mtime_nsec = strtoull(str_mtime.substr(pos + 1).c_str(), 0, 10);
    }
  }

  if (openOpaque->Get("eos.etag")) {
    ext_etag = openOpaque->Get("eos.etag");
  }

  if (openOpaque->Get("eos.xattr")) {
    std::vector<std::string> xattr_keys;
    eos::common::StringConversion::GetKeyValueMap(openOpaque->Get("eos.xattr"),
        ext_xattr_map, "=", "#", &xattr_keys);

    for (auto it = xattr_keys.begin(); it != xattr_keys.end(); ++it) {
      if (it->substr(0, 5) != "user.") {
        ext_xattr_map.erase(*it);
      }
    }
  }

  if ((!isInjection) && (isCreation || (open_flags & O_TRUNC))) {
    eos_info("blocksize=%llu lid=%x", LayoutId::GetBlocksize(new_lid), new_lid);
    layoutId = new_lid;
    {
      std::shared_ptr<eos::IFileMD> fmdnew;

      if (!byfid) {
        try {
          fmdnew = dmd->findFile(fileName);
        } catch (eos::MDException& e) {
          if ((!isAtomicUpload) && (fmdnew != fmd)) {
            // file has been recreated in the meanwhile
            return Emsg(epname, error, EEXIST, "open file (file recreated)", path);
          }
        }
      }

      // Set the layout and commit new meta data
      fmd->setLayoutId(layoutId);

      if (isFuse && (open_flags & O_TRUNC)) {
        std::string s;

        if (fmd->hasAttribute("sys.fusex.state")) {
          s = fmd->getAttribute("sys.fusex.state");
        }

        s += "T";
        fmd->setAttribute("sys.fusex.state",
                          eos::common::StringConversion::ReduceString(s).c_str());
      }

      // if specified set an external modification/creation time
      if (ext_mtime_sec) {
        eos::IFileMD::ctime_t mtime;
        mtime.tv_sec = ext_mtime_sec;
        mtime.tv_nsec = ext_mtime_nsec;
        fmd->setMTime(mtime);
      } else {
        fmd->setMTimeNow();
      }

      if (ext_ctime_sec) {
        eos::IFileMD::ctime_t ctime;
        ctime.tv_sec = ext_ctime_sec;
        ctime.tv_nsec = ext_ctime_nsec;
        fmd->setCTime(ctime);
      }

      if (isCreation) {
        // store the birth time as an extended attribute
        eos::IFileMD::ctime_t ctime;
        fmd->getCTime(ctime);
        char btime[256];
        snprintf(btime, sizeof(btime), "%lu.%lu", ctime.tv_sec, ctime.tv_nsec);
        fmd->setAttribute("sys.eos.btime", btime);
      } else {
        fmd->setATimeNow(0);
      }

      // if specified set an external temporary ETAG
      if (ext_etag.length()) {
        fmd->setAttribute("sys.tmp.etag", ext_etag);
      }

      for (auto it = ext_xattr_map.begin(); it != ext_xattr_map.end(); ++it) {
        fmd->setAttribute(it->first, it->second);
      }

      if (acl.EvalUserAttrFile()) {
        // we inherit existing ACLs during (atomic) versioning
        fmd->setAttribute("user.acl", acl.UserAttrFile());
        fmd->setAttribute("sys.eval.useracl", "1");
      }

      try {
        eos::common::RWMutexWriteLock ns_wr_lock(gOFS->eosViewRWMutex);
        eos::FileIdentifier fmd_id = fmd->getIdentifier();
        std::shared_ptr<eos::IContainerMD> cmd =
          gOFS->eosDirectoryService->getContainerMD(cid);
        eos::ContainerIdentifier cmd_id = cmd->getIdentifier();
        eos::ContainerIdentifier pcmd_id = cmd->getParentIdentifier();
        cmd->setMTimeNow();

        if (isCreation || (!fmd->getNumLocation())) {
          eos::IQuotaNode* ns_quota = gOFS->eosView->getQuotaNode(cmd.get());

          if (ns_quota) {
            ns_quota->addFile(fmd.get());
          }
        }

        ns_wr_lock.Release();
        COMMONTIMING("filemd::update", &tm);
        gOFS->eosView->updateFileStore(fmd.get());
        cmd->notifyMTimeChange(gOFS->eosDirectoryService);
        gOFS->eosView->updateContainerStore(cmd.get());
        gOFS->FuseXCastRefresh(fmd_id, cmd_id);
        gOFS->FuseXCastRefresh(cmd_id, pcmd_id);
        COMMONTIMING("fusex::bc", &tm);
      } catch (eos::MDException& e) {
        errno = e.getErrno();
        std::string errmsg = e.getMessage().str();
        eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                  e.getErrno(), e.getMessage().str().c_str());
        gOFS->MgmStats.Add("OpenFailedQuota", vid.uid, vid.gid, 1);
        return Emsg(epname, error, errno, "open file", errmsg.c_str());
      }
    }
  }

  // 0-size files can be read from the MGM if this is not FUSE access
  // atomic files are only served from here and also rain files are skipped
  if (!isRW && !fmd->getSize() && (!isFuse || isAtomicName)) {
    if (isAtomicName || (!LayoutId::IsRain(layoutId))) {
      eos_info("msg=\"0-size file read from the MGM\" path=%s", path);
      mIsZeroSize = true;
      return SFS_OK;
    }
  }

  // @todo(esindril) the tag is wrong should actually be mgm.uid
  capability += "&mgm.ruid=";
  capability += (int) vid.uid;
  capability += "&mgm.rgid=";
  capability += (int) vid.gid;
  // @todo(esindril) not used and should be removed
  capability += "&mgm.uid=99";
  capability += "&mgm.gid=99";
  capability += "&mgm.path=";
  {
    // an '&' will create a failure on the FST
    XrdOucString safepath = spath.c_str();
    eos::common::StringConversion::SealXrdPath(safepath);
    capability += safepath;
  }
  capability += "&mgm.manager=";
  capability += gOFS->ManagerId.c_str();
  capability += "&mgm.fid=";
  std::string hex_fid;

  if (!isRW) {
    const char* val;

    if ((val = openOpaque->Get("eos.clonefst")) && (strlen(val) < 32)) {
      hex_fid = fmd->getCloneFST();
      eos_debug("open read eos.clonefst %s hex_fid %s", val, hex_fid.c_str());

      if (hex_fid != val) {
        return Emsg(epname, error, EINVAL, "open - invalid clonefst argument", path);
      }
    }
  }

  if (hex_fid.empty()) {
    hex_fid = eos::common::FileId::Fid2Hex(mFid);
  }

  capability += hex_fid.c_str();
  XrdOucString sizestring;
  capability += "&mgm.cid=";
  capability += eos::common::StringConversion::GetSizeString(sizestring, cid);
  // add the mgm.sec information to the capability
  capability += "&mgm.sec=";
  capability += eos::common::SecEntity::ToKey(client, app_name.c_str()).c_str();

  if (attrmap.count("user.tag")) {
    capability += "&mgm.container=";
    capability += attrmap["user.tag"].c_str();
  }

  // Size which will be reserved with a placement of one replica for the file
  unsigned long long bookingsize = 0;
  bool hasClientBookingSize = false;
  unsigned long long targetsize = 0;
  unsigned long long minimumsize = 0;
  unsigned long long maximumsize = 0;

  if (attrmap.count("sys.forced.bookingsize")) {
    // we allow only a system attribute not to get fooled by a user
    bookingsize = strtoull(attrmap["sys.forced.bookingsize"].c_str(), 0, 10);
  } else {
    if (attrmap.count("user.forced.bookingsize")) {
      bookingsize = strtoull(attrmap["user.forced.bookingsize"].c_str(), 0, 10);
    } else {
      bookingsize = 1024ll; // 1k as default

      if (openOpaque->Get("eos.bookingsize")) {
        bookingsize = strtoull(openOpaque->Get("eos.bookingsize"), 0, 10);
        hasClientBookingSize = true;
      } else {
        if (openOpaque->Get("oss.asize")) {
          bookingsize = strtoull(openOpaque->Get("oss.asize"), 0, 10);
          hasClientBookingSize = true;
        }
      }
    }
  }

  if (attrmap.count("sys.forced.minsize")) {
    minimumsize = strtoull(attrmap["sys.forced.minsize"].c_str(), 0, 10);
  }

  if (attrmap.count("sys.forced.maxsize")) {
    maximumsize = strtoull(attrmap["sys.forced.maxsize"].c_str(), 0, 10);
  }

  if (openOpaque->Get("oss.asize")) {
    targetsize = strtoull(openOpaque->Get("oss.asize"), 0, 10);
  }

  if (openOpaque->Get("eos.targetsize")) {
    targetsize = strtoull(openOpaque->Get("eos.targetsize"), 0, 10);
  }

  std::string spacename = space.c_str();
  auto strategy = gOFS->mFsScheduler->getPlacementStrategy(spacename);
  const char* strategy_cstr;

  if ((strategy_cstr = openOpaque->Get("eos.schedulingstrategy"))) {
    strategy = placement::strategy_from_str(strategy_cstr);
    eos_debug("msg=\"using scheduling strategy\" strategy=%s",
              strategy_to_str(strategy).c_str());
  }

  bool use_geoscheduler = strategy ==
                          placement::PlacementStrategyT::kGeoScheduler;
  //eos::mgm::FileSystem* filesystem = 0;
  std::vector<unsigned int> selectedfs;
  std::vector<unsigned int> excludefs = GetExcludedFsids();
  std::vector<std::string> proxys;
  std::vector<std::string> firewalleps;
  // file systems which are unavailable during a read operation
  std::vector<unsigned int> unavailfs;
  // file systems which have been replaced with a new reconstructed stripe
  std::vector<unsigned int> replacedfs;
  int retc = 0;
  bool isRecreation = false;

  // Place a new file
  if (isCreation || (!fmd->getNumLocation()) || isInjection) {
    const char* containertag = 0;

    if (attrmap.count("user.tag")) {
      containertag = attrmap["user.tag"].c_str();
    }

    /// ###############
    // if the client should go through a firewall entrypoint, try to get it
    // if the scheduled fs need to be accessed through a dataproxy, try to get it
    // if any of the two fails, the scheduling operation fails
    Scheduler::PlacementArguments plctargs;
    plctargs.alreadyused_filesystems = &selectedfs;

    if (isRepair) {
      plctargs.bookingsize = bookingsize ? bookingsize : gOFS->getFuseBookingSize();
    } else {
      plctargs.bookingsize = isFuse ? gOFS->getFuseBookingSize() : bookingsize;
    }

    plctargs.dataproxys = &proxys;
    plctargs.firewallentpts = &firewalleps;
    plctargs.forced_scheduling_group_index = forced_group;
    plctargs.grouptag = containertag;
    plctargs.lid = layoutId;
    plctargs.inode = (ino64_t) fmd->getId();
    plctargs.path = path;
    plctargs.plctTrgGeotag = &targetgeotag;
    plctargs.plctpolicy = plctplcy;
    plctargs.exclude_filesystems = &excludefs;
    plctargs.selected_filesystems = &selectedfs;
    plctargs.spacename = &spacename;
    plctargs.truncate = open_flags & O_TRUNC;
    plctargs.vid = &vid;

    if (!plctargs.isValid()) {
      // there is something wrong in the arguments of file placement
      return Emsg(epname, error, EINVAL, "open - invalid placement argument", path);
    }

    if (!use_geoscheduler) {
      COMMONTIMING("PlctScheduler::FilePlacement", &tm);
      uint64_t n_replicas_ = eos::common::LayoutId::GetStripeNumber(layoutId) + 1;

      if (n_replicas_ > std::numeric_limits<uint8_t>::max()) {
        eos_err("msg=\"too many replicas requested\" n_replicas=%" PRIu64, n_replicas_);
        return Emsg(epname, error, EINVAL, "open - too many replicas requested", path);
      }

      uint8_t n_replicas = static_cast<uint8_t>(n_replicas_);
      placement::PlacementArguments args{n_replicas, placement::ConfigStatus::kRW, strategy};

      if (!excludefs.empty()) {
        args.excludefs = excludefs;
      }

      if (forced_group >= 0) {
        args.forced_group_index = forced_group;
      }

      auto ret = gOFS->mFsScheduler->schedule(spacename,
                                              args);
      COMMONTIMING("PlctScheduler::FilePlaced", &tm);

      if (ret.is_valid_placement(n_replicas)) {
        for (int i = 0; i < n_replicas; i++) {
          selectedfs.push_back(ret.ids[i]);
        }

        // TODO: this should be demoted to DEBUG once we have a proper understanding
        eos_info("msg=\"FlatScheduler selected filesystems\" fs=%s",
                 ret.result_string().c_str());
      } else {
        // Fallback to classic geoscheduler on failure
        eos_err("msg =\"no valid placement found with FlatScheduler\" ret=%d, err_msg=%s",
                ret.ret_code, ret.error_string().c_str());
        use_geoscheduler = true;
        gOFS->MgmStats.Add("FScheduler::Placement::Failed", vid.uid, vid.gid, 1);
      }
    }

    if (use_geoscheduler) {
      COMMONTIMING("Scheduler::FilePlacement", &tm);
      eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
      retc = Quota::FilePlacement(&plctargs);
      COMMONTIMING("Scheduler::FilePlaced", &tm);
    }

    // reshuffle the selectedfs by returning as first entry the lowest if the
    // sum of the fsid is odd the highest if the sum is even
    if (selectedfs.size() > 0) {
      std::vector<unsigned int> newselectedfs;
      auto result = std::minmax_element(selectedfs.begin(), selectedfs.end());
      int sum = std::accumulate(selectedfs.begin(), selectedfs.end(), 0);

      if ((sum % 2) == 0) {
        newselectedfs.push_back(*result.second);
      } else {
        newselectedfs.push_back(*result.first);
      }

      for (const auto& i : selectedfs) {
        if (i != newselectedfs.front()) {
          newselectedfs.push_back(i);
        }
      }

      selectedfs.swap(newselectedfs);
    }
  } else {
    // Access existing file - fill the vector with the existing locations
    for (unsigned int i = 0; i < fmd->getNumLocation(); i++) {
      auto loc = fmd->getLocation(i);

      if (loc != 0 && loc != eos::common::TAPE_FS_ID) {
        selectedfs.push_back(loc);
        excludefs.push_back(loc);
      }
    }

    eos::IFileMD::LocationVector unlinked = fmd->getUnlinkedLocations();

    for (auto loc : unlinked) {
      excludefs.push_back(loc);
    }

    if (selectedfs.empty()) {
      // this file has not a single existing replica
      gOFS->MgmStats.Add("OpenFileOffline", vid.uid, vid.gid, 1);
      // Fire and forget a sync::offline workflow event
      errno = 0;
      workflow.SetFile(path, mFid);
      const auto workflowType = openOpaque->Get("eos.workflow") != nullptr ?
                                openOpaque->Get("eos.workflow") : "default";
      std::string workflowErrorMsg;
      const auto ret_wfe = workflow.Trigger("sync::offline", std::string{workflowType},
                                            vid,
                                            ininfo, workflowErrorMsg);

      if (ret_wfe < 0 && errno == ENOKEY) {
        eos_debug("msg=\"no workflow defined for sync::offline\"");
      } else {
        eos_info("msg=\"workflow trigger returned\" retc=%d errno=%d event=\"sync::offline\"",
                 ret_wfe, errno);
      }

      return Emsg(epname, error, ENODEV, "open - no disk replica exists", path);
    }

    /// ###############
    // reconstruction opens files in RW mode but we actually need RO mode in this case
    /// ###############
    // if the client should go through a firewall entrypoint, try to get it
    // if the scheduled fs need to be accessed through a dataproxy, try to get it
    // if any of the two fails, the scheduling operation fails
    Scheduler::AccessArguments acsargs;
    acsargs.bookingsize = fmd->getSize();
    acsargs.dataproxys = &proxys;
    acsargs.firewallentpts = &firewalleps;
    acsargs.forcedfsid = forcedFsId;
    acsargs.forcedspace = space.c_str();
    acsargs.fsindex = &fsIndex;
    acsargs.isRW = isPioReconstruct ? false : isRW;
    acsargs.lid = layoutId;
    acsargs.inode = (ino64_t) fmd->getId();
    acsargs.locationsfs = &selectedfs;
    acsargs.tried_cgi = &tried_cgi;
    acsargs.unavailfs = &unavailfs;
    acsargs.vid = &vid;

    if (!acsargs.isValid()) {
      // there is something wrong in the arguments of file access
      return Emsg(epname, error, EINVAL, "open - invalid access argument", path);
    }

    {
      COMMONTIMING("Scheduler::FileAccess", &tm);
      eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
      retc = Scheduler::FileAccess(&acsargs);
      COMMONTIMING("Scheduler::FileAccessed", &tm);
    }

    if (acsargs.isRW) {
      // If this is an update, we don't have to send the client to cgi
      // excluded locations, we tell that the file is unreachable
      for (size_t k = 0; k < selectedfs.size(); k++) {
        // if the fs is available
        if (std::find(unavailfs.begin(), unavailfs.end(),
                      selectedfs[k]) != unavailfs.end()) {
          eos_info("msg=\"location %d is excluded as an unavailable filesystem"
                   " - returning ENETUNREACH\"", selectedfs[k]);
          retc = ENETUNREACH;
          break;
        }
      }
    }

    if ((retc == ENETUNREACH) || (retc == EROFS) || isRepair) {
      if (isRW && ((((fmd->getSize() == 0) && (bookingsize == 0)) || isRepair))) {
        // File-recreation due to offline/full file systems
        const char* containertag = 0;

        if (attrmap.count("user.tag")) {
          containertag = attrmap["user.tag"].c_str();
        }

        isCreation = true;
        /// ###############
        // if the client should go through a firewall entrypoint, try to get it
        // if the scheduled fs need to be accessed through a dataproxy, try to get it
        // if any of the two fails, the scheduling operation fails
        Scheduler::PlacementArguments plctargs;
        plctargs.alreadyused_filesystems = &excludefs;
        plctargs.bookingsize = bookingsize;
        plctargs.dataproxys = &proxys;
        plctargs.firewallentpts = &firewalleps;
        plctargs.forced_scheduling_group_index = forced_group;
        plctargs.grouptag = containertag;
        plctargs.lid = layoutId;
        plctargs.inode = (ino64_t) fmd->getId();
        plctargs.path = path;
        plctargs.plctTrgGeotag = &targetgeotag;
        plctargs.plctpolicy = plctplcy;
        plctargs.exclude_filesystems = &excludefs;
        plctargs.selected_filesystems = &selectedfs;
        std::string spacename = space.c_str();
        plctargs.spacename = &spacename;
        plctargs.truncate = open_flags & O_TRUNC;
        plctargs.vid = &vid;

        if (!plctargs.isValid()) {
          // there is something wrong in the arguments of file placement
          return Emsg(epname, error, EINVAL, "open - invalid placement argument", path);
        }

        if (!use_geoscheduler) {
          COMMONTIMING("PlctScheduler::FilePlacement", &tm);
          uint8_t n_replicas = eos::common::LayoutId::GetStripeNumber(layoutId) + 1;
          placement::PlacementArguments args{n_replicas, placement::ConfigStatus::kRW, strategy};

          if (!excludefs.empty()) {
            args.excludefs = excludefs;
          }

          if (forced_group >= 0) {
            args.forced_group_index = forced_group;
          }

          auto ret = gOFS->mFsScheduler->schedule(spacename,
                                                  args);
          COMMONTIMING("PlctScheduler::FilePlaced", &tm);

          if (ret.is_valid_placement(n_replicas)) {
            for (int i = 0; i < n_replicas; i++) {
              selectedfs.push_back(ret.ids[i]);
            }
          } else {
            eos_info("msg =\"no valid placement found with FSScheduler\" ret=%d, err_msg=%s",
                     ret.ret_code, ret.error_string().c_str());
            use_geoscheduler = true;
          }
        }

        if (use_geoscheduler) {
          COMMONTIMING("Scheduler::FilePlacement", &tm);
          eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
          retc = Quota::FilePlacement(&plctargs);
          COMMONTIMING("Scheduler::FilePlaced", &tm);
        }

        eos_info("msg=\"file-recreation due to offline/full locations\" path=%s retc=%d",
                 path, retc);
        isRecreation = true;
        // end scope fs_rd_lock
      } else {
        // Normal read failed, try to reply with the triedrc value if this
        // exists in the URL otherwise we'll return ENETUNREACH which is a
        // client recoverable error.
        char* triedrc = openOpaque->Get("triedrc");

        if (triedrc) {
          int errno_tried = GetTriedrcErrno(triedrc);

          if (errno_tried) {
            return Emsg(epname, error, errno_tried, "open file", path);
          }
        }
      }
    }

    if (retc == EXDEV) {
      // Indicating that the layout requires the replacement of stripes
      retc = 0; // TODO: we currently don't support repair on the fly mode
    }
  }

  LogSchedulingInfo(selectedfs, proxys, firewalleps);

  if (retc) {
    // If we don't have quota we don't bounce the client back
    if ((retc != ENOSPC) && (retc != EDQUOT)) {
      // INLINE Workflows
      int stalltime = 0;
      workflow.SetFile(path, fmd->getId());
      std::string errorMsg;

      if ((stalltime = workflow.Trigger("open", "enonet", vid, ininfo,
                                        errorMsg)) > 0) {
        eos_info("msg=\"triggered ENOENT workflow\" path=%s", path);
        return gOFS->Stall(error, stalltime, ""
                           "File is currently unavailable - triggered workflow!");
      }

      // check if we have a global redirect or stall for offline files
      MAYREDIRECT_ENONET;
      MAYSTALL_ENONET;
      MAYREDIRECT_ENETUNREACH;
      MAYSTALL_ENETUNREACH;

      // check if the dir attributes tell us to let clients rebounce
      if (attrmap.count("sys.stall.unavailable")) {
        int stalltime = atoi(attrmap["sys.stall.unavailable"].c_str());

        if (stalltime) {
          // stall the client
          gOFS->MgmStats.Add("OpenStalled", vid.uid, vid.gid, 1);
          eos_info("attr=sys info=\"stalling file since replica's are down\" path=%s rw=%d",
                   path, isRW);
          return gOFS->Stall(error, stalltime,
                             "Required filesystems are currently unavailable!");
        }
      }

      if (attrmap.count("user.stall.unavailable")) {
        int stalltime = atoi(attrmap["user.stall.unavailable"].c_str());

        if (stalltime) {
          // stall the client
          gOFS->MgmStats.Add("OpenStalled", vid.uid, vid.gid, 1);
          eos_info("attr=user info=\"stalling file since replica's are down\" path=%s rw=%d",
                   path, isRW);
          return gOFS->Stall(error, stalltime,
                             "Required filesystems are currently unavailable!");
        }
      }

      if ((attrmap.count("sys.redirect.enonet"))) {
        // there is a redirection setting here if files are unaccessible
        redirectionhost = "";
        redirectionhost = attrmap["sys.redirect.enonet"].c_str();
        int portpos = 0;

        if ((portpos = redirectionhost.find(":")) != STR_NPOS) {
          XrdOucString port = redirectionhost;
          port.erase(0, portpos + 1);
          ecode = atoi(port.c_str());
          redirectionhost.erase(portpos);
        } else {
          ecode = 1094;
        }

        if (!gOFS->SetRedirectionInfo(error, redirectionhost.c_str(), ecode)) {
          eos_err("msg=\"failed setting redirection\" path=\"%s\"", path);
          return SFS_ERROR;
        }

        rcode = SFS_REDIRECT;
        gOFS->MgmStats.Add("RedirectENONET", vid.uid, vid.gid, 1);
        return rcode;
      }

      if (!gOFS->mMaster->IsMaster() && gOFS->mMaster->IsRemoteMasterOk()) {
        // Redirect ENONET to the actual master
        int port {0};
        std::string hostname;
        std::string master_id = gOFS->mMaster->GetMasterId();

        if (!eos::common::ParseHostNamePort(master_id, hostname, port)) {
          eos_err("msg=\"failed parsing remote master info\", id=%s",
                  master_id.c_str());
          return Emsg(epname, error, retc, "open file - failed parsing remote "
                      "master info", path);
        }

        redirectionhost = hostname.c_str();
        ecode = port;

        if (!gOFS->SetRedirectionInfo(error, redirectionhost.c_str(), ecode)) {
          eos_err("msg=\"failed setting redirection\" path=\"%s\"", path);
          return SFS_ERROR;
        }

        rcode = SFS_REDIRECT;
        gOFS->MgmStats.Add("RedirectENONET", vid.uid, vid.gid, 1);
        return rcode;
      }

      gOFS->MgmStats.Add("OpenFileOffline", vid.uid, vid.gid, 1);
    } else {
      // Remove the created file from the namespace as root since somebody could
      // have a no-delete ACL. Do this only if there are no replicas already
      // attached to the file md entry. If there are, this means the current
      // thread was blocked in scheduling and a retry of the client went
      // through successfully. If we delete the entry we end up with data lost.
      if (isCreation) {
        bool do_remove = false;

        try {
          eos::Prefetcher::prefetchFileMDAndWait(gOFS->eosView, creation_path.c_str());
          eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex);
          auto tmp_fmd = gOFS->eosView->getFile(creation_path.c_str());

          if (isAtomicUpload || (tmp_fmd->getNumLocation() == 0)) {
            do_remove = true;
          }
        } catch (eos::MDException& e) {
          if (isAtomicUpload) {
            do_remove = true;
          }
        }

        if (do_remove) {
          eos::common::VirtualIdentity vidroot = eos::common::VirtualIdentity::Root();
          gOFS->_rem(creation_path.c_str(), error, vidroot, 0, false, false, false);
        }
      }

      gOFS->MgmStats.Add("OpenFailedQuota", vid.uid, vid.gid, 1);
    }

    if (isRW) {
      if (retc == ENOSPC) {
        return Emsg(epname, error, retc, "get free physical space", path);
      }

      if (retc == EDQUOT) {
        return Emsg(epname, error, retc,
                    "get quota space - quota not defined or exhausted", path);
      }

      return Emsg(epname, error, retc, "access quota space", path);
    } else {
      return Emsg(epname, error, retc, "open file ", path);
    }
  } else {
    if (isRW) {
      // we want to define the order of chunks during creation, so we attach also rain layouts
      if (isCreation && hasClientBookingSize && ((bookingsize == 0) ||
          ocUploadUuid.length() || (LayoutId::IsRain(layoutId)))) {
        // ---------------------------------------------------------------------
        // if this is a creation we commit the scheduled replicas NOW
        // we do the same for chunked/parallel uploads
        // ---------------------------------------------------------------------
        {
          // get an empty file checksum
          std::string binchecksum = LayoutId::GetEmptyFileChecksum(layoutId);
          eos::Buffer cx;
          cx.putData(binchecksum.c_str(), binchecksum.size());

          // FUSEX repair access needs to retrieve the file by fid
          // TODO: Refactor isCreation and isRecreation code paths
          //eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);
          // -------------------------------------------------------------------

          try {
            std::string locations;

            if (fmd->hasAttribute("sys.fs.tracking")) {
              locations = fmd->getAttribute("sys.fs.tracking");
            }

            if (isRecreation) {
              fmd->unlinkAllLocations();
              locations += "=";
            }

            if (isRecreation) {
              std::string s;

              if (fmd->hasAttribute("sys.fusex.state")) {
                s = fmd->getAttribute("sys.fusex.state");
              }

              s += "Z";
              fmd->setAttribute("sys.fusex.state",
                                eos::common::StringConversion::ReduceString(s).c_str());
            }

            for (auto& fsid : selectedfs) {
              fmd->addLocation(fsid);
              locations += "+";
              locations += std::to_string(fsid);
            }

            fmd->setAttribute("sys.fs.tracking",
                              eos::common::StringConversion::ReduceString(locations).c_str());
            fmd->setChecksum(cx);
            gOFS->eosView->updateFileStore(fmd.get());
          } catch (eos::MDException& e) {
            errno = e.getErrno();
            std::string errmsg = e.getMessage().str();
            eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                      e.getErrno(), e.getMessage().str().c_str());
            gOFS->MgmStats.Add("OpenFailedQuota", vid.uid, vid.gid, 1);
            return Emsg(epname, error, errno, "open file", errmsg.c_str());
          }

          // -------------------------------------------------------------------
        }
        mIsZeroSize = true;
      }

      if (isFuse && !isCreation) {
        // ---------------------------------------------------------------------
        // if we come from fuse for an update
        // consistently redirect to the highest fsid having if possible the same
        // geotag as the client
        // ---------------------------------------------------------------------
        if (byfid) {
          // the new FUSE client needs to have the replicas attached after the
          // first open call
          std::string locations;

          try {
            if (fmd->hasAttribute("sys.fs.tracking")) {
              locations = fmd->getAttribute("sys.fs.tracking");
            }

            for (auto& fsid : selectedfs) {
              fmd->addLocation(fsid);
              locations += "+";
              locations += std::to_string(fsid);
            }

            gOFS->eosView->updateFileStore(fmd.get());
          } catch (eos::MDException& e) {
            errno = e.getErrno();
            std::string errmsg = e.getMessage().str();
            eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                      e.getErrno(), e.getMessage().str().c_str());
            gOFS->MgmStats.Add("OpenFailedQuota", vid.uid, vid.gid, 1);
            return Emsg(epname, error, errno, "open file", errmsg.c_str());
          }
        }

        eos::common::FileSystem::fsid_t fsid = 0;
        fsIndex = 0;
        std::string fsgeotag;
        {
          eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);

          for (size_t k = 0; k < selectedfs.size(); k++) {
            auto filesystem = FsView::gFsView.mIdView.lookupByID(selectedfs[k]);
            fsgeotag = "";

            if (filesystem) {
              fsgeotag = filesystem->GetString("stat.geotag");
            }

            // if the fs is available
            if (std::find(unavailfs.begin(), unavailfs.end(), selectedfs[k]) ==
                unavailfs.end()) {
              // take the highest fsid with the same geotag if possible
              if ((vid.geolocation.empty() ||
                   (fsgeotag.find(vid.geolocation) != std::string::npos)) &&
                  (selectedfs[k] > fsid)) {
                fsIndex = k;
                fsid = selectedfs[k];
              }
            }
          }
        } // fs_rd_lock scope

        // if the client has a geotag which does not match any of the fs's
        if (!fsIndex) {
          fsid = 0;

          for (size_t k = 0; k < selectedfs.size(); k++)
            if (selectedfs[k] > fsid) {
              fsIndex = k;
              fsid = selectedfs[k];
            }
        }

        // EOS-2787
        // reshuffle the selectedfs to set if available the highest with matching geotag in front
        if (fsid) {
          std::vector<unsigned int> newselectedfs;
          newselectedfs.push_back(fsid);

          for (const auto& i : selectedfs) {
            if (i != newselectedfs.front()) {
              newselectedfs.push_back(i);
            }
          }

          selectedfs.swap(newselectedfs);
          fsIndex = 0;
        }
      }
    } else {
      if (!fmd->getSize()) {
        // 0-size files can be read from the MGM if this is not FUSE access and
        // also if this is not a rain file
        if (!isFuse && !LayoutId::IsRain(layoutId)) {
          mIsZeroSize = true;
          return SFS_OK;
        }
      }
    }
  }

  // If this is a RAIN layout, we want a nice round-robin for the entry
  // server since it  has the burden of encoding and traffic fan-out
  if (isRW && LayoutId::IsRain(layoutId)) {
    fsIndex = mFid % selectedfs.size();
    eos_static_info("msg=\"selecting entry-server\" fsIndex=%lu fsid=%lu "
                    "fxid=%97llx mod=%lu", fsIndex, selectedfs[fsIndex],
                    mFid, selectedfs.size());
  }

  // If behaviour enabled then add preference to always select the file system
  // with the lowest fsid as the entry point for rain read/recover operations
  if (gOFS->mBehaviourCfg->Exists(eos::common::BehaviourType::RainMinFsidEntry)) {
    fsIndex = EnforceRainMinFsidEntry(selectedfs);
  }

  // Get the redirection host from the selected entry in the vector
  if (!selectedfs[fsIndex]) {
    eos_err("msg=\"0 filesystem in selection\" fxid=%08llx", mFid);
    return Emsg(epname, error, ENETUNREACH, "received filesystem id 0", path);
  }

  XrdOucString piolist = "";
  XrdOucString infolog = "";
  std::string fs_hostport, fs_host, fs_port, fs_http_port, fs_prefix,
      fs_host_alias, fs_port_alias;
  uint32_t fs_id;
  {
    eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
    auto filesystem = FsView::gFsView.mIdView.lookupByID(selectedfs[fsIndex]);

    if (!filesystem) {
      return Emsg(epname, error, ENETUNREACH,
                  "received non-existent filesystem", path);
    }

    fs_hostport = filesystem->GetString("hostport");
    fs_host = filesystem->GetString("host");
    fs_port = filesystem->GetString("port");
    fs_host_alias = filesystem->GetString("stat.alias.host");
    fs_port_alias = filesystem->GetString("stat.alias.port");

    // allow FST host alias
    if (fs_host_alias.length()) {
      fs_host = fs_host_alias;

      if (fs_port_alias.length()) {
        fs_port = fs_port_alias;
      }

      fs_hostport = fs_host + std::string(":") + fs_port;
      eos_info("redirection-alias=\"%s:%s\"", fs_host_alias.c_str(),
               fs_port_alias.c_str());
    }

    fs_http_port = filesystem->GetString("stat.http.port");
    fs_prefix = filesystem->GetPath();
    fs_id = filesystem->GetId();
  } // fs_rd_lock scope

  // Set the FST gateway for clients who are geo-tagged with default
  if ((firewalleps.size() > fsIndex) && (proxys.size() > fsIndex)) {
    // Do this with forwarding proxy syntax only if the firewall entrypoint is
    // different from the endpoint
    if (!(firewalleps[fsIndex].empty()) &&
        ((!proxys[fsIndex].empty() && firewalleps[fsIndex] != proxys[fsIndex]) ||
         (firewalleps[fsIndex] != fs_hostport))) {
      // Build the URL for the forwarding proxy and must have the following
      // redirection proxy:port?eos.fstfrw=endpoint:port/abspath
      auto idx = firewalleps[fsIndex].rfind(':');

      if (idx != std::string::npos) {
        targethost = firewalleps[fsIndex].substr(0, idx).c_str();
        targetport = atoi(firewalleps[fsIndex].substr(idx + 1,
                          std::string::npos).c_str());
        targethttpport = 8001;
      } else {
        targethost = firewalleps[fsIndex].c_str();
        targetport = 0;
        targethttpport = 8001;
      }

      std::ostringstream oss;
      oss << targethost << "?" << "eos.fstfrw=";

      // Check if we have to redirect to the fs host or to a proxy
      if (proxys[fsIndex].empty()) {
        oss << fs_host << ":" << fs_port;
      } else {
        oss << proxys[fsIndex];
      }

      redirectionhost = oss.str().c_str();
      redirectionhost += "&";
    } else {
      if (proxys[fsIndex].empty()) { // there is no proxy to use
        targethost  = fs_host.c_str();
        targetport  = atoi(fs_port.c_str());
        targethttpport  = atoi(fs_http_port.c_str());

        // default xrootd & http port
        if (!targetport) {
          targetport = 1095;
        }

        if (!targethttpport) {
          targethttpport = 8001;
        }
      } else { // we have a proxy to use
        auto idx = proxys[fsIndex].rfind(':');

        if (idx != std::string::npos) {
          targethost = proxys[fsIndex].substr(0, idx).c_str();
          targetport = atoi(proxys[fsIndex].substr(idx + 1, std::string::npos).c_str());
          targethttpport = 8001;
        } else {
          targethost = proxys[fsIndex].c_str();
          targetport = 0;
          targethttpport = 0;
        }
      }

      redirectionhost = targethost;
      redirectionhost += "?";
    }

    if (!proxys[fsIndex].empty()) {
      if (!(fs_prefix.empty())) {
        XrdOucString s = "mgm.fsprefix";
        s += "=";
        s += fs_prefix.c_str();
        s.replace(":", "#COL#");
        redirectionhost += s;
      }
    }
  } else {
    // There is no proxy or firewall entry point to use
    targethost  = fs_host.c_str();
    targetport  = atoi(fs_port.c_str());
    targethttpport  = atoi(fs_http_port.c_str());
    redirectionhost = targethost;
    redirectionhost += "?";
  }

  // ---------------------------------------------------------------------------
  // Rebuild the layout ID (for read it should indicate only the number of
  // available stripes for reading);
  // For 'pio' mode we hand out plain layouts to the client and add the IO
  // layout as an extra field
  // ---------------------------------------------------------------------------
  // Get the unique set of file systems
  std::set<unsigned int> ufs(selectedfs.begin(), selectedfs.end());
  ufs.insert(pio_reconstruct_fs.begin(), pio_reconstruct_fs.end());
  // If file system 0 sentinel is present then it must be removed
  ufs.erase(0u);
  new_lid = LayoutId::GetId(
              isPio ? LayoutId::kPlain :
              LayoutId::GetLayoutType(layoutId),
              (isPio ? LayoutId::kNone :
               LayoutId::GetChecksum(layoutId)),
              isPioReconstruct ? static_cast<int>(ufs.size()) : static_cast<int>
              (selectedfs.size()),
              LayoutId::GetBlocksizeType(layoutId),
              LayoutId::GetBlockChecksum(layoutId));

  // For RAIN layouts we need to keep the original number of stripes since this
  // is used to compute the different groups and block sizes in the FSTs
  if ((LayoutId::IsRain(layoutId))) {
    LayoutId::SetStripeNumber(new_lid,
                              LayoutId::GetStripeNumber(layoutId));
  }

  capability += "&mgm.lid=";
  capability += static_cast<int>(new_lid);
  // space to be prebooked/allocated
  capability += "&mgm.bookingsize=";

  if (isPioReconstruct) {
    // For pio reconstruct the booking size needs to be 0,
    // the recovery will fail on non xfs filesystem otherwise.
    capability += "0";
  } else {
    capability +=
      eos::common::StringConversion::GetSizeString(sizestring, bookingsize);
  }

  if (minimumsize) {
    capability += "&mgm.minsize=";
    capability += eos::common::StringConversion::GetSizeString(sizestring,
                  minimumsize);
  }

  if (maximumsize) {
    capability += "&mgm.maxsize=";
    capability += eos::common::StringConversion::GetSizeString(sizestring,
                  maximumsize);
  }

  // Expected size of the target file on close
  if (targetsize) {
    capability += "&mgm.targetsize=";
    capability += eos::common::StringConversion::GetSizeString(sizestring,
                  targetsize);
  }

  if (LayoutId::GetLayoutType(layoutId) == LayoutId::kPlain) {
    capability += "&mgm.fsid=";
    capability += (int) fs_id;
  }

  if (isRepairRead) {
    capability += "&mgm.repairread=1";
  }

  if (mIsZeroSize) {
    capability += "&mgm.zerosize=1";
  }

  // Add the store flag for RAIN reconstruct jobs
  if (isPioReconstruct) {
    capability += "&mgm.rain.store=1";
    // Append also the mgm.rain.size since we can't deduce at the FST during
    // the recovery step and we need it for the stat information
    capability += "&mgm.rain.size=";
    capability += std::to_string(fmdsize).c_str();
  }

  if (bandwidth.length() && (bandwidth != "0")) {
    capability += "&mgm.iobw=";
    capability += bandwidth.c_str();
  }

  if ((LayoutId::GetLayoutType(layoutId) == LayoutId::kReplica) ||
      (LayoutId::IsRain(layoutId))) {
    capability += "&mgm.fsid=";
    capability += (int) fs_id;
    eos::mgm::FileSystem* repfilesystem = 0;
    replacedfs.resize(selectedfs.size());

    // If replacement has been specified try to get new locations for
    // reconstruction or for missing stripes
    if (isPioReconstruct && !(pio_reconstruct_fs.empty())) {
      const char* containertag = 0;

      if (attrmap.count("user.tag")) {
        containertag = attrmap["user.tag"].c_str();
      }

      // Get the scheduling group of one of the stripes
      if (fmd->getNumLocation() == 0) {
        eos_err("msg=\"no locations available for file\"");
        return Emsg(epname, error, EIO, "get any locations for file", path);
      }

      eos::common::FileSystem::fs_snapshot_t orig_snapshot;
      unsigned int orig_id = fmd->getLocation(0);
      {
        eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
        // Note this is a eos::common::filesystem not a mgm one
        auto orig_fs = FsView::gFsView.mIdView.lookupByID(orig_id);

        if (!orig_fs) {
          return Emsg(epname, error, EINVAL, "reconstruct filesystem", path);
        }

        orig_fs->SnapShotFileSystem(orig_snapshot);
      } // fs_rd_lock
      forced_group = orig_snapshot.mGroupIndex;
      // Add new stripes if file doesn't have the nomial number
      auto stripe_diff = (LayoutId::GetStripeNumber(fmd->getLayoutId()) + 1) -
                         selectedfs.size();
      // Create a plain layout with the number of replacement stripes to be
      // scheduled in the file placement routine
      unsigned long plain_lid = new_lid;

      if (pio_reconstruct_fs.find(0) != pio_reconstruct_fs.end()) {
        LayoutId::SetStripeNumber(plain_lid, stripe_diff - 1);
      } else {
        LayoutId::SetStripeNumber(plain_lid,
                                  pio_reconstruct_fs.size() - 1 + stripe_diff);
      }

      eos_info("msg=\"nominal stripes:%d reconstructed stripes=%d group_idx=%d\"",
               LayoutId::GetStripeNumber(new_lid) + 1,
               LayoutId::GetStripeNumber(plain_lid) + 1,
               forced_group);
      // Compute the size of the stripes to be placed
      unsigned long num_data_stripes = LayoutId::GetStripeNumber(layoutId) + 1 -
                                       LayoutId::GetRedundancyStripeNumber(layoutId);
      uint64_t plain_book_sz = (uint64_t)std::ceil((float)fmd->getSize() /
                               LayoutId::GetBlocksize(layoutId));
      plain_book_sz = std::ceil((float) plain_book_sz / std::pow(num_data_stripes,
                                2)) *
                      num_data_stripes * LayoutId::GetBlocksize(layoutId) + LayoutId::OssXsBlockSize;
      eos_info("msg=\"plain booking size is %llu", plain_book_sz);
      eos::common::VirtualIdentity rootvid = eos::common::VirtualIdentity::Root();
      // Attempt to use a firewall entrypoint or a dataproxy if required, if any
      // of the two fail, then scheduling fails
      Scheduler::PlacementArguments plctargs;
      plctargs.alreadyused_filesystems = &selectedfs;
      plctargs.bookingsize = plain_book_sz;
      plctargs.dataproxys = &proxys;
      plctargs.firewallentpts = &firewalleps;
      plctargs.forced_scheduling_group_index = forced_group;
      plctargs.grouptag = containertag;
      plctargs.lid = plain_lid;
      plctargs.inode = (ino64_t) fmd->getId();
      plctargs.path = path;
      plctargs.plctTrgGeotag = &targetgeotag;
      plctargs.plctpolicy = plctplcy;
      plctargs.exclude_filesystems = &excludefs;
      plctargs.selected_filesystems = &pio_replacement_fs;
      std::string spacename = space.c_str();
      plctargs.spacename = &spacename;
      plctargs.truncate = false;
      plctargs.vid = &rootvid;

      if (!plctargs.isValid()) {
        return Emsg(epname, error, EIO, "open - invalid placement argument", path);
      }

      COMMONTIMING("Scheduler::FilePlacement", &tm);
      {
        eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
        retc = Quota::FilePlacement(&plctargs);
      }
      COMMONTIMING("Scheduler::FilePlaced", &tm);
      LogSchedulingInfo(selectedfs, proxys, firewalleps);

      if (retc) {
        gOFS->MgmStats.Add("OpenFailedReconstruct", rootvid.uid, rootvid.gid, 1);
        return Emsg(epname, error, retc, "schedule stripes for reconstruction", path);
      }

      for (const auto& elem : pio_replacement_fs) {
        eos_debug("msg=\"reconstruction scheduled on new fs\" fsid=%lu num=%lu",
                  elem, pio_replacement_fs.size());
      }

      auto selection_diff = (LayoutId::GetStripeNumber(fmd->getLayoutId()) + 1)
                            - selectedfs.size();
      eos_info("msg=\"fs selection summary\" nominal=%d actual=%d diff=%d",
               (LayoutId::GetStripeNumber(fmd->getLayoutId()) + 1),
               selectedfs.size(), selection_diff);

      // If there are stripes missing then fill them in from the replacements
      if (pio_replacement_fs.size() < selection_diff) {
        eos_err("msg=\"not enough replacement fs\" need=%lu have=%lu",
                selection_diff, pio_replacement_fs.size());
        return Emsg(epname, error, retc, "schedule enough stripes for reconstruction",
                    path);
      }

      for (size_t i = 0; i < selection_diff; ++i) {
        selectedfs.push_back(pio_replacement_fs.back());
        pio_replacement_fs.pop_back();
      }
    }

    replacedfs.resize(selectedfs.size());
    {
      // Put all the replica urls into the capability,
      // this is all under a view lock
      eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);

      for (unsigned int i = 0; i < selectedfs.size(); ++i) {
        if (!selectedfs[i]) {
          eos_err("%s", "msg=\"fsid 0 in replica vector\"");
        }

        // Logic to discover filesystems to be reconstructed
        bool replace = false;

        if (isPioReconstruct) {
          replace = (pio_reconstruct_fs.find(selectedfs[i]) != pio_reconstruct_fs.end());
        }

        if (replace) {
          // If we don't find any replacement
          if (pio_replacement_fs.empty()) {
            return Emsg(epname, error, EIO, "get replacement file system", path);
          }

          // Take one replacement filesystem from the replacement list
          replacedfs[i] = selectedfs[i];
          selectedfs[i] = pio_replacement_fs.back();
          pio_replacement_fs.pop_back();
          eos_info("msg=\"replace fs\" old-fsid=%u new-fsid=%u", replacedfs[i],
                   selectedfs[i]);
        } else {
          // There is no replacement happening
          replacedfs[i] = 0;
        }

        repfilesystem = FsView::gFsView.mIdView.lookupByID(selectedfs[i]);

        if (!repfilesystem) {
          // Don't fail IO on a shadow file system but throw a critical error
          // message
          eos_crit("msg=\"Unable to get replica filesystem information\" "
                   "path=\"%s\" fsid=%d", path, selectedfs[i]);
          continue;
        }

        if (replace) {
          fsIndex = i;

          // Set the FST gateway if this is available otherwise the actual FST
          if ((firewalleps.size() > fsIndex) && (proxys.size() > fsIndex) &&
              !(firewalleps[fsIndex].empty()) &&
              ((!proxys[fsIndex].empty() && firewalleps[fsIndex] != proxys[fsIndex]) ||
               (firewalleps[fsIndex] != repfilesystem->GetString("hostport")))) {
            // Build the URL for the forwarding proxy and must have the following
            // redirection proxy:port?eos.fstfrw=endpoint:port/abspath
            auto idx = firewalleps[fsIndex].rfind(':');

            if (idx != std::string::npos) {
              targethost = firewalleps[fsIndex].substr(0, idx).c_str();
              targetport = atoi(firewalleps[fsIndex].substr(idx + 1,
                                std::string::npos).c_str());
              targethttpport = 8001;
            } else {
              targethost = firewalleps[fsIndex].c_str();
              targetport = 0;
              targethttpport = 0;
            }

            std::ostringstream oss;
            oss << targethost << "?"
                << "eos.fstfrw=";

            // check if we have to redirect to the fs host or to a proxy
            if (proxys[fsIndex].empty()) {
              oss << repfilesystem->GetString("host").c_str() << ":"
                  << repfilesystem->GetString("port").c_str();
            } else {
              oss << proxys[fsIndex];
            }

            redirectionhost = oss.str().c_str();
          } else {
            if ((proxys.size() > fsIndex) && !proxys[fsIndex].empty()) {
              // We have a proxy to use
              (void) proxys[fsIndex].c_str();
              auto idx = proxys[fsIndex].rfind(':');

              if (idx != std::string::npos) {
                targethost = proxys[fsIndex].substr(0, idx).c_str();
                targetport = atoi(proxys[fsIndex].substr(idx + 1, std::string::npos).c_str());
                targethttpport = 8001;
              } else {
                targethost = proxys[fsIndex].c_str();
                targetport = 0;
                targethttpport = 0;
              }
            } else {
              // There is no proxy to use
              targethost  = repfilesystem->GetString("host").c_str();
              targetport  = atoi(repfilesystem->GetString("port").c_str());
              targethttpport  = atoi(repfilesystem->GetString("stat.http.port").c_str());
            }

            redirectionhost = targethost;
            redirectionhost += "?";
          }

          // point at the right vector entry
          fsIndex = i;
        }

        capability += "&mgm.url";
        capability += (int) i;
        capability += "=root://";
        XrdOucString replicahost = "";
        int replicaport = 0;

        // -----------------------------------------------------------------------
        // Logic to mask 'offline' filesystems
        // -----------------------------------------------------------------------
        for (unsigned int k = 0; k < unavailfs.size(); ++k) {
          if (selectedfs[i] == unavailfs[k]) {
            replicahost = "__offline_";
            break;
          }
        }

        if ((proxys.size() > i) && !proxys[i].empty()) {
          // We have a proxy to use
          auto idx = proxys[i].rfind(':');

          if (idx != std::string::npos) {
            replicahost = proxys[i].substr(0, idx).c_str();
            replicaport =
              atoi(proxys[i].substr(idx + 1, std::string::npos).c_str());
          } else {
            replicahost = proxys[i].c_str();
            replicaport = 0;
          }
        } else {
          // There is no proxy to use
          replicahost += repfilesystem->GetString("host").c_str();
          replicaport = atoi(repfilesystem->GetString("port").c_str());
        }

        capability += replicahost;
        capability += ":";
        capability += replicaport;
        capability += "//";
        // add replica fsid
        capability += "&mgm.fsid";
        capability += (int)i;
        capability += "=";
        capability += (int)repfilesystem->GetId();

        if ((proxys.size() > i) && !proxys[i].empty()) {
          std::string fsprefix = repfilesystem->GetPath();

          if (!fsprefix.empty()) {
            XrdOucString s = "mgm.fsprefix";
            s += (int)i;
            s += "=";
            s += fsprefix.c_str();
            s.replace(":", "#COL#");
            capability += s;
          }
        }

        if (isPio) {
          if (replacedfs[i]) {
            // Add the drop message to the replacement capability
            capability += "&mgm.drainfsid";
            capability += (int)i;
            capability += "=";
            capability += (int)replacedfs[i];
          }

          piolist += "pio.";
          piolist += (int)i;
          piolist += "=";
          piolist += replicahost;
          piolist += ":";
          piolist += replicaport;
          piolist += "&";
        }

        eos_debug("msg=\"redirection url\" %d => %s", i, replicahost.c_str());
        infolog += "target[";
        infolog += (int)i;
        infolog += "]=(";
        infolog += replicahost.c_str();
        infolog += ",";
        infolog += (int)repfilesystem->GetId();
        infolog += ") ";
      }
    } // fs_rd_lock
  }

  // ---------------------------------------------------------------------------
  // Encrypt capability
  // ---------------------------------------------------------------------------
  XrdOucEnv incapability(capability.c_str());
  eos::common::SymKey* symkey = eos::common::gSymKeyStore.GetCurrentKey();
  eos_debug("capability=%s\n", capability.c_str());
  int caprc = 0;
  XrdOucEnv* capabilityenvRaw = nullptr;

  if ((caprc = eos::common::SymKey::CreateCapability(&incapability,
               capabilityenvRaw,
               symkey, gOFS->mCapabilityValidity))) {
    return Emsg(epname, error, caprc, "sign capability", path);
  }

  std::unique_ptr<XrdOucEnv> capabilityenv(capabilityenvRaw);
  int caplen = 0;

  if (isPio) {
    redirectionhost = piolist;
    redirectionhost += "mgm.lid=";
    redirectionhost += static_cast<int>(layoutId);
    redirectionhost += "&mgm.logid=";
    redirectionhost += this->logId;
    redirectionhost += capabilityenv->Env(caplen);
  } else {
    redirectionhost += capabilityenv->Env(caplen);
    redirectionhost += "&mgm.logid=";
    redirectionhost += this->logId;

    if (openOpaque->Get("eos.blockchecksum")) {
      redirectionhost += "&mgm.blockchecksum=";
      redirectionhost += openOpaque->Get("eos.blockchecksum");
    } else {
      if ((!isRW) && (LayoutId::GetLayoutType(layoutId) == LayoutId::kReplica)) {
        redirectionhost += "&mgm.blockchecksum=ignore";
      }
    }

    if (openOpaque->Get("eos.checksum") || openOpaque->Get("eos.cloneid")) {
      redirectionhost += "&mgm.checksum=";
      redirectionhost += openOpaque->Get("eos.checksum");
    }

    if (openOpaque->Get("eos.mtime")) {
      redirectionhost += "&mgm.mtime=0";
    }

    // For the moment we redirect only on storage nodes
    redirectionhost += "&mgm.replicaindex=";
    redirectionhost += (int) fsIndex;
    redirectionhost += "&mgm.replicahead=";
    redirectionhost += (int) fsIndex;
  }

  if (vid.prot == "https") {
    struct stat buf;
    std::string etag;
    eos::common::VirtualIdentity rootvid = eos::common::VirtualIdentity::Root();
    // get the current ETAG
    gOFS->_stat(path, &buf, error, rootvid, "", &etag);
    redirectionhost += "&mgm.etag=";

    if (!etag.length()) {
      redirectionhost += "undef";
    } else {
      redirectionhost += etag.c_str();
    }
  }

  // add the MGM hex id for this file
  redirectionhost += "&mgm.id=";
  redirectionhost += hex_fid.c_str();

  if (isFuse) {
    redirectionhost += "&mgm.mtime=0";
  } else {
    if (!isRW)  {
      eos::IFileMD::ctime_t mtime;

      try {
        fmd->getMTime(mtime);
        redirectionhost += "&mgm.mtime=";
        std::string smtime;
        smtime += std::to_string(mtime.tv_sec);
        redirectionhost += smtime.c_str();
      } catch (eos::MDException& ex) {
      }
    }
  }

  // Also trigger synchronous create workflow event if it's defined
  if (isCreation) {
    errno = 0;
    workflow.SetFile(path, mFid);
    auto workflowType = openOpaque->Get("eos.workflow") != nullptr ?
                        openOpaque->Get("eos.workflow") : "default";
    std::string errorMsg;
    auto ret_wfe = workflow.Trigger("sync::create", std::string{workflowType}, vid,
                                    ininfo, errorMsg);

    if (ret_wfe < 0 && errno == ENOKEY) {
      eos_debug("msg=\"no workflow defined for sync::create\"");
    } else {
      eos_info("msg=\"workflow trigger returned\" retc=%d errno=%d", ret_wfe, errno);

      if (ret_wfe != 0) {
        // Remove the file from the namespace in this case
        try {
          eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);
          gOFS->eosView->removeFile(fmd.get());
        } catch (eos::MDException& ex) {
          eos_err("Failed to remove file from namespace in case of create workflow error. Reason: %s",
                  ex.what());
        }

        return Emsg(epname, error, ret_wfe, errorMsg.c_str(),
                    path);
      }
    }
  }

  // add workflow cgis, has to come after create workflow
  workflow.SetFile(path, mFid);

  if (isRW) {
    redirectionhost += workflow.getCGICloseW(currentWorkflow.c_str(), vid).c_str();
  } else {
    redirectionhost += workflow.getCGICloseR(currentWorkflow.c_str()).c_str();
  }

  // Notify tape garbage collector if tape support is enabled
  if (gOFS->mTapeEnabled) {
    try {
      eos::common::RWMutexReadLock tgc_ns_rd_lock(gOFS->eosViewRWMutex);
      const auto tgcFmd = gOFS->eosFileService->getFileMD(mFid);
      const bool isATapeFile = tgcFmd->hasAttribute("sys.archive.file_id");
      tgc_ns_rd_lock.Release();

      if (isATapeFile) {
        if (isRW) {
          const std::string tgcSpace = nullptr != space.c_str() ? space.c_str() : "";
          gOFS->mTapeGc->fileOpenedForWrite(tgcSpace, mFid);
        } else {
          const auto fsId = getFirstDiskLocation(selectedfs);
          const std::string tgcSpace = FsView::gFsView.mIdView.lookupSpaceByID(fsId);
          gOFS->mTapeGc->fileOpenedForRead(tgcSpace, mFid);
        }
      }
    } catch (...) {
      // Ignore any garbage collection exceptions
    }
  }

  // Always redirect
  if ((vid.prot == "https") || (vid.prot == "http")) {
    ecode = targethttpport;
  } else {
    ecode = targetport;
  }

  rcode = SFS_REDIRECT;
  XrdOucString predirectionhost = redirectionhost.c_str();
  eos::common::StringConversion::MaskTag(predirectionhost, "cap.msg");
  eos::common::StringConversion::MaskTag(predirectionhost, "cap.sym");

  if (isRW) {
    eos_info("op=write path=%s info=%s %s redirection=%s xrd_port=%d "
             "http_port=%d", path, pinfo.c_str(), infolog.c_str(),
             predirectionhost.c_str(), targetport, targethttpport);
  } else {
    eos_info("op=read path=%s info=%s %s redirection=%s xrd_port=%d "
             "http_port=%d", path, pinfo.c_str(), infolog.c_str(),
             predirectionhost.c_str(), targetport, targethttpport);
  }

  EXEC_TIMING_END("Open");
  COMMONTIMING("end", &tm);
  char clientinfo[1024];
  snprintf(clientinfo, sizeof(clientinfo),
           "open:rt=%.02f io:bw=%s io:sched=%d io:type=%s io:prio=%s io:redirect=%s:%d",
           __exec_time__, bandwidth.length() ? bandwidth.c_str() : "inf", schedule,
           iotype.length() ? iotype.c_str() : "buffered",
           ioprio.length() ? ioprio.c_str() : "default", targethost.c_str(), ecode);
  std::string sclientinfo(clientinfo);
  std::string zclientinfo;
  eos::common::SymKey::ZBase64(sclientinfo, zclientinfo);
  redirectionhost += "&eos.clientinfo=";
  redirectionhost += zclientinfo.c_str();

  if (!gOFS->SetRedirectionInfo(error, redirectionhost.c_str(), ecode)) {
    eos_err("msg=\"failed setting redirection\" path=\"%s\"", path);
    return SFS_ERROR;
  }

  eos_info("path=%s %s duration=%0.03fms timing=%s",
           path, clientinfo, tm.RealTime(), tm.Dump().c_str());
  return rcode;
}


//----------------------------------------------------------------------------
// Read a partial result of a 'proc' interface command
//----------------------------------------------------------------------------
XrdSfsXferSize
XrdMgmOfsFile::read(XrdSfsFileOffset offset,
                    char* buff,
                    XrdSfsXferSize blen)
{
  static const char* epname = "read";

  if (mIsZeroSize) {
    return 0;
  }

  if (mProcCmd) {
    return mProcCmd->read(offset, buff, blen);
  }

  return Emsg(epname, error, EOPNOTSUPP, "read", fileName.c_str());
}

//------------------------------------------------------------------------------
// Read file pages into a buffer and return corresponding checksums
//------------------------------------------------------------------------------
XrdSfsXferSize
XrdMgmOfsFile::pgRead(XrdSfsFileOffset offset, char* buffer,
                      XrdSfsXferSize rdlen, uint32_t* csvec, uint64_t opts)
{
  XrdSfsXferSize bytes;

  if ((bytes = read(offset, buffer, rdlen)) <= 0) {
    return bytes;
  }

  // Generate the crc's
  XrdOucPgrwUtils::csCalc(buffer, offset, bytes, csvec);
  return bytes;
}

/*----------------------------------------------------------------------------*/
/*
 * @brief close a file object
 *
 * @return SFS_OK
 *
 * The close on the MGM is called only for files opened using the 'proc' e.g.
 * EOS shell comamnds. By construction failures can happen only during the open
 * of a 'proc' file e.g. the close always succeeds!
 */
/*----------------------------------------------------------------------------*/
int
XrdMgmOfsFile::close()
{
  oh = -1;

  if (mProcCmd) {
    mProcCmd->close();
    return SFS_OK;
  }

  return SFS_OK;
}

/*----------------------------------------------------------------------------*/
/*
 * @brief stat the size of an open 'proc' command/file
 *
 * @param buf stat struct where to store information
 * @return SFS_OK if open proc file otherwise SFS_ERROR
 *
 * For 'proc' files the result is created during the file open call.
 * The stat function will fill the size of the created result into the stat
 * buffer.
 */
/*----------------------------------------------------------------------------*/
int
XrdMgmOfsFile::stat(struct stat* buf)
{
  static const char* epname = "stat";

  if (mIsZeroSize) {
    memset(buf, 0, sizeof(struct stat));
    return 0;
  }

  if (mProcCmd) {
    return mProcCmd->stat(buf);
  }

  return Emsg(epname, error, EOPNOTSUPP, "stat", fileName.c_str());
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfsFile::sync()
/*----------------------------------------------------------------------------*/
/*
 * sync an open file - no implemented (no use case)
 *
 * @return SFS_ERROR and EOPNOTSUPP
 */
/*----------------------------------------------------------------------------*/
{
  static const char* epname = "sync";
  return Emsg(epname, error, EOPNOTSUPP, "sync", fileName.c_str());
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfsFile::sync(XrdSfsAio* aiop)
/*----------------------------------------------------------------------------*/
/*
 * aio sync an open file - no implemented (no use case)
 *
 * @return SFS_ERROR and EOPNOTSUPP
 */
/*----------------------------------------------------------------------------*/
{
  static const char* epname = "sync";
  // Execute this request in a synchronous fashion
  //
  return Emsg(epname, error, EOPNOTSUPP, "sync", fileName.c_str());
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfsFile::truncate(XrdSfsFileOffset flen)
/*----------------------------------------------------------------------------*/
/*
 * truncate an open file - no implemented (no use case)
 *
 * @return SFS_ERROR and EOPNOTSUPP
 */
/*----------------------------------------------------------------------------*/
{
  static const char* epname = "trunc";
  return Emsg(epname, error, EOPNOTSUPP, "truncate", fileName.c_str());
}

/*----------------------------------------------------------------------------*/
XrdMgmOfsFile::~XrdMgmOfsFile()
/*----------------------------------------------------------------------------*/
/*
 * @brief destructor
 *
 * Cleans-up the file object on destruction
 */
/*----------------------------------------------------------------------------*/
{
  if (oh > 0) {
    close();
  }

  if (openOpaque) {
    delete openOpaque;
    openOpaque = 0;
  }
}

//------------------------------------------------------------------------------
/*
 * @brief create an error message for a file object
 *
 * @param pfx message prefix value
 * @param einfo error text/code object
 * @param ecode error code
 * @param op name of the operation performed
 * @param target target of the operation e.g. file name etc.
 *
 * @return SFS_ERROR in all cases
 *
 * This routines prints also an error message into the EOS log.
 */
//------------------------------------------------------------------------------
int
XrdMgmOfsFile::Emsg(const char* pfx,
                    XrdOucErrInfo& einfo,
                    int ecode,
                    const char* op,
                    const char* target)
{
  char etext[128], buffer[4096];

  // Get the reason for the error
  if (ecode < 0) {
    ecode = -ecode;
  }

  if (eos::common::strerror_r(ecode, etext, sizeof(etext))) {
    sprintf(etext, "reason unknown (%d)", ecode);
  }

  // Format the error message
  snprintf(buffer, sizeof(buffer), "Unable to %s %s; %s", op, target, etext);
  eos_err("Unable to %s %s; %s", op, target, etext);
  // Place the error message in the error object and return
  einfo.setErrInfo(ecode, buffer);
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// Check if this is a client retry with exclusion of some diskserver. This
// happens usually for CMS workflows. To distinguish such a scenario from
// a legitimate retry due to a recoverable error, we need to search for the
// "tried=" opaque tag without a corresponding "triedrc=" tag.
//------------------------------------------------------------------------------
bool
XrdMgmOfsFile::IsRainRetryWithExclusion(bool is_rw, unsigned long lid) const
{
  if (!is_rw && eos::common::LayoutId::IsRain(lid)) {
    char* tried_info = openOpaque->Get("tried");

    if ((tried_info == nullptr) || strlen(tried_info) == 0) {
      return false;
    }

    // Don't exclude if tried information contains a globally unique cluster
    // ID which has the form: +<port><host>
    bool exclude = false;
    auto endpoints =  eos::common::StringTokenizer::split
                      <std::list<std::string>>(tried_info, ',');

    for (const auto& ep : endpoints) {
      if (!ep.empty() && (ep[0] != '+')) {
        exclude = true;
        break;
      }
    }

    if (openOpaque->Get("triedrc") == nullptr) {
      return exclude;
    }
  }

  return false;
}


//------------------------------------------------------------------------------
// Parse the triedrc opaque info and return the corresponding error number
//------------------------------------------------------------------------------
int
XrdMgmOfsFile::GetTriedrcErrno(const std::string& input) const
{
  if (input.empty()) {
    return 0;
  }

  std::vector<std::string> vect_err;
  eos::common::StringConversion::Tokenize(input, vect_err, ",");

  for (const auto& elem : vect_err) {
    if (elem == "enoent") {
      return ENOENT;
    } else if (elem == "ioerr") {
      return EIO;
    } else if (elem == "fserr") {
      return EFAULT;
    } else if (elem == "srverr") {
      return EFAULT;
    }
  }

  return 0;
}

//------------------------------------------------------------------------------
// Handle (delegated) TPC redirection
//------------------------------------------------------------------------------
bool
XrdMgmOfsFile::RedirectTpcAccess()
{
  if (!gOFS->mTpcRedirect) {
    return false;
  }

  const char* tpc_key = openOpaque->Get("tpc.key");

  if (tpc_key == nullptr) {
    return false;
  }

  bool is_delegated_tpc = (strncmp(tpc_key, "delegate", 8) == 0);
  // Support the tpc.dlgon=1 marker for XRootD client >= 4.11.2
  const char* dlg_marker = openOpaque->Get("tpc.dlgon");

  if (dlg_marker) {
    is_delegated_tpc = is_delegated_tpc || (strncmp(dlg_marker, "1", 1) == 0);
  }

  auto it = gOFS->mTpcRdrInfo.find(is_delegated_tpc);

  // If rdr info not present or if host is empty then skip
  if ((it == gOFS->mTpcRdrInfo.end()) || (it->second.first.empty())) {
    return false;
  }

  error.setErrInfo(it->second.second, it->second.first.c_str());
  eos_info("msg=\"tpc %s redirect\" rdr_host=%s rdr_port=%i",
           is_delegated_tpc ? "delegated" : "undelegated",
           it->second.first.c_str(), it->second.second);
  return true;
}

//------------------------------------------------------------------------------
// Dump scheduling info
//------------------------------------------------------------------------------
void
XrdMgmOfsFile::LogSchedulingInfo(const std::vector<unsigned int>& selected_fs,
                                 const std::vector<std::string>& proxy_eps,
                                 const std::vector<std::string>& fwall_eps) const
{
  eos::common::Logging& g_logging = eos::common::Logging::GetInstance();

  if (g_logging.gLogMask & LOG_MASK(LOG_DEBUG)) {
    std::ostringstream oss;
    oss << "selectedfs: ";

    for (const auto& elem : selected_fs) {
      oss << elem << "  ";
    }

    oss << "proxys: ";

    for (const auto& elem : proxy_eps) {
      oss << elem << "  ";
    }

    oss << "firewallentrypoints: ";

    for (const auto& elem : fwall_eps) {
      oss << elem << "  ";
    }

    eos_debug("msg=\"scheduling info %s\"", oss.str().c_str());
  }
}

//------------------------------------------------------------------------------
// Get file system ids excluded from scheduling
//------------------------------------------------------------------------------
std::vector<unsigned int>
XrdMgmOfsFile::GetExcludedFsids() const
{
  std::vector<unsigned int> fsids;
  std::string sfsids;

  if (openOpaque) {
    sfsids = (openOpaque->Get("eos.excludefsid") ?
              openOpaque->Get("eos.excludefsid") : "");
  }

  if (sfsids.empty()) {
    return fsids;
  }

  auto lst_ids = eos::common::StringTokenizer::split<std::list<std::string>>
                 (sfsids, ',');

  for (const auto& sid : lst_ids) {
    try {
      fsids.push_back(std::stoul(sid));
    } catch (...) {}
  }

  return fsids;
}
