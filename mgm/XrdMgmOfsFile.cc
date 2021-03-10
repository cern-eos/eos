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
#include "mgm/txengine/TransferEngine.hh"
#include "mgm/Recycle.hh"
#include "mgm/Macros.hh"
#include "mgm/ZMQ.hh"
#include "mgm/Master.hh"
#include "mgm/tgc/MultiSpaceTapeGc.hh"
#include "namespace/utils/Attributes.hh"
#include "namespace/Prefetcher.hh"
#include "namespace/Resolver.hh"
#include "XrdOss/XrdOss.hh"
#include "XrdSec/XrdSecInterface.hh"
#include "XrdSfs/XrdSfsAio.hh"
#include "common/Constants.hh"

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
    throw DiskLocationNotFound("Failed to find d isk location");
  }

  if (EOS_TAPE_FSID != locations.at(0)) {
    return locations.at(0);
  }

  if (2 > locations.size()) {
    throw DiskLocationNotFound("Failed to find d isk location");
  }

  return locations.at(1);
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
  gOFS->FuseXCastContainer(dirMd->getIdentifier());
  gOFS->FuseXCastContainer(dirMd->getParentIdentifier());       /* cloneMd */
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

/*----------------------------------------------------------------------------*/
int
XrdMgmOfsFile::open(eos::common::VirtualIdentity* invid,
                    const char* inpath,
                    XrdSfsFileOpenMode open_mode,
                    mode_t Mode,
                    const XrdSecEntity* client,
                    const char* ininfo)
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
{
  using eos::common::LayoutId;
  static const char* epname = "open";
  const char* tident = error.getErrUser();
  errno = 0;
  EXEC_TIMING_BEGIN("Open");
  XrdOucString spath = inpath;
  XrdOucString sinfo = ininfo;
  SetLogId(logId, tident);
  {
    EXEC_TIMING_BEGIN("IdMap");

    if (spath.beginswith("/zteos64:")) {
      sinfo += "&authz=";
      sinfo += spath.c_str() + 1;
      ininfo = sinfo.c_str();
    }

    // Handle token authz for XrdMacaroons or XrdSciTokens
    if (!HandleTokenAuthz(const_cast<XrdSecEntity*>(client), inpath,
                          ininfo ? ininfo : "")) {
      eos_err("msg=\"failed token authz\" path=\"%s\" opaque=\"%s\"",
              inpath, (ininfo ? ininfo : ""));
      return Emsg(epname, error, EPERM, "open - token authorization for path",
                  inpath);
    }

    if (!invid) {
      eos::common::Mapping::IdMap(client, ininfo, tident, vid);
    } else {
      vid = *invid;
    }

    EXEC_TIMING_END("IdMap");
  }
  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);
  SetLogId(logId, vid, tident);
  NAMESPACEMAP;
  BOUNCE_ILLEGAL_NAMES;
  BOUNCE_NOT_ALLOWED;
  spath = path;

  if (!spath.beginswith("/proc/") && spath.endswith("/")) {
    return Emsg(epname, error, EISDIR,
                "open - you specified a directory as target file name", path);
  }

  int open_flag = 0;
  int isRW = 0;
  int isRewrite = 0;
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
  int crOpts = (Mode & SFS_O_MKPTH) ? XRDOSS_mkpath : 0;

  // Set the actual open mode and find mode
  //
  if (open_mode & SFS_O_CREAT) {
    open_mode = SFS_O_CREAT;
  } else if (open_mode & SFS_O_TRUNC) {
    open_mode = SFS_O_TRUNC;
  }

  switch (open_mode & (SFS_O_RDONLY | SFS_O_WRONLY | SFS_O_RDWR |
                       SFS_O_CREAT | SFS_O_TRUNC)) {
  case SFS_O_CREAT:
    open_flag = O_RDWR | O_CREAT | O_EXCL;
    crOpts |= XRDOSS_new;
    isRW = 1;
    break;

  case SFS_O_TRUNC:
    open_flag |= O_RDWR | O_CREAT | O_TRUNC;
    isRW = 1;
    break;

  case SFS_O_RDONLY:
    open_flag = O_RDONLY;
    isRW = 0;
    break;

  case SFS_O_WRONLY:
    open_flag = O_WRONLY;
    isRW = 1;
    break;

  case SFS_O_RDWR:
    open_flag = O_RDWR;
    isRW = 1;
    break;

  default:
    open_flag = O_RDONLY;
    isRW = 0;
    break;
  }

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

  if (ProcInterface::IsWriteAccess(path, pinfo.c_str())) {
    SET_ACCESSMODE_W;
  }

  MAYSTALL;
  MAYREDIRECT;
  XrdOucString currentWorkflow = "default";
  unsigned long long byfid = 0;
  unsigned long long bypid = 0;

  /* check paths starting with fid: fxid: ino: ... */
  if (spath.beginswith("fid:") || spath.beginswith("fxid:") ||
      spath.beginswith("ino:")) {
    WAIT_BOOT;
    // reference by fid+fsid
    byfid = eos::Resolver::retrieveFileIdentifier(spath).getUnderlyingUInt64();

    try {
      eos::Prefetcher::prefetchFileMDAndWait(gOFS->eosView, byfid);
      eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__,
                                        __FILE__);
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

  openOpaque = new XrdOucEnv(ininfo);

  // Handle (delegated) tpc redirection for writes
  if (isRW && RedirectTpcAccess()) {
    return SFS_REDIRECT;
  }

  {
    // figure out if this is FUSE access
    const char* val = 0;

    if ((val = openOpaque->Get("eos.app"))) {
      XrdOucString application = val;

      if (application == "fuse") {
        isFuse = true;
      }

      if (application.beginswith("fuse::")) {
        isFuse = true;
      }
    }

    if ((val = openOpaque->Get("xrd.appname"))) {
      XrdOucString application = val;

      if (application == "xrootdfs") {
        isFuse = true;
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
  eos_debug("mode=%x create=%x truncate=%x", open_mode, SFS_O_CREAT, SFS_O_TRUNC);

  // proc filter
  if (ProcInterface::IsProcAccess(path)) {
    if (gOFS->mExtAuthz &&
        (vid.prot != "sss") &&
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
      eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__,
                                        __FILE__);
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

  if (open_flag & O_CREAT) {
    AUTHORIZE(client, openOpaque, AOP_Create, "create", inpath, error);
  } else {
    AUTHORIZE(client, openOpaque, (isRW ? AOP_Update : AOP_Read), "open",
              inpath, error);
    isRewrite = true;
  }

  eos::common::Path cPath(path);
  // indicate the scope for a possible token
  vid.scope = cPath.GetPath();

  if (cPath.isAtomicFile()) {
    isAtomicName = true;
  }

  // prevent any access to a recycling bin for writes
  if (isRW && cPath.GetFullPath().beginswith(Recycle::gRecyclingPrefix.c_str())) {
    return Emsg(epname, error, EPERM,
                "open file - nobody can write to a recycling bin",
                cPath.GetParentPath());
  }

  // check if we have to create the full path
  if (Mode & SFS_O_MKPTH) {
    eos_debug("%s", "msg=\"SFS_O_MKPTH was requested\"");
    XrdSfsFileExistence file_exists;
    int ec = gOFS->_exists(cPath.GetParentPath(), file_exists, error, vid, 0);

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
  // Get the directory meta data if it exists
  std::shared_ptr<eos::IContainerMD> dmd = nullptr;
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
      eos::Prefetcher::prefetchFileMDAndWait(gOFS->eosView, cPath.GetPath());
    }

    eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex, __FUNCTION__,
                                            __LINE__, __FILE__);

    try {
      if (byfid) {
        dmd = gOFS->eosDirectoryService->getContainerMD(bypid);
      } else {
        dmd = gOFS->eosView->getContainer(cPath.GetParentPath());
      }

      // get the attributes out
      gOFS->_attr_ls(gOFS->eosView->getUri(dmd.get()).c_str(), error, vid, 0,
                     attrmap, false);
      // extract workflows
      workflow.Init(&attrmap);

      if (dmd) {
        try {
          if (ocUploadUuid.length()) {
            eos::common::Path aPath(cPath.GetAtomicPath(attrmap.count("sys.versioning"),
                                    ocUploadUuid));
            fmd = gOFS->eosView->getFile(aPath.GetPath());
          } else {
            fmd = gOFS->eosView->getFile(cPath.GetPath());
          }

          if (fmd) {
            /* in case of a hard link, may need to switch to target */
            /* A hard link to another file */
            if (fmd->hasAttribute(XrdMgmOfsFile::k_mdino)) {
              std::shared_ptr<eos::IFileMD> gmd;
              uint64_t mdino = std::stoll(fmd->getAttribute(XrdMgmOfsFile::k_mdino));
              gmd = gOFS->eosFileService->getFileMD(eos::common::FileId::InodeToFid(mdino));
              eos_info("hlnk switched from %s (%#lx) to file %s (%#lx)",
                       fmd->getName().c_str(), fmd->getId(),
                       gmd->getName().c_str(), gmd->getId());
              fmd = gmd;
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
          fileId = fmd->getId();
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
          gOFS->_attr_ls(cPath.GetSubPath(2), error, vid, 0, attrmap, false);
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

    // -------------------------------------------------------------------------
    // Check for sys.ownerauth entries, which let people operate as the owner of
    // the directory
    // -------------------------------------------------------------------------
    bool sticky_owner = false;

    if (attrmap.count("sys.owner.auth")) {
      if (attrmap["sys.owner.auth"] == "*") {
        sticky_owner = true;
      } else {
        attrmap["sys.owner.auth"] += ",";
        std::string ownerkey = vid.prot.c_str();
        ownerkey += ":";

        if (vid.prot == "gsi") {
          ownerkey += vid.dn;
        } else {
          ownerkey += vid.uid_string;
        }

        if ((attrmap["sys.owner.auth"].find(ownerkey)) != std::string::npos) {
          eos_info("msg=\"client authenticated as directory owner\" path=\"%s\"uid=\"%u=>%u\" gid=\"%u=>%u\"",
                   path, vid.uid, vid.gid, d_uid, d_gid);
          // yes the client can operate as the owner, we rewrite the virtual
          // identity to the directory uid/gid pair
          vid.uid = d_uid;
          vid.gid = d_gid;
        }
      }
    }

    // -------------------------------------------------------------------------
    // ACL and permission check
    // -------------------------------------------------------------------------
    if (dotFxid and (not vid.sudoer) and (vid.uid != 0)) {
      /* restricted: this could allow access to a file hidden by the hierarchy */
      eos_debug(".fxid=%d uid %d sudoer %d", dotFxid, vid.uid, vid.sudoer);
      errno = EPERM;
      return Emsg(epname, error, errno, "open file - open by fxid denied", path);
    }

    eos::IFileMD::XAttrMap attrmapF;

    if (fmd) {
      eos::listAttributes(gOFS->eosView, fmd.get(), attrmapF, false);
    } else {
      gOFS->_attr_ls(cPath.GetPath(), error, vid, 0, attrmapF, false);
    }

    acl.SetFromAttrMap(attrmap, vid, &attrmapF);

    eos_info("acl=%d r=%d w=%d wo=%d egroup=%d shared=%d mutable=%d facl=%d",
             acl.HasAcl(), acl.CanRead(), acl.CanWrite(), acl.CanWriteOnce(),
             acl.HasEgroup(), isSharedFile, acl.IsMutable(),
	     acl.EvalUserAttrFile());

    if (acl.HasAcl()) {
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
        if (fmd && ((open_mode & SFS_O_TRUNC) == 0)) {
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
    if (fmd != nullptr && fmd->getAttributes().count("sys.proc")) {
      ns_rd_lock.Release();
      return open("/proc/user/", open_mode, Mode, client,
                  fmd->getAttribute("sys.proc").c_str());
    }
  }

  // Set the versioning depth if it is defined
  if (attrmap.count("sys.versioning")) {
    versioning = atoi(attrmap["sys.versioning"].c_str());
  } else {
    if (attrmap.count("user.versioning")) {
      versioning = atoi(attrmap["user.versioning"].c_str());
    }
  }

  // get user desired versioning
  if (versioning_cgi.length()) {
    versioning = atoi(versioning_cgi.c_str());
  }


  if (attrmap.count("sys.forced.atomic")) {
    isAtomicUpload = atoi(attrmap["sys.forced.atomic"].c_str());
  } else {
    if (attrmap.count("user.forced.atomic")) {
      isAtomicUpload = atoi(attrmap["user.forced.atomic"].c_str());
    } else {
      if (openOpaque->Get("eos.atomic")) {
        isAtomicUpload = true;
      }
    }
  }

  if (openOpaque->Get("eos.injection")) {
    isInjection = true;
  }

  if (openOpaque->Get("eos.repair")) {
    isRepair = true;
  }

  if (openOpaque->Get("eos.repairread")) {
    isRepairRead = true;
  }

  // disable atomic uploads for FUSE clients
  if (isFuse) {
    isAtomicUpload = false;
  }

  // disable injection in fuse clients
  if (isFuse) {
    isInjection = false;
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

    if (!isInjection && (open_mode & SFS_O_TRUNC) && fmd) {
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
          eos::common::Path cPath(path);
          XrdOucString vdir;
          vdir += cPath.GetVersionDirectory();
          // atomic uploads need just to purge version to max-1, the version is created on commit
          // purge might return an error if the file was not yet existing/versioned
          gOFS->PurgeVersion(vdir.c_str(), error, versioning - 1);
          errno = 0;
        } else {
          // handle the versioning for a specific file ID
          if (gOFS->Version(fileId, error, vid, versioning)) {
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
      if (!(fmd) && ((open_flag & O_CREAT))) {
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
      if (!(open_flag & O_CREAT)) {
        // Open for write for non existing file without creation flag
        return Emsg(epname, error, errno, "open file without creation flag", path);
      } else {
        // creation of a new file or isOcUpload
        {
          // -------------------------------------------------------------------
          std::shared_ptr<eos::IFileMD> ref_fmd;
          eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__,
                                             __FILE__);

          try {
            // we create files with the uid/gid of the parent directory
            if (isAtomicUpload) {
              eos::common::Path cPath(path);
              creation_path = cPath.GetAtomicPath(versioning, ocUploadUuid);
              eos_info("atomic-path=%s", creation_path.c_str());

              try {
                ref_fmd = gOFS->eosView->getFile(path);
              } catch (eos::MDException& e) {
                // empty
              }
            }

            // Avoid any race condition when opening for creation O_EXCL
            if (open_flag & O_EXCL) {
              try {
                fmd = gOFS->eosView->getFile(creation_path);
              } catch (eos::MDException& e1) {
                // empty
              }

              if (fmd) {
                gOFS->MgmStats.Add("OpenFailedExists", vid.uid, vid.gid, 1);
                return Emsg(epname, error, EEXIST, "create file - (O_EXCL)", path);
              }
            }

            fmd = gOFS->eosView->createFile(creation_path, vid.uid, vid.gid);

            if (ocUploadUuid.length()) {
              fmd->setFlags(0);
            } else {
              fmd->setFlags(Mode & (S_IRWXU | S_IRWXG | S_IRWXO));
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


              gOFS->eosView->updateFileStore(ref_fmd.get());
            }

            fileId = fmd->getId();
            fmdlid = fmd->getLayoutId();
            // oc chunks start with flags=0
            cid = fmd->getContainerId();
            std::shared_ptr<eos::IContainerMD> cmd =
              gOFS->eosDirectoryService->getContainerMD(cid);
            cmd->setMTimeNow();
            cmd->notifyMTimeChange(gOFS->eosDirectoryService);
            gOFS->eosView->updateContainerStore(cmd.get());
            eos::ContainerIdentifier cmd_id = cmd->getIdentifier();
            eos::ContainerIdentifier cmd_pid = cmd->getParentIdentifier();
            lock.Release();
            gOFS->FuseXCastContainer(cmd_id);
            gOFS->FuseXCastContainer(cmd_pid);
            gOFS->FuseXCastRefresh(cmd_id, cmd_pid);
            gOFS->mReplicationTracker->Create(fmd);
          } catch (eos::MDException& e) {
            fmd.reset();
            errno = e.getErrno();
            eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n",
                      e.getErrno(), e.getMessage().str().c_str());
          };

          // -------------------------------------------------------------------
        }

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
      if (open_flag & O_EXCL) {
        gOFS->MgmStats.Add("OpenFailedExists", vid.uid, vid.gid, 1);
        return Emsg(epname, error, EEXIST, "create file (O_EXCL)", path);
      }
    }
  } else {
    if (!fmd) {
      // check if there is a redirect or stall for missing entries
      MAYREDIRECT_ENOENT;
      MAYSTALL_ENOENT;
    }

    if ((!fmd) && (attrmap.count("sys.redirect.enoent"))) {
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
      return rcode;
    }

    if ((!fmd)) {
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
        fileId))) {
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
                  fmd->getCloneFST().c_str(), open_mode & SFS_O_TRUNC);
        snprintf(sbuff, sizeof(sbuff), "&mgm.cloneid=%ld&mgm.cloneFST=%s", cloneId,
                 fmd->getCloneFST().c_str());
        capability += sbuff;
      }
    } else {
      capability += "&mgm.access=read";
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
  eos::common::RWMutexReadLock
  fs_rd_lock(FsView::gFsView.ViewMutex, __FUNCTION__, __LINE__, __FILE__);
  // select space and layout according to policies
  Policy::GetLayoutAndSpace(path, attrmap, vid, new_lid, space, *openOpaque,
                            forcedFsId, forced_group);
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

  if ((!isInjection) && (isCreation || (open_mode == SFS_O_TRUNC))) {
    eos_info("blocksize=%llu lid=%x", LayoutId::GetBlocksize(new_lid), new_lid);
    layoutId = new_lid;
    {
      std::shared_ptr<eos::IFileMD> fmdnew;
      eos::common::RWMutexWriteLock ns_wr_lock(gOFS->eosViewRWMutex, __FUNCTION__,
          __LINE__, __FILE__);

      if (!byfid) {
        try {
          fmdnew = gOFS->eosView->getFile(path);
        } catch (eos::MDException& e) {
          if ((!isAtomicUpload) && (fmdnew != fmd)) {
            // file has been recreated in the meanwhile
            return Emsg(epname, error, EEXIST, "open file (file recreated)", path);
          }
        }
      }

      // Set the layout and commit new meta data
      fmd->setLayoutId(layoutId);

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
        eos::FileIdentifier fmd_id = fmd->getIdentifier();
        gOFS->eosView->updateFileStore(fmd.get());
        std::shared_ptr<eos::IContainerMD> cmd =
          gOFS->eosDirectoryService->getContainerMD(cid);
        cmd->setMTimeNow();
        cmd->notifyMTimeChange(gOFS->eosDirectoryService);
        gOFS->eosView->updateContainerStore(cmd.get());
        eos::ContainerIdentifier cmd_id = cmd->getIdentifier();
        eos::ContainerIdentifier pcmd_id = cmd->getParentIdentifier();

        if (isCreation || (!fmd->getNumLocation())) {
          eos::IQuotaNode* ns_quota = gOFS->eosView->getQuotaNode(cmd.get());

          if (ns_quota) {
            ns_quota->addFile(fmd.get());
          }
        }

        ns_wr_lock.Release();
        gOFS->FuseXCastFile(fmd_id);
        gOFS->FuseXCastContainer(cmd_id);
        gOFS->FuseXCastContainer(pcmd_id);
        gOFS->FuseXCastRefresh(cmd_id, pcmd_id);
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
      isZeroSizeFile = true;
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

    while (safepath.replace("&", "#AND#")) {
    }

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
    hex_fid = eos::common::FileId::Fid2Hex(fileId);
  }

  capability += hex_fid.c_str();
  XrdOucString sizestring;
  capability += "&mgm.cid=";
  capability += eos::common::StringConversion::GetSizeString(sizestring, cid);
  // add the mgm.sec information to the capability
  capability += "&mgm.sec=";
  capability += eos::common::SecEntity::ToKey(client,
                openOpaque->Get("eos.app")).c_str();

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

  eos::mgm::FileSystem* filesystem = 0;
  std::vector<unsigned int> selectedfs;
  std::vector<unsigned int> excludefs = GetExcludedFsids();
  std::vector<std::string> proxys;
  std::vector<std::string> firewalleps;
  // file systems which are unavailable during a read operation
  std::vector<unsigned int> unavailfs;
  // file systems which have been replaced with a new reconstructed stripe
  std::vector<unsigned int> replacedfs;
  std::vector<unsigned int>::const_iterator sfs;
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
    plctargs.bookingsize = isFuse ? gOFS->getFuseBookingSize() : bookingsize;
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
    plctargs.truncate = open_mode & SFS_O_TRUNC;
    plctargs.vid = &vid;

    if (!plctargs.isValid()) {
      // there is something wrong in the arguments of file placement
      return Emsg(epname, error, EINVAL, "open - invalid placement argument", path);
    }

    retc = Quota::FilePlacement(&plctargs);

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
      workflow.SetFile(path, fileId);
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

    retc = Scheduler::FileAccess(&acsargs);

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
        plctargs.truncate = open_mode & SFS_O_TRUNC;
        plctargs.vid = &vid;

        if (!plctargs.isValid()) {
          // there is something wrong in the arguments of file placement
          return Emsg(epname, error, EINVAL, "open - invalid placement argument", path);
        }

        retc = Quota::FilePlacement(&plctargs);
        eos_info("msg=\"file-recreation due to offline/full locations\" path=%s retc=%d",
                 path, retc);
        isRecreation = true;
      } else {
        // Normal read failed, try to reply with the tiredrc value if this
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
          eos::Prefetcher::prefetchFileMDAndWait(gOFS->eosView, path);
          eos::common::RWMutexReadLock rd_lock(gOFS->eosViewRWMutex, __FUNCTION__,
                                               __LINE__, __FILE__);
          auto tmp_fmd = gOFS->eosView->getFile(path);

          if (tmp_fmd->getNumLocation() == 0) {
            do_remove = true;
          }
        } catch (eos::MDException& e) {}

        if (do_remove) {
          eos::common::VirtualIdentity vidroot = eos::common::VirtualIdentity::Root();
          gOFS->_rem(cPath.GetPath(), error, vidroot, 0, false, false, false);
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
      if (isCreation && hasClientBookingSize && ((bookingsize == 0) ||
          ocUploadUuid.length())) {
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
          if (byfid) {
            eos::Prefetcher::prefetchFileMDAndWait(gOFS->eosView, byfid);
          } else {
            eos::Prefetcher::prefetchFileMDAndWait(gOFS->eosView, creation_path);
          }

          eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__,
                                             __FILE__);
          // -------------------------------------------------------------------

          try {
            if (byfid) {
              fmd = gOFS->eosFileService->getFileMD(byfid);
            } else {
              fmd = gOFS->eosView->getFile(creation_path);
            }

            if (!fmd) {
              errno = ENOENT;
              gOFS->MgmStats.Add("OpenFailedENOENT", vid.uid, vid.gid, 1);
              return Emsg(epname, error, errno, "open file - file is not existing");
            }

            if (isRecreation) {
              fmd->unlinkAllLocations();
            }

            for (auto& fsid : selectedfs) {
              fmd->addLocation(fsid);
            }

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
        isZeroSizeFile = true;
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
          eos::Prefetcher::prefetchFileMDAndWait(gOFS->eosView, byfid);
          eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__,
                                             __FILE__);

          try {
            fmd = gOFS->eosFileService->getFileMD(byfid);

            for (auto& fsid : selectedfs) {
              fmd->addLocation(fsid);
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

        for (size_t k = 0; k < selectedfs.size(); k++) {
          filesystem = FsView::gFsView.mIdView.lookupByID(selectedfs[k]);
          fsgeotag = "";

          if (filesystem) {
            fsgeotag = filesystem->GetString("stat.geotag");
          }

          // if the fs is available
          if (std::find(unavailfs.begin(), unavailfs.end(),
                        selectedfs[k]) == unavailfs.end()) {
            // take the highest fsid with the same geotag if possible
            if ((vid.geolocation.empty() ||
                 (fsgeotag.find(vid.geolocation) != std::string::npos)) &&
                (selectedfs[k] > fsid)) {
              fsIndex = k;
              fsid = selectedfs[k];
            }
          }
        }

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
          isZeroSizeFile = true;
          return SFS_OK;
        }
      }
    }
  }

  // If this is a RAIN layout, we want a nice round-robin for the entry
  // server since it  has the burden of encoding and traffic fan-out
  if (isRW && LayoutId::IsRain(layoutId)) {
    fsIndex = fileId % selectedfs.size();
    eos_static_info("selecting entry-server fsIndex=%lu fsid=%lu fxid=%lx mod=%lu",
                    fsIndex, selectedfs[fsIndex], fileId, selectedfs.size());
  }

  // Get the redirection host from the selected entry in the vector
  if (!selectedfs[fsIndex]) {
    eos_err("%s", "msg=\"0 filesystem in selection\"");
    return Emsg(epname, error, ENETUNREACH, "received filesystem id 0", path);
  }

  filesystem = FsView::gFsView.mIdView.lookupByID(selectedfs[fsIndex]);

  if (!filesystem) {
    return Emsg(epname, error, ENETUNREACH, "received non-existent filesystem",
                path);
  }

  // Set the FST gateway for clients who are geo-tagged with default
  if ((firewalleps.size() > fsIndex) && (proxys.size() > fsIndex)) {
    // Do this with forwarding proxy syntax only if the firewall entrypoint is
    // different from the endpoint
    if (!(firewalleps[fsIndex].empty()) &&
        ((!proxys[fsIndex].empty() && firewalleps[fsIndex] != proxys[fsIndex]) ||
         (firewalleps[fsIndex] != filesystem->GetString("hostport")))) {
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
        oss << filesystem->GetString("host").c_str() << ":" <<
            filesystem->GetString("port").c_str();
      } else {
        oss << proxys[fsIndex];
      }

      redirectionhost = oss.str().c_str();
      redirectionhost += "&";
    } else {
      if (proxys[fsIndex].empty()) { // there is no proxy to use
        targethost  = filesystem->GetString("host").c_str();
        targetport  = atoi(filesystem->GetString("port").c_str());
        targethttpport  = atoi(filesystem->GetString("stat.http.port").c_str());

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
      std::string fsprefix = filesystem->GetPath();

      if (!(fsprefix.empty())) {
        XrdOucString s = "mgm.fsprefix";
        s += "=";
        s += fsprefix.c_str();
        s.replace(":", "#COL#");
        redirectionhost += s;
      }
    }
  } else {
    // There is no proxy or firewall entry point to use
    targethost  = filesystem->GetString("host").c_str();
    targetport  = atoi(filesystem->GetString("port").c_str());
    targethttpport  = atoi(filesystem->GetString("stat.http.port").c_str());
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
  capability += eos::common::StringConversion::GetSizeString(sizestring,
                bookingsize);

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
    capability += (int) filesystem->GetId();
  }

  if (isRepairRead) {
    capability += "&mgm.repairread=1";
  }

  if (isZeroSizeFile) {
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

  XrdOucString infolog = "";
  XrdOucString piolist = "";

  if ((LayoutId::GetLayoutType(layoutId) == LayoutId::kReplica) ||
      (LayoutId::IsRain(layoutId))) {
    capability += "&mgm.fsid=";
    capability += (int) filesystem->GetId();
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
      eos::mgm::FileSystem* orig_fs = FsView::gFsView.mIdView.lookupByID(orig_id);

      if (!orig_fs) {
        return Emsg(epname, error, EINVAL, "reconstruct filesystem", path);
      }

      orig_fs->SnapShotFileSystem(orig_snapshot);
      forced_group = orig_snapshot.mGroupIndex;
      // Add new stripes if file doesn't have the nomial number
      auto stripe_diff = (LayoutId::GetStripeNumber(fmd->getLayoutId()) + 1) -
                         selectedfs.size();
      // Create a plain layout with the number of replacement stripes to be
      // scheduled in the file placement routine
      unsigned long plain_lid = new_lid;
      LayoutId::SetStripeNumber(plain_lid,
                                pio_reconstruct_fs.size() - 1 + stripe_diff);
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

      retc = Quota::FilePlacement(&plctargs);
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

      // If there as stripes missing then fill them in from the replacements
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

    // Put all the replica urls into the capability
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
          oss << targethost << "?" << "eos.fstfrw=";

          // check if we have to redirect to the fs host or to a proxy
          if (proxys[fsIndex].empty()) {
            oss << repfilesystem->GetString("host").c_str() << ":" <<
                repfilesystem->GetString("port").c_str();
          } else {
            oss << proxys[fsIndex];
          }

          redirectionhost = oss.str().c_str();
        } else {
          if ((proxys.size() > fsIndex) && !proxys[fsIndex].empty())  {
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
          replicaport = atoi(proxys[i].substr(idx + 1, std::string::npos).c_str());
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
      capability += (int) i;
      capability += "=";
      capability += (int) repfilesystem->GetId();

      if ((proxys.size() > i) && !proxys[i].empty()) {
        std::string fsprefix = repfilesystem->GetPath();

        if (!fsprefix.empty()) {
          XrdOucString s = "mgm.fsprefix";
          s += (int) i;
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
          capability += (int) i;
          capability += "=";
          capability += (int) replacedfs[i];
        }

        piolist += "pio.";
        piolist += (int) i;
        piolist += "=";
        piolist += replicahost;
        piolist += ":";
        piolist += replicaport;
        piolist += "&";
      }

      eos_debug("msg=\"redirection url\" %d => %s", i, replicahost.c_str());
      infolog += "target[";
      infolog += (int) i;
      infolog += "]=(";
      infolog += replicahost.c_str();
      infolog += ",";
      infolog += (int) repfilesystem->GetId();
      infolog += ") ";
    }
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
    workflow.SetFile(path, fileId);
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
          eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__,
                                             __FILE__);
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
  workflow.SetFile(path, fileId);

  if (isRW) {
    redirectionhost += workflow.getCGICloseW(currentWorkflow.c_str(), vid).c_str();
  } else {
    redirectionhost += workflow.getCGICloseR(currentWorkflow.c_str()).c_str();
  }

  // Notify tape garbage collector if tape support is enabled
  if (gOFS->mTapeEnabled) {
    try {
      eos::common::RWMutexReadLock tgc_ns_rd_lock(gOFS->eosViewRWMutex, __FUNCTION__,
          __LINE__, __FILE__);
      const auto tgcFmd = gOFS->eosFileService->getFileMD(fileId);
      const bool isATapeFile = tgcFmd->hasAttribute("sys.archive.file_id");
      tgc_ns_rd_lock.Release();

      if (isATapeFile) {
        if (isRW) {
          const std::string tgcSpace = nullptr != space.c_str() ? space.c_str() : "";
          gOFS->mTapeGc->fileOpenedForWrite(tgcSpace, fileId);
        } else {
          const auto fsId = getFirstDiskLocation(selectedfs);
          const std::string tgcSpace = FsView::gFsView.mIdView.lookupSpaceByID(fsId);
          gOFS->mTapeGc->fileOpenedForRead(tgcSpace, fileId);
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

  if (!gOFS->SetRedirectionInfo(error, redirectionhost.c_str(), ecode)) {
    eos_err("msg=\"failed setting redirection\" path=\"%s\"", path);
    return SFS_ERROR;
  }

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

  eos_info("info=\"redirection\" hostport=%s:%d", predirectionhost.c_str(),
           ecode);

  if (attrmap.count("sys.force.atime")) {
    // -------------------------------------------------------------------------
    // we are supposed to track the access time of a file.
    // since we don't have an atime field we use the change time of the file
    // we only update the atime if the current atime is older than the age
    // value given by the attribute
    // -------------------------------------------------------------------------
    const char* app = nullptr;

    if (!(app = openOpaque->Get("eos.app")) ||
        (
          (strcmp(app, "balancer")) &&
          (strcmp(app, "drainer")) &&
          (strcmp(app, "converter"))
        )
       ) {
      // we are supposed to update the change time with the access since this
      // is any kind of external access
      time_t now = time(nullptr);
      XrdOucString sage = attrmap["sys.force.atime"].c_str();
      time_t age = eos::common::StringConversion::GetSizeFromString(sage);
      eos::Prefetcher::prefetchFileMDAndWait(gOFS->eosView, path);
      eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__,
                                         __FILE__);

      try {
        fmd = gOFS->eosView->getFile(path);
        eos::IFileMD::ctime_t ctime;
        fmd->getCTime(ctime);

        if ((ctime.tv_sec + age) < now) {
          // only update within the resolution of the access tracking
          fmd->setCTimeNow();
          gOFS->eosView->updateFileStore(fmd.get());
          eos::FileIdentifier fmd_id = fmd->getIdentifier();
          lock.Release();
          gOFS->FuseXCastFile(fmd_id);
        }

        errno = 0;
      } catch (eos::MDException& e) {
        errno = e.getErrno();
        eos_warning("msg=\"failed to update access time\" path=\"%s\" ec=%d emsg=\"%s\"\n",
                    path, e.getErrno(), e.getMessage().str().c_str());
      }
    }
  }

  EXEC_TIMING_END("Open");
  return rcode;
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfsFile::close()
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
{
  oh = -1;

  if (mProcCmd) {
    mProcCmd->close();
    return SFS_OK;
  }

  return SFS_OK;
}

XrdSfsXferSize
XrdMgmOfsFile::read(XrdSfsFileOffset offset,
                    char* buff,
                    XrdSfsXferSize blen)
/*----------------------------------------------------------------------------*/
/*
 * read a partial result of a 'proc' interface command
 *
 * @param offset where to read from the result
 * @param buff buffer where to place the result
 * @param blen maximum size to read
 *
 * @return number of bytes read upon success or SFS_ERROR
 *
 * This read is only used to stream back 'proc' command results to the EOS
 * shell since all normal files get a redirection or error during the file open.
 */
/*----------------------------------------------------------------------------*/
{
  static const char* epname = "read";

  if (isZeroSizeFile) {
    return 0;
  }

  if (mProcCmd) {
    return mProcCmd->read(offset, buff, blen);
  }

  return Emsg(epname, error, EOPNOTSUPP, "read", fileName.c_str());
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfsFile::read(XrdSfsAio* aiop)
/*----------------------------------------------------------------------------*/
/*
 * aio flavour of a read - not supported
 * @return SFS_ERROR and EOPNOTSUPP
 */
/*----------------------------------------------------------------------------*/
{
  static const char* epname = "read";

  if (isZeroSizeFile) {
    return 0;
  }

  // Execute this request in a synchronous fashion
  //
  return Emsg(epname, error, EOPNOTSUPP, "read", fileName.c_str());
}

/*----------------------------------------------------------------------------*/
XrdSfsXferSize
XrdMgmOfsFile::write(XrdSfsFileOffset offset,
                     const char* buff,
                     XrdSfsXferSize blen)
/*----------------------------------------------------------------------------*/
/*
 * @brief write a block to an open file - not implemented (no use case)
 *
 * @return SFS_ERROR and EOPNOTSUPP
 */
/*----------------------------------------------------------------------------*/
{
  static const char* epname = "write";
  return Emsg(epname, error, EOPNOTSUPP, "write", fileName.c_str());
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfsFile::write(XrdSfsAio* aiop)
/*----------------------------------------------------------------------------*/
/*
 * @brief write a block to an open file - not implemented (no use case)
 *
 * @return SFS_ERROR and EOPNOTSUPP
 */
/*----------------------------------------------------------------------------*/
{
  static const char* epname = "write";
  // Execute this request in a synchronous fashion
  return Emsg(epname, error, EOPNOTSUPP, "write", fileName.c_str());
}

/*----------------------------------------------------------------------------*/
int
XrdMgmOfsFile::stat(struct stat* buf)
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
{
  static const char* epname = "stat";

  if (isZeroSizeFile) {
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
// Handle (HTTP TPC) token authorization and extract from the token the username
// of the client executing the current operation. This will populate the
// XrdSecEntity.name field used later on to establish the virtual identity.
//------------------------------------------------------------------------------
bool
XrdMgmOfsFile::HandleTokenAuthz(XrdSecEntity* client, const std::string& path,
                                const std::string& opaque)
{
  // @todo (esindril) this is just a workaround for the fact that XrdHttp
  // does not properly populate the prot field in the XrdSecEntity object.
  // See https://github.com/xrootd/xrootd/issues/1122
  if (client &&
      (strlen(client->tident) == 4) &&
      (strcmp(client->tident, "http") == 0)) {
    XrdOucEnv op_env(opaque.c_str());
    std::string authz = (op_env.Get("authz") ? op_env.Get("authz") : "");

    // If opaque info contains bearer authorization info then call the token
    // authorization library to get the username for the client
    if (!authz.empty() && (authz.find("Bearer%20") == 0)) {
      // @todo (esindril) this should be mapped to the correct operation type
      Access_Operation oper = AOP_Stat;

      if (gOFS->mTokenAuthz &&
          (gOFS->mTokenAuthz->Access(client, path.c_str(), oper, &op_env)
           == XrdAccPriv_None)) {
        return false;
      }
    }
  }

  return true;
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
