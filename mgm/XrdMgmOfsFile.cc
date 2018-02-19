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
#include "common/ZMQ.hh"
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
#include "mgm/txengine/TransferEngine.hh"
#include "mgm/Recycle.hh"
#include "mgm/Macros.hh"
#include "mgm/ZMQ.hh"
#include "mgm/Master.hh"
#include "authz/XrdCapability.hh"
#include "XrdOss/XrdOss.hh"
#include "XrdSec/XrdSecInterface.hh"
#include "XrdSfs/XrdSfsAio.hh"

#ifdef __APPLE__
#define ECOMM 70
#endif

#ifndef S_IAMB
#define S_IAMB  0x1FF
#endif


/******************************************************************************/
/******************************************************************************/
/* MGM File Interface                                                         */
/******************************************************************************/
/******************************************************************************/

/*----------------------------------------------------------------------------*/
int
XrdMgmOfsFile::open(const char* inpath,
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
  static const char* epname = "open";
  const char* tident = error.getErrUser();
  errno = 0;
  EXEC_TIMING_BEGIN("Open");
  SetLogId(logId, tident);
  {
    EXEC_TIMING_BEGIN("IdMap");
    eos::common::Mapping::IdMap(client, ininfo, tident, vid);
    EXEC_TIMING_END("IdMap");
  }
  gOFS->MgmStats.Add("IdMap", vid.uid, vid.gid, 1);
  SetLogId(logId, vid, tident);
  NAMESPACEMAP;
  BOUNCE_ILLEGAL_NAMES;
  BOUNCE_NOT_ALLOWED;
  XrdOucString spath = path;
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
  // flag indicating a new injection - upload of a file into a stub without physical location
  bool isInjection = false;
  // flag indicating to drop the current disk replica in the policy space                                                       
  bool isRepair = false;

  // chunk upload ID
  XrdOucString ocUploadUuid = "";
  // list of filesystem IDs to reconstruct
  std::vector<unsigned int> PioReconstructFsList;
  // list of filesystem IDs usable for replacement
  std::vector<unsigned int> PioReplacementFsList;
  // of RAIN files
  // tried hosts CGI
  std::string tried_cgi;
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

  if ((spath.beginswith("fid:") || (spath.beginswith("fxid:")) ||
       (spath.beginswith("ino:")))) {
    WAIT_BOOT;

    // reference by fid+fsid
    if (spath.beginswith("fid:")) {
      spath.replace("fid:", "");
      byfid = strtoull(spath.c_str(), 0, 10);
    }

    if (spath.beginswith("fxid:")) {
      spath.replace("fxid:", "");
      byfid = strtoull(spath.c_str(), 0, 16);
    }

    if (spath.beginswith("ino:")) {
      spath.replace("ino:", "");
      byfid = strtoull(spath.c_str(), 0, 16);
      byfid = eos::common::FileId::InodeToFid(byfid);
    }

    try {
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

  openOpaque = new XrdOucEnv(ininfo);
  {
    // figure out if this is FUSE access
    const char* val = 0;

    if ((val = openOpaque->Get("eos.app"))) {
      XrdOucString application = val;

      if (application == "fuse") {
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

  // ---------------------------------------------------------------------------
  // discover PIO reconstruction mode
  // ---------------------------------------------------------------------------
  XrdOucString sPioRecover = (openOpaque) ?
                             openOpaque->Get("eos.pio.action") : "";

  if (sPioRecover == "reconstruct") {
    isPioReconstruct = true;
  }

  {
    // -------------------------------------------------------------------------
    // discover PIO reconstruction filesystems (stripes to be replaced)
    // -------------------------------------------------------------------------
    std::string sPioRecoverFs = (openOpaque) ?
                                (openOpaque->Get("eos.pio.recfs") ? openOpaque->Get("eos.pio.recfs") : "")
                                : "";
    std::vector<std::string> fsToken;
    eos::common::StringConversion::Tokenize(sPioRecoverFs, fsToken, ",");

    if (openOpaque->Get("eos.pio.recfs") && fsToken.empty()) {
      // -----------------------------------------------------------------------
      // if there is a list announced there should be atleast one filesystem
      // mentioned for reconstruction
      // -----------------------------------------------------------------------
      return Emsg(epname, error, EINVAL, "open - you specified a list of"
                  " reconstruction filesystems but the list is empty", path);
    }

    for (size_t i = 0; i < fsToken.size(); i++) {
      errno = 0;
      unsigned int rfs = (unsigned int) strtol(fsToken[i].c_str(), 0, 10);
      XrdOucString srfs = "";
      srfs += (int) rfs;

      if (errno || (srfs != fsToken[i].c_str())) {
        return Emsg(epname,
                    error,
                    EINVAL,
                    "open - you specified a list of "
                    "reconstruction filesystems but "
                    "the list contains non numerical or illegal id's",
                    path);
      }

      // store in the reconstruction filesystem list
      PioReconstructFsList.push_back(rfs);
    }
  }

  int rcode = SFS_ERROR;
  XrdOucString redirectionhost = "invalid?";
  XrdOucString targethost = "";
  int targetport = atoi(gOFS->MgmOfsTargetPort.c_str());
  int ecode = 0;
  unsigned long fmdlid = 0;
  unsigned long long cid = 0;
  eos_debug("mode=%x create=%x truncate=%x", open_mode, SFS_O_CREAT, SFS_O_TRUNC);

  // proc filter
  if (ProcInterface::IsProcAccess(path)) {
    if (gOFS->Authorization &&
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
      mProcCmd = ProcInterface::GetProcCommand(tident, vid, path, ininfo);

      if (mProcCmd) {
        mProcCmd->SetLogId(logId, vid, tident);
        rcode = mProcCmd->open(path, ininfo, vid, &error);

        // If we need to stall the client then save the IProcCommand object and
        // add it to the map for when the client comes back.
        if (rcode > 0) {
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
  eos_debug("authorize start");

  if (open_flag & O_CREAT) {
    AUTHORIZE(client, openOpaque, AOP_Create, "create", inpath, error);
  } else {
    AUTHORIZE(client, openOpaque, (isRW ? AOP_Update : AOP_Read), "open",
              inpath, error);
    isRewrite = true;
  }

  eos_debug("msg=\"authorize done\"");
  eos::common::Path cPath(path);

  // prevent any access to a recycling bin for writes
  if (isRW && cPath.GetFullPath().beginswith(Recycle::gRecyclingPrefix.c_str())) {
    return Emsg(epname, error, EPERM,
                "open file - nobody can write to a recycling bin",
                cPath.GetParentPath());
  }

  // check if we have to create the full path
  if (Mode & SFS_O_MKPTH) {
    eos_debug("msg=\"SFS_O_MKPTH was requested\"");
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
    eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex);

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
        } catch (eos::MDException& e) {
          fmd.reset();
        }

        if (!fmd) {
          if (dmd->findContainer(cPath.GetName())) {
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

        d_uid = dmd->getCUid();
        d_gid = dmd->getCGid();
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

          rcode = SFS_REDIRECT;
          error.setErrInfo(ecode, redirectionhost.c_str());
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
    acl.Set(attrmap.count("sys.acl") ? attrmap["sys.acl"] : std::string(""),
            attrmap.count("user.acl") ? attrmap["user.acl"] : std::string(""),
            vid,
            attrmap.count("sys.eval.useracl"));
    eos_info("acl=%d r=%d w=%d wo=%d egroup=%d shared=%d mutable=%d",
             acl.HasAcl(), acl.CanRead(), acl.CanWrite(), acl.CanWriteOnce(),
             acl.HasEgroup(),
             isSharedFile,
             acl.IsMutable());

    if (acl.HasAcl()) {
      if (isRW) {
        // write case
        if ((!acl.CanWrite()) && (!acl.CanWriteOnce())) {
          // we have to check the standard permissions
          stdpermcheck = true;
        }
      } else {
        // read case
        if ((!acl.CanRead())) {
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

    if ((!isSharedFile || isRW) && stdpermcheck
        && (!dmd->access(vid.uid, vid.gid, (isRW) ? W_OK | X_OK : R_OK | X_OK))) {
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
        (fmdsize != 0) &&
        ((eos::common::LayoutId::GetLayoutType(fmdlid) ==
          eos::common::LayoutId::kRaidDP) ||
         (eos::common::LayoutId::GetLayoutType(fmdlid) ==
          eos::common::LayoutId::kArchive) ||
         (eos::common::LayoutId::GetLayoutType(fmdlid) ==
          eos::common::LayoutId::kRaid6))) {
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
        eos_info("keep attached to existing fmd in chunked upload");
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
            if ((!stdpermcheck) && (!acl.CanWrite())) {
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
    if ((!fmd)) {
      if (!(open_flag & O_CREAT)) {
        // write open of not existing file without creation flag
        return Emsg(epname, error, errno, "open file without creation flag", path);
      } else {
        // creation of a new file or isOcUpload
        {
          // -------------------------------------------------------------------
          eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);

          try {
            if (!fmd) {
              // we create files with the uid/gid of the parent directory
              if (isAtomicUpload) {
                eos::common::Path cPath(path);
                creation_path = cPath.GetAtomicPath(versioning, ocUploadUuid);
                eos_info("atomic-path=%s", creation_path.c_str());
              }

              fmd = gOFS->eosView->createFile(creation_path, vid.uid, vid.gid);

              if (ocUploadUuid.length()) {
                fmd->setFlags(0);
              } else {
                fmd->setFlags(Mode & (S_IRWXU | S_IRWXG | S_IRWXO));
              }
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
            gOFS->FuseXCast(cmd->getId());
	          gOFS->FuseXCast(cmd->getParentId());
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
        return Emsg(epname, error, EEXIST, "create file", path);
      }

      if (acl.HasAcl()) {
        if (!acl.CanUpdate()) {
          // the ACL has !u set - we don't allow to do file updates
          gOFS->MgmStats.Add("OpenFailedNoUpdate", vid.uid, vid.gid, 1);
          return Emsg(epname, error, EPERM, "update file - fobidden by ACL",
                      path);
        }
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

      rcode = SFS_REDIRECT;
      error.setErrInfo(ecode, redirectionhost.c_str());
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

  if (isPioReconstruct) {
    capability += "&mgm.access=update";
  } else {
    if (isRW) {
      if (isRewrite) {
        capability += "&mgm.access=update";
      } else {
        capability += "&mgm.access=create";
      }
    } else {
      capability += "&mgm.access=read";
    }
  }

  // ---------------------------------------------------------------------------
  // forward some allowed user opaque tags
  // ---------------------------------------------------------------------------
  unsigned long layoutId = (isCreation) ? eos::common::LayoutId::kPlain : fmdlid;
  // the client can force to read a file on a defined file system
  unsigned long forcedFsId = 0;
  // the client can force to place a file in a specified group of a space
  long forcedGroup = -1;
  // this is the filesystem defining the client access point in the selection
  // vector - for writes it is always 0, for reads it comes out of the
  // FileAccess function
  unsigned long fsIndex = 0;
  XrdOucString space = "default";
  unsigned long new_lid = 0;
  // select space and layout according to policies
  Policy::GetLayoutAndSpace(path, attrmap, vid, new_lid, space, *openOpaque,
                            forcedFsId, forcedGroup);
  eos::mgm::Scheduler::tPlctPolicy plctplcy;
  std::string targetgeotag;
  // get placement policy
  Policy::GetPlctPolicy(path, attrmap, vid, *openOpaque, plctplcy, targetgeotag);
  // @todo (jmakai): fix this - the same lock is taken later on in ShouldStall-IsKnownNode
  eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
  unsigned long long ext_mtime_sec = 0;
  unsigned long long ext_mtime_nsec = 0;
  unsigned long long ext_ctime_sec = 0;
  unsigned long long ext_ctime_nsec = 0;

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

  if ((!isInjection) && (isCreation || ((open_mode == SFS_O_TRUNC)))) {
    eos_info("blocksize=%llu lid=%x",
             eos::common::LayoutId::GetBlocksize(new_lid), new_lid);
    layoutId = new_lid;
    {
      std::shared_ptr<eos::IFileMD> fmdnew;
      eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);

      if (!byfid) {
        try {
          fmdnew = gOFS->eosView->getFile(path);
        } catch (eos::MDException& e) {
          // TODO: this should be review to see if it is possible
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

      try {
        gOFS->eosView->updateFileStore(fmd.get());
        gOFS->FuseXCast(eos::common::FileId::FidToInode(fmd->getId()));
        std::shared_ptr<eos::IContainerMD> cmd =
          gOFS->eosDirectoryService->getContainerMD(cid);
        cmd->setMTimeNow();
        cmd->notifyMTimeChange(gOFS->eosDirectoryService);
        gOFS->eosView->updateContainerStore(cmd.get());
        gOFS->FuseXCast(cmd->getId());
	gOFS->FuseXCast(cmd->getParentId());

        if (isCreation || (!fmd->getNumLocation())) {
          eos::IQuotaNode* ns_quota = gOFS->eosView->getQuotaNode(cmd.get());

          if (ns_quota) {
            ns_quota->addFile(fmd.get());
          }
        }
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

  capability += "&mgm.ruid=";
  capability += (int) vid.uid;
  capability += "&mgm.rgid=";
  capability += (int) vid.gid;
  capability += "&mgm.uid=";
  capability += (int) vid.uid_list[0];
  capability += "&mgm.gid=";
  capability += (int) vid.gid_list[0];
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
  XrdOucString hexfid;
  eos::common::FileId::Fid2Hex(fileId, hexfid);
  capability += hexfid;
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
  unsigned long long bookingsize;
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
    plctargs.bookingsize = bookingsize;
    plctargs.dataproxys = &proxys;
    plctargs.firewallentpts = &firewalleps;
    plctargs.forced_scheduling_group_index = forcedGroup;
    plctargs.grouptag = containertag;
    plctargs.lid = layoutId;
    plctargs.inode = (ino64_t) fmd->getId();
    plctargs.path = path;
    plctargs.plctTrgGeotag = &targetgeotag;
    plctargs.plctpolicy = plctplcy;
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
  } else {
    // Access existing file - fill the vector with the existing locations
    for (unsigned int i = 0; i < fmd->getNumLocation(); i++) {
      int loc = fmd->getLocation(i);

      if (loc) {
        selectedfs.push_back(loc);
      }
    }

    if (selectedfs.empty()) {
      // this file has not a single existing replica
      gOFS->MgmStats.Add("OpenFileOffline", vid.uid, vid.gid, 1);
      return Emsg(epname, error, ENODEV, "open - no replica exists", path);
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

    retc = Quota::FileAccess(&acsargs);

    if ( ((retc == ENETUNREACH) || (retc == EROFS)) && 
	 ( ((!fmd->getSize()) && (!bookingsize)) || (isRepair) ) ) {
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
      plctargs.alreadyused_filesystems = &selectedfs;
      plctargs.bookingsize = bookingsize;
      plctargs.dataproxys = &proxys;
      plctargs.firewallentpts = &firewalleps;
      plctargs.forced_scheduling_group_index = forcedGroup;
      plctargs.grouptag = containertag;
      plctargs.lid = layoutId;
      plctargs.inode = (ino64_t) fmd->getId();
      plctargs.path = path;
      plctargs.plctTrgGeotag = &targetgeotag;
      plctargs.plctpolicy = plctplcy;
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
    }

    if (retc == EXDEV) {
      // Indicating that the layout requires the replacement of stripes
      retc = 0; // TODO: we currently don't support repair on the fly mode
    }
  }

  /// ###############
  eos::common::Logging& g_logging = eos::common::Logging::GetInstance();

  if (g_logging.gLogMask & LOG_MASK(LOG_DEBUG)) {
    std::stringstream strstr;
    strstr << "\nselectedfs are : ";

    for (const auto& it : selectedfs) {
      strstr << it << "  ";
    }

    strstr << "\nproxys are : ";

    for (const auto& it : proxys) {
      strstr << it << "  ";
    }

    strstr << "\nfirewallentrypoints are : ";

    for (const auto& it : firewalleps) {
      strstr << it << "  ";
    }

    strstr << "  and retc=" << retc;
    eos_static_debug(strstr.str().c_str());
  }

  /// ###############

  if (retc) {
    // If we don't have quota we don't bounce the client back
    if ((retc != ENOSPC) && (retc != EDQUOT)) {
      // INLINE Workflows
      int stalltime = 0;
      workflow.SetFile(path, fmd->getId());

      if ((stalltime = workflow.Trigger("open", "enonet", vid)) > 0) {
        eos_info("msg=\"triggered ENOENT workflow\" path=%s", path);
        return gOFS->Stall(error, stalltime, ""
                           "File is currently unavailable - triggered workflow!");
      }

      // check if we have a global redirect or stall for offline files
      MAYREDIRECT_ENONET;
      MAYSTALL_ENONET;
      MAYREDIRECT_ENETUNREACH;
      MAYSTALL_ENETUNREACH;

      // INLINE REPAIR
      // - if files are less than 1GB we try to repair them inline - max. 3 time
      if ((!isCreation) && isRW && attrmap.count("sys.heal.unavailable") &&
          (fmd->getSize() < (1 * 1024 * 1024 * 1024))) {
        int nmaxheal = 3;

        if (attrmap.count("sys.heal.unavailable")) {
          nmaxheal = atoi(attrmap["sys.heal.unavailable"].c_str());
        }

        int nheal = 0;
        gOFS->MgmHealMapMutex.Lock();

        if (gOFS->MgmHealMap.count(fileId)) {
          nheal = gOFS->MgmHealMap[fileId];
        }

        // if there was already a healing
        if (nheal >= nmaxheal) {
          // we tried nmaxheal times to heal, so we abort now and
          // return an error to the client
          gOFS->MgmHealMap.erase(fileId);
          gOFS->MgmHealMap.resize(0);
          gOFS->MgmHealMapMutex.UnLock();
          gOFS->MgmStats.Add("OpenFailedHeal", vid.uid, vid.gid, 1);
          XrdOucString msg = "heal file with inaccessible replica's after ";
          msg += (int) nmaxheal;
          msg += " tries - giving up";
          eos_err("%s", msg.c_str());
          return Emsg(epname, error, ENOSR, msg.c_str(), path);
        }

        eos_info("msg=\"in-line healing\" path=%s", path);
        // increase the heal counter for that file id
        gOFS->MgmHealMap[fileId] = nheal + 1;
        gOFS->MgmHealMapMutex.UnLock();
        auto proc_cmd = ProcInterface::GetProcCommand(tident, vid);

        if (proc_cmd) {
          // Issue the version command
          XrdOucString cmd = "mgm.cmd=file&mgm.subcmd=version&"
                             "mgm.purge.version=-1&mgm.path=";
          cmd += path;
          proc_cmd->open("/proc/user/", cmd.c_str(), vid, &error);
          proc_cmd->close();
          int stalltime = 1; // let the client come back quickly

          if (attrmap.count("sys.stall.unavailable")) {
            stalltime = atoi(attrmap["sys.stall.unavailable"].c_str());
          }

          gOFS->MgmStats.Add("OpenStalledHeal", vid.uid, vid.gid, 1);
          eos_info("attr=sys info=\"stalling file\" path=%s rw=%d stalltime=%d nstall=%d",
                   path, isRW, stalltime, nheal);
          return gOFS->Stall(error, stalltime, ""
                             "Required filesystems are currently unavailable!");
        } else {
          return Emsg(epname, error, ENOMEM, "allocate proc command object for ",
                      path);
        }
      }

      // ----------------------------------------------------------------------
      // ASYNC REPAIR
      // - for big files if defined
      // check if we should try to heal offline replicas (rw mode only)
      // ----------------------------------------------------------------------
      if ((!isCreation) && isRW && attrmap.count("sys.heal.unavailable")) {
        int nmaxheal = atoi(attrmap["sys.heal.unavailable"].c_str());
        int nheal = 0;
        gOFS->MgmHealMapMutex.Lock();

        if (gOFS->MgmHealMap.count(fileId)) {
          nheal = gOFS->MgmHealMap[fileId];
        }

        // if there was already a healing
        if (nheal >= nmaxheal) {
          // we tried nmaxheal times to heal, so we abort now and
          // return an error to the client
          gOFS->MgmHealMap.erase(fileId);
          gOFS->MgmHealMap.resize(0);
          gOFS->MgmHealMapMutex.UnLock();
          gOFS->MgmStats.Add("OpenFailedHeal", vid.uid, vid.gid, 1);
          XrdOucString msg = "heal file with inaccesible replica's after ";
          msg += (int) nmaxheal;
          msg += " tries - giving up";
          eos_err("%s", msg.c_str());
          return Emsg(epname, error, ENOSR, msg.c_str(), path);
        } else {
          // increase the heal counter for that file id
          gOFS->MgmHealMap[fileId] = nheal + 1;
          auto proc_cmd = ProcInterface::GetProcCommand(tident, vid);

          if (proc_cmd) {
            // issue the adjustreplica command as root
            eos::common::Mapping::VirtualIdentity vidroot;
            eos::common::Mapping::Copy(vid, vidroot);
            eos::common::Mapping::Root(vidroot);
            XrdOucString cmd = "mgm.cmd=file&mgm.subcmd=adjustreplica&"
                               "mgm.file.express=1&mgm.path=";
            cmd += path;
            proc_cmd->open("/proc/user/", cmd.c_str(), vidroot, &error);
            proc_cmd->close();
            int stalltime = 60; // 1 min by default

            if (attrmap.count("sys.stall.unavailable")) {
              stalltime = atoi(attrmap["sys.stall.unavailable"].c_str());
            }

            gOFS->MgmStats.Add("OpenStalledHeal", vid.uid, vid.gid, 1);
            eos_info("attr=sys info=\"stalling file\" path=%s rw=%d stalltime=%d nstall=%d",
                     path, isRW, stalltime, nheal);
            gOFS->MgmHealMapMutex.UnLock();
            return gOFS->Stall(error, stalltime, ""
                               "Required filesystems are currently unavailable!");
          } else {
            gOFS->MgmHealMapMutex.UnLock();
            return Emsg(epname, error, ENOMEM, "allocate proc command object for ",
                        path);
          }
        }
      }

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

        rcode = SFS_REDIRECT;
        error.setErrInfo(ecode, redirectionhost.c_str());
        gOFS->MgmStats.Add("RedirectENONET", vid.uid, vid.gid, 1);
        return rcode;
      }

      if (!gOFS->MgmMaster.IsMaster() && gOFS->MgmMaster.IsRemoteMasterOk()) {
        // redirect ENONET to an alive remote master
        redirectionhost = gOFS->MgmMaster.GetMasterHost();
        ecode = 1094;
        rcode = SFS_REDIRECT;
        error.setErrInfo(ecode, redirectionhost.c_str());
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
          eos::common::RWMutexReadLock rd_lock(gOFS->eosViewRWMutex);
          auto tmp_fmd = gOFS->eosView->getFile(path);

          if (tmp_fmd->getNumLocation() == 0) {
            do_remove = true;
          }
        } catch (eos::MDException& e) {}

        if (do_remove) {
          eos::common::Mapping::VirtualIdentity vidroot;
          eos::common::Mapping::Root(vidroot);
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
          std::string binchecksum = eos::common::LayoutId::GetEmptyFileChecksum(layoutId);
          eos::Buffer cx;
          cx.putData(binchecksum.c_str(), binchecksum.size());
          eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);
          // -------------------------------------------------------------------

          try {
            fmd = gOFS->eosView->getFile(creation_path);

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
          eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);

          try {
            fmd = gOFS->eosFileService->getFileMD(byfid);

            if (isRecreation) {
              fmd->unlinkAllLocations();
            }

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
          filesystem = 0;
          fsgeotag = "";

          if (FsView::gFsView.mIdView.count(selectedfs[k])) {
            filesystem = FsView::gFsView.mIdView[selectedfs[k]];
            fsgeotag = filesystem->GetString("stat.geotag");
          }

          // if the fs is available
          if (std::find(unavailfs.begin(), unavailfs.end(),
                        selectedfs[k]) == unavailfs.end()) {
            // take the highest fsid with the same geotag if possible
            if ((vid.geolocation.empty() || fsgeotag == vid.geolocation) &&
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
      }
    } else {
      if (!isFuse && !fmd->getSize()) {
        // 0-size files can be read from the MGM if this is not FUSE access!
        isZeroSizeFile = true;
        return SFS_OK;
      }
    }
  }

  // Get the redirection host from the selected entry in the vector
  if (!selectedfs[fsIndex]) {
    eos_err("0 filesystem in selection");
    return Emsg(epname, error, ENETUNREACH, "received filesystem id 0", path);
  }

  if (FsView::gFsView.mIdView.count(selectedfs[fsIndex])) {
    filesystem = FsView::gFsView.mIdView[selectedfs[fsIndex]];
  } else {
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
      } else {
        targethost = firewalleps[fsIndex].c_str();
        targetport = 0;
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
      } else { // we have a proxy to use
        auto idx = proxys[fsIndex].rfind(':');

        if (idx != std::string::npos) {
          targethost = proxys[fsIndex].substr(0, idx).c_str();
          targetport = atoi(proxys[fsIndex].substr(idx + 1, std::string::npos).c_str());
        } else {
          targethost = proxys[fsIndex].c_str();
          targetport = 0;
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
    redirectionhost = targethost;
    redirectionhost += "?";
  }

  // ---------------------------------------------------------------------------
  // Rebuild the layout ID (for read it should indicate only the number of
  // available stripes for reading);
  // For 'pio' mode we hand out plain layouts to the client and add the IO
  // layout as an extra field
  // ---------------------------------------------------------------------------
  std::set<unsigned long> ufs;
  {
    // get the unique number of filesystems
    for (size_t i = 0; i < selectedfs.size(); i++) {
      ufs.insert(selectedfs[i]);
    }

    for (size_t i = 0; i < PioReconstructFsList.size(); i++) {
      ufs.insert(PioReconstructFsList[i]);
    }
  }
  new_lid = eos::common::LayoutId::GetId(
              isPio ? eos::common::LayoutId::kPlain :
              eos::common::LayoutId::GetLayoutType(layoutId),
              (isPio ? eos::common::LayoutId::kNone :
               eos::common::LayoutId::GetChecksum(layoutId)),
              isPioReconstruct ? static_cast<int>(ufs.size()) : static_cast<int>
              (selectedfs.size()),
              eos::common::LayoutId::GetBlocksizeType(layoutId),
              eos::common::LayoutId::GetBlockChecksum(layoutId));
  unsigned long orig_type = eos::common::LayoutId::GetLayoutType(layoutId);

  // For RAIN layouts we need to keep the original number of stripes since this
  // is used to compute the different groups and block sizes in the FSTs
  if ((orig_type == eos::common::LayoutId::kRaidDP) ||
      (orig_type == eos::common::LayoutId::kRaid6) ||
      (orig_type == eos::common::LayoutId::kArchive)) {
    eos::common::LayoutId::SetStripeNumber(new_lid,
                                           eos::common::LayoutId::GetStripeNumber(layoutId));
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

  // expected size of the target file on close
  if (targetsize) {
    capability += "&mgm.targetsize=";
    capability += eos::common::StringConversion::GetSizeString(sizestring,
                  targetsize);
  }

  if (eos::common::LayoutId::GetLayoutType(layoutId) ==
      eos::common::LayoutId::kPlain) {
    capability += "&mgm.fsid=";
    capability += (int) filesystem->GetId();
  }

  XrdOucString infolog = "";
  XrdOucString piolist = "";

  if ((eos::common::LayoutId::GetLayoutType(layoutId) ==
       eos::common::LayoutId::kReplica) ||
      (eos::common::LayoutId::GetLayoutType(layoutId) ==
       eos::common::LayoutId::kRaidDP) ||
      (eos::common::LayoutId::GetLayoutType(layoutId) ==
       eos::common::LayoutId::kArchive) ||
      (eos::common::LayoutId::GetLayoutType(layoutId) ==
       eos::common::LayoutId::kRaid6)) {
    capability += "&mgm.fsid=";
    capability += (int) filesystem->GetId();
    eos::mgm::FileSystem* repfilesystem = 0;
    replacedfs.resize(selectedfs.size());

    // -------------------------------------------------------------------------
    // if replacement has been specified try to get new locations for reco.
    // -------------------------------------------------------------------------

    if (isPioReconstruct && !(PioReconstructFsList.empty())) {
      const char* containertag = 0;

      if (attrmap.count("user.tag")) {
        containertag = attrmap["user.tag"].c_str();
      }

      // -----------------------------------------------------------------------
      // create a plain layout with the number of replacement stripes to be
      // scheduled in the file placement routine
      // -----------------------------------------------------------------------
      unsigned long plainLayoutId = new_lid;
      eos::common::LayoutId::SetStripeNumber(plainLayoutId,
                                             PioReconstructFsList.size() - 1);
      // -----------------------------------------------------------------------
      // get the original placement group of the first fs to reconstruct
      {
        eos::common::FileSystem::fs_snapshot orig_snapshot;
        // get an original filesystem which is not in the reconstruction list
        unsigned int orig_fs = 0;

        for (unsigned int i = 0; i < fmd->getNumLocation(); i++) {
          orig_fs = fmd->getLocation(i);
          bool isInReco = false;

          for (unsigned int j = 0; j < PioReconstructFsList.size(); j++) {
            if (orig_fs == PioReconstructFsList[j]) {
              isInReco = true;
              break;
            }
          }

          if (!isInReco) {
            break;
          }

          orig_fs = 0;
        }

        if (!orig_fs) {
          // there is no original filesystem which is not in reconstruction
          return Emsg(epname, error, EINVAL, "get original filesystem for reconstruction",
                      path);
        }

        if (!FsView::gFsView.mIdView.count(orig_fs)) {
          // not existing original filesystem
          return Emsg(epname, error, EINVAL, "reconstruct filesystem", path);
        }

        // get an original filesystem which is not in the reconstruction list
        eos::mgm::FileSystem* origfs = FsView::gFsView.mIdView[orig_fs];
        origfs->SnapShotFileSystem(orig_snapshot);
        forcedGroup = orig_snapshot.mGroupIndex;
      }
      // -----------------------------------------------------------------------
      eos_info("nstripes=%d => nstripes=%d [ sub-group=%d ]",
               eos::common::LayoutId::GetStripeNumber(new_lid),
               eos::common::LayoutId::GetStripeNumber(plainLayoutId),
               forcedGroup);
      // -----------------------------------------------------------------------
      // compute the size of the stripes to be placed
      // -----------------------------------------------------------------------
      unsigned long long plainBookingSize =
        fmd->getSize() /
        (eos::common::LayoutId::GetStripeNumber(layoutId) + 1);
      plainBookingSize += 4096;
      plainBookingSize *= PioReconstructFsList.size();
      eos::common::Mapping::VirtualIdentity rootvid;
      eos::common::Mapping::Root(rootvid);
      /// ###############
      // if the client should go through a firewall entrypoint, try to get it
      // if the scheduled fs need to be accessed through a dataproxy, try to get it
      // if any of the two fails, the scheduling operation fails
      Scheduler::PlacementArguments plctargs;
      plctargs.alreadyused_filesystems = &selectedfs;
      plctargs.bookingsize = plainBookingSize;
      plctargs.dataproxys = &proxys;
      plctargs.firewallentpts = &firewalleps;
      plctargs.forced_scheduling_group_index = forcedGroup;
      plctargs.grouptag = containertag;
      plctargs.lid = plainLayoutId;
      plctargs.inode = (ino64_t) fmd->getId();
      plctargs.path = path;
      plctargs.plctTrgGeotag = &targetgeotag;
      plctargs.plctpolicy = plctplcy;
      plctargs.selected_filesystems = &PioReplacementFsList;
      std::string spacename = space.c_str();
      plctargs.spacename = &spacename;
      plctargs.truncate = false;
      plctargs.vid = &rootvid;

      if (!plctargs.isValid()) {
        // there is something wrong in the arguments of file placement
        return Emsg(epname, error, EIO, "open - invalid placement argument", path);
      }

      retc = Quota::FilePlacement(&plctargs);

      /// ###############
      if (g_logging.gLogMask & LOG_MASK(LOG_DEBUG)) {
        std::stringstream strstr;
        strstr << "\nselectedfs are : ";

        for (const auto& it : selectedfs) {
          strstr << it << "  ";
        }

        strstr << "\nproxys are : ";

        for (const auto& it : proxys) {
          strstr << it << "  ";
        }

        strstr << "\nfirewallentrypoints are : ";

        for (const auto& it : firewalleps) {
          strstr << it << "  ";
        }

        strstr << "  and retc=" << retc;
        eos_static_debug(strstr.str().c_str());
      }

      /// ###############

      if (retc) {
        // the placement didn't work, we cannot schedule reconstruction
        gOFS->MgmStats.Add("OpenFailedReconstruct", rootvid.uid, rootvid.gid, 1);
        return Emsg(epname, error, retc, "schedule stripes for reconstruction", path);
      }

      for (int i = 0; i < (int) PioReplacementFsList.size(); i++) {
        eos_debug("msg=\"scheduled fs for reconstruction\" rec-fsid=%lu nrecofs=%lu",
                  PioReplacementFsList[i], PioReplacementFsList.size());
      }

      // add fsid=0 filesystems to the selection vector if it has less than the nominal replica
      auto selection_diff = (eos::common::LayoutId::GetStripeNumber(
                              fmd->getLayoutId()) + 1) - selectedfs.size();
      eos_info("selection-diff=%d %d/%d", selection_diff,
               (eos::common::LayoutId::GetStripeNumber(fmd->getLayoutId()) + 1),
               selectedfs.size());

      if (selection_diff > 0) {
        unavailfs.push_back(0);

        for (auto i = 0ul; i < selection_diff; i++) {
          selectedfs.push_back(0);
          eos_info("msg=\"adding fsid=0 as missing filesystem\"");
        }
      }
    }

    // put all the replica urls into the capability
    for (unsigned int i = 0; i < selectedfs.size(); ++i) {
      if (!selectedfs[i]) {
        eos_err("0 filesystem in replica vector");
      }

      // -----------------------------------------------------------------------
      // Logic to discover filesystems to be reconstructed
      // -----------------------------------------------------------------------
      bool replace = false;

      if (isPioReconstruct) {
        for (size_t k = 0; k < PioReconstructFsList.size(); k++) {
          if (selectedfs[i] == PioReconstructFsList[k]) {
            replace = true;
            break;
          }
        }
      }

      if (replace) {
        if (PioReplacementFsList.empty()) {
          // if we don't have found any filesystem to be used as a replacement
          return Emsg(epname,
                      error,
                      EIO,
                      "get replacement file system",
                      path);
        }

        // ---------------------------------------------------------------------
        // take one replacement filesystem from the replacement list
        // ---------------------------------------------------------------------
        replacedfs[i] = selectedfs[i];
        selectedfs[i] = PioReplacementFsList.back();
        eos_info("msg=\"replace fs\" old-fsid=%u new-fsid=%u", replacedfs[i],
                 selectedfs[i]);
        PioReplacementFsList.pop_back();
      } else {
        // there is no replacement happening
        replacedfs[i] = 0;
      }

      if (FsView::gFsView.mIdView.count(selectedfs[i])) {
        repfilesystem = FsView::gFsView.mIdView[selectedfs[i]];
      } else {
        repfilesystem = 0;
      }

      if (!repfilesystem) {
        // don't fail IO on a shadow file system but throw a ciritical error message
        eos_crit("msg=\"Unable to get replica filesystem information\" "
                 "path=\"%s\" fsid=%d", path, selectedfs[i]);
        continue;
      } else {
        if (replace) {
          // point at the right vector entry
          fsIndex = i;

          // Set the FST gateway if this is available otherwise the actual FST
          if ((firewalleps.size() > fsIndex) && (proxys.size() > fsIndex) &&
              !(firewalleps[fsIndex].empty()) &&
              ((!proxys[fsIndex].empty() && firewalleps[fsIndex] != proxys[fsIndex]) ||
               (firewalleps[fsIndex] != filesystem->GetString("hostport")))) {
            // Build the URL for the forwarding proxy and must have the following
            // redirection proxy:port?eos.fstfrw=endpoint:port/abspath
            auto idx = firewalleps[fsIndex].rfind(':');

            if (idx != std::string::npos) {
              targethost = firewalleps[fsIndex].substr(0, idx).c_str();
              targetport = atoi(firewalleps[fsIndex].substr(idx + 1,
                                std::string::npos).c_str());
            } else {
              targethost = firewalleps[fsIndex].c_str();
              targetport = 0;
            }

            std::ostringstream oss;
            oss << targethost << "?" << "eos.fstfrw=";

            // check if we have to redirect to the fs host or to a proxy
            if (proxys[fsIndex].empty()) {
              oss << filesystem->GetString("host").c_str() << ":" <<
                  filesystem->GetString("port").c_str();
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
              } else {
                targethost = proxys[fsIndex].c_str();
                targetport = 0;
              }
            } else {
              // There is no proxy to use
              targethost  = filesystem->GetString("host").c_str();
              targetport  = atoi(filesystem->GetString("port").c_str());
            }

            redirectionhost = targethost;
            redirectionhost += "?";
          }

          // point at the right vector entry
          fsIndex = i;
        }
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

      eos_debug("Redirection Url %d => %s", i, replicahost.c_str());
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
  XrdOucEnv* capabilityenv = 0;
  eos::common::SymKey* symkey = eos::common::gSymKeyStore.GetCurrentKey();
  eos_debug("capability=%s\n", capability.c_str());
  int caprc = 0;

  if ((caprc = gCapabilityEngine.Create(&incapability, capabilityenv, symkey,
                                        gOFS->mCapabilityValidity))) {
    return Emsg(epname, error, caprc, "sign capability", path);
  }

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
      if ((!isRW) && (eos::common::LayoutId::GetLayoutType(layoutId) ==
                      eos::common::LayoutId::kReplica)) {
        redirectionhost += "&mgm.blockchecksum=ignore";
      }
    }

    if (openOpaque->Get("eos.checksum")) {
      redirectionhost += "&mgm.checksum=";
      redirectionhost += openOpaque->Get("eos.checksum");
    }

    if (openOpaque->Get("eos.mtime")) {
      redirectionhost += "&mgm.time=";
      redirectionhost += openOpaque->Get("eos.mtime");
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
    eos::common::Mapping::VirtualIdentity rootvid;
    eos::common::Mapping::Root(rootvid);
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
  redirectionhost += hexfid;

  if (isFuse) {
    redirectionhost += "&mgm.mtime=0";
  }

  // add workflow cgis
  if (isRW) {
    redirectionhost += workflow.getCGICloseW(currentWorkflow.c_str()).c_str();
  } else {
    redirectionhost += workflow.getCGICloseR(currentWorkflow.c_str()).c_str();
  }

  // Always redirect
  ecode = targetport;
  rcode = SFS_REDIRECT;
  error.setErrInfo(ecode, redirectionhost.c_str());

  if (redirectionhost.length() > (int) XrdOucEI::Max_Error_Len) {
    return Emsg(epname, error, ENOMEM,
                "open file - capability exceeds 2kb limit", path);
  }

  XrdOucString predirectionhost = redirectionhost.c_str();
  eos::common::StringConversion::MaskTag(predirectionhost, "cap.msg");
  eos::common::StringConversion::MaskTag(predirectionhost, "cap.sym");

  if (isRW) {
    eos_info("op=write path=%s info=%s %s redirection=%s:%d",
             path, pinfo.c_str(), infolog.c_str(), predirectionhost.c_str(),
             ecode);
  } else {
    eos_info("op=read  path=%s info=%s %s redirection=%s:%d",
             path, pinfo.c_str(), infolog.c_str(), predirectionhost.c_str(),
             ecode);
  }

  eos_info("info=\"redirection\" hostport=%s:%d", predirectionhost.c_str(),
           ecode);

  delete capabilityenv;

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
      eos::common::RWMutexWriteLock lock(gOFS->eosViewRWMutex);

      try {
        fmd = gOFS->eosView->getFile(path);
        eos::IFileMD::ctime_t ctime;
        fmd->getCTime(ctime);

        if ((ctime.tv_sec + age) < now) {
          // only update within the resolution of the access tracking
          fmd->setCTimeNow();
          gOFS->eosView->updateFileStore(fmd.get());
          gOFS->FuseXCast(eos::common::FileId::FidToInode(fmd->getId()));
        }

        errno = 0;
      } catch (eos::MDException& e) {
        errno = e.getErrno();
        eos_warning("msg=\"failed to update access time\" path=\"%s\" ec=%d emsg=\"%s\"\n",
                    path, e.getErrno(), e.getMessage().str().c_str());
      }
    }
  }

  bool shouldWF = openOpaque->Get("eos.noworkflow") != nullptr
                  ? true : !(std::string{openOpaque->Get("eos.noworkflow")} == std::string{"1"});

  // Also trigger synchronous create workflow event if it's defined
  if(shouldWF && isCreation) {
    errno = 0;
    int ret_wfe = 0;
    workflow.SetFile(path, fileId);
    auto workflowType = openOpaque->Get("eos.workflow") != nullptr ? openOpaque->Get("eos.workflow") : "default";
    if ((ret_wfe = workflow.Trigger("sync::create", std::string{workflowType}, vid) < 0) && errno == ENOKEY) {
      eos_info("msg=\"no workflow defined for sync::create\"");
    } else {
      eos_info("msg=\"workflow trigger returned\" retc=%d errno=%d", ret_wfe, errno);
      if (ret_wfe != 0) {
        // Error from the workflow
        rcode = SFS_ERROR;
      }
    }
  }

  // Also trigger synchronous open-write workflow event if it's defined
  if(shouldWF && isRW) {
    errno = 0;
    int ret_wfe = 0;
    workflow.SetFile(path, fileId);
    auto workflowType = openOpaque->Get("eos.workflow") != nullptr ? openOpaque->Get("eos.workflow") : "default";
    if ((ret_wfe = workflow.Trigger("sync::openw", std::string{workflowType}, vid) < 0) && errno == ENOKEY) {
      eos_info("msg=\"no workflow defined for sync::openw\"");
    } else {
      eos_info("msg=\"workflow trigger returned\" retc=%d errno=%d", ret_wfe, errno);
      if (ret_wfe != 0) {
        // Error from the workflow
        rcode = SFS_ERROR;
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

  // Make sure the offset is not too large
  //
#if _FILE_OFFSET_BITS!=64

  if (offset > 0x000000007fffffff) {
    return Emsg(epname, error, EFBIG, "read", fileName.c_str());
  }

#endif

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
  // Make sure the offset is not too large
  //
#if _FILE_OFFSET_BITS!=64

  if (offset > 0x000000007fffffff) {
    return Emsg(epname, error, EFBIG, "write", fileName.c_str());
  }

#endif
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
  // Make sure the offset is not too larg
#if _FILE_OFFSET_BITS!=64

  if (flen > 0x000000007fffffff) {
    return Emsg(epname, error, EFBIG, "truncate", fileName.c_str());
  }

#endif
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


/*----------------------------------------------------------------------------*/
int
XrdMgmOfsFile::Emsg(const char* pfx,
                    XrdOucErrInfo& einfo,
                    int ecode,
                    const char* op,
                    const char* target)
/*----------------------------------------------------------------------------*/
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
/*----------------------------------------------------------------------------*/
{
  char* etext, buffer[4096], unkbuff[64];

  // ---------------------------------------------------------------------------
  // Get the reason for the error
  // ---------------------------------------------------------------------------
  if (ecode < 0) {
    ecode = -ecode;
  }

  if (!(etext = strerror(ecode))) {
    sprintf(unkbuff, "reason unknown (%d)", ecode);
    etext = unkbuff;
  }

  // ---------------------------------------------------------------------------
  // Format the error message
  // ---------------------------------------------------------------------------
  snprintf(buffer, sizeof(buffer), "Unable to %s %s; %s", op, target, etext);
  eos_err("Unable to %s %s; %s", op, target, etext);
  // ---------------------------------------------------------------------------
  // Print it out if debugging is enabled
  // ---------------------------------------------------------------------------
#ifndef NODEBUG
  //   XrdMgmOfs::eDest->Emsg(pfx, buffer);
#endif
  // ---------------------------------------------------------------------------
  // Place the error message in the error object and return
  // ---------------------------------------------------------------------------
  einfo.setErrInfo(ecode, buffer);
  return SFS_ERROR;
}
