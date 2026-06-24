//------------------------------------------------------------------------------
// @file: FileCmd.cc
// @author: Octavian-Mihai Matei - CERN
//------------------------------------------------------------------------------

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

#include "FileCmd.hh"
#include "common/LayoutId.hh"
#include "common/Path.hh"
#include "common/SecEntity.hh"
#include "common/Utils.hh"
#include "mgm/access/Access.hh"
#include "mgm/acl/Acl.hh"
#include "mgm/convert/ConversionTag.hh"
#include "mgm/convert/ConverterEngine.hh"
#include "mgm/macros/Macros.hh"
#include "mgm/misc/Constants.hh"
#include "mgm/ofs/XrdMgmOfs.hh"
#include "mgm/policy/Policy.hh"
#include "mgm/proc/ProcInterface.hh"
#include "mgm/quota/Quota.hh"
#include "mgm/stat/Stat.hh"
#include "mgm/xattr/XattrLock.hh"
#include "namespace/Resolver.hh"
#include "namespace/interface/IContainerMDSvc.hh"
#include "namespace/interface/IFileMDSvc.hh"
#include "namespace/interface/IView.hh"
#include "namespace/utils/Attributes.hh"
#include "namespace/utils/Checksum.hh"
#include <XrdCl/XrdClCopyProcess.hh>
#include <math.h>
#include <memory>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Resolve the target path of the 'file' command from the Metadata field
//------------------------------------------------------------------------------
bool
FileCmd::ResolvePath(const eos::console::Metadata& md, std::string& spath,
                     unsigned long long& fid, eos::console::ReplyProto& reply,
                     bool allow_empty_path)
{
  fid = 0ull;
  spath.clear();

  if (md.id()) {
    fid = md.id();
    std::string lpath;
    std::string err_msg;

    if (GetPathFromFid(lpath, fid, err_msg)) {
      reply.set_std_err(err_msg);
    }

    spath = lpath;
  } else {
    spath = md.path();
  }

  std::string inpath = spath;
  std::string store_path = inpath;
  eos::mgm::NamespaceMap(store_path, nullptr, mVid);
  spath = store_path;
  std::string err_check;
  int errno_check = 0;

  if (IsOperationForbidden(spath, mVid, err_check, errno_check)) {
    reply.set_std_err(err_check);
    reply.set_retc(errno_check);
    return false;
  }

  if (spath.empty() && !fid && !allow_empty_path) {
    reply.set_std_err("error: you have to give a path name to call 'file'");
    reply.set_retc(EINVAL);
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Method implementing the specific behavior of the command executed by the
// asynchronous thread
//------------------------------------------------------------------------------
eos::console::ReplyProto
FileCmd::ProcessRequest() noexcept
{
  eos::console::ReplyProto reply;
  eos::console::FileProto file = mReqProto.file();
  std::string spath;
  unsigned long long fid = 0ull;

  switch (file.FileCommand_case()) {
  case eos::console::FileProto::kDrop:
    if (!ResolvePath(file.md(), spath, fid, reply, true)) {
      return reply;
    }

    DropSubcmd(file.drop(), spath, fid, reply);
    break;

  case eos::console::FileProto::kLayout:
    if (!ResolvePath(file.md(), spath, fid, reply, false)) {
      return reply;
    }

    LayoutSubcmd(file.layout(), spath, reply);
    break;

  case eos::console::FileProto::kVerify:
    if (!ResolvePath(file.md(), spath, fid, reply, false)) {
      return reply;
    }

    VerifySubcmd(file.verify(), spath, reply);
    break;

  case eos::console::FileProto::kMove:
    if (!ResolvePath(file.md(), spath, fid, reply, false)) {
      return reply;
    }

    MoveSubcmd(file.move(), spath, reply);
    break;

  case eos::console::FileProto::kReplicate:
    if (!ResolvePath(file.md(), spath, fid, reply, false)) {
      return reply;
    }

    ReplicateSubcmd(file.replicate(), spath, reply);
    break;

  case eos::console::FileProto::kShare:
    if (!ResolvePath(file.md(), spath, fid, reply, false)) {
      return reply;
    }

    ShareSubcmd(file.share(), spath, reply);
    break;

  case eos::console::FileProto::kRename:
    if (!ResolvePath(file.md(), spath, fid, reply, false)) {
      return reply;
    }

    RenameSubcmd(file.rename(), spath, reply);
    break;

  case eos::console::FileProto::kRenameWithSymlink:
    if (!ResolvePath(file.md(), spath, fid, reply, false)) {
      return reply;
    }

    RenameWithSymlinkSubcmd(file.rename_with_symlink(), spath, reply);
    break;

  case eos::console::FileProto::kSymlink:
    if (!ResolvePath(file.md(), spath, fid, reply, false)) {
      return reply;
    }

    SymlinkSubcmd(file.symlink(), spath, reply);
    break;

  case eos::console::FileProto::kWorkflow:
    if (!ResolvePath(file.md(), spath, fid, reply, false)) {
      return reply;
    }

    WorkflowSubcmd(file.workflow(), spath, reply);
    break;

  case eos::console::FileProto::kTag:
    if (!ResolvePath(file.md(), spath, fid, reply, false)) {
      return reply;
    }

    TagSubcmd(file.tag(), spath, fid, reply);
    break;

  case eos::console::FileProto::kCopy:
    if (!ResolvePath(file.md(), spath, fid, reply, false)) {
      return reply;
    }

    CopySubcmd(file.copy(), spath, reply);
    break;

  case eos::console::FileProto::kConvert:
    if (!ResolvePath(file.md(), spath, fid, reply, false)) {
      return reply;
    }

    ConvertSubcmd(file.convert(), spath, reply);
    break;

  case eos::console::FileProto::kTouch: {
    std::string spathid = file.md().id() ? std::to_string(file.md().id()) : "";

    if (!ResolvePath(file.md(), spath, fid, reply, false)) {
      return reply;
    }

    TouchSubcmd(file.touch(), spath, spathid, reply);
    break;
  }

  case eos::console::FileProto::kAdjustreplica:
    if (!ResolvePath(file.md(), spath, fid, reply, false)) {
      return reply;
    }

    AdjustReplicaSubcmd(file.adjustreplica(), spath, reply);
    break;

  case eos::console::FileProto::kPurge:
    if (!ResolvePath(file.md(), spath, fid, reply, false)) {
      return reply;
    }

    PurgeSubcmd(file.purge(), spath, reply);
    break;

  case eos::console::FileProto::kVersion:
    if (!ResolvePath(file.md(), spath, fid, reply, false)) {
      return reply;
    }

    VersionSubcmd(file.version(), spath, reply);
    break;

  case eos::console::FileProto::kVersions:
    if (!ResolvePath(file.md(), spath, fid, reply, false)) {
      return reply;
    }

    VersionsSubcmd(file.versions(), spath, reply);
    break;

  case eos::console::FileProto::kFileinfo:
    FileinfoSubcmd(file.fileinfo(), reply);
    break;

  case eos::console::FileProto::kRegister:
    // Not handled here - file registration goes through the dedicated
    // RequestProto::kRecord / FileRegisterCmd path. There is no
    // 'file register' CLI subcommand.
    reply.set_std_err("error: not supported");
    reply.set_retc(EINVAL);
    break;

  case eos::console::FileProto::kResync:
    // There is no standalone 'file resync' CLI subcommand - resync is only
    // exposed as a flag on 'file verify' (see FileVerifyProto.resync()).
    reply.set_std_err("error: not supported");
    reply.set_retc(EINVAL);
    break;

  case eos::console::FileProto::kCheck:
    // 'file check' stays on the legacy text protocol (client-side handling),
    // explicitly out of scope for this migration.
    reply.set_std_err("error: not supported");
    reply.set_retc(EINVAL);
    break;

  default:
    reply.set_std_err("error: not supported");
    reply.set_retc(EINVAL);
  }

  return reply;
}

//------------------------------------------------------------------------------
// Execute drop subcommand
//------------------------------------------------------------------------------
void
FileCmd::DropSubcmd(const eos::console::FileDropProto& drop, const std::string& spath,
                    unsigned long long fid, eos::console::ReplyProto& reply)
{
  XrdOucErrInfo mError;
  std::ostringstream std_out, std_err;
  bool forceRemove = drop.force();
  unsigned long fsid = drop.fsid();

  if (gOFS->_dropstripe(spath.c_str(), fid, mError, mVid, fsid, forceRemove)) {
    std_err << "error: unable to drop stripe";
    reply.set_retc(errno);
  } else {
    std_out << "success: dropped stripe on fs=" << (int)fsid;
  }

  reply.set_std_out(std_out.str());
  reply.set_std_err(std_err.str());
}

//------------------------------------------------------------------------------
// Execute layout subcommand
//------------------------------------------------------------------------------
void
FileCmd::LayoutSubcmd(const eos::console::FileLayoutProto& layout,
                      const std::string& spath, eos::console::ReplyProto& reply)
{
  XrdOucErrInfo mError;
  std::ostringstream std_out, std_err;
  int checksum_type = eos::common::LayoutId::kNone;
  XrdOucString ne = "eos.layout.checksum=";
  ne += layout.checksum().c_str();
  XrdOucEnv env(ne.c_str());
  int newstripenumber = (int)layout.stripes();
  std::string newlayoutstring = layout.type();
  bool has_cksum = !layout.checksum().empty();

  if (!newstripenumber && !has_cksum && newlayoutstring.empty()) {
    std_err << "error: you have to give a valid number of stripes"
               " as an argument to call 'file layout' or a valid checksum or a layout id";
    reply.set_retc(EINVAL);
  } else if (newstripenumber && ((newstripenumber < 1) || (newstripenumber > 255))) {
    std_err << "error: you have to give a valid number of stripes"
               " as an argument to call 'file layout'";
    reply.set_retc(EINVAL);
  } else if (has_cksum && ((checksum_type = eos::common::LayoutId::GetChecksumFromEnv(
                                env)) == eos::common::LayoutId::kNone)) {
    std_err << "error: you have to give a valid checksum typ0e"
               " as an argument to call 'file layout'";
    reply.set_retc(EINVAL);
  } else {
    // only root can do that
    if (mVid.uid == 0) {
      std::shared_ptr<eos::IFileMD> fmd;
      eos::common::RWMutexWriteLock viewWriteLock;
      XrdOucString xspath = spath.c_str();

      if (xspath.beginswith("fid:") || xspath.beginswith("fxid:")) {
        WAIT_BOOT;
        unsigned long long lfid =
            Resolver::retrieveFileIdentifier(xspath).getUnderlyingUInt64();
        viewWriteLock.Grab(gOFS->eosViewRWMutex);

        try {
          fmd = gOFS->eosFileService->getFileMD(lfid);
        } catch (eos::MDException& e) {
          errno = e.getErrno();
          std_err << "error: cannot retrieve file meta data - " << e.getMessage().str();
          eos_debug("caught exception %d %s\n", e.getErrno(),
                    e.getMessage().str().c_str());
        }
      } else {
        viewWriteLock.Grab(gOFS->eosViewRWMutex);

        try {
          fmd = gOFS->eosView->getFile(spath.c_str());
        } catch (eos::MDException& e) {
          errno = e.getErrno();
          std_err << "error: cannot retrieve file meta data - " << e.getMessage().str();
          eos_debug("caught exception %d %s\n", e.getErrno(),
                    e.getMessage().str().c_str());
        }
      }

      if (fmd) {
        bool only_replica = false;
        bool only_tape = false;
        bool any_layout = false;

        if (fmd->getNumLocation() > 0) {
          only_replica = true;
        } else {
          any_layout = true;
        }

        if (fmd->getNumLocation() == 1) {
          if (fmd->hasLocation(EOS_TAPE_FSID)) {
            only_tape = true;
          }
        }

        if (!has_cksum) {
          checksum_type = eos::common::LayoutId::GetChecksum(fmd->getLayoutId());
        }

        if (!newstripenumber) {
          newstripenumber =
              eos::common::LayoutId::GetStripeNumber(fmd->getLayoutId()) + 1;
        }

        int lid = eos::common::LayoutId::kReplica;
        unsigned long newlayout = eos::common::LayoutId::GetId(
            lid, checksum_type, newstripenumber,
            eos::common::LayoutId::GetBlocksizeType(fmd->getLayoutId()));

        if (newlayoutstring.length()) {
          newlayout = strtol(newlayoutstring.c_str(), 0, 16);
        }

        if ((only_replica &&
             (((eos::common::LayoutId::GetLayoutType(fmd->getLayoutId()) ==
                eos::common::LayoutId::kReplica) ||
               (eos::common::LayoutId::GetLayoutType(fmd->getLayoutId()) ==
                eos::common::LayoutId::kPlain)) &&
              (eos::common::LayoutId::GetLayoutType(newlayout) ==
               eos::common::LayoutId::kReplica))) ||
            only_tape || any_layout) {
          fmd->setLayoutId(newlayout);
          std_out << "success: setting layout to "
                  << eos::common::LayoutId::PrintLayoutString(newlayout)
                  << " for path=" << spath;
          gOFS->eosView->updateFileStore(fmd.get());
        } else {
          reply.set_retc(EPERM);
          std_err << "error: you can only change the number of "
                     "stripes for files with replica layout or files without locations";
        }
      } else {
        reply.set_retc(errno);
        std_err << "error: no such file";
      }

      viewWriteLock.Release();
    } else {
      reply.set_retc(EPERM);
      std_err << "error: you have to take role 'root' to execute this command";
    }
  }

  reply.set_std_out(std_out.str());
  reply.set_std_err(std_err.str());
}

//------------------------------------------------------------------------------
// Execute verify subcommand
//------------------------------------------------------------------------------
void
FileCmd::VerifySubcmd(const eos::console::FileVerifyProto& verify,
                      const std::string& in_spath, eos::console::ReplyProto& reply)
{
  XrdOucErrInfo mError;
  std::ostringstream std_out, std_err;
  std::string spath = in_spath;
  XrdOucString option = "";
  bool computechecksum = verify.checksum();
  bool commitchecksum = verify.commitchecksum();
  bool commitsize = verify.commitsize();
  bool commitfmd = verify.commitfmd();
  bool doresync = verify.resync();

  if (computechecksum) {
    option += "&mgm.verify.compute.checksum=1";
  }

  if (commitchecksum) {
    option += "&mgm.verify.commit.checksum=1";
  }

  if (commitsize) {
    option += "&mgm.verify.commit.size=1";
  }

  if (commitfmd) {
    option += "&mgm.verify.commit.fmd=1";
  }

  if (verify.rate()) {
    option += "&mgm.verify.rate=";
    option += std::to_string(verify.rate()).c_str();
  }

  int acceptfsid = (int)verify.fsid();
  int retc = 0;

  // only root can do that
  if (mVid.uid == 0) {
    eos::common::RWMutexReadLock viewReadLock(gOFS->eosViewRWMutex);
    std::shared_ptr<eos::IFileMD> fmd;
    XrdOucString xspath = spath.c_str();

    if (xspath.beginswith("fid:") || xspath.beginswith("fxid:")) {
      WAIT_BOOT;
      unsigned long long lfid =
          Resolver::retrieveFileIdentifier(xspath).getUnderlyingUInt64();

      try {
        fmd = gOFS->eosFileService->getFileMD(lfid);
        std::string fullpath = gOFS->eosView->getUri(fmd.get());
        spath = fullpath;
      } catch (eos::MDException& e) {
        errno = e.getErrno();
        std_err << "error: cannot retrieve file meta data - " << e.getMessage().str();
        eos_debug("caught exception %d %s\n", e.getErrno(), e.getMessage().str().c_str());
      }
    } else {
      try {
        fmd = gOFS->eosView->getFile(spath.c_str());
      } catch (eos::MDException& e) {
        errno = e.getErrno();
        std_err << "error: cannot retrieve file meta data - " << e.getMessage().str();
        eos_debug("caught exception %d %s\n", e.getErrno(), e.getMessage().str().c_str());
      }
    }

    if (fmd) {
      eos::IFileMD::LocationVector::const_iterator it;
      bool isRAIN = false;
      eos::IFileMD::LocationVector locations = fmd->getLocations();
      eos::common::LayoutId::layoutid_t fmdlid = fmd->getLayoutId();
      eos::common::FileId::fileid_t fileid = fmd->getId();

      if ((eos::common::LayoutId::GetLayoutType(fmdlid) ==
           eos::common::LayoutId::kRaidDP) ||
          (eos::common::LayoutId::GetLayoutType(fmdlid) ==
           eos::common::LayoutId::kArchive) ||
          (eos::common::LayoutId::GetLayoutType(fmdlid) ==
           eos::common::LayoutId::kRaid6)) {
        isRAIN = true;
      }

      if (computechecksum && commitchecksum) {
        try {
          auto dmd = gOFS->eosDirectoryService->getContainerMD(fmd->getContainerId());
          eos::IContainerMD::XAttrMap attrmap;
          eos::listAttributes(gOFS->eosView, dmd.get(), attrmap, false);

          if (attrmap.count(SYS_ALTCHECKSUMS)) {
            option += "&mgm.verify.compute.altchecksum=";
            option += attrmap[SYS_ALTCHECKSUMS].c_str();
          }
        } catch (eos::MDException& e) {
          eos_debug("msg=\"failed to get parent container for altchecksum\" "
                    "fxid=%08llx cxid=%08llx errno=%d emsg=\"%s\"",
                    fmd->getId(), fmd->getContainerId(), e.getErrno(),
                    e.getMessage().str().c_str());
        }
      }

      viewReadLock.Release();
      retc = 0;
      bool acceptfound = false;

      for (it = locations.begin(); it != locations.end(); ++it) {
        if (acceptfsid && (acceptfsid != (int)*it)) {
          continue;
        }

        if (acceptfsid) {
          acceptfound = true;
        }

        if (doresync) {
          int lretc = gOFS->QueryResync(fileid, (int)*it, true);

          if (!lretc) {
            std_out << "success: sending FMD resync to fsid=" << (int)*it
                    << " for path=" << spath << "\n";
          } else {
            std_err << "error: failed to send FMD resync to fsid=" << (int)*it << "\n";
            retc = errno;
          }
        } else {
          if (isRAIN) {
            int lretc = gOFS->QueryResync(fileid, (int)*it);

            if (!lretc) {
              std_out << "success: sending resync for RAIN layout to fsid=" << (int)*it
                      << " for path=" << spath << "\n";
            } else {
              retc = errno;
            }
          } else {
            int lretc = gOFS->_verifystripe(spath.c_str(), mError, mVid,
                                            (unsigned long)*it, option.c_str());

            if (!lretc) {
              std_out << "success: sending verify to fsid=" << (int)*it
                      << " for path=" << spath << "\n";
            } else {
              retc = errno;
            }
          }
        }

        if (acceptfsid && (!acceptfound)) {
          int lretc = gOFS->_verifystripe(spath.c_str(), mError, mVid,
                                          (unsigned long)acceptfsid, option.c_str());

          if (!lretc) {
            std_out << "success: sending forced verify to fsid=" << acceptfsid
                    << " for path=" << spath << "\n";
          } else {
            retc = errno;
          }
        }
      }
    }
  } else {
    retc = EPERM;
    std_err << "error: you have to take role 'root' to execute this command";
  }

  reply.set_retc(retc);
  reply.set_std_out(std_out.str());
  reply.set_std_err(std_err.str());
}

//------------------------------------------------------------------------------
// Execute move subcommand
//------------------------------------------------------------------------------
void
FileCmd::MoveSubcmd(const eos::console::FileMoveProto& move, const std::string& spath,
                    eos::console::ReplyProto& reply)
{
  XrdOucErrInfo mError;
  std::ostringstream std_out, std_err;
  unsigned long sourcefsid = move.fsid1();
  unsigned long targetfsid = move.fsid2();

  if (gOFS->_movestripe(spath.c_str(), mError, mVid, sourcefsid, targetfsid)) {
    std_err << "error: unable to move stripe";
    reply.set_retc(errno);
  } else {
    std_out << "success: scheduled move from source fs=" << sourcefsid
            << " => target fs=" << targetfsid;
  }

  reply.set_std_out(std_out.str());
  reply.set_std_err(std_err.str());
}

//------------------------------------------------------------------------------
// Execute replicate subcommand
//------------------------------------------------------------------------------
void
FileCmd::ReplicateSubcmd(const eos::console::FileReplicateProto& replicate,
                         const std::string& spath, eos::console::ReplyProto& reply)
{
  XrdOucErrInfo mError;
  std::ostringstream std_out, std_err;
  unsigned long sourcefsid = replicate.fsid1();
  unsigned long targetfsid = replicate.fsid2();

  if (gOFS->_copystripe(spath.c_str(), mError, mVid, sourcefsid, targetfsid)) {
    std_err << "error: unable to replicate stripe";
    reply.set_retc(errno);
  } else {
    std_out << "success: scheduled replication from source fs=" << sourcefsid
            << " => target fs=" << targetfsid;
  }

  reply.set_std_out(std_out.str());
  reply.set_std_err(std_err.str());
}

//------------------------------------------------------------------------------
// Execute share subcommand
//------------------------------------------------------------------------------
void
FileCmd::ShareSubcmd(const eos::console::FileShareProto& share, const std::string& spath,
                     eos::console::ReplyProto& reply)
{
  XrdOucErrInfo mError;
  std::ostringstream std_out, std_err;
  time_t expires = (time_t)share.expires();

  if (!expires) {
    // default is 30 days
    expires = (time_t)(time(NULL) + (30 * 86400));
  }

  std::string sharepath = gOFS->CreateSharePath(spath.c_str(), "", expires, mError, mVid);

  if (mVid.uid != 0) {
    // non-root users cannot create shared URLs with validity > 90 days
    if ((expires - time(NULL)) > (90 * 86400)) {
      std_err << "error: you cannot request shared URLs with a validity longer than 90 "
                 "days!\n";
      reply.set_retc(EINVAL);
      sharepath = "";
    }
  }

  if (sharepath.empty()) {
    std_err << "error: unable to create URLs for file sharing";

    if (!reply.retc()) {
      reply.set_retc(errno);
    }
  } else {
    XrdOucString httppath = "http://";
    httppath += gOFS->HostName;
    httppath += ":";
    httppath += gOFS->mHttpdPort;
    httppath += "/";
    size_t qpos = sharepath.find("?");
    std::string httpunenc = sharepath;
    httpunenc.erase(qpos);
    std::string httpenc = eos::common::StringConversion::curl_escaped(httpunenc);
    // remove /#curl#
    httpenc.erase(0, 7);
    httppath += httpenc.c_str();
    httppath += httpenc.c_str();
    XrdOucString cgi = sharepath.c_str();
    cgi.erase(0, qpos);

    while (cgi.replace("+", "%2B", qpos)) {
    }

    httppath += cgi.c_str();
    XrdOucString rootUrl = "root://";
    rootUrl += gOFS->ManagerId;
    rootUrl += "/";
    rootUrl += sharepath.c_str();
    std_out << "[ root ]: " << rootUrl.c_str() << "\n";
    std_out << "[ http ]: " << httppath.c_str() << "\n";
  }

  reply.set_std_out(std_out.str());
  reply.set_std_err(std_err.str());
}

//------------------------------------------------------------------------------
// Execute rename subcommand
//------------------------------------------------------------------------------
void
FileCmd::RenameSubcmd(const eos::console::FileRenameProto& rename,
                      const std::string& spath, eos::console::ReplyProto& reply)
{
  XrdOucErrInfo mError;
  std::ostringstream std_out, std_err;
  // source path is given via Metadata.md.path (already resolved into spath),
  // destination is the new_path field
  XrdOucString source = spath.c_str();
  XrdOucString target = rename.new_path().c_str();
  XrdOucString resolvedSource;
  XrdOucString resolvedTarget;
  std::string err_msg_str;
  int retc = 0;
  XrdOucString xerr;

  if (!ResolveIdentifierToPath(source, resolvedSource, xerr, retc)) {
    reply.set_std_err(xerr.c_str());
    reply.set_retc(retc);
    return;
  }

  if (!ResolveIdentifierToPath(target, resolvedTarget, xerr, retc)) {
    reply.set_std_err(xerr.c_str());
    reply.set_retc(retc);
    return;
  }

  mVid.scope = eos::common::Path::Overlap(resolvedSource.c_str(), resolvedTarget.c_str());

  if (gOFS->rename(resolvedSource.c_str(), resolvedTarget.c_str(), mError, mVid, 0, 0,
                   true)) {
    std_err << "error: " << mError.getErrText();
    reply.set_retc(errno);
  } else {
    std_out << "success: renamed '" << resolvedSource.c_str() << "' to '"
            << resolvedTarget.c_str() << "'";
  }

  reply.set_std_out(std_out.str());
  reply.set_std_err(std_err.str());
}

//------------------------------------------------------------------------------
// Execute rename_with_symlink subcommand
//------------------------------------------------------------------------------
void
FileCmd::RenameWithSymlinkSubcmd(const eos::console::FileRenameWithSymlinkProto& rename,
                                 const std::string& spath,
                                 eos::console::ReplyProto& reply)
{
  XrdOucErrInfo mError;
  std::ostringstream std_out, std_err;
  XrdOucString source = spath.c_str();
  XrdOucString target = rename.destination_dir().c_str();
  XrdOucString resolvedSource;
  XrdOucString resolvedTarget;
  int retc = 0;
  XrdOucString xerr;

  if (!ResolveIdentifierToPath(source, resolvedSource, xerr, retc)) {
    reply.set_std_err(xerr.c_str());
    reply.set_retc(retc);
    return;
  }

  if (!ResolveIdentifierToPath(target, resolvedTarget, xerr, retc)) {
    reply.set_std_err(xerr.c_str());
    reply.set_retc(retc);
    return;
  }

  mVid.scope = eos::common::Path::Overlap(resolvedSource.c_str(), resolvedTarget.c_str());

  if (gOFS->_rename_with_symlink(resolvedSource.c_str(), resolvedTarget.c_str(), mError,
                                 mVid, 0, 0, true, true)) {
    std_err << "error: " << mError.getErrText();
    reply.set_retc(errno);
  } else {
    std_out << "success: renamed '" << resolvedSource.c_str() << "' to '"
            << resolvedTarget.c_str() << "'";
  }

  reply.set_std_out(std_out.str());
  reply.set_std_err(std_err.str());
}

//------------------------------------------------------------------------------
// Execute symlink subcommand
//------------------------------------------------------------------------------
void
FileCmd::SymlinkSubcmd(const eos::console::FileSymlinkProto& symlink,
                       const std::string& spath, eos::console::ReplyProto& reply)
{
  XrdOucErrInfo mError;
  std::ostringstream std_out, std_err;
  XrdOucString source = spath.c_str();
  XrdOucString target = symlink.target_path().c_str();
  XrdOucString resolvedSource;
  XrdOucString resolvedTarget;
  bool force = symlink.force();
  int retc = 0;
  XrdOucString xerr;

  if (!ResolveIdentifierToPath(source, resolvedSource, xerr, retc)) {
    reply.set_std_err(xerr.c_str());
    reply.set_retc(retc);
    return;
  }

  if (!ResolveIdentifierToPath(target, resolvedTarget, xerr, retc)) {
    reply.set_std_err(xerr.c_str());
    reply.set_retc(retc);
    return;
  }

  if (gOFS->symlink(resolvedSource.c_str(), resolvedTarget.c_str(), mError, mVid, 0, 0,
                    force)) {
    std_err << "error: unable to link";
    reply.set_retc(errno);
  } else {
    std_out << "success: linked '" << resolvedSource.c_str() << "' to '"
            << resolvedTarget.c_str() << "'";
  }

  reply.set_std_out(std_out.str());
  reply.set_std_err(std_err.str());
}

//------------------------------------------------------------------------------
// Execute workflow subcommand
//------------------------------------------------------------------------------
void
FileCmd::WorkflowSubcmd(const eos::console::FileWorkflowProto& workflow,
                        const std::string& in_spath, eos::console::ReplyProto& reply)
{
  XrdOucErrInfo mError;
  std::ostringstream std_out, std_err;
  XrdOucString spath = in_spath.c_str();
  XrdOucString event = workflow.event().c_str();
  XrdOucString wf = workflow.workflow().c_str();
  unsigned long long fid = 0;

  if (!event.length() || !wf.length()) {
    reply.set_std_err("error: you have to specify a workflow and an event!\n");
    reply.set_retc(EINVAL);
    return;
  }

  if (spath.beginswith("fid:") || spath.beginswith("fxid:")) {
    unsigned long long lfid =
        Resolver::retrieveFileIdentifier(spath).getUnderlyingUInt64();
    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
    std::shared_ptr<eos::IFileMD> fmd;

    try {
      fmd = gOFS->eosFileService->getFileMD(lfid);
      spath = gOFS->eosView->getUri(fmd.get()).c_str();
      fid = lfid;
    } catch (eos::MDException& e) {
      eos_debug("caught exception %d %s\n", e.getErrno(), e.getMessage().str().c_str());
      std_err << "error: " << mError.getErrText();
      reply.set_retc(errno);
      reply.set_std_err(std_err.str());
      return;
    }
  } else {
    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
    std::shared_ptr<eos::IFileMD> fmd;

    try {
      fmd = gOFS->eosView->getFile(spath.c_str());
      fid = fmd->getId();
    } catch (eos::MDException& e) {
      eos_debug("caught exception %d %s\n", e.getErrno(), e.getMessage().str().c_str());
      std_err << "error: " << mError.getErrText();
      reply.set_retc(errno);
      reply.set_std_err(std_err.str());
      return;
    }
  }

  XrdSfsFSctl args;
  XrdOucString opaque = "mgm.pcmd=event&mgm.fid=";
  XrdOucString lSec;
  opaque += eos::common::FileId::Fid2Hex(fid).c_str();
  opaque += "&mgm.logid=";
  opaque += logId;
  opaque += "&mgm.event=";
  opaque += event.c_str();
  opaque += "&mgm.workflow=";
  opaque += wf.c_str();
  opaque += "&mgm.path=";
  opaque += spath.c_str();
  opaque += "&mgm.ruid=";
  opaque += (int)mVid.uid;
  opaque += "&mgm.rgid=";
  opaque += (int)mVid.gid;
  XrdSecEntity lClient(mVid.prot.c_str());
  lClient.name = (char*)mVid.name.c_str();
  lClient.tident = (char*)mVid.tident.c_str();
  lClient.host = (char*)mVid.host.c_str();
  lSec = "&mgm.sec=";
  lSec += eos::common::SecEntity::ToKey(&lClient, "eos").c_str();
  opaque += lSec;
  args.Arg1 = spath.c_str();
  args.Arg1Len = spath.length();
  args.Arg2 = opaque.c_str();
  args.Arg2Len = opaque.length();

  if (gOFS->FSctl(SFS_FSCTL_PLUGIN, args, mError, &lClient) != SFS_DATA) {
    std_err << "error: unable to run workflow '" << event.c_str()
            << "' : " << mError.getErrText();
    reply.set_retc(errno);
  } else {
    std_out << "success: triggered workflow  '" << event.c_str() << "' on '"
            << spath.c_str() << "'";
  }

  reply.set_std_out(std_out.str());
  reply.set_std_err(std_err.str());
}

//------------------------------------------------------------------------------
// Execute tag subcommand
//------------------------------------------------------------------------------
void
FileCmd::TagSubcmd(const eos::console::FileTagProto& tag, const std::string& spath,
                   unsigned long long fid, eos::console::ReplyProto& reply)
{
  std::ostringstream std_out, std_err;

  if (!((mVid.prot == "sss") && mVid.hasUid(DAEMONUID)) && (mVid.uid != 0)) {
    reply.set_std_err("error: permission denied - you have to be root to "
                      "run the 'tag' command");
    reply.set_retc(EPERM);
    return;
  }

  bool do_add = tag.add();
  bool do_rm = tag.remove();
  bool do_unlink = tag.unlink();
  int fsid = (int)tag.fsid();

  if ((fsid == 0) || (!do_add && !do_rm && !do_unlink)) {
    std_err << "error: no valid filesystem id and/or operation (+/-/~) "
               "provided e.g. 'file tag /myfile +1000'\n";
    reply.set_std_err(std_err.str());
    reply.set_retc(EINVAL);
    return;
  }

  std::shared_ptr<eos::IFileMD> fmd = nullptr;

  try {
    if (fid) {
      fmd = gOFS->eosFileService->getFileMD(fid);
    } else {
      fmd = gOFS->eosView->getFile(spath.c_str());
    }

    eos::MDLocking::FileWriteLock fwLock(fmd.get());

    if (do_add && fmd->hasLocation(fsid)) {
      std_err << "error: file '" << spath << "' is already located on fs=" << fsid;
      reply.set_std_err(std_err.str());
      reply.set_retc(EINVAL);
      return;
    } else if ((do_rm || do_unlink) &&
               (!fmd->hasLocation(fsid) && !fmd->hasUnlinkedLocation(fsid))) {
      std_err << "error: file '" << spath << "' is not located on fs=" << fsid;
      reply.set_std_err(std_err.str());
      reply.set_retc(EINVAL);
      return;
    } else {
      if (do_add) {
        fmd->addLocation(fsid);
        std_out << "success: added location to file '";
      }

      if (do_rm || do_unlink) {
        fmd->unlinkLocation(fsid);

        if (do_rm) {
          std_out << "success: removed location from file '";
          fmd->removeLocation(fsid);
        } else {
          std_out << "success: unlinked location from file '";
        }
      }

      gOFS->eosView->updateFileStore(fmd.get());
      std_out << spath << "' on fs=" << fsid;
    }
  } catch (eos::MDException& e) {
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),
              e.getMessage().str().c_str());
  }

  if (!fmd) {
    std_err << "error: unable to get file meta data of file '" << spath << "'";
    reply.set_std_err(std_err.str());
    reply.set_retc(errno);
    return;
  }

  reply.set_std_out(std_out.str());
  reply.set_std_err(std_err.str());
}

//------------------------------------------------------------------------------
// Execute copy subcommand
//------------------------------------------------------------------------------
void
FileCmd::CopySubcmd(const eos::console::FileCopyProto& copy, const std::string& in_spath,
                    eos::console::ReplyProto& reply)
{
  XrdOucErrInfo mError;
  std::ostringstream std_out, std_err;
  XrdOucString src = in_spath.c_str();
  XrdOucString dst = copy.dst().c_str();

  if (!dst.length()) {
    std_err << "error: missing destination argument";
    reply.set_retc(EINVAL);
    reply.set_std_out(std_out.str());
    reply.set_std_err(std_err.str());
    return;
  }

  struct stat srcbuf;
  struct stat dstbuf;

  if (gOFS->_stat(src.c_str(), &srcbuf, mError, mVid, "")) {
    std_err << "error: " << mError.getErrText();
    reply.set_retc(errno);
    reply.set_std_out(std_out.str());
    reply.set_std_err(std_err.str());
    return;
  }

  bool silent = copy.silent();
  bool clone = copy.clone();
  bool force = copy.force();

  if (silent) {
    // no info output
  } else {
    if (clone) {
      std_out << "info: cloning '";
    } else {
      std_out << "info: copying '";
    }

    std_out << in_spath << "' => '" << dst.c_str() << "' ...\n";
  }

  int dstat = gOFS->_stat(dst.c_str(), &dstbuf, mError, mVid, "");

  if (!force && !dstat) {
    std_err << "error: the target file exists - use '-f' to force the copy";
    reply.set_retc(EEXIST);
    reply.set_std_out(std_out.str());
    reply.set_std_err(std_err.str());
    return;
  }

  if (gOFS->_access(src.c_str(), R_OK, mError, mVid, "") ||
      gOFS->_access(dst.c_str(), W_OK, mError, mVid, "")) {
    std_err << "error: " << mError.getErrText();
    reply.set_retc(errno);
    reply.set_std_out(std_out.str());
    reply.set_std_err(std_err.str());
    return;
  }

  std::vector<std::string> lCopySourceList;
  std::vector<std::string> lCopyTargetList;
  std::map<std::string, std::set<std::string>> found;

  if (S_ISDIR(srcbuf.st_mode) && S_ISDIR(dstbuf.st_mode)) {
    XrdOucString xstdErr;

    if (!gOFS->_find(src.c_str(), mError, xstdErr, mVid, found)) {
      for (auto dirit = found.begin(); dirit != found.end(); dirit++) {
        for (auto fileit = dirit->second.begin(); fileit != dirit->second.end();
             fileit++) {
          std::string src_path = dirit->first;
          std::string end_path = src_path;
          end_path.erase(0, src.length());
          src_path += *fileit;
          std::string dst_path = dst.c_str();
          dst_path += end_path;
          dst_path += *fileit;
          lCopySourceList.push_back(src_path);
          lCopyTargetList.push_back(dst_path);
          std_out << "info: copying '" << src_path << "' => '" << dst_path << "' ... \n";
        }
      }
    } else {
      std_err << "error: find failed";
    }
  } else {
    lCopySourceList.push_back(src.c_str());
    lCopyTargetList.push_back(dst.c_str());
  }

  int retc = 0;

  for (size_t i = 0; i < lCopySourceList.size(); i++) {
    XrdCl::PropertyList properties;
    XrdCl::PropertyList result;

    if (srcbuf.st_size) {
      properties.Set("thirdParty", "only");
    }

    properties.Set("force", true);
    properties.Set("posc", false);
    properties.Set("coerce", false);
    std::string source = lCopySourceList[i];
    std::string target = lCopyTargetList[i];
    std::string sizestring;
    std::string cgi = "eos.ruid=";
    cgi += eos::common::StringConversion::GetSizeString(sizestring,
                                                        (unsigned long long)mVid.uid);
    cgi += "&eos.rgid=";
    cgi += eos::common::StringConversion::GetSizeString(sizestring,
                                                        (unsigned long long)mVid.gid);
    cgi += "&eos.app=filecopy";

    if (clone) {
      char clonetime[256];
      snprintf(clonetime, sizeof(clonetime) - 1, "&eos.ctime=%lu&eos.mtime=%lu",
               srcbuf.st_ctime, srcbuf.st_mtime);
      cgi += clonetime;
    }

    XrdCl::URL url_src;
    url_src.SetProtocol("root");
    url_src.SetHostName("localhost");
    url_src.SetUserName("root");
    url_src.SetParams(cgi);
    url_src.SetPath(source);
    XrdCl::URL url_trg;
    url_trg.SetProtocol("root");
    url_trg.SetHostName("localhost");
    url_trg.SetUserName("root");
    url_trg.SetParams(cgi);
    url_trg.SetPath(target);
    properties.Set("source", url_src);
    properties.Set("target", url_trg);
    properties.Set("sourceLimit", (uint16_t)1);
    properties.Set("chunkSize", (uint32_t)(4 * 1024 * 1024));
    properties.Set("parallelChunks", (uint8_t)1);
    XrdCl::CopyProcess lCopyProcess;
    lCopyProcess.AddJob(properties, &result);
    XrdCl::XRootDStatus lTpcPrepareStatus = lCopyProcess.Prepare();
    eos_static_info("[tpc]: %s=>%s %s", url_src.GetURL().c_str(),
                    url_trg.GetURL().c_str(), lTpcPrepareStatus.ToStr().c_str());

    if (lTpcPrepareStatus.IsOK()) {
      XrdCl::XRootDStatus lTpcStatus = lCopyProcess.Run(0);
      eos_static_info("[tpc]: %s %d", lTpcStatus.ToStr().c_str(), lTpcStatus.IsOK());

      if (lTpcStatus.IsOK()) {
        if (!silent) {
          std_out << "success: copy done '" << source << "'\n";
        }
      } else {
        std_err << "error: copy failed ' " << source << "' - " << lTpcStatus.ToStr()
                << std::endl;
        retc = EIO;
      }
    } else {
      std_err << "error: copy failed - " << lTpcPrepareStatus.ToStr() << std::endl;
      retc = EIO;
    }
  }

  reply.set_retc(retc);
  reply.set_std_out(std_out.str());
  reply.set_std_err(std_err.str());
}

//------------------------------------------------------------------------------
// Execute convert subcommand
//------------------------------------------------------------------------------
void
FileCmd::ConvertSubcmd(const eos::console::FileConvertProto& convert,
                       const std::string& spath, eos::console::ReplyProto& reply)
{
  XrdOucErrInfo mError;
  std::ostringstream std_out, std_err;
  int retc = 0;

  if ((gOFS->_access(spath.c_str(), W_OK, mError, mVid, "") != SFS_OK)) {
    std_err << "error: you have no write permission on '" << spath << "'";
    retc = EPERM;
  } else {
    while (1) {
      using eos::common::LayoutId;
      LayoutId::eChecksum echecksum{LayoutId::eChecksum::kNone};
      XrdOucString layout = convert.layout().c_str();
      XrdOucString space = convert.target_space().c_str();
      XrdOucString plctplcy = convert.placement_policy().c_str();
      XrdOucString checksum = convert.checksum().c_str();
      XrdOucString option = convert.rewrite() ? "rewrite" : "";

      if (plctplcy.length()) {
        if (plctplcy != "scattered" && !plctplcy.beginswith("hybrid:") &&
            !plctplcy.beginswith("gathered:")) {
          std_err << "error: placement policy is invalid";
          retc = EINVAL;
          break;
        }

        if (plctplcy != "scattered") {
          std::string policy = plctplcy.c_str();
          std::string targetgeotag = policy.substr(policy.find(':') + 1);
          std::string tmp_geotag = eos::common::SanitizeGeoTag(targetgeotag);

          if (tmp_geotag != targetgeotag) {
            std_err << tmp_geotag;
            retc = EINVAL;
            break;
          }
        }

        plctplcy = "~" + plctplcy;
      } else {
        plctplcy = "";
      }

      if (checksum.length()) {
        int xs = LayoutId::GetChecksumFromString(checksum.c_str());

        if (xs != -1) {
          echecksum = static_cast<LayoutId::eChecksum>(xs);
        }
      }

      if (!space.length()) {
        eos::common::Path cPath(spath.c_str());
        eos::IContainerMD::XAttrMap map;
        int rc = gOFS->_attr_ls(cPath.GetParentPath(), mError, mVid, (const char*)0, map);

        if (rc || (!map.count("sys.forced.space") && !map.count("user.forced.space"))) {
          std_err << "error: cannot get default space settings from parent "
                     "directory attributes";
          retc = EINVAL;
        } else {
          if (map.count("sys.forced.space")) {
            space = map["sys.forced.space"].c_str();
          } else {
            space = map["user.forced.space"].c_str();
          }
        }
      }

      if (space.length()) {
        if (!layout.length() && (option != "rewrite")) {
          std_err << "error: conversion layout has to be defined";
          retc = EINVAL;
        } else {
          std::shared_ptr<eos::IFileMD> fmd;
          int fsid = 0;
          eos::common::LayoutId::layoutid_t layoutid = 0;
          eos::common::FileId::fileid_t fileid = 0;
          {
            eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);

            try {
              fmd = gOFS->eosView->getFile(spath.c_str());
              layoutid = fmd->getLayoutId();
              fileid = fmd->getId();

              if (fmd->getNumLocation()) {
                eos::IFileMD::LocationVector loc_vect = fmd->getLocations();
                fsid = *(loc_vect.begin());
              }
            } catch (eos::MDException& e) {
              errno = e.getErrno();
              eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),
                        e.getMessage().str().c_str());
            }
          }

          if (!fmd) {
            std_err << "error: unable to get file meta data of file " << spath;
            retc = errno;
          } else {
            std::string conversiontag;

            if (option == "rewrite") {
              if (layout.length() == 0) {
                std_out << "info: rewriting file with identical layout id\n";
                char hexlayout[17];
                snprintf(hexlayout, sizeof(hexlayout) - 1, "%08llx", (long long)layoutid);
                layout = hexlayout;
              }

              if (!fsid) {
                std_err << "error: file has no replica attached\n";
                retc = ENODEV;
                break;
              }

              {
                eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
                FileSystem* filesystem = FsView::gFsView.mIdView.lookupByID(fsid);

                if (!filesystem) {
                  std_err << "error: couldn't find filesystem in view\n";
                  retc = EINVAL;
                  break;
                }

                space = filesystem->GetString("schedgroup").c_str();
                space.erase(space.find("."));
                std_out << "info:: rewriting into space '" << space.c_str() << "'\n";
              }
            }

            if (eos::common::StringConversion::IsHexNumber(layout.c_str(), "%08x")) {
              conversiontag = ConversionTag::Get(fileid, space.c_str(), layout.c_str(),
                                                 std::string(""), false);
              std_out << "info: conversion based on hexadecimal layout id\n";
            } else {
              unsigned long layout_type = 0;
              unsigned long layout_stripes = 0;
              std::string lLayout = layout.c_str();
              std::string lLayoutName;
              std::string lLayoutStripes;

              if (eos::common::StringConversion::SplitKeyValue(lLayout, lLayoutName,
                                                               lLayoutStripes)) {
                XrdOucString lLayoutString = "eos.layout.type=";
                lLayoutString += lLayoutName.c_str();
                lLayoutString += "&eos.layout.nstripes=";
                lLayoutString += lLayoutStripes.c_str();

                if (echecksum == eos::common::LayoutId::eChecksum::kNone) {
                  echecksum = static_cast<eos::common::LayoutId::eChecksum>(
                      eos::common::LayoutId::GetChecksum(layoutid));
                }

                XrdOucEnv lLayoutEnv(lLayoutString.c_str());
                layout_type = eos::common::LayoutId::GetLayoutFromEnv(lLayoutEnv);
                layout_stripes =
                    eos::common::LayoutId::GetStripeNumberFromEnv(lLayoutEnv);
                layoutid = eos::common::LayoutId::GetId(
                    layout_type, echecksum, layout_stripes, eos::common::LayoutId::k4M,
                    eos::common::LayoutId::kCRC32C,
                    eos::common::LayoutId::GetRedundancyStripeNumber(layoutid));
                conversiontag = ConversionTag::Get(fileid, space.c_str(), layoutid,
                                                   plctplcy.c_str(), false);
                std_out << "info: conversion based layout+stripe arguments\n";
              } else {
                conversiontag = ConversionTag::Get(fileid, space.c_str(), layout.c_str(),
                                                   plctplcy.c_str(), false);
                std_out << "info: conversion based conversion attribute name\n";
              }
            }

            std::string err_msg;

            if (gOFS->mConverterEngine->ScheduleJob(fmd->getId(), conversiontag,
                                                    err_msg)) {
              std_out << "success: pushed conversion job '" << conversiontag
                      << "' to QuarkDB";
            } else {
              std_err << "error: failed to schedule conversion '" << conversiontag;

              if (err_msg.empty()) {
                std_err << " msg=\"" << err_msg << "\"";
              }

              retc = EINVAL;
              break;
            }
          }
        }
      }

      break; // while 1
    }
  }

  reply.set_retc(retc);
  reply.set_std_out(std_out.str());
  reply.set_std_err(std_err.str());
}

//------------------------------------------------------------------------------
// Execute touch subcommand
//------------------------------------------------------------------------------
void
FileCmd::TouchSubcmd(const eos::console::TouchProto& touch, const std::string& spath,
                     const std::string& spathid, eos::console::ReplyProto& reply)
{
  XrdOucErrInfo mError;
  std::ostringstream std_out, std_err;
  bool useLayout = !touch.nolayout();
  bool truncate = touch.truncate();
  bool absorb = touch.absorb();
  bool lock = false;
  bool unlock = false;
  time_t lifetime = 86400;
  size_t size = touch.size();
  std::string errmsg;
  bool userwildcard = false;
  bool appwildcard = false;
  const std::string& wildcard = touch.wildcard();

  if (!wildcard.empty() && (wildcard != "user") && (wildcard != "app")) {
    reply.set_std_err(
        "error: invalid wildcard type specified, can be only 'user' or 'app'\n");
    reply.set_retc(EINVAL);
    return;
  } else {
    if (!wildcard.empty()) {
      if (wildcard == "user") {
        userwildcard = true;
      } else {
        appwildcard = true;
      }
    }
  }

  const std::string& lockop = touch.lockop();

  if (lockop == "lock") {
    lock = true;
    unlock = false;
  } else if (lockop == "unlock") {
    unlock = true;
    lock = false;
  } else if (!lockop.empty()) {
    std_err << "error: invalid lock operation specified - can be either "
               "'lock' or 'unlock' '"
            << lockop << "'";
    reply.set_std_err(std_err.str());
    reply.set_retc(EINVAL);
    return;
  }

  if (!touch.lockop_lifetime().empty()) {
    lifetime = atoi(touch.lockop_lifetime().c_str());
  }

  const char* hardlinkpath =
      touch.hardlinkpath().empty() ? nullptr : touch.hardlinkpath().c_str();
  const char* checksuminfo =
      touch.checksuminfo().empty() ? nullptr : touch.checksuminfo().c_str();

  if (spath.empty()) {
    std_err << "error: There is no file with given id! '" << spathid << "'";
    reply.set_std_out(std_out.str());
    reply.set_std_err(std_err.str());
    reply.set_retc(ENOENT);
    return;
  }

  if (gOFS->_touch(spath.c_str(), mError, mVid, 0, true, useLayout, truncate, size,
                   absorb, hardlinkpath, checksuminfo, &errmsg)) {
    std_err << "error: unable to touch '" << spath << "'";

    if (errmsg.length()) {
      std_err << "\n" << errmsg;
    }

    reply.set_retc(errno);
    reply.set_std_out(std_out.str());
    reply.set_std_err(std_err.str());
    return;
  }

  if (lock) {
    XattrLock applock;
    errno = 0;

    if (applock.Lock(spath.c_str(), false, lifetime, mVid, userwildcard, appwildcard)) {
      std_out << "success: created exclusive lock for '" << spath << "'\n"
              << applock.Dump();
    } else {
      std_err << "error: cannot get exclusive lock for '" << spath << "'\n"
              << applock.Dump();
      reply.set_retc(errno);
      reply.set_std_out(std_out.str());
      reply.set_std_err(std_err.str());
      return;
    }
  }

  if (unlock) {
    XattrLock applock;

    if (applock.Unlock(spath.c_str(), mVid)) {
      std_out << "success: removed exclusive lock for '" << spath << "'\n"
              << applock.Dump();
    } else {
      if (errno == ENODATA) {
        std_out << "info: there was no exclusive lock for '" << spath << "'\n";
      } else {
        std_err << "error: failed to remove exclusive lock for '" << spath << "'\n"
                << applock.Dump();
        reply.set_retc(errno);
        reply.set_std_out(std_out.str());
        reply.set_std_err(std_err.str());
        return;
      }
    }
  }

  std_out << "success: touched '" << spath << "'";

  if (errmsg.length()) {
    std_out << "\n" << errmsg;
  }

  reply.set_std_out(std_out.str());
  reply.set_std_err(std_err.str());
}

//------------------------------------------------------------------------------
// Execute adjustreplica subcommand
//------------------------------------------------------------------------------
void
FileCmd::AdjustReplicaSubcmd(const eos::console::FileAdjustreplicaProto& adjust,
                             const std::string& in_spath, eos::console::ReplyProto& reply)
{
  XrdOucErrInfo mError;
  std::ostringstream std_out, std_err;
  XrdOucString spath = in_spath.c_str();
  int retc = 0;

  // Only root can do that
  if (mVid.uid) {
    reply.set_retc(EPERM);
    reply.set_std_err("error: you have to take role 'root' to execute this command");
    return;
  }

  uint32_t lid = 0ul;
  uint64_t size = 0ull;
  unsigned long long fid = 0ull;
  std::shared_ptr<eos::IFileMD> fmd{nullptr};
  eos::IFileMD::LocationVector loc_vect;
  bool nodrop = adjust.nodrop();
  int icreationsubgroup = -1;
  std::string creationspace = adjust.space();

  if (!adjust.subgroup().empty()) {
    icreationsubgroup = atoi(adjust.subgroup().c_str());
  }

  {
    eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex);

    if (spath.beginswith("fid:") || spath.beginswith("fxid:")) {
      WAIT_BOOT;
      unsigned long long lfid =
          Resolver::retrieveFileIdentifier(spath).getUnderlyingUInt64();

      try {
        fmd = gOFS->eosFileService->getFileMD(lfid);
      } catch (eos::MDException& e) {
        errno = e.getErrno();
        std_err << "error: cannot retrieve file meta data - " << e.getMessage().str();
        eos_debug("caught exception %d %s\n", e.getErrno(), e.getMessage().str().c_str());
      }
    } else {
      try {
        fmd = gOFS->eosView->getFile(spath.c_str());
      } catch (eos::MDException& e) {
        errno = e.getErrno();
        std_err << "error: cannot retrieve file meta data - " << e.getMessage().str();
        eos_debug("caught exception %d %s\n", e.getErrno(), e.getMessage().str().c_str());
      }
    }

    if (fmd) {
      fid = fmd->getId();
      lid = fmd->getLayoutId();
      loc_vect = fmd->getLocations();
      size = fmd->getSize();
    } else {
      reply.set_retc(errno ? errno : EINVAL);
      reply.set_std_err(std_err.str());
      return;
    }
  }

  std::string refspace = "";
  std::string space = "default";
  unsigned int forcedsubgroup = 0;

  if (eos::common::LayoutId::GetLayoutType(lid) == eos::common::LayoutId::kReplica) {
    unsigned int nrep_online = 0;
    unsigned int nrep = loc_vect.size();
    unsigned int nrep_layout = eos::common::LayoutId::GetStripeNumber(lid) + 1;
    std::vector<unsigned int> src_fs;
    eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);

    for (auto loc_it = loc_vect.begin(); loc_it != loc_vect.end(); ++loc_it) {
      if (*loc_it == 0) {
        eos_err("msg=\"skip file system with id 0\" fxid=%08llx", fid);
        continue;
      }

      FileSystem* filesystem = FsView::gFsView.mIdView.lookupByID(*loc_it);

      if (filesystem) {
        eos::common::FileSystem::fs_snapshot_t snapshot;
        filesystem->SnapShotFileSystem(snapshot, true);
        space = snapshot.mSpace;

        if (!refspace.length()) {
          refspace = space;
        } else {
          if (space != refspace) {
            eos_warning("msg=\"replicas are in different spaces\" "
                        "fxid=%08llx space=%s req_space=%s",
                        fid, space.c_str(), refspace.c_str());
            continue;
          }
        }

        forcedsubgroup = snapshot.mGroupIndex;

        if ((snapshot.mConfigStatus > eos::common::ConfigStatus::kDrain) &&
            (snapshot.mStatus == eos::common::BootStatus::kBooted)) {
          ++nrep_online;
          src_fs.insert(src_fs.begin(), *loc_it);
        } else {
          src_fs.push_back(*loc_it);
        }
      } else {
        eos_err("msg=\"skip unknown file system\" fsid=%lu fxid=%08llx", *loc_it, fid);
      }
    }

    eos_debug("path=%s nrep=%lu nrep-layout=%lu nrep-online=%lu", spath.c_str(), nrep,
              nrep_layout, nrep_online);

    if (nrep_layout > nrep_online) {
      if (creationspace.length()) {
        space = creationspace;
      }

      if (icreationsubgroup != -1) {
        forcedsubgroup = icreationsubgroup;
      }

      if (creationspace.length()) {
        forcedsubgroup = -1;
      }

      int nrep_new = nrep_layout - nrep_online;
      eos_debug("msg=\"creating %d new replicas\" fxid=%08llx space=%s "
                "forcedsubgroup=%d icreationsubgroup=%d",
                nrep_new, fid, space.c_str(), forcedsubgroup, icreationsubgroup);
      unsigned long fs_indx;
      std::vector<unsigned int> selectedfs;
      std::vector<unsigned int> unavailfs;
      std::vector<unsigned int> excludefs;

      if (!adjust.exclude_fs().empty()) {
        unsigned int exclude_fsid = strtoul(adjust.exclude_fs().c_str(), 0, 10);

        if (exclude_fsid) {
          excludefs.push_back(exclude_fsid);
          src_fs.erase(std::remove(src_fs.begin(), src_fs.end(), exclude_fsid),
                       src_fs.end());
        }
      }

      std::string tried_cgi;
      int layoutId = eos::common::LayoutId::GetId(eos::common::LayoutId::kReplica,
                                                  eos::common::LayoutId::kNone, nrep_new);
      eos::common::Path cPath(spath.c_str());
      eos::IContainerMD::XAttrMap attrmap;
      gOFS->_attr_ls(cPath.GetParentPath(), mError, mVid, (const char*)0, attrmap);
      eos::mgm::Scheduler::tPlctPolicy plctplcy;
      std::string targetgeotag;
      XrdOucEnv plctEnv("");
      Policy::GetPlctPolicy(spath.c_str(), attrmap, mVid, plctEnv, plctplcy,
                            targetgeotag);
      Scheduler::PlacementArguments plctargs;
      plctargs.alreadyused_filesystems = &src_fs;
      plctargs.bookingsize = size;
      plctargs.forced_scheduling_group_index = forcedsubgroup;
      plctargs.lid = layoutId;
      plctargs.inode = (ino64_t)fid;
      plctargs.path = spath.c_str();
      plctargs.plctTrgGeotag = &targetgeotag;
      plctargs.plctpolicy = plctplcy;
      plctargs.exclude_filesystems = &excludefs;
      plctargs.selected_filesystems = &selectedfs;
      plctargs.spacename = &space;
      plctargs.truncate = true;
      plctargs.vid = &mVid;

      if (!plctargs.isValid()) {
        std_err << "error: invalid argument for file placement";
        retc = EINVAL;
      } else {
        errno = retc = Quota::FilePlacement(&plctargs);

        if (!errno) {
          Scheduler::AccessArguments acsargs;
          acsargs.bookingsize = 0;
          acsargs.forcedspace = space.c_str();
          acsargs.fsindex = &fs_indx;
          acsargs.isRW = false;
          acsargs.lid = (unsigned long)lid;
          acsargs.inode = (ino64_t)fid;
          acsargs.locationsfs = &src_fs;
          acsargs.tried_cgi = &tried_cgi;
          acsargs.unavailfs = &unavailfs;
          acsargs.vid = &mVid;

          if (!acsargs.isValid()) {
            std_err << "error: invalid argument for file access";
            retc = EINVAL;
          } else {
            for (unsigned int i = 0; i < selectedfs.size(); ++i) {
              errno = Scheduler::FileAccess(&acsargs);

              if (!errno) {
                unsigned int src_fsid = src_fs[fs_indx];

                if (gOFS->_replicatestripe(fmd.get(), spath.c_str(), mError, mVid,
                                           src_fsid, selectedfs[i], false)) {
                  retc = mError.getErrInfo();
                  std::ostringstream serr;
                  serr << "error: unable to replicate stripe " << src_fsid << " => "
                       << selectedfs[i] << " msg=" << mError.getErrText() << std::endl;

                  if (std_out.str().length()) {
                    std_err << std_out.str();
                  }

                  std_err << serr.str();
                } else {
                  std_out << "success: scheduled replication from source fs=" << src_fsid
                          << " => target fs=" << selectedfs[i] << std::endl;
                }
              } else {
                retc = ENOSPC;
                std_err << "error: create new replicas => no source available: " << spath
                        << "\n";
              }
            }
          }
        } else {
          std_err << "error: create new replicas => cannot place replicas: " << spath
                  << "\n";
        }
      }
    } else {
      // Run this in case of over-replication
      if ((nrep_layout < nrep) && (nodrop == false)) {
        unsigned int n2delete = nrep - nrep_layout;
        std::multimap<common::ConfigStatus, int /*fsid*/> statemap;
        std::multimap<std::string /*schedgroup*/, int /*fsid*/> groupmap;
        eos_debug("msg=\"drop %d replicas\" space=%s group=%d fxid=%08llx", n2delete,
                  creationspace.c_str(), icreationsubgroup, fid);
        {
          for (auto loc_it = loc_vect.begin(); loc_it != loc_vect.end(); ++loc_it) {
            if (!(*loc_it)) {
              eos_err("msg=\"skip file system with id 0\" fxid=%08llx", fid);
              continue;
            }

            eos::common::FileSystem::fsid_t fsid = *loc_it;
            FileSystem* filesystem = FsView::gFsView.mIdView.lookupByID(fsid);
            eos::common::FileSystem::fs_snapshot_t fs;

            if (filesystem && filesystem->SnapShotFileSystem(fs, true)) {
              statemap.insert(
                  std::pair<common::ConfigStatus, int>(fs.mConfigStatus, fsid));
              groupmap.insert(std::pair<std::string, int>(fs.mGroup, fsid));
            }
          }
        }
        std::string cspace = creationspace;

        if (!cspace.empty() && (icreationsubgroup > 0)) {
          cspace += "." + std::to_string(icreationsubgroup);
        }

        std::multimap<common::ConfigStatus, int> limitedstatemap;

        for (auto sit = groupmap.begin(); sit != groupmap.end(); ++sit) {
          if (sit->first.find(cspace) != 0) {
            continue;
          }

          common::ConfigStatus state = eos::common::ConfigStatus::kRW;

          for (auto state_it = statemap.begin(); state_it != statemap.end(); ++state_it) {
            if (state_it->second == sit->second) {
              state = state_it->first;
              break;
            }
          }

          limitedstatemap.insert(
              std::pair<common::ConfigStatus, int>(state, sit->second));
        }

        std::vector<unsigned long> fsid2delete;

        for (auto lit = limitedstatemap.begin(); lit != limitedstatemap.end(); ++lit) {
          fsid2delete.push_back(lit->second);

          if (fsid2delete.size() == n2delete) {
            break;
          }
        }

        if (fsid2delete.size() != n2delete) {
          std_err << "warning: cannot adjust replicas according to your requirement:"
                  << " space=" << creationspace << " subgroup=" << icreationsubgroup
                  << std::endl;
        }

        eos::common::RWMutexWriteLock ns_wr_lock(gOFS->eosViewRWMutex);

        try {
          auto fmd2 = gOFS->eosFileService->getFileMD(fid);

          for (unsigned int i = 0; i < fsid2delete.size(); i++) {
            if (fmd2->hasLocation(fsid2delete[i])) {
              fmd2->unlinkLocation(fsid2delete[i]);
              eos_debug("msg=\"removing location\" fsid=%lu fxid=%08llx", fsid2delete[i],
                        fid);
              std_out << "success: dropping replica on fsid=" << (int)fsid2delete[i]
                      << "\n";
            }
          }

          gOFS->eosView->updateFileStore(fmd2.get());
        } catch (eos::MDException& e) {
          errno = e.getErrno();
          eos_debug("msg=\"caught exception\" errno=%d msg=\"%s\"", e.getErrno(),
                    e.getMessage().str().c_str());
          std_err << "error: drop excess replicas => cannot unlink location - "
                  << e.getMessage().str() << std::endl;
        }
      }
    }
  } else {
    // This is a rain layout, we try to rewrite the file using the converter
    if (eos::common::LayoutId::IsRain(lid)) {
      eos::console::FileConvertProto convproto;
      convproto.set_rewrite(true);
      eos::console::ReplyProto convReply;
      ConvertSubcmd(convproto, spath.c_str(), convReply);
      std_out << convReply.std_out();
      std_err << convReply.std_err();
      retc = convReply.retc();
    } else {
      retc = EINVAL;
      std_out << "warning: no action for this layout type (neither replica nor rain)\n";
    }
  }

  reply.set_retc(retc);
  reply.set_std_out(std_out.str());
  reply.set_std_err(std_err.str());
}

//------------------------------------------------------------------------------
// Execute purge subcommand
//------------------------------------------------------------------------------
void
FileCmd::PurgeSubcmd(const eos::console::FilePurgeProto& purge, const std::string& spath,
                     eos::console::ReplyProto& reply)
{
  XrdOucErrInfo mError;
  std::ostringstream std_out, std_err;
  int max_versions = purge.purge_version();

  if (!max_versions) {
    max_versions = -1; // read the max version from the parent xattr
  }

  struct stat buf;

  if (gOFS->_stat(spath.c_str(), &buf, mError, mVid, "")) {
    std_err << "error: unable to stat path=" << spath;
    reply.set_retc(errno);
    reply.set_std_err(std_err.str());
    return;
  }

  XrdOucString version_dir;
  eos::common::Path cPath(spath.c_str());
  version_dir += cPath.GetParentPath();
  version_dir += "/.sys.v#.";
  version_dir += cPath.GetName();
  version_dir += "/";

  if (gOFS->PurgeVersion(version_dir.c_str(), mError, max_versions)) {
    if (mError.getErrInfo()) {
      reply.set_retc(mError.getErrInfo());
      std_err << "error: unable to purge versions for path=" << spath
              << "\nerror: " << mError.getErrText();
    } else {
      std_err << "info: no versions to purge for path=" << spath;
    }

    reply.set_std_err(std_err.str());
    return;
  }

  reply.set_std_out(std_out.str());
  reply.set_std_err(std_err.str());
}

//------------------------------------------------------------------------------
// Execute version subcommand
//------------------------------------------------------------------------------
void
FileCmd::VersionSubcmd(const eos::console::FileVersionProto& version,
                       const std::string& spath, eos::console::ReplyProto& reply)
{
  XrdOucErrInfo mError;
  std::ostringstream std_out, std_err;
  int maxversion = version.purge_version();

  if (!maxversion) {
    // Match legacy behaviour: max_count absent => -1 (no purge limit change);
    // max_count == "0" (parses to 0) is rejected as illegal.
    maxversion = -1;
  }

  struct stat buf;

  if (gOFS->_stat(spath.c_str(), &buf, mError, mVid, "")) {
    std_err << "error; unable to stat path=" << spath;
    reply.set_retc(errno);
    reply.set_std_err(std_err.str());
    return;
  }

  // Third party copy the file to a temporary name
  eos::common::Path atomicPath(spath.c_str());
  eos::console::FileCopyProto copyProto;
  copyProto.set_dst(atomicPath.GetAtomicPath(true));
  eos::console::ReplyProto copyReply;
  CopySubcmd(copyProto, spath, copyReply);
  std_out << copyReply.std_out();
  std_err << copyReply.std_err();

  if (!copyReply.retc()) {
    if (maxversion > 0) {
      XrdOucString versiondir;
      eos::common::Path cPath(spath.c_str());
      versiondir += cPath.GetParentPath();
      versiondir += "/.sys.v#.";
      versiondir += cPath.GetName();
      versiondir += "/";

      if (gOFS->PurgeVersion(versiondir.c_str(), mError, maxversion)) {
        std_err << "error: unable to purge versions of path=" << spath << "\n"
                << "error: " << mError.getErrText();
        reply.set_retc(mError.getErrInfo());
        reply.set_std_out(std_out.str());
        reply.set_std_err(std_err.str());
        return;
      }
    }

    std_out << "info: created new version of '" << spath << "'";

    if (maxversion > 0) {
      std_out << " keeping " << maxversion << " versions!";
    }
  } else {
    reply.set_retc(copyReply.retc());
  }

  reply.set_std_out(std_out.str());
  reply.set_std_err(std_err.str());
}

//------------------------------------------------------------------------------
// Execute versions subcommand
//------------------------------------------------------------------------------
void
FileCmd::VersionsSubcmd(const eos::console::FileVersionsProto& versions,
                        const std::string& spath, eos::console::ReplyProto& reply)
{
  XrdOucErrInfo mError;
  std::ostringstream std_out, std_err;
  std::string grab = versions.grab_version();

  if (grab == "-1") {
    // list versions - delegate to the legacy 'ls -l' proc command exactly as
    // the original ProcCommand::File() did, to preserve the listing format
    ProcCommand Cmd;
    eos::common::Path vpath(spath.c_str());
    XrdOucString info;
    info += "&mgm.cmd=ls&mgm.option=-l";
    info += "&mgm.path=";
    info += vpath.GetVersionDirectory();
    Cmd.open("/proc/user", info.c_str(), mVid, &mError);
    XrdOucString lStdOut, lStdErr;
    Cmd.AddOutput(lStdOut, lStdErr);
    Cmd.close();
    int retc = Cmd.GetRetc();

    if (retc && (retc == ENOENT)) {
      std_err << "error: no version exists for '" << spath << "'";
      reply.set_std_err(std_err.str());
      return;
    }

    reply.set_retc(retc);
    reply.set_std_out(lStdOut.c_str() ? lStdOut.c_str() : "");
    reply.set_std_err(lStdErr.c_str() ? lStdErr.c_str() : "");
    return;
  }

  eos::common::Path vpath(spath.c_str());
  struct stat buf;
  struct stat vbuf;

  if (gOFS->_stat(spath.c_str(), &buf, mError, mVid, "")) {
    std_err << "error; unable to stat path=" << spath;
    reply.set_retc(errno);
    reply.set_std_err(std_err.str());
    return;
  }

  // grab version
  std::string versionname = grab;

  if (versionname.empty()) {
    std_err << "error: you have to provide the version you want to stage!";
    reply.set_retc(EINVAL);
    reply.set_std_err(std_err.str());
    return;
  }

  XrdOucString versionpath = vpath.GetVersionDirectory();
  versionpath += versionname.c_str();

  if (gOFS->_stat(versionpath.c_str(), &vbuf, mError, mVid, "")) {
    std_err << "error: failed to stat your provided version path='" << versionpath.c_str()
            << "'";
    reply.set_retc(errno);
    reply.set_std_err(std_err.str());
    return;
  }

  // now stage a new version of the existing file
  XrdOucString versionedpath;

  if (gOFS->Version(eos::common::FileId::InodeToFid(buf.st_ino), mError, mVid, -1,
                    &versionedpath)) {
    std_err << "error: unable to create a version of path=" << spath << "\n"
            << "error: " << mError.getErrText();
    reply.set_retc(mError.getErrInfo());
    reply.set_std_err(std_err.str());
    return;
  }

  // and stage back the desired version
  if (gOFS->rename(versionpath.c_str(), spath.c_str(), mError, mVid)) {
    std_err << "error: unable to stage '" << versionpath.c_str() << "' back to '" << spath
            << "'";
    reply.set_retc(errno);
    reply.set_std_err(std_err.str());
    return;
  }

  {
    // Copy the xattrs of the current file to the newly restored one
    std::set<std::string> exclude_xattrs{"sys.utrace", "sys.vtrace"};
    eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex);
    auto versioned_fmd = gOFS->eosView->getFile(versionedpath.c_str());
    auto restored_fmd = gOFS->eosView->getFile(spath.c_str());

    if (!versioned_fmd || !restored_fmd) {
      reply.set_std_err("error: failed to copy xattrs");
      reply.set_retc(EINVAL);
      return;
    }

    eos::IFileMD::XAttrMap map_xattrs = versioned_fmd->getAttributes();

    for (const auto& xattr : map_xattrs) {
      if (exclude_xattrs.find(xattr.first) == exclude_xattrs.end()) {
        restored_fmd->setAttribute(xattr.first, xattr.second);
      }
    }

    gOFS->eosView->updateFileStore(restored_fmd.get());
  }

  std_out << "success: staged '" << versionpath.c_str() << "' back to '" << spath << "'"
          << " - the previous file is now '" << versionedpath.c_str() << "'";
  reply.set_std_out(std_out.str());
  reply.set_std_err(std_err.str());
}

//------------------------------------------------------------------------------
// Execute fileinfo subcommand
//------------------------------------------------------------------------------
void
FileCmd::FileinfoSubcmd(const eos::console::FileinfoProto& info,
                        eos::console::ReplyProto& reply)
{
  std::string path = info.md().path();

  if (path.empty()) {
    if (info.md().ino()) {
      path = "inode:" + std::to_string(info.md().ino());
    } else if (info.md().id()) {
      bool isContainer = (info.md().type() == eos::console::CONTAINER);
      path = (isContainer ? "pid:" : "fid:") + std::to_string(info.md().id());
    }

    if (path.empty()) {
      reply.set_std_err("error: path is empty");
      reply.set_retc(EINVAL);
      return;
    }
  }

  std::string std_out, std_err;
  ProcCommand cmd;
  XrdOucErrInfo error;
  std::string cmd_in = "mgm.cmd=fileinfo";
  bool is_identifier = (path.find("fid:") == 0) || (path.find("fxid:") == 0) ||
                       (path.find("pid:") == 0) || (path.find("pxid:") == 0) ||
                       (path.find("inode:") == 0);

  if (is_identifier) {
    cmd_in += "&mgm.path=" + path;
  } else {
    // the path may contain CGI-reserved characters (e.g. '&') that would
    // otherwise break parsing of this opaque string on the MGM side -
    // escape it the same way the console client's AppendEncodedPath() does
    cmd_in += "&mgm.path=" + eos::common::StringConversion::curl_escaped(path);
    cmd_in += "&eos.encodepath=1";
  }

  if (WantsJsonOutput()) {
    cmd_in += "&mgm.format=json";
  }

  if (info.path() || info.fid() || info.fxid() || info.size() || info.checksum() ||
      info.fullpath() || info.proxy() || info.monitoring() || info.wnc() || info.env()) {
    cmd_in += "&mgm.file.info.option=";
  }

  if (info.path()) {
    cmd_in += "--path";
  }

  if (info.fid()) {
    cmd_in += "--fid";
  }

  if (info.fxid()) {
    cmd_in += "--fxid";
  }

  if (info.size()) {
    cmd_in += "--size";
  }

  if (info.checksum()) {
    cmd_in += "--checksum";
  }

  if (info.fullpath()) {
    cmd_in += "--fullpath";
  }

  if (info.proxy()) {
    cmd_in += "--proxy";
  }

  if (info.monitoring() || info.wnc()) {
    cmd_in += "-m";
  }

  if (info.env()) {
    cmd_in += "--env";
  }

  // Run as the calling user (unlike GrpcRestGwInterface::FileinfoCall, which
  // intentionally uses root for the REST gateway) so normal per-path
  // permission checks still apply.
  cmd.open("/proc/user", cmd_in.c_str(), mVid, &error);
  cmd.AddOutput(std_out, std_err);
  cmd.close();

  // ProcCommand::MakeResult() (invoked from open()) unconditionally seals
  // '&' into '#and#' in place in stdOut/stdErr/stdJson (for the legacy
  // opaque CGI transport, where the old CLI unseals it back after parsing
  // 'mgm.proc.stdout='/'mgm.proc.json=' etc. out of the raw response) -
  // undo that sealing here since we hand the text back directly without
  // going through that transport.
  if (WantsJsonOutput()) {
    XrdOucString json_out = cmd.GetStdJson();
    eos::common::StringConversion::UnSeal(json_out);
    reply.set_std_out(json_out.c_str() ? json_out.c_str() : "");
  } else {
    XrdOucString text_out = std_out.c_str();
    eos::common::StringConversion::UnSeal(text_out);
    reply.set_std_out(text_out.c_str() ? text_out.c_str() : "");
  }

  XrdOucString err_out = std_err.c_str();
  eos::common::StringConversion::UnSeal(err_out);
  reply.set_std_err(err_out.c_str() ? err_out.c_str() : "");
  reply.set_retc(cmd.GetRetc());
}

EOSMGMNAMESPACE_END
