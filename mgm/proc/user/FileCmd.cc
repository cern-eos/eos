//------------------------------------------------------------------------------
//! @file FileCmd.cc
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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

#include "mgm/proc/user/FileCmd.hh"
#include "File.pb.h"
#include "build/proto/File.pb.h"
#include "mgm/Acl.hh"
#include "mgm/Constants.hh"
#include "mgm/Namespace.hh"
#include "mgm/XrdMgmOfs.hh"
#include "common/Path.hh"
#include "namespace/Prefetcher.hh"
#include "namespace/utils/Attributes.hh"
#include <XrdCl/XrdClCopyProcess.hh>
#include <unistd.h>
#include "common/Timing.hh"

#include "mgm/proc/ProcInterface.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Acl.hh"
#include "mgm/Quota.hh"
#include "mgm/Macros.hh"
#include "mgm/Policy.hh"
#include "mgm/Stat.hh"
#include "mgm/convert/ConverterEngine.hh"
#include "mgm/convert/ConversionTag.hh"
#include "mgm/XattrLock.hh"
#include "mgm/Constants.hh"
#include "common/Utils.hh"
#include "common/Path.hh"
#include "common/LayoutId.hh"
#include "common/SecEntity.hh"
#include "namespace/interface/IContainerMDSvc.hh"
#include "namespace/interface/IFileMDSvc.hh"
#include "namespace/interface/IView.hh"
#include "namespace/utils/Checksum.hh"
#include "namespace/utils/Attributes.hh"
#include "namespace/Resolver.hh"
#include <XrdCl/XrdClCopyProcess.hh>
#include <math.h>
#include <memory>
#include "namespace/utils/Etag.hh"
#include "common/Fmd.hh"

#include <json/json.h>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Process request
//------------------------------------------------------------------------------
eos::console::ReplyProto
FileCmd::ProcessRequest() noexcept
{
  eos::console::ReplyProto reply;

  // Check which command type is present
  if (mReqProto.has_file()) {
    const auto& file = mReqProto.file();
    // Get the path from metadata
    std::string path = file.md().path();

    if (path.empty() && !file.has_drop()) {
      reply.set_std_err("error: you have to give a path name to call 'file'");
      reply.set_retc(EINVAL);
      return reply;
    }

    // Route to appropriate handler based on subcmd
    if (file.has_adjustreplica()) {
      reply =  AdjustReplicaSubcmd(file);
    } else if (file.has_check()) {
      reply =  GetMdLocationSubcmd(file);
    } else if (file.has_convert()) {
      reply =  ConvertSubcmd(file);
    } else if (file.has_copy()) {
      reply =  CopySubcmd(file);
    } else if (file.has_drop()) {
      reply =  DropSubcmd(file);
    } else if (file.has_fileinfo()) {
      reply =  FileinfoSubcmd(file);
    } else if (file.has_layout()) {
      reply =  LayoutSubcmd(file);
    } else if (file.has_move()) {
      reply =  MoveSubcmd(file);
    } else if (file.has_purge()) {
      reply =  PurgeSubcmd(file);
    } else if (file.has_rename()) {
      reply =  RenameSubcmd(file);
    } else if (file.has_rename_with_symlink()) {
      reply =  RenameWithSymlinkSubcmd(file);
    } else if (file.has_replicate()) {
      reply =  ReplicateSubcmd(file);
    } else if (file.has_share()) {
      reply =  ShareSubcmd(file);
    } else if (file.has_symlink()) {
      reply =  SymlinkSubcmd(file);
    } else if (file.has_tag()) {
      reply =  TagSubcmd(file);
    } else if (file.has_touch()) {
      reply =  TouchSubcmd(file);
    } else if (file.has_verify()) {
      reply =  VerifySubcmd(file);
    } else if (file.has_version()) {
      reply =  VersionSubcmd(file);
    } else if (file.has_versions()) {
      reply =  VersionsSubcmd(file);
    } else if (file.has_workflow()) {
      reply =  WorkflowSubcmd(file);
    } else {
      reply.set_std_err("error: unknown file subcommand");
      reply.set_retc(EINVAL);
    }
  } else {
    reply.set_std_err("error: no file command specified");
    reply.set_retc(EINVAL);
  }

  return reply;
}
//------------------------------------------------------------------------------
// Touch subcommand - simplified without helper methods
//------------------------------------------------------------------------------
eos::console::ReplyProto
FileCmd::TouchSubcmd(const eos::console::FileProto& file) noexcept
{
  eos::console::ReplyProto reply;
  const auto& touch = file.touch();
  // Get path directly
  std::string path = file.md().path();

  if (path.empty()) {
    reply.set_std_err("error: path is required");
    reply.set_retc(EINVAL);
    return reply;
  }

  // Extract touch options
  bool useLayout = !touch.nolayout();
  bool truncate = touch.truncate();
  bool absorb = touch.absorb();
  uint64_t size = touch.size();
  const char* hardlinkpath = touch.hardlinkpath().empty() ? nullptr :
                             touch.hardlinkpath().c_str();
  const char* checksuminfo = touch.checksuminfo().empty() ? nullptr :
                             touch.checksuminfo().c_str();
  // Lock operation
  bool lock = false;
  bool unlock = false;
  time_t lifetime = 86400;
  bool userwildcard = false;
  bool appwildcard = false;

  if (!touch.lockop().empty()) {
    if (touch.lockop() == "lock") {
      lock = true;
      unlock = false;
    } else if (touch.lockop() == "unlock") {
      unlock = true;
      lock = false;
    } else {
      reply.set_std_err("error: invalid lock operation specified - can be either 'lock' or 'unlock' '"
                        + touch.lockop() + "'");
      reply.set_retc(EINVAL);
      return reply;
    }

    // Parse lock lifetime
    if (!touch.lockop_lifetime().empty()) {
      lifetime = atoi(touch.lockop_lifetime().c_str());
    }
  }

  // Wildcard type
  if (!touch.wildcard().empty()) {
    if (touch.wildcard() == "user") {
      userwildcard = true;
    } else if (touch.wildcard() == "app") {
      appwildcard = true;
    } else {
      reply.set_std_err("error: invalid wildcard type specified, can be only 'user' or 'app'");
      reply.set_retc(EINVAL);
      return reply;
    }
  }

  // Perform touch operation
  std::string errmsg;
  XrdOucErrInfo error;

  if (gOFS->_touch(path.c_str(), error, mVid, 0, true, useLayout, truncate,
                   size, absorb, hardlinkpath, checksuminfo, &errmsg)) {
    std::string stdErr = "error: unable to touch '";
    stdErr += path;
    stdErr += "'";

    if (!errmsg.empty()) {
      stdErr += "\n";
      stdErr += errmsg;
    }

    reply.set_std_err(stdErr);
    reply.set_retc(errno);
    return reply;
  }

  std::string stdOut;

  // Handle lock operation
  if (lock) {
    XattrLock applock;
    errno = 0;

    if (applock.Lock(path.c_str(), false, lifetime, mVid, userwildcard,
                     appwildcard)) {
      stdOut += "success: created exclusive lock for '";
      stdOut += path;
      stdOut += "'\n";
      stdOut += applock.Dump();
    } else {
      std::string stdErr = "error: cannot get exclusive lock for '";
      stdErr += path;
      stdErr += "'\n";
      stdErr += applock.Dump();
      reply.set_std_err(stdErr);
      reply.set_retc(errno);
      return reply;
    }
  }

  // Handle unlock operation
  if (unlock) {
    XattrLock applock;

    if (applock.Unlock(path.c_str(), mVid)) {
      stdOut += "success: removed exclusive lock for '";
      stdOut += path;
      stdOut += "'\n";
      stdOut += applock.Dump();
    } else {
      if (errno == ENODATA) {
        stdOut += "info: there was no exclusive lock for '";
        stdOut += path;
        stdOut += "'\n";
      } else {
        std::string stdErr = "error: failed to remove exclusive lock for '";
        stdErr += path;
        stdErr += "'\n";
        stdErr += applock.Dump();
        reply.set_std_err(stdErr);
        reply.set_retc(errno);
        return reply;
      }
    }
  }

  stdOut += "success: touched '";
  stdOut += path;
  stdOut += "'";

  if (!errmsg.empty()) {
    stdOut += "\n";
    stdOut += errmsg;
  }

  reply.set_std_out(stdOut);
  reply.set_retc(0);
  return reply;
}

//------------------------------------------------------------------------------
// Fileinfo subcommand - returns file or directory metadata information
//------------------------------------------------------------------------------
eos::console::ReplyProto
FileCmd::FileinfoSubcmd(const eos::console::FileProto& file) noexcept
{
  eos::console::ReplyProto reply;

  if (!file.has_fileinfo()) {
    reply.set_std_err("error: fileinfo command not present");
    reply.set_retc(EINVAL);
    return reply;
  }

  // Get path directly
  std::string path = file.md().path();

  if (path.empty()) {
    reply.set_std_err("error: path is required");
    reply.set_retc(EINVAL);
    return reply;
  }

  gOFS->MgmStats.Add("FileInfo", mVid.uid, mVid.gid, 1);
  // Get metadata - check if it's a file or directory
  eos::common::RWMutexReadLock viewReadLock(gOFS->eosViewRWMutex);
  std::shared_ptr<eos::IFileMD> fmd;
  std::shared_ptr<eos::IContainerMD> cmd;
  bool is_directory = false;
  XrdOucString spath = path.c_str();

  try {
    if (spath.beginswith("fid:") || spath.beginswith("fxid:")) {
      unsigned long long fid = Resolver::retrieveFileIdentifier(
                                 spath).getUnderlyingUInt64();
      fmd = gOFS->eosFileService->getFileMD(fid);
    } else if (spath.beginswith("pid:") || spath.beginswith("pxid:")) {
      unsigned long long cid = Resolver::retrieveFileIdentifier(
                                 spath).getUnderlyingUInt64();
      cmd = gOFS->eosDirectoryService->getContainerMD(cid);
      is_directory = true;
    } else {
      // Try as file first
      try {
        fmd = gOFS->eosView->getFile(spath.c_str());
      } catch (eos::MDException& e) {
        // If file lookup fails, try as directory
        try {
          cmd = gOFS->eosView->getContainer(spath.c_str());
          is_directory = true;
        } catch (eos::MDException& e2) {
          // Neither file nor directory found
          reply.set_std_err(SSTR("error: cannot retrieve file or directory meta data - "
                                 << e2.getMessage().str()));
          reply.set_retc(e2.getErrno());
          return reply;
        }
      }
    }
  } catch (eos::MDException& e) {
    reply.set_std_err(SSTR("error: cannot retrieve meta data - "
                           << e.getMessage().str()));
    reply.set_retc(e.getErrno());
    return reply;
  }

  if (!fmd && !cmd) {
    reply.set_std_err("error: file or directory not found");
    reply.set_retc(errno);
    return reply;
  }

  // Generate JSON output
  Json::Value json_output;

  if (is_directory && cmd) {
    GenerateDirectoryJSON(cmd, path, json_output);
  } else if (fmd) {
    GenerateFileJSON(fmd, path, json_output);
  }

  viewReadLock.Release();
  // Convert JSON to string
  Json::StreamWriterBuilder builder;
  std::string json_str = Json::writeString(builder, json_output);
  reply.set_std_out(json_str);
  reply.set_retc(0);
  return reply;
}

//------------------------------------------------------------------------------
// Generate JSON for file metadata
//------------------------------------------------------------------------------
void
FileCmd::GenerateFileJSON(std::shared_ptr<eos::IFileMD> fmd,
                          const std::string& path,
                          Json::Value& json) noexcept
{
  using eos::common::LayoutId;
  using eos::common::FileId;
  // Basic identifiers
  json["id"] = (Json::Value::UInt64)fmd->getId();
  json["fxid"] = FileId::Fid2Hex(fmd->getId());
  json["inode"] = (Json::Value::UInt64)FileId::FidToInode(fmd->getId());
  json["pid"] = (Json::Value::UInt64)fmd->getContainerId();
  json["pxid"] = FileId::Fid2Hex(fmd->getContainerId());
  json["name"] = fmd->getName();
  json["path"] = path;
  json["type"] = "file";
  // Timestamps
  eos::IFileMD::ctime_t ctime, mtime, atime{0, 0}, btime{0, 0};
  fmd->getCTime(ctime);
  fmd->getMTime(mtime);
  fmd->getATime(atime);
  eos::IFileMD::XAttrMap xattrs = fmd->getAttributes();

  if (xattrs.count("sys.eos.btime")) {
    eos::common::Timing::Timespec_from_TimespecStr(xattrs["sys.eos.btime"], btime);
  }

  json["ctime"] = (Json::Value::UInt64)ctime.tv_sec;
  json["ctime_ns"] = (Json::Value::UInt64)ctime.tv_nsec;
  json["mtime"] = (Json::Value::UInt64)mtime.tv_sec;
  json["mtime_ns"] = (Json::Value::UInt64)mtime.tv_nsec;
  json["atime"] = (Json::Value::UInt64)atime.tv_sec;
  json["atime_ns"] = (Json::Value::UInt64)atime.tv_nsec;
  json["btime"] = (Json::Value::UInt64)btime.tv_sec;
  json["btime_ns"] = (Json::Value::UInt64)btime.tv_nsec;
  // File properties
  json["size"] = (Json::Value::UInt64)fmd->getSize();
  json["uid"] = fmd->getCUid();
  json["gid"] = fmd->getCGid();
  json["mode"] = fmd->getFlags();
  json["nlink"] = (Json::Value::UInt64)(fmd->isLink() ? 1 :
                                        fmd->getNumLocation());
  // Layout information
  uint32_t lid = fmd->getLayoutId();
  json["layout"] = LayoutId::GetLayoutTypeString(lid);
  json["nstripes"] = (int)(LayoutId::GetStripeNumber(lid) + 1);
  json["blocksize"] = LayoutId::GetBlocksize(lid);
  json["layoutid"] = FileId::Fid2Hex(lid);
  // Checksum
  json["checksumtype"] = LayoutId::GetChecksumString(lid);
  std::string xs;
  eos::appendChecksumOnStringAsHex(fmd.get(), xs);
  json["checksum"] = xs;
  // Alternative checksums
  auto altchecksums = fmd->getAltXs();

  if (!altchecksums.empty()) {
    Json::Value alt_xs_array(Json::arrayValue);

    for (auto [type, altxs] : altchecksums) {
      Json::Value alt_xs_obj;
      alt_xs_obj["type"] = LayoutId::GetChecksumString(type);
      alt_xs_obj["value"] = altxs;
      alt_xs_array.append(alt_xs_obj);
    }

    json["altchecksums"] = alt_xs_array;
  }

  // ETag
  std::string etag;
  eos::calculateEtag(fmd.get(), etag);
  json["etag"] = etag;
  // Status
  json["status"] = FileMDToStatus(fmd);

  // Link/hardlink information
  if (fmd->isLink()) {
    json["target"] = fmd->getLink();
  } else if (fmd->hasAttribute(SYS_HARD_LINK)) {
    json["target"] = fmd->getAttribute(SYS_HARD_LINK);
  }

  // Redundancy
  std::string redundancy = LayoutId::GetRedundancySymbol(
                             fmd->hasLocation(EOS_TAPE_FSID),
                             LayoutId::GetRedundancy(lid, fmd->getNumLocation()),
                             fmd->getSize()
                           );
  json["redundancy"] = redundancy;

  // Extended attributes
  if (!xattrs.empty()) {
    Json::Value xattr_obj;

    for (const auto& [key, value] : xattrs) {
      xattr_obj[key] = value;
    }

    json["xattr"] = xattr_obj;
  }

  // Locations (filesystem information)
  Json::Value locations_array(Json::arrayValue);
  eos::IFileMD::LocationVector loc_vect = fmd->getLocations();
  const std::string hex_fid = FileId::Fid2Hex(fmd->getId());

  for (auto loc_it = loc_vect.begin(); loc_it != loc_vect.end(); ++loc_it) {
    if (!(*loc_it)) {
      continue; // Skip fsid 0
    }

    Json::Value loc_obj;
    loc_obj["fsid"] = *loc_it;
    eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
    eos::common::FileSystem* filesystem = FsView::gFsView.mIdView.lookupByID(
                                            *loc_it);

    if (filesystem) {
      eos::common::FileSystem::fs_snapshot_t fs;

      if (filesystem->SnapShotFileSystem(fs, true)) {
        std::string fstpath = FileId::FidPrefix2FullPath(hex_fid.c_str(),
                              fs.mPath.c_str());
        loc_obj["host"] = fs.mHost;
        loc_obj["mountpoint"] = fs.mPath;
        loc_obj["fstpath"] = fstpath;
        loc_obj["schedgroup"] = fs.mGroup;
        loc_obj["geotag"] = filesystem->GetString("stat.geotag");
        loc_obj["status"] = eos::common::FileSystem::GetStatusAsString(fs.mStatus);
        loc_obj["boot"] = filesystem->GetString("stat.boot");
        loc_obj["configstatus"] = filesystem->GetString("configstatus");
        loc_obj["active"] = filesystem->GetString("stat.active");

        if (!fs.mForceGeoTag.empty()) {
          loc_obj["forcegeotag"] = fs.mForceGeoTag;
        }
      }
    }

    locations_array.append(loc_obj);
  }

  json["locations"] = locations_array;
  // Unlinked locations
  eos::IFileMD::LocationVector unlink_vect = fmd->getUnlinkedLocations();

  if (!unlink_vect.empty()) {
    Json::Value unlinked_array(Json::arrayValue);

    for (auto uloc : unlink_vect) {
      unlinked_array.append(uloc);
    }

    json["unlinked_locations"] = unlinked_array;
  }

  // Tape information
  if (fmd->hasLocation(EOS_TAPE_FSID)) {
    Json::Value tape_obj;
    tape_obj["archive_id"] = xattrs.count("sys.archive.file_id") ?
                             xattrs["sys.archive.file_id"] : "undef";
    tape_obj["storage_class"] = xattrs.count("sys.archive.storage_class") ?
                                xattrs["sys.archive.storage_class"] : "none";
    json["tape"] = tape_obj;
  }

  // Encryption/obfuscation
  if (xattrs.count("user.obfuscate.key")) {
    if (xattrs.count("user.encrypted")) {
      json["encryption"] = "encrypted";
    } else {
      json["encryption"] = "obfuscated";
    }
  }
}

//------------------------------------------------------------------------------
// Generate JSON for directory metadata
//------------------------------------------------------------------------------
void
FileCmd::GenerateDirectoryJSON(std::shared_ptr<eos::IContainerMD> cmd,
                               const std::string& path,
                               Json::Value& json) noexcept
{
  using eos::common::FileId;
  // Basic identifiers
  json["id"] = (Json::Value::UInt64)cmd->getId();
  json["fxid"] = FileId::Fid2Hex(cmd->getId());
  json["inode"] = (Json::Value::UInt64)cmd->getId();
  json["pid"] = (Json::Value::UInt64)cmd->getParentId();
  json["pxid"] = FileId::Fid2Hex(cmd->getParentId());
  json["name"] = cmd->getName();
  json["path"] = path;
  json["type"] = "directory";
  // Timestamps
  eos::IContainerMD::ctime_t ctime, mtime, tmtime, btime{0, 0};
  cmd->getCTime(ctime);
  cmd->getMTime(mtime);
  cmd->getTMTime(tmtime);
  eos::IContainerMD::XAttrMap xattrs = cmd->getAttributes();

  if (xattrs.count("sys.eos.btime")) {
    eos::common::Timing::Timespec_from_TimespecStr(xattrs["sys.eos.btime"], btime);
  }

  json["ctime"] = (Json::Value::UInt64)ctime.tv_sec;
  json["ctime_ns"] = (Json::Value::UInt64)ctime.tv_nsec;
  json["mtime"] = (Json::Value::UInt64)mtime.tv_sec;
  json["mtime_ns"] = (Json::Value::UInt64)mtime.tv_nsec;
  json["tmtime"] = (Json::Value::UInt64)tmtime.tv_sec;
  json["tmtime_ns"] = (Json::Value::UInt64)tmtime.tv_nsec;
  json["btime"] = (Json::Value::UInt64)btime.tv_sec;
  json["btime_ns"] = (Json::Value::UInt64)btime.tv_nsec;
  // Directory properties
  json["uid"] = cmd->getCUid();
  json["gid"] = cmd->getCGid();
  json["mode"] = cmd->getMode();
  json["flags"] = cmd->getFlags();
  json["nlink"] = 1;
  // Tree statistics
  json["treesize"] = (Json::Value::UInt64)cmd->getTreeSize();
  json["treecontainers"] = (Json::Value::UInt64)cmd->getTreeContainers();
  json["treefiles"] = (Json::Value::UInt64)cmd->getTreeFiles();
  // Direct children counts
  json["ncontainers"] = (Json::Value::UInt64)cmd->getNumContainers();
  json["nfiles"] = (Json::Value::UInt64)cmd->getNumFiles();
  // ETag
  std::string etag;
  eos::calculateEtag(cmd.get(), etag);
  json["etag"] = etag;

  // Extended attributes
  if (!xattrs.empty()) {
    Json::Value xattr_obj;

    for (const auto& [key, value] : xattrs) {
      xattr_obj[key] = value;
    }

    json["xattr"] = xattr_obj;
  }
}

//------------------------------------------------------------------------------
// Helper function to determine file status (reused from original code)
//------------------------------------------------------------------------------
std::string
FileCmd::FileMDToStatus(std::shared_ptr<eos::IFileMD> fmd) noexcept
{
  int tape_copy = 0;

  if (fmd->hasAttribute(SYS_HARD_LINK)) {
    return "hardlink";
  }

  if (fmd->isLink()) {
    return "symlink";
  }

  if (fmd->hasLocation(EOS_TAPE_FSID)) {
    tape_copy++;
  }

  if (fmd->getNumLocation() == 0) {
    if (fmd->getSize() == 0) {
      return "healthy";
    }

    if (fmd->getNumUnlinkedLocation()) {
      return "pending_deletion";
    }

    return "locations::uncommitted";
  }

  if (fmd->getNumLocation() < (eos::common::LayoutId::GetStripeNumber(
                                 fmd->getLayoutId()) + 1 + tape_copy)) {
    return "locations::incomplete";
  }

  if (fmd->getNumLocation() > (eos::common::LayoutId::GetStripeNumber(
                                 fmd->getLayoutId()) + 1 + tape_copy)) {
    return "locations::overreplicated";
  }

  eos::IFileMD::XAttrMap xattrs = fmd->getAttributes();
  std::string fs = xattrs["sys.fusex.state"];

  if (fs.length()) {
    if (fs.length() > 1) {
      std::string b2 = fs.substr(fs.length() - 2);

      if (b2 == "±") {
        return "fuse::needsflush";
      }
    }

    if (fs.back() == 'Z') {
      return "fuse::repairing";
    }

    if (fs.back() == '|') {
      size_t spos = fs.rfind("±", fs.length() - 1);
      size_t ncommits = 0;

      if (spos != std::string::npos) {
        spos++;

        for (size_t i = spos; i < fs.length(); ++i) {
          if (fs.at(i) == '+') {
            ncommits++;
          }
        }
      }

      if (eos::common::LayoutId::GetLayoutType(fmd->getLayoutId()) ==
          eos::common::LayoutId::kReplica) {
        if (fmd->getSize() && (ncommits < fmd->getNumLocation())) {
          return "fuse::missingcommits";
        }
      }
    }
  }

  return "healthy";
}

//------------------------------------------------------------------------------
// GetMdLocation subcommand - returns metadata location information in JSON
//------------------------------------------------------------------------------
eos::console::ReplyProto
FileCmd::GetMdLocationSubcmd(const eos::console::FileProto& file) noexcept
{
  eos::console::ReplyProto reply;

  if (!file.has_check()) {
    reply.set_std_err("error: check command not present");
    reply.set_retc(EINVAL);
    return reply;
  }

  std::string path = file.md().path();

  if (path.empty()) {
    reply.set_std_err("error: path is required");
    reply.set_retc(EINVAL);
    return reply;
  }

  gOFS->MgmStats.Add("FileCheck", mVid.uid, mVid.gid, 1);
  // Get file metadata
  eos::common::RWMutexReadLock viewReadLock(gOFS->eosViewRWMutex);
  std::shared_ptr<eos::IFileMD> fmd;
  XrdOucString spath = path.c_str();

  try {
    if (spath.beginswith("fid:") || spath.beginswith("fxid:")) {
      unsigned long long fid = Resolver::retrieveFileIdentifier(
                                 spath).getUnderlyingUInt64();
      fmd = gOFS->eosFileService->getFileMD(fid);
    } else {
      fmd = gOFS->eosView->getFile(spath.c_str());
    }
  } catch (eos::MDException& e) {
    reply.set_std_err(SSTR("error: cannot retrieve file meta data - "
                           << e.getMessage().str()));
    reply.set_retc(e.getErrno());
    return reply;
  }

  if (!fmd) {
    reply.set_std_err("error: file not found");
    reply.set_retc(errno);
    return reply;
  }

  // Generate JSON output
  Json::Value json;
  GenerateMdLocationJSON(fmd, path, json);
  viewReadLock.Release();
  // Convert JSON to string
  Json::StreamWriterBuilder builder;
  builder["indentation"] = "  ";
  std::string json_str = Json::writeString(builder, json);
  reply.set_std_out(json_str);
  reply.set_retc(0);
  return reply;
}

//------------------------------------------------------------------------------
// Generate JSON for file check information
//------------------------------------------------------------------------------
void
FileCmd::GenerateMdLocationJSON(std::shared_ptr<eos::IFileMD> fmd,
                                const std::string& path,
                                Json::Value& json) noexcept
{
  using eos::common::FileId;
  using eos::common::LayoutId;
  const std::string hex_fid = FileId::Fid2Hex(fmd->getId());
  uint32_t lid = fmd->getLayoutId();
  // Basic file information
  json["fid"] = hex_fid;
  json["path"] = path;
  json["size"] = (Json::Value::UInt64)fmd->getSize();
  json["nrep"] = (int)fmd->getNumLocation();
  json["nstripes"] = (int)(LayoutId::GetStripeNumber(lid) + 1);
  // Checksum information
  json["checksumtype"] = LayoutId::GetChecksumString(lid);
  std::string checksum_str;
  eos::appendChecksumOnStringAsHex(fmd.get(), checksum_str, 0x00,
                                   SHA256_DIGEST_LENGTH);
  json["checksum"] = checksum_str;
  // Layout information
  json["layout"] = LayoutId::GetLayoutTypeString(lid);
  json["layoutid"] = FileId::Fid2Hex(lid);
  // Replica locations array
  Json::Value replicas(Json::arrayValue);
  eos::IFileMD::LocationVector loc_vect = fmd->getLocations();

  for (auto loc_it = loc_vect.begin(); loc_it != loc_vect.end(); ++loc_it) {
    // Ignore filesystem id 0
    if (!(*loc_it)) {
      eos_err("msg=\"found file on fsid=0\" fxid=%08llx", fmd->getId());
      continue;
    }

    Json::Value replica;
    replica["fsid"] = *loc_it;
    replica["fid"] = hex_fid;
    eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
    eos::common::FileSystem* filesystem = FsView::gFsView.mIdView.lookupByID(
                                            *loc_it);

    if (filesystem) {
      // Get filesystem information needed for check
      replica["hostport"] = filesystem->GetString("hostport");
      replica["host"] = filesystem->GetString("host");
      replica["port"] = (Json::Value::Int64)filesystem->GetLongLong("port");
      replica["bootstat"] = filesystem->GetString("stat.boot");
      replica["configstatus"] = filesystem->GetString("configstatus");
      replica["status"] = eos::common::FileSystem::GetStatusAsString(
                            filesystem->GetStatus());
      replica["path"] = filesystem->GetString("path");
      replica["schedgroup"] = filesystem->GetString("schedgroup");
      // Generate full filesystem path
      std::string fstpath = FileId::FidPrefix2FullPath(
                              hex_fid.c_str(),
                              filesystem->GetPath().c_str());
      replica["fstpath"] = fstpath;
    } else {
      replica["error"] = "filesystem not found";
    }

    fs_rd_lock.Release();
    replicas.append(replica);
  }

  json["replicas"] = replicas;
  // Add unlinked locations if any
  eos::IFileMD::LocationVector unlink_vect = fmd->getUnlinkedLocations();

  if (!unlink_vect.empty()) {
    Json::Value unlinked(Json::arrayValue);

    for (auto uloc : unlink_vect) {
      unlinked.append(uloc);
    }

    json["unlinked_locations"] = unlinked;
  }
}

//------------------------------------------------------------------------------
// Layout subcommand
//------------------------------------------------------------------------------
eos::console::ReplyProto
FileCmd::LayoutSubcmd(const eos::console::FileProto& file) noexcept
{
  eos::console::ReplyProto reply;
  XrdOucString spath = file.md().path().c_str();
  const auto& layout = file.layout();

  // Check root permission inline
  if (mVid.uid != 0) {
    reply.set_std_err("error: you need to be root to execute this command");
    reply.set_retc(EPERM);
    return reply;
  }

  XrdOucString stripes;
  XrdOucString cksum;
  XrdOucString layout_type;
  int checksum_type = eos::common::LayoutId::kNone;
  int newstripenumber = 0;
  std::string newlayoutstring;

  if (layout.stripes()) {
    newstripenumber = layout.stripes();
    stripes = std::to_string(newstripenumber).c_str();
  }

  if (layout.checksum().length()) {
    cksum = layout.checksum().c_str();
    XrdOucString ne = "eos.layout.checksum=";
    ne += cksum;
    XrdOucEnv env(ne.c_str());
    checksum_type = eos::common::LayoutId::GetChecksumFromEnv(env);
  }

  if (layout.type().length()) {
    newlayoutstring = layout.type();
    layout_type = newlayoutstring.c_str();
  }

  if (!stripes.length() && !cksum.length() && !newlayoutstring.length()) {
    reply.set_std_err("error: you have to give a valid number of stripes"
                      " as an argument to call 'file layout' or a valid checksum or a layout id");
    reply.set_retc(EINVAL);
    return reply;
  } else if (stripes.length() &&
             ((newstripenumber < 1) || (newstripenumber > 255))) {
    reply.set_std_err("error: you have to give a valid number of stripes"
                      " as an argument to call 'file layout'");
    reply.set_retc(EINVAL);
    return reply;
  } else if (cksum.length() && (checksum_type == eos::common::LayoutId::kNone)) {
    reply.set_std_err("error: you have to give a valid checksum type"
                      " as an argument to call 'file layout'");
    reply.set_retc(EINVAL);
    return reply;
  }

  std::shared_ptr<eos::IFileMD> fmd;
  eos::common::RWMutexWriteLock viewWriteLock;

  try {
    if (spath.beginswith("fid:") || spath.beginswith("fxid:")) {
      unsigned long long fid = Resolver::retrieveFileIdentifier(
                                 spath).getUnderlyingUInt64();
      viewWriteLock.Grab(gOFS->eosViewRWMutex);
      fmd = gOFS->eosFileService->getFileMD(fid);
    } else {
      viewWriteLock.Grab(gOFS->eosViewRWMutex);
      fmd = gOFS->eosView->getFile(spath.c_str());
    }
  } catch (eos::MDException& e) {
    reply.set_std_err(SSTR("error: cannot retrieve file meta data - "
                           << e.getMessage().str()));
    reply.set_retc(e.getErrno());
    return reply;
  }

  if (!fmd) {
    reply.set_std_err("error: no such file");
    reply.set_retc(errno);
    viewWriteLock.Release();
    return reply;
  }

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

  if (!cksum.length()) {
    checksum_type = eos::common::LayoutId::GetChecksum(fmd->getLayoutId());
  }

  if (!newstripenumber) {
    newstripenumber = eos::common::LayoutId::GetStripeNumber(
                        fmd->getLayoutId()) + 1;
  }

  int lid = eos::common::LayoutId::kReplica;
  unsigned long newlayout = eos::common::LayoutId::GetId(
                              lid,
                              checksum_type,
                              newstripenumber,
                              eos::common::LayoutId::GetBlocksizeType(fmd->getLayoutId())
                            );

  if (newlayoutstring.length()) {
    newlayout = strtol(newlayoutstring.c_str(), 0, 16);
  }

  if ((only_replica &&
       (((eos::common::LayoutId::GetLayoutType(fmd->getLayoutId()) ==
          eos::common::LayoutId::kReplica) ||
         (eos::common::LayoutId::GetLayoutType(fmd->getLayoutId()) ==
          eos::common::LayoutId::kPlain)) &&
        (eos::common::LayoutId::GetLayoutType(newlayout) ==
         eos::common::LayoutId::kReplica))) || only_tape || any_layout) {
    fmd->setLayoutId(newlayout);
    std::ostringstream oss;
    oss << "success: setting layout to "
        << eos::common::LayoutId::PrintLayoutString(newlayout)
        << " for path=" << spath.c_str();
    reply.set_std_out(oss.str());
    // Commit new layout
    gOFS->eosView->updateFileStore(fmd.get());
    reply.set_retc(0);
  } else {
    reply.set_std_err("error: you can only change the number of "
                      "stripes for files with replica layout or files without locations");
    reply.set_retc(EPERM);
  }

  viewWriteLock.Release();
  return reply;
}

//------------------------------------------------------------------------------
// Share subcommand
//------------------------------------------------------------------------------
eos::console::ReplyProto
FileCmd::ShareSubcmd(const eos::console::FileProto& file) noexcept
{
  eos::console::ReplyProto reply;
  XrdOucString spath = file.md().path().c_str();
  const auto& share = file.share();
  time_t expires = 0;

  if (share.expires()) {
    expires = (time_t) share.expires();
  } else {
    // Default is 30 days
    expires = (time_t)(time(NULL) + (30 * 86400));
  }

  XrdOucErrInfo error;
  std::string sharepath;
  sharepath = gOFS->CreateSharePath(spath.c_str(), "", expires, error, mVid);

  if (mVid.uid != 0) {
    // Non-root users cannot create shared URLs with validity > 90 days
    if ((expires - time(NULL)) > (90 * 86400)) {
      reply.set_std_err("error: you cannot request shared URLs with a validity longer than 90 days!");
      reply.set_retc(EINVAL);
      return reply;
    }
  }

  if (!sharepath.length()) {
    reply.set_std_err("error: unable to create URLs for file sharing");
    reply.set_retc(errno);
    return reply;
  }

  XrdOucString httppath = "http://";
  httppath += gOFS->HostName;
  httppath += ":";
  httppath += gOFS->mHttpdPort;
  httppath += "/";
  size_t qpos = sharepath.find("?");
  std::string httpunenc = sharepath;
  httpunenc.erase(qpos);
  std::string httpenc = eos::common::StringConversion::curl_escaped(httpunenc);
  // Remove /#curl#
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
  std::ostringstream oss;
  oss << "[ root ]: " << rootUrl.c_str() << "\n";
  oss << "[ http ]: " << httppath.c_str() << "\n";
  reply.set_std_out(oss.str());
  reply.set_retc(0);
  return reply;
}

//------------------------------------------------------------------------------
// Workflow subcommand
//------------------------------------------------------------------------------
eos::console::ReplyProto
FileCmd::WorkflowSubcmd(const eos::console::FileProto& file) noexcept
{
  eos::console::ReplyProto reply;
  XrdOucString spath = file.md().path().c_str();
  const auto& workflow = file.workflow();
  XrdOucString event = workflow.event().c_str();
  XrdOucString workflow_name = workflow.workflow().c_str();
  unsigned long long fid = 0;

  if (!event.length() || !workflow_name.length()) {
    reply.set_std_err("error: you have to specify a workflow and an event!");
    reply.set_retc(EINVAL);
    return reply;
  }

  if (spath.beginswith("fid:") || spath.beginswith("fxid:")) {
    // Reference by fid+fsid
    fid = Resolver::retrieveFileIdentifier(spath).getUnderlyingUInt64();
    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
    std::shared_ptr<eos::IFileMD> fmd;

    try {
      fmd = gOFS->eosFileService->getFileMD(fid);
      spath = gOFS->eosView->getUri(fmd.get()).c_str();
    } catch (eos::MDException& e) {
      reply.set_std_err(SSTR("error: " << e.getMessage().str()));
      reply.set_retc(e.getErrno());
      return reply;
    }
  } else {
    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
    std::shared_ptr<eos::IFileMD> fmd;

    try {
      fmd = gOFS->eosView->getFile(spath.c_str());
      fid = fmd->getId();
    } catch (eos::MDException& e) {
      reply.set_std_err(SSTR("error: " << e.getMessage().str()));
      reply.set_retc(e.getErrno());
      return reply;
    }
  }

  XrdSfsFSctl args;
  XrdOucString opaque = "mgm.pcmd=event&mgm.fid=";
  XrdOucString lSec;
  opaque += eos::common::FileId::Fid2Hex(fid).c_str();
  opaque += "&mgm.logid=";
  opaque += this->logId;
  opaque += "&mgm.event=";
  opaque += event.c_str();
  opaque += "&mgm.workflow=";
  opaque += workflow_name.c_str();
  opaque += "&mgm.path=";
  opaque += spath.c_str();
  opaque += "&mgm.ruid=";
  opaque += (int) mVid.uid;
  opaque += "&mgm.rgid=";
  opaque += (int) mVid.gid;
  XrdSecEntity lClient(mVid.prot.c_str());
  lClient.name = (char*) mVid.name.c_str();
  lClient.tident = (char*) mVid.tident.c_str();
  lClient.host = (char*) mVid.host.c_str();
  lSec = "&mgm.sec=";
  lSec += eos::common::SecEntity::ToKey(&lClient, "eos").c_str();
  opaque += lSec;
  args.Arg1 = spath.c_str();
  args.Arg1Len = spath.length();
  args.Arg2 = opaque.c_str();
  args.Arg2Len = opaque.length();
  XrdOucErrInfo error;

  if (gOFS->FSctl(SFS_FSCTL_PLUGIN, args, error, &lClient) != SFS_DATA) {
    reply.set_std_err(SSTR("error: unable to run workflow '" << event.c_str()
                           << "' : " << error.getErrText()));
    reply.set_retc(errno);
  } else {
    reply.set_std_out(SSTR("success: triggered workflow '" << event.c_str()
                           << "' on '" << spath.c_str() << "'"));
    reply.set_retc(0);
  }

  return reply;
}

//------------------------------------------------------------------------------
// Version subcommand
//------------------------------------------------------------------------------
eos::console::ReplyProto
FileCmd::VersionSubcmd(const eos::console::FileProto& file) noexcept
{
  eos::console::ReplyProto reply;
  XrdOucString spath = file.md().path().c_str();
  const auto& version = file.version();
  int maxversion = version.purge_version();
  XrdOucErrInfo error;
  // Validate file exists inline
  struct stat buf;

  if (gOFS->_stat(spath.c_str(), &buf, error, mVid, "")) {
    reply.set_std_err(SSTR("error: unable to stat path=" << spath.c_str()));
    reply.set_retc(errno);
    return reply;
  }

  // Third party copy the file to a temporary name
  eos::common::Path atomicPath(spath.c_str());
  std::string atomic_target = atomicPath.GetAtomicPath(true);
  // Create a copy request
  eos::console::RequestProto copy_req;
  auto* copy_file = copy_req.mutable_file();
  copy_file->mutable_md()->set_path(spath.c_str());
  auto* copy_cmd = copy_file->mutable_copy();
  copy_cmd->set_dst(atomic_target);
  copy_cmd->set_force(true);
  // Execute the copy
  FileCmd copy_command(std::move(copy_req), mVid);
  eos::console::ReplyProto copy_reply = copy_command.ProcessRequest();

  if (copy_reply.retc() != 0) {
    reply.set_std_err(SSTR("error: failed to create version - "
                           << copy_reply.std_err()));
    reply.set_retc(copy_reply.retc());
    return reply;
  }

  if (maxversion > 0) {
    XrdOucString versiondir;
    eos::common::Path cPath(spath.c_str());
    versiondir += cPath.GetParentPath();
    versiondir += "/.sys.v#.";
    versiondir += cPath.GetName();
    versiondir += "/";

    if (gOFS->PurgeVersion(versiondir.c_str(), error, maxversion)) {
      reply.set_std_err(SSTR("error: unable to purge versions of path="
                             << spath.c_str() << "\nerror: " << error.getErrText()));
      reply.set_retc(error.getErrInfo());
      return reply;
    }
  }

  // Everything worked well
  std::ostringstream oss;
  oss << "info: created new version of '" << spath.c_str() << "'";

  if (maxversion > 0) {
    oss << " keeping " << maxversion << " versions!";
  }

  reply.set_std_out(oss.str());
  reply.set_retc(0);
  return reply;
}

//------------------------------------------------------------------------------
// Versions subcommand
//------------------------------------------------------------------------------
eos::console::ReplyProto
FileCmd::VersionsSubcmd(const eos::console::FileProto& file) noexcept
{
  eos::console::ReplyProto reply;
  XrdOucString spath = file.md().path().c_str();
  const auto& versions = file.versions();
  XrdOucString grab = versions.grab_version().c_str();

  if (grab == "-1") {
    // List versions - need to call ls command
    eos::common::Path vpath(spath.c_str());
    // TODO: This needs to be adapted to call LsCmd directly
    // For now, return error indicating list functionality needs separate implementation
    reply.set_std_err("error: listing versions not yet implemented in protobuf version");
    reply.set_retc(ENOSYS);
    return reply;
  } else {
    eos::common::Path vpath(spath.c_str());
    struct stat buf;
    struct stat vbuf;
    XrdOucErrInfo error;

    if (gOFS->_stat(spath.c_str(), &buf, error, mVid, "")) {
      reply.set_std_err(SSTR("error: unable to stat path=" << spath.c_str()));
      reply.set_retc(errno);
      return reply;
    }

    // Grab version
    XrdOucString versionname = grab;

    if (!versionname.length()) {
      reply.set_std_err("error: you have to provide the version you want to stage!");
      reply.set_retc(EINVAL);
      return reply;
    }

    XrdOucString versionpath = vpath.GetVersionDirectory();
    versionpath += versionname;

    if (gOFS->_stat(versionpath.c_str(), &vbuf, error, mVid, "")) {
      reply.set_std_err(SSTR("error: failed to stat your provided version path='"
                             << versionpath.c_str() << "'"));
      reply.set_retc(errno);
      return reply;
    }

    // Now stage a new version of the existing file
    XrdOucString versionedpath;

    if (gOFS->Version(eos::common::FileId::InodeToFid(buf.st_ino), error,
                      mVid, -1, &versionedpath)) {
      reply.set_std_err(SSTR("error: unable to create a version of path="
                             << spath.c_str() << "\nerror: " << error.getErrText()));
      reply.set_retc(error.getErrInfo());
      return reply;
    }

    // And stage back the desired version
    if (gOFS->rename(versionpath.c_str(), spath.c_str(), error, mVid)) {
      reply.set_std_err(SSTR("error: unable to stage '" << versionpath.c_str()
                             << "' back to '" << spath.c_str() << "'"));
      reply.set_retc(errno);
      return reply;
    } else {
      {
        // Copy the xattrs of the current file to the newly restored one
        std::set<std::string> exclude_xattrs {"sys.utrace", "sys.vtrace"};
        eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex);

        try {
          auto versioned_fmd = gOFS->eosView->getFile(versionedpath.c_str());
          auto restored_fmd = gOFS->eosView->getFile(spath.c_str());

          if (!versioned_fmd || !restored_fmd) {
            reply.set_std_err("error: failed to copy xattrs");
            reply.set_retc(EINVAL);
            return reply;
          }

          eos::IFileMD::XAttrMap map_xattrs = versioned_fmd->getAttributes();

          for (const auto& xattr : map_xattrs) {
            if (exclude_xattrs.find(xattr.first) == exclude_xattrs.end()) {
              restored_fmd->setAttribute(xattr.first, xattr.second);
            }
          }

          gOFS->eosView->updateFileStore(restored_fmd.get());
        } catch (eos::MDException& e) {
          reply.set_std_err(SSTR("error: failed to copy xattrs - "
                                 << e.getMessage().str()));
          reply.set_retc(e.getErrno());
          return reply;
        }
      }
      reply.set_std_out(SSTR("success: staged '" << versionpath.c_str()
                             << "' back to '" << spath.c_str()
                             << "' - the previous file is now '"
                             << versionedpath.c_str() << "'"));
      reply.set_retc(0);
    }
  }

  return reply;
}

//------------------------------------------------------------------------------
// Tag subcommand
//------------------------------------------------------------------------------
eos::console::ReplyProto
FileCmd::TagSubcmd(const eos::console::FileProto& file) noexcept
{
  eos::console::ReplyProto reply;
  XrdOucString spath = file.md().path().c_str();
  const auto& tag = file.tag();

  if (!((mVid.prot == "sss") && mVid.hasUid(DAEMONUID)) && (mVid.uid != 0)) {
    reply.set_std_err("error: permission denied - you have to be root to "
                      "run the 'tag' command");
    reply.set_retc(EPERM);
    return reply;
  }

  bool do_add = tag.add();
  bool do_rm = tag.remove();
  bool do_unlink = tag.unlink();
  int fsid = tag.fsid();

  if ((fsid == 0) || (!do_add && !do_rm && !do_unlink)) {
    reply.set_std_err("error: no valid filesystem id and/or operation (+/-/~) "
                      "provided e.g. 'file tag /myfile +1000'");
    reply.set_retc(EINVAL);
    return reply;
  }

  std::shared_ptr<eos::IFileMD> fmd = nullptr;
  // Get fid if path starts with fid: or fxid:
  eos::IFileMD::id_t fid = 0ull;

  if (spath.beginswith("fid:") || spath.beginswith("fxid:")) {
    fid = Resolver::retrieveFileIdentifier(spath).getUnderlyingUInt64();
  }

  try {
    if (fid) {
      fmd = gOFS->eosFileService->getFileMD(fid);
    } else {
      fmd = gOFS->eosView->getFile(spath.c_str());
    }

    eos::MDLocking::FileWriteLock fwLock(fmd.get());

    if (do_add && fmd->hasLocation(fsid)) {
      reply.set_std_err(SSTR("error: file '" << spath.c_str()
                             << "' is already located on fs=" << fsid));
      reply.set_retc(EINVAL);
      return reply;
    } else if ((do_rm || do_unlink) &&
               (!fmd->hasLocation(fsid) && !fmd->hasUnlinkedLocation(fsid))) {
      reply.set_std_err(SSTR("error: file '" << spath.c_str()
                             << "' is not located on fs=" << fsid));
      reply.set_retc(EINVAL);
      return reply;
    } else {
      if (do_add) {
        fmd->addLocation(fsid);
        reply.set_std_out(SSTR("success: added location to file '"
                               << spath.c_str() << "' on fs=" << fsid));
      }

      if (do_rm || do_unlink) {
        fmd->unlinkLocation(fsid);

        if (do_rm) {
          fmd->removeLocation(fsid);
          reply.set_std_out(SSTR("success: removed location from file '"
                                 << spath.c_str() << "' on fs=" << fsid));
        } else {
          reply.set_std_out(SSTR("success: unlinked location from file '"
                                 << spath.c_str() << "' on fs=" << fsid));
        }
      }

      gOFS->eosView->updateFileStore(fmd.get());
      reply.set_retc(0);
    }
  } catch (eos::MDException& e) {
    reply.set_std_err(SSTR("error: unable to get file meta data of file '"
                           << spath.c_str() << "' - " << e.getMessage().str()));
    reply.set_retc(e.getErrno());
    return reply;
  }

  if (!fmd) {
    reply.set_std_err(SSTR("error: unable to get file meta data of file '"
                           << spath.c_str() << "'"));
    reply.set_retc(errno);
    return reply;
  }

  return reply;
}

//------------------------------------------------------------------------------
// Convert subcommand
//------------------------------------------------------------------------------
eos::console::ReplyProto
FileCmd::ConvertSubcmd(const eos::console::FileProto& file) noexcept
{
  eos::console::ReplyProto reply;
  XrdOucString spath = file.md().path().c_str();
  const auto& convert = file.convert();
  XrdOucErrInfo error;

  // Check access permissions on source
  if (gOFS->_access(spath.c_str(), W_OK, error, mVid, "") != SFS_OK) {
    reply.set_std_err(SSTR("error: you have no write permission on '"
                           << spath.c_str() << "'"));
    reply.set_retc(EPERM);
    return reply;
  }

  std::ostringstream oss_out;
  std::ostringstream oss_err;
  int retc = 0;

  do {
    using eos::common::LayoutId;
    LayoutId::eChecksum echecksum{LayoutId::eChecksum::kNone};
    XrdOucString layout = convert.layout().c_str();
    XrdOucString space = convert.target_space().c_str();
    XrdOucString plctplcy = convert.placement_policy().c_str();
    XrdOucString checksum = convert.checksum().c_str();
    bool is_rewrite = convert.rewrite();

    if (plctplcy.length()) {
      // Check that the placement policy is valid
      if (plctplcy != "scattered" &&
          !plctplcy.beginswith("hybrid:") &&
          !plctplcy.beginswith("gathered:")) {
        reply.set_std_err("error: placement policy is invalid");
        reply.set_retc(EINVAL);
        return reply;
      }

      // Check geotag in case of hybrid or gathered policy
      if (plctplcy != "scattered") {
        std::string policy = plctplcy.c_str();
        std::string targetgeotag = policy.substr(policy.find(':') + 1);
        std::string tmp_geotag = eos::common::SanitizeGeoTag(targetgeotag);

        if (tmp_geotag != targetgeotag) {
          reply.set_std_err(tmp_geotag);
          reply.set_retc(EINVAL);
          return reply;
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
      // Get target space from the layout settings
      eos::common::Path cPath(spath.c_str());
      eos::IContainerMD::XAttrMap map;
      int rc = gOFS->_attr_ls(cPath.GetParentPath(), error, mVid,
                              (const char*) 0, map);

      if (rc || (!map.count("sys.forced.space") && !map.count("user.forced.space"))) {
        reply.set_std_err("error: cannot get default space settings from parent "
                          "directory attributes");
        reply.set_retc(EINVAL);
        return reply;
      } else {
        if (map.count("sys.forced.space")) {
          space = map["sys.forced.space"].c_str();
        } else {
          space = map["user.forced.space"].c_str();
        }
      }
    }

    if (space.length()) {
      if (!layout.length() && !is_rewrite) {
        reply.set_std_err("error: conversion layout has to be defined");
        reply.set_retc(EINVAL);
        return reply;
      } else {
        // Get the file meta data
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
            reply.set_std_err(SSTR("error: unable to get file meta data of file "
                                   << spath.c_str()));
            reply.set_retc(e.getErrno());
            return reply;
          }
        }

        if (!fmd) {
          reply.set_std_err(SSTR("error: unable to get file meta data of file "
                                 << spath.c_str()));
          reply.set_retc(errno);
          return reply;
        } else {
          std::string conversiontag;

          if (is_rewrite) {
            if (layout.length() == 0) {
              oss_out << "info: rewriting file with identical layout id\n";
              char hexlayout[17];
              snprintf(hexlayout, sizeof(hexlayout) - 1, "%08llx",
                       (long long) layoutid);
              layout = hexlayout;
            }

            // Get the space this file is currently hosted
            if (!fsid) {
              reply.set_std_err("error: file has no replica attached\n");
              reply.set_retc(ENODEV);
              return reply;
            }

            // Figure out which space this fsid is in
            {
              eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
              FileSystem* filesystem = FsView::gFsView.mIdView.lookupByID(fsid);

              if (!filesystem) {
                reply.set_std_err("error: couldn't find filesystem in view\n");
                reply.set_retc(EINVAL);
                return reply;
              }

              // Get the space of that filesystem
              space = filesystem->GetString("schedgroup").c_str();
              space.erase(space.find("."));
              oss_out << "info: rewriting into space '" << space.c_str() << "'\n";
            }
          }

          if (eos::common::StringConversion::IsHexNumber(layout.c_str(), "%08x")) {
            conversiontag = ConversionTag::Get(fileid, space.c_str(), layout.c_str(),
                                               std::string(""), false);
            oss_out << "info: conversion based on hexadecimal layout id\n";
          } else {
            unsigned long layout_type = 0;
            unsigned long layout_stripes = 0;
            // Check if it was provided as <layout>:<stripes>
            std::string lLayout = layout.c_str();
            std::string lLayoutName;
            std::string lLayoutStripes;

            if (eos::common::StringConversion::SplitKeyValue(lLayout,
                lLayoutName, lLayoutStripes)) {
              XrdOucString lLayoutString = "eos.layout.type=";
              lLayoutString += lLayoutName.c_str();
              lLayoutString += "&eos.layout.nstripes=";
              lLayoutString += lLayoutStripes.c_str();

              // Unless explicitly stated, use the layout checksum
              if (echecksum == eos::common::LayoutId::eChecksum::kNone) {
                echecksum = static_cast<eos::common::LayoutId::eChecksum>(
                              eos::common::LayoutId::GetChecksum(layoutid));
              }

              XrdOucEnv lLayoutEnv(lLayoutString.c_str());
              layout_type = eos::common::LayoutId::GetLayoutFromEnv(lLayoutEnv);
              layout_stripes = eos::common::LayoutId::GetStripeNumberFromEnv(lLayoutEnv);
              // Re-create layout id by merging in the layout stripes, type & checksum
              layoutid = eos::common::LayoutId::GetId(
                           layout_type,
                           echecksum,
                           layout_stripes,
                           eos::common::LayoutId::k4M,
                           eos::common::LayoutId::kCRC32C,
                           eos::common::LayoutId::GetRedundancyStripeNumber(layoutid));
              conversiontag = ConversionTag::Get(fileid, space.c_str(), layoutid,
                                                 plctplcy.c_str(), false);
              oss_out << "info: conversion based layout+stripe arguments\n";
            } else {
              // Assume this is the name of an attribute
              conversiontag = ConversionTag::Get(fileid, space.c_str(), layout.c_str(),
                                                 plctplcy.c_str(), false);
              oss_out << "info: conversion based conversion attribute name\n";
            }
          }

          std::string err_msg;

          // Push conversion job to QuarkDB
          if (gOFS->mConverterEngine->ScheduleJob(fmd->getId(),
                                                  conversiontag, err_msg)) {
            oss_out << "success: pushed conversion job '" << conversiontag
                    << "' to QuarkDB";
          } else {
            oss_err << "error: failed to schedule conversion '" << conversiontag << "'";

            if (!err_msg.empty()) {
              oss_err << " msg=\"" << err_msg << "\"";
            }

            retc = EINVAL;
            break;
          }
        }
      }
    }
  } while (0);

  reply.set_std_out(oss_out.str());

  if (retc) {
    reply.set_std_err(oss_err.str());
  }

  reply.set_retc(retc);
  return reply;
}

//------------------------------------------------------------------------------
// Purge subcommand
//------------------------------------------------------------------------------
eos::console::ReplyProto
FileCmd::PurgeSubcmd(const eos::console::FileProto& file) noexcept
{
  eos::console::ReplyProto reply;
  XrdOucString spath = file.md().path().c_str();
  const auto& purge = file.purge();
  int max_versions = purge.purge_version();
  XrdOucErrInfo error;
  // Validate file exists inline
  struct stat buf;

  if (gOFS->_stat(spath.c_str(), &buf, error, mVid, "")) {
    reply.set_std_err(SSTR("error: unable to stat path=" << spath.c_str()));
    reply.set_retc(errno);
    return reply;
  }

  XrdOucString version_dir;
  eos::common::Path cPath(spath.c_str());
  version_dir += cPath.GetParentPath();
  version_dir += "/.sys.v#.";
  version_dir += cPath.GetName();
  version_dir += "/";

  if (gOFS->PurgeVersion(version_dir.c_str(), error, max_versions)) {
    if (error.getErrInfo()) {
      reply.set_std_err(SSTR("error: unable to purge versions for path="
                             << spath.c_str() << "\nerror: " << error.getErrText()));
      reply.set_retc(error.getErrInfo());
    } else {
      reply.set_std_err(SSTR("info: no versions to purge for path=" <<
                             spath.c_str()));
      reply.set_retc(0);
    }

    return reply;
  }

  reply.set_std_out(SSTR("success: purged versions for path=" << spath.c_str()));
  reply.set_retc(0);
  return reply;
}

//------------------------------------------------------------------------------
// Adjust replica subcommand
//------------------------------------------------------------------------------
eos::console::ReplyProto
FileCmd::AdjustReplicaSubcmd(const eos::console::FileProto& file) noexcept
{
  eos::console::ReplyProto reply;
  XrdOucString spath = file.md().path().c_str();
  const auto& adjustreplica = file.adjustreplica();

  // Check root permission inline
  if (mVid.uid != 0) {
    reply.set_std_err("error: you need to be root to execute this command");
    reply.set_retc(EPERM);
    return reply;
  }

  uint32_t lid = 0ul;
  uint64_t size = 0ull;
  unsigned long long fid = 0ull;
  std::shared_ptr<eos::IFileMD> fmd {nullptr};
  eos::IFileMD::LocationVector loc_vect;
  bool nodrop = adjustreplica.nodrop();
  int icreationsubgroup = -1;
  std::string creationspace = adjustreplica.space();

  if (adjustreplica.subgroup().length()) {
    icreationsubgroup = atoi(adjustreplica.subgroup().c_str());
  }

  std::ostringstream oss_out;
  std::ostringstream oss_err;
  int retc = 0;
  {
    eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex);

    // Reference by fid+fsid
    if (spath.beginswith("fid:") || spath.beginswith("fxid:")) {
      fid = Resolver::retrieveFileIdentifier(spath).getUnderlyingUInt64();

      try {
        fmd = gOFS->eosFileService->getFileMD(fid);
      } catch (eos::MDException& e) {
        reply.set_std_err(SSTR("error: cannot retrieve file meta data - "
                               << e.getMessage().str()));
        reply.set_retc(e.getErrno());
        return reply;
      }
    } else {
      // Reference by path
      try {
        fmd = gOFS->eosView->getFile(spath.c_str());
      } catch (eos::MDException& e) {
        reply.set_std_err(SSTR("error: cannot retrieve file meta data - "
                               << e.getMessage().str()));
        reply.set_retc(e.getErrno());
        return reply;
      }
    }

    if (fmd) {
      fid = fmd->getId();
      lid = fmd->getLayoutId();
      loc_vect = fmd->getLocations();
      size = fmd->getSize();
    } else {
      reply.set_std_err("error: file not found");
      reply.set_retc(errno ? errno : EINVAL);
      return reply;
    }
  }
  std::string refspace = "";
  std::string space = "default";
  unsigned int forcedsubgroup = 0;

  if (eos::common::LayoutId::GetLayoutType(lid) ==
      eos::common::LayoutId::kReplica) {
    // Check the configured and available replicas
    unsigned int nrep_online = 0;
    unsigned int nrep = loc_vect.size();
    unsigned int nrep_layout = eos::common::LayoutId::GetStripeNumber(lid) + 1;
    // Give priority to healthy file systems during scheduling
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
        // Remember the spacename
        space = snapshot.mSpace;

        if (!refspace.length()) {
          refspace = space;
        } else {
          if (space != refspace) {
            eos_warning("msg=\"replicas are in different spaces\" "
                        "fxid=%08llx space=%s req_space=%s", fid,
                        space.c_str(), refspace.c_str());
            continue;
          }
        }

        forcedsubgroup = snapshot.mGroupIndex;

        if ((snapshot.mConfigStatus > eos::common::ConfigStatus::kDrain) &&
            (snapshot.mStatus == eos::common::BootStatus::kBooted)) {
          // This is an accessible replica
          ++nrep_online;
          src_fs.insert(src_fs.begin(), *loc_it);
        } else {
          // Give less priority to unhealthy file systems
          src_fs.push_back(*loc_it);
        }
      } else {
        eos_err("msg=\"skip unknown file system\" fsid=%lu fxid=%08llx",
                *loc_it, fid);
      }
    }

    eos_debug("path=%s nrep=%lu nrep-layout=%lu nrep-online=%lu",
              spath.c_str(), nrep, nrep_layout, nrep_online);

    if (nrep_layout > nrep_online) {
      // Set the desired space & subgroup if provided
      if (creationspace.length()) {
        space = creationspace;
      }

      if (icreationsubgroup != -1) {
        forcedsubgroup = icreationsubgroup;
      }

      // If space explicitly set, don't force a particular subgroup
      if (creationspace.length()) {
        forcedsubgroup = -1;
      }

      // Trigger async replication if not enough replicas online
      int nrep_new = nrep_layout - nrep_online;
      eos_debug("msg=\"creating %d new replicas\" fxid=%08llx space=%s "
                "forcedsubgroup=%d icreationsubgroup=%d", nrep_new,
                fid, space.c_str(), forcedsubgroup, icreationsubgroup);
      unsigned long fs_indx;
      std::vector<unsigned int> selectedfs;
      std::vector<unsigned int> unavailfs;
      std::vector<unsigned int> excludefs;

      if (adjustreplica.exclude_fs().length()) {
        unsigned int exclude_fsid = strtoul(adjustreplica.exclude_fs().c_str(), 0, 10);

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
      XrdOucErrInfo error;
      gOFS->_attr_ls(cPath.GetParentPath(), error, mVid, (const char*) 0, attrmap);
      eos::mgm::Scheduler::tPlctPolicy plctplcy;
      std::string targetgeotag;
      XrdOucEnv opaque("");
      // Get placement policy
      Policy::GetPlctPolicy(spath.c_str(), attrmap, mVid, opaque,
                            plctplcy, targetgeotag);
      Scheduler::PlacementArguments plctargs;
      plctargs.alreadyused_filesystems = &src_fs;
      plctargs.bookingsize = size;
      plctargs.forced_scheduling_group_index = forcedsubgroup;
      plctargs.lid = layoutId;
      plctargs.inode = (ino64_t) fid;
      plctargs.path = spath.c_str();
      plctargs.plctTrgGeotag = &targetgeotag;
      plctargs.plctpolicy = plctplcy;
      plctargs.exclude_filesystems = &excludefs;
      plctargs.selected_filesystems = &selectedfs;
      plctargs.spacename = &space;
      plctargs.truncate = true;
      plctargs.vid = &mVid;

      if (!plctargs.isValid()) {
        reply.set_std_err("error: invalid argument for file placement");
        reply.set_retc(EINVAL);
        return reply;
      } else {
        errno = retc = Quota::FilePlacement(&plctargs);

        if (!errno) {
          Scheduler::AccessArguments acsargs;
          acsargs.bookingsize = 0;
          acsargs.forcedspace = space.c_str();
          acsargs.fsindex = &fs_indx;
          acsargs.isRW = false;
          acsargs.lid = (unsigned long) lid;
          acsargs.inode = (ino64_t) fid;
          acsargs.locationsfs = &src_fs;
          acsargs.tried_cgi = &tried_cgi;
          acsargs.unavailfs = &unavailfs;
          acsargs.vid = &mVid;

          if (!acsargs.isValid()) {
            reply.set_std_err("error: invalid argument for file access");
            reply.set_retc(EINVAL);
            return reply;
          } else {
            // We got a new replication vector
            for (unsigned int i = 0; i < selectedfs.size(); ++i) {
              errno = Scheduler::FileAccess(&acsargs);

              if (!errno) {
                // This is now our source filesystem
                unsigned int src_fsid = src_fs[fs_indx];

                if (gOFS->_replicatestripe(fmd.get(), spath.c_str(),
                                           error, mVid, src_fsid,
                                           selectedfs[i], false)) {
                  retc = error.getErrInfo();
                  oss_err << "error: unable to replicate stripe "
                          << src_fsid << " => " << selectedfs[i]
                          << " msg=" << error.getErrText() << "\n";

                  // Add message from previous successful replication job
                  if (oss_out.str().length()) {
                    oss_err << oss_out.str();
                  }
                } else {
                  oss_out << "success: scheduled replication from source fs="
                          << src_fsid << " => target fs="
                          << selectedfs[i] << "\n";
                }
              } else {
                retc = ENOSPC;
                oss_err << "error: create new replicas => no source available: "
                        << spath.c_str() << "\n";
              }
            }
          }
        } else {
          oss_err << "error: create new replicas => cannot place replicas: "
                  << spath.c_str() << "\n";
        }
      }
    } else {
      // Run this in case of over-replication
      if ((nrep_layout < nrep) && (nodrop == false)) {
        unsigned int n2delete = nrep - nrep_layout;
        std::multimap <common::ConfigStatus, int /*fsid*/> statemap;
        std::multimap <std::string /*schedgroup*/, int /*fsid*/> groupmap;
        eos_debug("msg=\"drop %d replicas\" space=%s group=%d fxid=%08llx",
                  n2delete, creationspace.c_str(), icreationsubgroup, fid);
        {
          for (auto loc_it = loc_vect.begin();
               loc_it != loc_vect.end(); ++loc_it) {
            if (!(*loc_it)) {
              eos_err("msg=\"skip file system with id 0\" fxid=%08llx", fid);
              continue;
            }

            eos::common::FileSystem::fsid_t fsid = *loc_it;
            FileSystem* filesystem = FsView::gFsView.mIdView.lookupByID(fsid);
            eos::common::FileSystem::fs_snapshot_t fs;

            if (filesystem && filesystem->SnapShotFileSystem(fs, true)) {
              statemap.insert(std::pair<common::ConfigStatus, int>(fs.mConfigStatus, fsid));
              groupmap.insert(std::pair<std::string, int>(fs.mGroup, fsid));
            }
          }
        }
        std::string cspace = creationspace;

        if (!cspace.empty() && (icreationsubgroup > 0)) {
          cspace += SSTR("." << icreationsubgroup);
        }

        std::multimap <common::ConfigStatus, int> limitedstatemap;

        for (auto sit = groupmap.begin(); sit != groupmap.end(); ++sit) {
          // Use fsid only if they match the space and/or group req
          if (sit->first.find(cspace) != 0) {
            continue;
          }

          // Default to the highest state for safety reasons
          common::ConfigStatus state = eos::common::ConfigStatus::kRW;

          // get the state for each fsid matching
          for (auto state_it = statemap.begin();
               state_it != statemap.end(); ++state_it) {
            if (state_it->second == sit->second) {
              state = state_it->first;
              break;
            }
          }

          // fill the map containing only the candidates
          limitedstatemap.insert(std::pair<common::ConfigStatus, int>
                                 (state, sit->second));
        }

        std::vector<unsigned long> fsid2delete;

        for (auto lit = limitedstatemap.begin(); lit != limitedstatemap.end(); ++lit) {
          fsid2delete.push_back(lit->second);

          if (fsid2delete.size() == n2delete) {
            break;
          }
        }

        if (fsid2delete.size() != n2delete) {
          oss_err << "warning: cannot adjust replicas according to "
                  << "your requirement:"
                  << " space=" << creationspace
                  << " subgroup=" << icreationsubgroup << "\n";
        }

        eos::common::RWMutexWriteLock ns_wr_lock(gOFS->eosViewRWMutex);

        try {
          auto fmd = gOFS->eosFileService->getFileMD(fid);

          for (unsigned int i = 0; i < fsid2delete.size(); i++) {
            if (fmd->hasLocation(fsid2delete[i])) {
              fmd->unlinkLocation(fsid2delete[i]);
              eos_debug("msg=\"removing location\" fsid=%lu fxid=%08llx",
                        fsid2delete[i], fid);
              oss_out << "success: dropping replica on fsid="
                      << (int) fsid2delete[i] << "\n";
            }
          }

          gOFS->eosView->updateFileStore(fmd.get());
        } catch (eos::MDException& e) {
          eos_debug("msg=\"caught exception\" errno=%d msg=\"%s\"",
                    e.getErrno(), e.getMessage().str().c_str());
          oss_err << "error: drop excess replicas => cannot unlink "
                  << "location - " << e.getMessage().str() << "\n";
        }
      }
    }
  } else {
    // This is a rain layout, we try to rewrite the file using the converter
    if (eos::common::LayoutId::IsRain(lid)) {
      // Rewrite the file asynchronously using the converter
      XrdOucString info;
      info += "&mgm.cmd=file&mgm.subcmd=convert&mgm.option=rewrite&mgm.path=";
      info += spath.c_str();
      // TODO: This needs to be adapted to call ConvertSubcmd directly
      // For now, return error
      reply.set_std_err("error: RAIN layout rewrite not yet implemented in protobuf version");
      reply.set_retc(ENOSYS);
      return reply;
    } else {
      retc = EINVAL;
      oss_out <<
              "warning: no action for this layout type (neither replica nor rain)\n";
    }
  }

  reply.set_std_out(oss_out.str());

  if (retc) {
    reply.set_std_err(oss_err.str());
  }

  reply.set_retc(retc);
  return reply;
}

//------------------------------------------------------------------------------
// Drop stripe subcommand
//------------------------------------------------------------------------------
eos::console::ReplyProto
FileCmd::DropSubcmd(const eos::console::FileProto& file) noexcept
{
  eos::console::ReplyProto reply;
  std::string path = file.md().path();
  const auto& drop = file.drop();
  unsigned long fsid = drop.fsid();
  bool force_remove = drop.force();
  // Get fid if path starts with fid: or fxid:
  eos::IFileMD::id_t fid = 0ull;
  XrdOucString spath = path.c_str();

  if (spath.beginswith("fid:") || spath.beginswith("fxid:")) {
    fid = Resolver::retrieveFileIdentifier(spath).getUnderlyingUInt64();
  }

  XrdOucErrInfo error;

  if (gOFS->_dropstripe(path.c_str(), fid, error, mVid, fsid, force_remove)) {
    reply.set_std_err("error: unable to drop stripe");
    reply.set_retc(errno);
  } else {
    std::ostringstream oss;
    oss << "success: dropped stripe on fs=" << fsid;
    reply.set_std_out(oss.str());
    reply.set_retc(0);
  }

  return reply;
}

//------------------------------------------------------------------------------
// Symlink subcommand
//------------------------------------------------------------------------------
eos::console::ReplyProto
FileCmd::SymlinkSubcmd(const eos::console::FileProto& file) noexcept
{
  eos::console::ReplyProto reply;
  XrdOucString source = file.md().path().c_str();
  const auto& symlink = file.symlink();
  XrdOucString target = symlink.target_path().c_str();
  bool force = symlink.force();
  XrdOucErrInfo error;

  if (gOFS->symlink(source.c_str(), target.c_str(), error, mVid, 0, 0, force)) {
    reply.set_std_err("error: unable to link");
    reply.set_retc(errno);
  } else {
    reply.set_std_out(SSTR("success: linked '" << source.c_str()
                           << "' to '" << target.c_str() << "'"));
    reply.set_retc(0);
  }

  return reply;
}

//------------------------------------------------------------------------------
// Move subcommand
//------------------------------------------------------------------------------
eos::console::ReplyProto
FileCmd::MoveSubcmd(const eos::console::FileProto& file) noexcept
{
  eos::console::ReplyProto reply;
  std::string path = file.md().path();
  const auto& move = file.move();
  unsigned long sourcefsid = move.fsid1();
  unsigned long targetfsid = move.fsid2();
  XrdOucErrInfo error;

  if (gOFS->_movestripe(path.c_str(), error, mVid, sourcefsid, targetfsid)) {
    reply.set_std_err("error: unable to move stripe");
    reply.set_retc(errno);
  } else {
    std::ostringstream oss;
    oss << "success: scheduled move from source fs=" << sourcefsid
        << " => target fs=" << targetfsid;
    reply.set_std_out(oss.str());
    reply.set_retc(0);
  }

  return reply;
}

//------------------------------------------------------------------------------
// Replicate subcommand
//------------------------------------------------------------------------------
eos::console::ReplyProto
FileCmd::ReplicateSubcmd(const eos::console::FileProto& file) noexcept
{
  eos::console::ReplyProto reply;
  std::string path = file.md().path();
  const auto& replicate = file.replicate();
  unsigned long sourcefsid = replicate.fsid1();
  unsigned long targetfsid = replicate.fsid2();
  XrdOucErrInfo error;

  if (gOFS->_copystripe(path.c_str(), error, mVid, sourcefsid, targetfsid)) {
    reply.set_std_err("error: unable to replicate stripe");
    reply.set_retc(errno);
  } else {
    std::ostringstream oss;
    oss << "success: scheduled replication from source fs=" << sourcefsid
        << " => target fs=" << targetfsid;
    reply.set_std_out(oss.str());
    reply.set_retc(0);
  }

  return reply;
}

//------------------------------------------------------------------------------
// Rename with symlink subcommand
//------------------------------------------------------------------------------
eos::console::ReplyProto
FileCmd::RenameWithSymlinkSubcmd(const eos::console::FileProto& file) noexcept
{
  eos::console::ReplyProto reply;
  XrdOucString source = file.md().path().c_str();
  const auto& rename_symlink = file.rename_with_symlink();
  XrdOucString target = rename_symlink.destination_dir().c_str();
  XrdOucErrInfo error;

  if (gOFS->_rename_with_symlink(source.c_str(), target.c_str(),
                                 error, mVid, 0, 0, true, true)) {
    reply.set_std_err(SSTR("error: " << error.getErrText()));
    reply.set_retc(errno);
  } else {
    reply.set_std_out(SSTR("success: renamed '" << source.c_str()
                           << "' to '" << target.c_str() << "'"));
    reply.set_retc(0);
  }

  return reply;
}

//------------------------------------------------------------------------------
// Verify subcommand
//------------------------------------------------------------------------------
eos::console::ReplyProto
FileCmd::VerifySubcmd(const eos::console::FileProto& file) noexcept
{
  eos::console::ReplyProto reply;
  std::string path = file.md().path();
  const auto& verify = file.verify();

  // Check permissions inline - only root can do that
  if (mVid.uid != 0) {
    reply.set_std_err("error: you need to be root to execute this command");
    reply.set_retc(EPERM);
    return reply;
  }

  // Build option string
  XrdOucString option = "";

  if (verify.checksum()) {
    option += "&mgm.verify.compute.checksum=1";
  }

  if (verify.commitchecksum()) {
    option += "&mgm.verify.commit.checksum=1";
  }

  if (verify.commitsize()) {
    option += "&mgm.verify.commit.size=1";
  }

  if (verify.commitfmd()) {
    option += "&mgm.verify.commit.fmd=1";
  }

  if (verify.rate()) {
    option += "&mgm.verify.rate=";
    option += std::to_string(verify.rate()).c_str();
  }

  bool doresync = verify.resync();
  int acceptfsid = verify.fsid() ? verify.fsid() : 0;
  // Get file metadata
  eos::common::RWMutexReadLock viewReadLock(gOFS->eosViewRWMutex);
  std::shared_ptr<eos::IFileMD> fmd;
  XrdOucString spath = path.c_str();

  try {
    if (spath.beginswith("fid:") || spath.beginswith("fxid:")) {
      unsigned long long fid = Resolver::retrieveFileIdentifier(
                                 spath).getUnderlyingUInt64();
      fmd = gOFS->eosFileService->getFileMD(fid);
      std::string fullpath = gOFS->eosView->getUri(fmd.get());
      path = fullpath;
      spath = path.c_str();
    } else {
      fmd = gOFS->eosView->getFile(spath.c_str());
    }
  } catch (eos::MDException& e) {
    reply.set_std_err(SSTR("error: cannot retrieve file meta data - "
                           << e.getMessage().str()));
    reply.set_retc(e.getErrno());
    return reply;
  }

  if (!fmd) {
    reply.set_std_err("error: file not found");
    reply.set_retc(ENOENT);
    return reply;
  }

  // Copy out the locations vector
  eos::IFileMD::LocationVector locations = fmd->getLocations();
  eos::common::LayoutId::layoutid_t fmdlid = fmd->getLayoutId();
  eos::common::FileId::fileid_t fileid = fmd->getId();
  // Check if this is a RAIN layout
  bool isRAIN = false;

  if ((eos::common::LayoutId::GetLayoutType(fmdlid) ==
       eos::common::LayoutId::kRaidDP) ||
      (eos::common::LayoutId::GetLayoutType(fmdlid) ==
       eos::common::LayoutId::kArchive) ||
      (eos::common::LayoutId::GetLayoutType(fmdlid) ==
       eos::common::LayoutId::kRaid6)) {
    isRAIN = true;
  }

  // Get alternative checksums if computing and committing checksum
  if (verify.checksum() && verify.commitchecksum()) {
    try {
      auto dmd = gOFS->eosDirectoryService->getContainerMD(fmd->getContainerId());
      eos::IContainerMD::XAttrMap attrmap;
      eos::listAttributes(gOFS->eosView, dmd.get(), attrmap, false);

      if (attrmap.count(SYS_ALTCHECKSUMS)) {
        option += "&mgm.verify.compute.altchecksum=";
        option += attrmap[SYS_ALTCHECKSUMS].c_str();
      }
    } catch (eos::MDException& e) {
      // Continue without alternative checksums
    }
  }

  viewReadLock.Release();
  std::ostringstream oss_out;
  std::ostringstream oss_err;
  int retc = 0;
  bool acceptfound = false;
  XrdOucErrInfo error;

  // Iterate through all locations
  for (auto loc_it = locations.begin(); loc_it != locations.end(); ++loc_it) {
    // Skip if we're filtering by fsid and this isn't it
    if (acceptfsid && (acceptfsid != (int)*loc_it)) {
      continue;
    }

    if (acceptfsid) {
      acceptfound = true;
    }

    if (doresync) {
      // Send FMD resync request
      int lretc = gOFS->QueryResync(fileid, (int) * loc_it, true);

      if (!lretc) {
        oss_out << "success: sending FMD resync to fsid=" << *loc_it
                << " for path=" << path << "\n";
      } else {
        oss_err << "error: failed to send FMD resync to fsid=" << *loc_it << "\n";
        retc = errno;
      }
    } else {
      if (isRAIN) {
        // RAIN layouts only resync metadata records
        int lretc = gOFS->QueryResync(fileid, (int) * loc_it);

        if (!lretc) {
          oss_out << "success: sending resync for RAIN layout to fsid=" << *loc_it
                  << " for path=" << path << "\n";
        } else {
          retc = errno;
          oss_err << "error: failed to send RAIN resync to fsid=" << *loc_it << "\n";
        }
      } else {
        // Regular verification for non-RAIN layouts
        int lretc = gOFS->_verifystripe(spath.c_str(), error, mVid,
                                        (unsigned long) * loc_it, option.c_str());

        if (!lretc) {
          oss_out << "success: sending verify to fsid=" << *loc_it
                  << " for path=" << path << "\n";
        } else {
          retc = errno;
          oss_err << "error: failed to send verify to fsid=" << *loc_it
                  << " - " << error.getErrText() << "\n";
        }
      }
    }
  }

  // Handle forced verification of a not-registered replica
  if (acceptfsid && !acceptfound) {
    int lretc = gOFS->_verifystripe(spath.c_str(), error, mVid,
                                    (unsigned long)acceptfsid, option.c_str());

    if (!lretc) {
      oss_out << "success: sending forced verify to fsid=" << acceptfsid
              << " for path=" << path << "\n";
    } else {
      retc = errno;
      oss_err << "error: failed to send forced verify to fsid=" << acceptfsid
              << " - " << error.getErrText() << "\n";
    }
  }

  // Set reply
  reply.set_std_out(oss_out.str());

  if (retc) {
    reply.set_std_err(oss_err.str());
  }

  reply.set_retc(retc);
  return reply;
}


//------------------------------------------------------------------------------
// Copy subcommand
//------------------------------------------------------------------------------
eos::console::ReplyProto
FileCmd::CopySubcmd(const eos::console::FileProto& file) noexcept
{
  eos::console::ReplyProto reply;
  XrdOucString src = file.md().path().c_str();
  const auto& copy = file.copy();
  XrdOucString dst = copy.dst().c_str();

  if (!dst.length()) {
    reply.set_std_err("error: missing destination argument");
    reply.set_retc(EINVAL);
    return reply;
  }

  struct stat srcbuf;

  struct stat dstbuf;

  XrdOucErrInfo error;

  // Check that we can access source
  if (gOFS->_stat(src.c_str(), &srcbuf, error, mVid, "")) {
    reply.set_std_err(SSTR("error: " << error.getErrText()));
    reply.set_retc(errno);
    return reply;
  }

  std::ostringstream oss_out;
  std::ostringstream oss_err;
  bool silent = copy.silent();
  bool clone = copy.clone();
  bool force = copy.force();

  if (!silent) {
    if (clone) {
      oss_out << "info: cloning '";
    } else {
      oss_out << "info: copying '";
    }

    oss_out << src.c_str() << "' => '" << dst.c_str() << "' ...\n";
  }

  int dstat = gOFS->_stat(dst.c_str(), &dstbuf, error, mVid, "");

  if (!force && !dstat) {
    // There is no force flag and the target exists
    reply.set_std_err("error: the target file exists - use '-f' to force the copy");
    reply.set_retc(EEXIST);
    return reply;
  }

  // Check source and destination access
  if (gOFS->_access(src.c_str(), R_OK, error, mVid, "") ||
      gOFS->_access(dst.c_str(), W_OK, error, mVid, "")) {
    reply.set_std_err(SSTR("error: " << error.getErrText()));
    reply.set_retc(errno);
    return reply;
  }

  std::vector<std::string> lCopySourceList;
  std::vector<std::string> lCopyTargetList;
  // If this is a directory, create a list of files to copy
  std::map<std::string, std::set<std::string>> found;
  XrdOucString findErr;

  if (S_ISDIR(srcbuf.st_mode) && S_ISDIR(dstbuf.st_mode)) {
    if (!gOFS->_find(src.c_str(), error, findErr, mVid, found)) {
      // Add all to the copy source,target list
      for (auto dirit = found.begin(); dirit != found.end(); dirit++) {
        // Loop over dirs and add all the files
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

          if (!silent) {
            oss_out << "info: copying '" << src_path << "' => '"
                    << dst_path << "' ...\n";
          }
        }
      }
    } else {
      reply.set_std_err(SSTR("error: find failed - " << findErr.c_str()));
      reply.set_retc(errno);
      return reply;
    }
  } else {
    // Add a single file to the copy list
    lCopySourceList.push_back(src.c_str());
    lCopyTargetList.push_back(dst.c_str());
  }

  int retc = 0;

  for (size_t i = 0; i < lCopySourceList.size(); i++) {
    // Setup a TPC job
    XrdCl::PropertyList properties;
    XrdCl::PropertyList result;

    if (srcbuf.st_size) {
      // TPC for non-empty files
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
           (unsigned long long) mVid.uid);
    cgi += "&eos.rgid=";
    cgi += eos::common::StringConversion::GetSizeString(sizestring,
           (unsigned long long) mVid.gid);
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
    properties.Set("sourceLimit", (uint16_t) 1);
    properties.Set("chunkSize", (uint32_t)(4 * 1024 * 1024));
    properties.Set("parallelChunks", (uint8_t) 1);
    XrdCl::CopyProcess lCopyProcess;
    lCopyProcess.AddJob(properties, &result);
    XrdCl::XRootDStatus lTpcPrepareStatus = lCopyProcess.Prepare();
    eos_static_info("[tpc]: %s=>%s %s",
                    url_src.GetURL().c_str(),
                    url_trg.GetURL().c_str(),
                    lTpcPrepareStatus.ToStr().c_str());

    if (lTpcPrepareStatus.IsOK()) {
      XrdCl::XRootDStatus lTpcStatus = lCopyProcess.Run(0);
      eos_static_info("[tpc]: %s %d",
                      lTpcStatus.ToStr().c_str(),
                      lTpcStatus.IsOK());

      if (lTpcStatus.IsOK()) {
        if (!silent) {
          oss_out << "success: copy done '" << source << "'\n";
        }
      } else {
        oss_err << "error: copy failed '" << source << "' - "
                << lTpcStatus.ToStr().c_str() << "\n";
        retc = EIO;
      }
    } else {
      oss_err << "error: copy failed - " << lTpcPrepareStatus.ToStr().c_str() << "\n";
      retc = EIO;
    }
  }

  reply.set_std_out(oss_out.str());

  if (retc) {
    reply.set_std_err(oss_err.str());
  }

  reply.set_retc(retc);
  return reply;
}


//------------------------------------------------------------------------------
// Rename subcommand
//------------------------------------------------------------------------------
eos::console::ReplyProto
FileCmd::RenameSubcmd(const eos::console::FileProto& file) noexcept
{
  eos::console::ReplyProto reply;
  XrdOucString source = file.md().path().c_str();
  const auto& rename = file.rename();
  XrdOucString target = rename.new_path().c_str();
  XrdOucErrInfo error;

  if (gOFS->rename(source.c_str(), target.c_str(), error, mVid, 0, 0, true)) {
    reply.set_std_err(SSTR("error: " << error.getErrText()));
    reply.set_retc(errno);
  } else {
    reply.set_std_out(SSTR("success: renamed '" << source.c_str()
                           << "' to '" << target.c_str() << "'"));
    reply.set_retc(0);
  }

  return reply;
}

EOSMGMNAMESPACE_END
