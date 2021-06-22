//------------------------------------------------------------------------------
// @file DrainTransferJob.cc
// @author Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#include "mgm/drain/DrainTransferJob.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/FsView.hh"
#include "mgm/GeoTreeEngine.hh"
#include "mgm/Stat.hh"
#include "mgm/proc/proc_fs.hh"
#include "common/SecEntity.hh"
#include "common/LayoutId.hh"
#include "namespace/interface/IView.hh"
#include "namespace/ns_quarkdb/BackendClient.hh"
#include "namespace/ns_quarkdb/persistency/MetadataFetcher.hh"
#include "namespace/Prefetcher.hh"
#include "fmt/format.h"

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Save error message and set the status accordingly
//------------------------------------------------------------------------------
void DrainTransferJob::ReportError(const std::string& error)
{
  eos_err("%s", error.c_str());
  mErrorString = error;
  mStatus = Status::Failed;
}

//------------------------------------------------------------------------------
// Execute a thrid-party transfer
//------------------------------------------------------------------------------
void
DrainTransferJob::DoIt() noexcept
{
  using eos::common::LayoutId;
  UpdateMgmStats();
  eos_static_debug("msg=\"running job\" fsid_src=%i fsid_dst=%i fxid=%08llx",
                   mFsIdSource.load(), mFsIdTarget.load(), mFileId);

  if (mProgressHandler.ShouldCancel(0)) {
    ReportError(SSTR("msg=\"job cancelled before starting\" fxid="
                     << eos::common::FileId::Fid2Hex(mFileId)));
    UpdateMgmStats();
    return;
  }

  mStatus = Status::Running;
  FileDrainInfo fdrain;

  try {
    fdrain = GetFileInfo();
  } catch (const eos::MDException& e) {
    // This could be a ghost fid entry still present in the file system map
    // and we need to also drop it from there
    std::string out, err;
    auto root_vid = eos::common::VirtualIdentity::Root();
    (void) proc_fs_dropghosts(mFsIdSource, {mFileId}, root_vid, out, err);
    eos_info("msg=\"drain ghost entry successful\" fxid=%s",
             eos::common::FileId::Fid2Hex(mFileId).c_str());
    mStatus = Status::OK;
    UpdateMgmStats();
    return;
  }

  while (true) {
    if (!SelectDstFs(fdrain)) {
      ReportError(SSTR("msg=\"failed to select destination file system\" fxid="
                       << eos::common::FileId::Fid2Hex(mFileId)));
      UpdateMgmStats();
      return;
    }

    // Special case when deadling with 0-size replica files
    if ((fdrain.mProto.size() == 0) &&
        (LayoutId::GetLayoutType(fdrain.mProto.layout_id()) ==
         LayoutId::kReplica)) {
      mStatus = DrainZeroSizeFile(fdrain);
      UpdateMgmStats();
      return;
    }

    // Prepare the TPC copy job
    std::string log_id = LogId::GenerateLogId();
    XrdCl::URL url_src = BuildTpcSrc(fdrain, log_id);
    XrdCl::URL url_dst = BuildTpcDst(fdrain, log_id);

    // When no more sources are available the url_src is empty and mStatus is
    // properly set
    if (!url_src.IsValid() || !url_dst.IsValid()) {
      UpdateMgmStats();
      return;
    }

    // If enabled use xrootd connection pool to avoid bottelnecks on the
    // same physical connection
    eos::common::XrdConnIdHelper src_id_helper(gOFS->mXrdConnPool, url_src);
    eos::common::XrdConnIdHelper dst_id_helper(gOFS->mXrdConnPool, url_dst);
    // Populate the properties map of the transfer
    XrdCl::PropertyList properties;
    properties.Set("force", true);
    properties.Set("posc", false);
    properties.Set("coerce", false);
    properties.Set("source", url_src);
    properties.Set("target", url_dst);
    properties.Set("sourceLimit", (uint16_t) 1);
    properties.Set("chunkSize", (uint32_t)(4 * 1024 * 1024));
    properties.Set("parallelChunks", (uint8_t) 1);
    properties.Set("tpcTimeout", eos::common::FileId::EstimateTpcTimeout
                   (fdrain.mProto.size()).count());

    // Non-empty files run with TPC only
    if (fdrain.mProto.size()) {
      properties.Set("thirdParty", "only");
    }

    // Create the process job
    XrdCl::PropertyList result;
    XrdCl::CopyProcess cpy;
    cpy.AddJob(properties, &result);
    XrdCl::XRootDStatus prepare_st = cpy.Prepare();
    eos_info("[tpc]: app=%s logid=%s src_url=%s => dst_url=%s"
             " prepare_msg=%s", mAppTag.c_str(), log_id.c_str(),
             url_src.GetLocation().c_str(), url_dst.GetLocation().c_str(),
             prepare_st.ToStr().c_str());

    if (prepare_st.IsOK()) {
      XrdCl::XRootDStatus tpc_st = cpy.Run(&mProgressHandler);

      if (!tpc_st.IsOK()) {
        eos_err("%s", SSTR("src=" << url_src.GetLocation().c_str() <<
                           " dst=" << url_dst.GetLocation().c_str() <<
                           " logid=" << log_id <<
                           " tpc_err=" << tpc_st.ToStr()).c_str());

        // If cancellation requested no point in trying other replicas
        if (mProgressHandler.ShouldCancel(0)) {
          break;
        }

        // If file is being written then remove it from the drain tracker so
        // that it's retried one more time at the end
        if (tpc_st.errNo == EINPROGRESS) {
          eos_info("msg=\"skip file open in progress\" logid=%s", log_id.c_str());
          break;
        }
      } else {
        eos_info("msg=\"%s successful\" logid=%s fxid=%s", mAppTag.c_str(),
                 log_id.c_str(), eos::common::FileId::Fid2Hex(mFileId).c_str());
        mStatus = Status::OK;
        UpdateMgmStats();
        return;
      }
    } else {
      eos_err("%s", SSTR("msg=\"prepare failed\" logid="
                         << log_id.c_str()).c_str());
    }
  }

  mStatus = Status::Failed;
  UpdateMgmStats();
  return;
}

//------------------------------------------------------------------------------
// Get file metadata info
//------------------------------------------------------------------------------
DrainTransferJob::FileDrainInfo
DrainTransferJob::GetFileInfo() const
{
  std::ostringstream oss;
  FileDrainInfo fdrain;
  eos::Prefetcher::prefetchFileMDWithParentsAndWait(gOFS->eosView, mFileId);

  try {
    eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex, __FUNCTION__,
                                            __LINE__, __FILE__);
    std::shared_ptr<eos::IFileMD> fmd = gOFS->eosFileService->getFileMD(mFileId);
    fdrain.mFullPath = gOFS->eosView->getUri(fmd.get());
    fdrain.mProto.set_id(fmd->getId());
    fdrain.mProto.set_layout_id(fmd->getLayoutId());
    fdrain.mProto.set_cont_id(fmd->getContainerId());
    fdrain.mProto.set_uid(fmd->getCUid());
    fdrain.mProto.set_gid(fmd->getCGid());
    fdrain.mProto.set_size(fmd->getSize());
    auto xs = fmd->getChecksum();
    fdrain.mProto.set_checksum(xs.getDataPtr(), xs.getSize());
    auto vect_locations = fmd->getLocations();

    for (const auto loc : vect_locations) {
      fdrain.mProto.add_locations(loc);
    }
  } catch (eos::MDException& e) {
    eos_err("%s", SSTR("fxid=" << eos::common::FileId::Fid2Hex(mFileId)
                       << " errno=" << e.getErrno()
                       << " msg=\"" << e.getMessage().str() << "\"").c_str());
    throw e;
  }

  return fdrain;
}

//------------------------------------------------------------------------------
// Build TPC source url
//------------------------------------------------------------------------------
XrdCl::URL
DrainTransferJob::BuildTpcSrc(const FileDrainInfo& fdrain,
                              const std::string& log_id)
{
  using namespace eos::common;
  XrdCl::URL url_src;
  eos::common::FileSystem::fs_snapshot_t src_snapshot;
  unsigned long lid = fdrain.mProto.layout_id();
  unsigned long target_lid = LayoutId::SetLayoutType(lid, LayoutId::kPlain);

  // Mask block checksums (set to kNone) for replica layouts
  if (LayoutId::GetLayoutType(lid) == LayoutId::kReplica) {
    target_lid = LayoutId::SetBlockChecksum(target_lid, LayoutId::kNone);
  }

  if (eos::common::LayoutId::GetLayoutType(fdrain.mProto.layout_id()) <=
      eos::common::LayoutId::kReplica) {
    bool found = false;

    for (const auto id : fdrain.mProto.locations()) {
      // First try copying from a location different from the current draining
      // file system
      if ((id != mFsIdSource) && (mTriedSrcs.find(id) == mTriedSrcs.end())) {
        mTriedSrcs.insert(id);
        eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
        FileSystem* fs = FsView::gFsView.mIdView.lookupByID(id);

        if (fs) {
          fs->SnapShotFileSystem(src_snapshot);

          if (src_snapshot.mConfigStatus >= eos::common::ConfigStatus::kDrain) {
            found = true;
            break;
          }
        }
      }
    }

    // If nothing found and we didn't try the current file system give it a go
    if (!found && (mTriedSrcs.find(mFsIdSource) == mTriedSrcs.end())) {
      found = true;
      mTriedSrcs.insert(mFsIdSource);
      eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
      FileSystem* fs = FsView::gFsView.mIdView.lookupByID(mFsIdSource);

      if (!fs) {
        ReportError(SSTR("msg=\"fsid=" << mFsIdSource << " no longer in the list"));
        return url_src;
      }

      fs->SnapShotFileSystem(src_snapshot);
    }

    if (!found) {
      ReportError(SSTR("msg=\"no more replicas available\" " << "fxid="
                       << eos::common::FileId::Fid2Hex(fdrain.mProto.id())));
      return url_src;
    }
  } else {
    // For RAIN layouts we trigger a reconstruction only once
    if (mRainReconstruct) {
      ReportError(SSTR("msg=\"fxid=" << eos::common::FileId::Fid2Hex(
                         fdrain.mProto.id())
                       << " rain reconstruct already failed\""));
      return url_src;
    } else {
      mRainReconstruct = true;
    }
  }

  // Construct the source URL
  std::ostringstream src_params;
  mTxFsIdSource = src_snapshot.mId;

  if (mRainReconstruct) {
    src_params << "&mgm.path=" << StringConversion::SealXrdOpaque(fdrain.mFullPath)
               << "&mgm.manager=" << gOFS->ManagerId.c_str()
               << "&mgm.fid=" << eos::common::FileId::Fid2Hex(mFileId)
               << "&mgm.sec="
               << eos::common::SecEntity::ToKey(0, SSTR("eos/" << mAppTag).c_str())
               << "&eos.app=" << mAppTag
               << "&eos.ruid=0&eos.rgid=0";
  } else {
    src_params << "mgm.access=read"
               << "&mgm.lid=" << target_lid
               << "&mgm.cid=" << fdrain.mProto.cont_id()
               << "&mgm.ruid=1&mgm.rgid=1&mgm.uid=1&mgm.gid=1"
               << "&mgm.path=" << StringConversion::SealXrdOpaque(fdrain.mFullPath)
               << "&mgm.manager=" << gOFS->ManagerId.c_str()
               << "&mgm.fid=" << eos::common::FileId::Fid2Hex(mFileId)
               << "&mgm.sec="
               << eos::common::SecEntity::ToKey(0, SSTR("eos/" << mAppTag).c_str())
               << "&mgm.localprefix=" << src_snapshot.mPath.c_str()
               << "&mgm.fsid=" << src_snapshot.mId
               << "&mgm.sourcehostport=" << src_snapshot.mHostPort.c_str()
               << "&eos.app=" << mAppTag
               << "&eos.ruid=0&eos.rgid=0";
  }

  // Build the capability
  int caprc = 0;
  XrdOucEnv* output_cap = 0;
  XrdOucEnv input_cap(src_params.str().c_str());
  SymKey* symkey = eos::common::gSymKeyStore.GetCurrentKey();

  if ((caprc = SymKey::CreateCapability(&input_cap, output_cap,
                                        symkey, gOFS->mCapabilityValidity))) {
    ReportError(SSTR("msg=\"unable to create src capability, errno=" << caprc
                     << "\""));
    return url_src;
  }

  int cap_len = 0;
  std::ostringstream src_cap;

  if (mRainReconstruct) {
    url_src.SetPath(eos::common::StringConversion::curl_escaped(fdrain.mFullPath));
    url_src.SetHostName(gOFS->MgmOfsAlias.c_str());
    url_src.SetPort(gOFS->ManagerPort);
    src_cap << output_cap->Env(cap_len)
            << "&mgm.logid=" << log_id
            << "&eos.pio.action=reconstruct"
            << "&eos.pio.recfs=" << mFsIdSource
            << "&eos.encodepath=curl";
  } else {
    std::ostringstream oss_path;
    oss_path << "/replicate:" << eos::common::FileId::Fid2Hex(mFileId);
    url_src.SetPath(oss_path.str());
    url_src.SetHostName(src_snapshot.mHost.c_str());
    url_src.SetPort(src_snapshot.mPort);
    src_cap << output_cap->Env(cap_len)
            << "&mgm.logid=" << log_id
            << "&source.url=root://" << src_snapshot.mHostPort.c_str()
            << "//replicate:" << eos::common::FileId::Fid2Hex(mFileId);
  }

  url_src.SetParams(src_cap.str());
  url_src.SetProtocol("root");
  url_src.SetUserName("daemon");
  delete output_cap;
  return url_src;
}

//------------------------------------------------------------------------------
// Build TPC destination url
//------------------------------------------------------------------------------
XrdCl::URL
DrainTransferJob::BuildTpcDst(const FileDrainInfo& fdrain,
                              const std::string& log_id)
{
  using namespace eos::common;
  XrdCl::URL url_dst;
  eos::common::FileSystem::fs_snapshot_t dst_snapshot;
  unsigned long lid = fdrain.mProto.layout_id();
  unsigned long target_lid = LayoutId::SetLayoutType(lid, LayoutId::kPlain);

  // Mask block checksums (set to kNone) for replica layouts
  if (LayoutId::GetLayoutType(lid) == LayoutId::kReplica) {
    target_lid = LayoutId::SetBlockChecksum(target_lid, LayoutId::kNone);
  }

  {
    // Get destination fs snapshot
    eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
    FileSystem* fs = FsView::gFsView.mIdView.lookupByID(mFsIdTarget);

    if (!fs) {
      ReportError("msg=\"target file system not found\"");
      return url_dst;
    }

    fs->SnapShotFileSystem(dst_snapshot);
  }

  std::ostringstream xs_info;
  std::ostringstream dst_params;

  if (mRainReconstruct) {
    dst_params << "mgm.access=write"
               << "&mgm.ruid=1&mgm.rgid=1&mgm.uid=1&mgm.gid=1&mgm.fid=0"
               << "&mgm.lid=" << target_lid
               << "&mgm.cid=" << fdrain.mProto.cont_id()
               << "&mgm.manager=" << gOFS->ManagerId.c_str()
               << "&mgm.fsid=" << dst_snapshot.mId
               << "&mgm.sec="
               << eos::common::SecEntity::ToKey(0, SSTR("eos/" << mAppTag).c_str())
               << "&eos.app=" << mAppTag;
  } else {
    dst_params << "mgm.access=write"
               << "&mgm.lid=" << target_lid
               << "&mgm.source.lid=" << lid
               << "&mgm.source.ruid=" << fdrain.mProto.uid()
               << "&mgm.source.rgid=" << fdrain.mProto.gid()
               << "&mgm.cid=" << fdrain.mProto.cont_id()
               << "&mgm.ruid=1&mgm.rgid=1&mgm.uid=1&mgm.gid=1"
               << "&mgm.path=" << StringConversion::SealXrdOpaque(fdrain.mFullPath.c_str())
               << "&mgm.manager=" << gOFS->ManagerId.c_str()
               << "&mgm.fid=" << eos::common::FileId::Fid2Hex(mFileId)
               << "&mgm.sec="
               << eos::common::SecEntity::ToKey(0, SSTR("eos/" << mAppTag).c_str())
               << "&mgm.localprefix=" << dst_snapshot.mPath.c_str()
               << "&mgm.fsid=" << dst_snapshot.mId
               << "&mgm.sourcehostport=" << dst_snapshot.mHostPort.c_str()
               << "&mgm.bookingsize=" << fdrain.mProto.size()
               << "&eos.app=" << mAppTag
               << "&mgm.targetsize=" << fdrain.mProto.size();

    // This is true by default for drain, but when this is set to false, we get
    // a replication like behaviour similar to the adjustreplica command which
    // is useful for fsck for eg.
    if (mDropSrc) {
      dst_params << "&mgm.drainfsid=" << mFsIdSource;
    }

    if (!fdrain.mProto.checksum().empty()) {
      xs_info << "&mgm.checksum=";
      uint32_t xs_len = LayoutId::GetChecksumLen(lid);
      uint32_t data_len = fdrain.mProto.checksum().size();

      for (auto i = 0u; i < xs_len; ++i) {
        if (i >= data_len) {
          // Pad with zeros if necessary
          xs_info << '0';
        } else {
          xs_info << StringConversion::char_to_hex(fdrain.mProto.checksum()[i]);
        }
      }
    }
  }

  // Build the capability
  int caprc = 0;
  XrdOucEnv* output_cap = 0;
  XrdOucEnv input_cap(dst_params.str().c_str());
  SymKey* symkey = eos::common::gSymKeyStore.GetCurrentKey();

  if ((caprc = SymKey::CreateCapability(&input_cap, output_cap,
                                        symkey, gOFS->mCapabilityValidity))) {
    std::string err = "msg=\"unable to create dst capability, errno=";
    err += caprc;
    err += "\"";
    ReportError(err);
    return url_dst;
  }

  int cap_len = 0;
  std::ostringstream oss_cap;
  oss_cap << output_cap->Env(cap_len)
          << "&mgm.logid=" << log_id;

  // @Note: the mgm.checksum info needs to be unencrypted in the URL
  if (!xs_info.str().empty()) {
    oss_cap << xs_info.str();
  }

  url_dst.SetProtocol("root");
  url_dst.SetHostName(dst_snapshot.mHost.c_str());
  url_dst.SetPort(dst_snapshot.mPort);
  url_dst.SetUserName("daemon");
  url_dst.SetParams(oss_cap.str());
  std::ostringstream oss_path;
  oss_path << "/replicate:"
           << (mRainReconstruct ? "0" : eos::common::FileId::Fid2Hex(mFileId));
  url_dst.SetPath(oss_path.str());
  delete output_cap;
  return url_dst;
}

//------------------------------------------------------------------------------
// Select destiantion file system for current transfer
//------------------------------------------------------------------------------
bool
DrainTransferJob::SelectDstFs(const FileDrainInfo& fdrain)
{
  unsigned int nfilesystems = 1;
  unsigned int ncollocatedfs = 0;
  std::vector<FileSystem::fsid_t> new_repl;
  eos::common::FileSystem::fs_snapshot_t source_snapshot;
  eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
  eos::common::FileSystem* source_fs = FsView::gFsView.mIdView.lookupByID(
                                         mFsIdSource);

  if (source_fs == nullptr) {
    return false;
  }

  source_fs->SnapShotFileSystem(source_snapshot);
  FsGroup* group = FsView::gFsView.mGroupView[source_snapshot.mGroup];
  // Check other replicas for the file
  std::vector<std::string> fsid_geotags;
  std::vector<FileSystem::fsid_t> existing_repl;

  for (auto elem : fdrain.mProto.locations()) {
    existing_repl.push_back(elem);
  }

  if (!gOFS->mGeoTreeEngine->getInfosFromFsIds(existing_repl, &fsid_geotags, 0,
      0)) {
    eos_err("msg=\"failed to retrieve info for existing replicas\" fxid=%08llx",
            mFileId);
    return false;
  }

  bool res = gOFS->mGeoTreeEngine->placeNewReplicasOneGroup(
               group, nfilesystems,
               &new_repl,
               (ino64_t) fdrain.mProto.id(),
               NULL, // entrypoints
               NULL, // firewall
               GeoTreeEngine::draining,
               &existing_repl,
               &fsid_geotags,
               fdrain.mProto.size(),
               "",// start from geotag
               "",// client geo tag
               ncollocatedfs,
               &mExcludeDsts,
               &fsid_geotags); // excludeGeoTags

  if (!res || new_repl.empty())  {
    eos_err("msg=\"fxid=%08llx could not place new replica\"", mFileId);
    return false;
  }

  std::ostringstream oss;

  for (auto elem : new_repl) {
    oss << " " << (unsigned long)(elem);
  }

  // Return only one fs now
  mFsIdTarget = new_repl[0];
  mExcludeDsts.push_back(mFsIdTarget);
  eos_static_debug("msg=\"schedule placement retc=%d with fsids=%s\" ",
                   (int)res, oss.str().c_str());
  return true;
}

//------------------------------------------------------------------------------
// Drain 0-size file
//------------------------------------------------------------------------------
DrainTransferJob::Status
DrainTransferJob::DrainZeroSizeFile(const FileDrainInfo& fdrain)
{
  eos::common::RWMutexWriteLock wr_lock(gOFS->eosViewRWMutex, __FUNCTION__,
                                        __LINE__, __FILE__);
  std::shared_ptr<eos::IFileMD> file {nullptr};

  try {
    file = gOFS->eosFileService->getFileMD(fdrain.mProto.id());
  } catch (const eos::MDException& e) {
    // ignore, file will be null
  }

  if (file == nullptr) {
    return Status::Failed;
  }

  // We already have excess replicas just drop the current one
  if (file->getNumLocation() >
      eos::common::LayoutId::GetStripeNumber(fdrain.mProto.layout_id()) + 1) {
    file->unlinkLocation(mFsIdSource);
  } else {
    // Add the new location and remove the old one if requested
    file->addLocation(mFsIdTarget);

    if (mDropSrc) {
      file->unlinkLocation(mFsIdSource);
    }
  }

  gOFS->eosFileService->updateStore(file.get());
  return Status::OK;
}

//------------------------------------------------------------------------------
// Get drain job info based on the requested tags
//------------------------------------------------------------------------------
std::list<std::string>
DrainTransferJob::GetInfo(const std::list<std::string>& tags) const
{
  std::list<std::string> info;

  for (const auto& tag : tags) {
    // We only support the following tags
    if (tag == "fxid") {
      info.push_back(eos::common::FileId::Fid2Hex(mFileId));
    } else if (tag == "fs_src") {
      info.push_back(fmt::to_string(mFsIdSource));
    } else if (tag == "fs_dst") {
      info.push_back(fmt::to_string(mFsIdTarget));
    } else if (tag == "tx_fs_src") {
      info.push_back(fmt::to_string(mTxFsIdSource));
    } else if (tag == "start_timestamp") {
      std::time_t start_ts {(long int)mProgressHandler.mStartTimestampSec.load()};
      std::tm calendar_time;
      (void) localtime_r(&start_ts, &calendar_time);
      info.push_back(SSTR(std::put_time(&calendar_time, "%c %Z")));
    } else if (tag == "progress") {
      info.push_back(fmt::to_string(mProgressHandler.mProgress.load()) + "%");
    } else if (tag == "speed") {
      uint64_t now_sec = std::chrono::duration_cast<std::chrono::seconds>
                         (std::chrono::system_clock::now().time_since_epoch()).count();

      if (mProgressHandler.mStartTimestampSec < now_sec) {
        uint64_t duration_sec = now_sec - mProgressHandler.mStartTimestampSec;
        float rate = (mProgressHandler.mBytesTransferred / (1024 * 1024)) /
                     duration_sec;
        info.push_back(fmt::to_string(rate));
      } else {
        info.push_back("N/A");
      }
    } else if (tag == "err_msg") {
      info.push_back(mErrorString);
    } else {
      info.push_back("N/A");
    }
  }

  return info;
}

//------------------------------------------------------------------------------
// Update MGM stats depending on the type of transfer
//------------------------------------------------------------------------------
void
DrainTransferJob::UpdateMgmStats()
{
  std::string tag_stats;

  if (mAppTag == "drain") {
    tag_stats = "DrainCentral";
  } else {
    tag_stats = "Unknown";
  }

  if (mStatus == Status::OK) {
    tag_stats += "Successful";
  } else if (mStatus == Status::Failed) {
    tag_stats += "Failed";
  } else {
    tag_stats += "Started";
  }

  if (tag_stats != "Unknown") {
    gOFS->MgmStats.Add(tag_stats.c_str(), 0, 0, 1);
  }
}

EOSMGMNAMESPACE_END
