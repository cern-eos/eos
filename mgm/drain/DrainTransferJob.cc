//------------------------------------------------------------------------------
// @file DrainTransferJob.cc
// @author Andrea Manzi - CERN
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

#include "mgm/drain/DrainTransferJob.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/FsView.hh"
#include "mgm/GeoTreeEngine.hh"
#include "authz/XrdCapability.hh"
#include "common/SecEntity.hh"
#include "common/LayoutId.hh"
#include "common/Path.hh"
#include "namespace/interface/IView.hh"
#include "namespace/ns_quarkdb/BackendClient.hh"
#include "namespace/ns_quarkdb/persistency/MetadataFetcher.hh"
#include "XrdCl/XrdClCopyProcess.hh"


EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
DrainTransferJob::~DrainTransferJob()
{
  eos_debug("Destroying transfer job");

  if (mThread.joinable()) {
    mThread.join();
  }
}

//------------------------------------------------------------------------------
// Save error message and set the status accordingly
//------------------------------------------------------------------------------
void DrainTransferJob::ReportError(const std::string& error)
{
  eos_err(error.c_str());
  mErrorString = error;
  mStatus.store(Status::Failed);
}

//------------------------------------------------------------------------------
// Start thread doing the draining
//------------------------------------------------------------------------------
void DrainTransferJob::Start()
{
  mThread = std::thread(&DrainTransferJob::DoIt, this);
}

//------------------------------------------------------------------------------
// Implement the thrid-party transfer
//------------------------------------------------------------------------------
void
DrainTransferJob::DoIt()
{
  using eos::common::LayoutId;
  mStatus.store(Status::Running);
  FileDrainInfo fdrain;

  try {
    fdrain = GetFileInfo();
  } catch (const eos::MDException& e) {
    ReportError(std::string(e.what()));
    return;
  }

  if (!SelectDstFs(fdrain)) {
    ReportError("msg=\"failed to select destination file system\"");
    return;
  }

  // Take snapshots of the source and target file systems
  eos::common::FileSystem::fs_snapshot dst_snapshot;
  eos::common::FileSystem::fs_snapshot src_snapshot;
  {
    eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
    eos::common::FileSystem* source_fs = FsView::gFsView.mIdView[mFsIdSource];
    eos::common::FileSystem* target_fs = FsView::gFsView.mIdView[mFsIdTarget];

    if (!target_fs || !source_fs) {
      ReportError("msg=\"source/taget file system not found\"");
      return;
    }

    source_fs->SnapShotFileSystem(src_snapshot);
    target_fs->SnapShotFileSystem(dst_snapshot);
  }
  uint64_t lid = fdrain.mProto.layout_id();

  if ((LayoutId::GetLayoutType(lid) == LayoutId::kRaidDP) ||
      (LayoutId::GetLayoutType(lid) == LayoutId::kRaid6)) {
    // @todo (amanzi): to be implemented - run TPC with reconstruction
  } else {
    // Prepare the TPC copy job
    XrdCl::URL url_src = BuildTpcSrc(fdrain, src_snapshot);
    XrdCl::URL url_dst = BuildTpcDst(fdrain, dst_snapshot);

    if (!url_src.IsValid()) {
      ReportError("msg=\"invalid src url\"");
      return;
    }

    if (!url_dst.IsValid()) {
      ReportError("msg=\"invalid dst url\"");
      return;
    }

    XrdCl::PropertyList properties;
    properties.Set("force", true);
    properties.Set("posc", false);
    properties.Set("coerce", false);
    properties.Set("source", url_src);
    properties.Set("target", url_dst);
    properties.Set("sourceLimit", (uint16_t) 1);
    properties.Set("chunkSize", (uint32_t)(4 * 1024 * 1024));
    properties.Set("parallelChunks", (uint8_t) 1);
    properties.Set("tpcTimeout",  900);

    // Non-empty files run with TPC only
    if (fdrain.mProto.size()) {
      properties.Set("thirdParty", "only");
    }

    // Create the process job
    XrdCl::PropertyList result;
    XrdCl::CopyProcess cpy;
    cpy.AddJob(properties, &result);
    XrdCl::XRootDStatus prepare_st = cpy.Prepare();
    eos_notice("[tpc]: %s => %s prepare_msg=%s", url_src.GetURL().c_str(),
               url_dst.GetURL().c_str(), prepare_st.ToStr().c_str());

    if (prepare_st.IsOK()) {
      XrdCl::XRootDStatus tpc_st = cpy.Run(0);

      if (!tpc_st.IsOK()) {
        ReportError(tpc_st.ToStr().c_str());
      } else {
        eos_notice("msg=\"drain job completed successfully");
        mStatus.store(Status::OK);
      }
    } else {
      ReportError("msg=\"failed to prepare drain job\"");
    }
  }
}

//------------------------------------------------------------------------------
// Get file metadata info
//------------------------------------------------------------------------------
DrainTransferJob::FileDrainInfo
DrainTransferJob::GetFileInfo() const
{
  std::ostringstream oss;
  FileDrainInfo fdrain;
  eos_info("Get fid=%llu info", mFileId);

  if (gOFS->mQdbCluster.empty()) {
    try {
      eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex);
      std::shared_ptr<eos::IFileMD> fmd = gOFS->eosFileService->getFileMD(mFileId);
      fdrain.mProto.set_layout_id(fmd->getLayoutId());
      fdrain.mProto.set_cont_id(fmd->getContainerId());
      fdrain.mProto.set_uid(fmd->getCUid());
      fdrain.mProto.set_gid(fmd->getCGid());
      fdrain.mProto.set_size(fmd->getSize());
      fdrain.mFullPath = gOFS->eosView->getUri(fmd.get());
      fdrain.mProto.set_checksum(fmd->getChecksum().getDataPtr(),
                                 fmd->getChecksum().getSize());
    } catch (eos::MDException& e) {
      oss << "fxid=" << eos::common::FileId::Fid2Hex(mFileId)
          << " errno=" << e.getErrno()
          << " msg=\"" << e.getMessage().str() << "\"";
      eos_err("%s", oss.str().c_str());
      throw e;
    }
  } else {
    qclient::QClient* qcl = eos::BackendClient::getInstance(gOFS->mQdbCluster,
                            "drain");
    auto tmp = eos::MetadataFetcher::getFileFromId(*qcl, mFileId).get();
    std::swap<eos::ns::FileMdProto>(fdrain.mProto, tmp);
    // Get the full path to the file
    std::string dir_uri;
    {
      eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex);
      dir_uri = gOFS->eosView->getUri(fdrain.mProto.cont_id());
    }

    if (dir_uri.empty()) {
      oss << "msg\"no parent container id=" << fdrain.mProto.cont_id() << "\"";
      make_mdexception(ENOENT, oss.str());
    }

    fdrain.mFullPath = dir_uri + fdrain.mProto.name();
  }

  return fdrain;
}

//------------------------------------------------------------------------------
// Build TPC source url
//------------------------------------------------------------------------------
XrdCl::URL
DrainTransferJob::BuildTpcSrc(const FileDrainInfo& fdrain,
                              const eos::common::FileSystem::fs_snapshot& fs)
{
  using eos::common::LayoutId;
  XrdCl::URL url_src;
  uint64_t lid = fdrain.mProto.layout_id();
  uint64_t target_lid = lid & 0xffffff0f;

  // Mask block checksums only for replica layouts
  if ((LayoutId::GetLayoutType(lid) == LayoutId::kReplica) &&
      (LayoutId::GetBlockChecksum(lid) != LayoutId::kNone)) {
    target_lid &= 0xff0fffff;
  }

  std::ostringstream src_params;
  src_params << "mgm.access=read"
             << "&mgm.lid=" << target_lid
             << "&mgm.cid=" << fdrain.mProto.cont_id()
             << "&mgm.ruid=1&mgm.rgid=1&mgm.uid=1&mgm.gid=1"
             << "&mgm.path=" << fdrain.mFullPath
             << "&mgm.manager=" << gOFS->ManagerId.c_str()
             << "&mgm.fid=" << eos::common::FileId::Fid2Hex(mFileId)
             << "&mgm.sec=" << eos::common::SecEntity::ToKey(0, "eos/draining")
             << "&mgm.drainfsid=" << mFsIdSource
             << "&mgm.localprefix=" << fs.mPath.c_str()
             << "&mgm.fsid=" << fs.mId
             << "&mgm.sourcehostport=" << fs.mHostPort.c_str()
             << "&eos.app=drainer&eos.ruid=0&eos.rgid=0";
  // Build the capability
  int caprc = 0;
  XrdOucEnv* output_cap = 0;
  XrdOucEnv input_cap(src_params.str().c_str());
  eos::common::SymKey* symkey = eos::common::gSymKeyStore.GetCurrentKey();

  if ((caprc = gCapabilityEngine.Create(&input_cap, output_cap,
                                        symkey, gOFS->mCapabilityValidity))) {
    std::string err = "msg=\"unable to create src capability, errno=";
    err += caprc;
    err += "\"";
    ReportError(err);
    return url_src;
  }

  int cap_len = 0;
  std::ostringstream src_cap;
  src_cap << output_cap->Env(cap_len)
          << "&source.url=root://" << fs.mHostPort.c_str()
          << "//replicate:" << eos::common::FileId::Fid2Hex(mFileId);
  // Fill in the url object
  url_src.SetProtocol("root");
  url_src.SetHostName(fs.mHost.c_str());
  url_src.SetPort(stoi(fs.mPort));
  url_src.SetUserName("daemon");
  url_src.SetParams(src_cap.str());
  url_src.SetPath(fdrain.mFullPath);
  delete output_cap;
  return url_src;
}

//------------------------------------------------------------------------------
// Build TPC destination url
//------------------------------------------------------------------------------
XrdCl::URL
DrainTransferJob::BuildTpcDst(const FileDrainInfo& fdrain,
                              const eos::common::FileSystem::fs_snapshot& fs)
{
  using eos::common::LayoutId;
  XrdCl::URL url_dst;
  uint64_t lid = fdrain.mProto.layout_id();
  uint64_t target_lid = lid & 0xffffff0f;

  // Mask block checksums only for replica layouts
  if ((LayoutId::GetLayoutType(lid) == LayoutId::kReplica) &&
      (LayoutId::GetBlockChecksum(lid) != LayoutId::kNone)) {
    target_lid &= 0xff0fffff;
  }

  std::ostringstream dst_params;
  dst_params << "mgm.access=write"
             << "&mgm.lid=" << target_lid
             << "&mgm.source.lid=" << lid
             << "&mgm.source.ruid=" << fdrain.mProto.uid()
             << "&mgm.source.rgid=" << fdrain.mProto.gid()
             << "&mgm.cid=" << fdrain.mProto.cont_id()
             << "&mgm.ruid=1&mgm.rgid=1&mgm.uid=1&mgm.gid=1"
             << "&mgm.path=" << fdrain.mFullPath.c_str()
             << "&mgm.manager=" << gOFS->ManagerId.c_str()
             << "&mgm.fid=" << eos::common::FileId::Fid2Hex(mFileId)
             << "&mgm.sec=" << eos::common::SecEntity::ToKey(0, "eos/draining").c_str()
             << "&mgm.drainfsid=" << mFsIdSource
             << "&mgm.localprefix=" << fs.mPath.c_str()
             << "&mgm.fsid=" << fs.mId
             << "&mgm.sourcehostport=" << fs.mHostPort.c_str()
             << "&mgm.bookingsize=" << fdrain.mProto.size()
             << "&eos.app=drainer&eos.targetsize=" << fdrain.mProto.size();

  if (!fdrain.mProto.checksum().empty()) {
    dst_params << "&eos.checksum=";
    uint32_t xs_len = LayoutId::GetChecksumLen(lid);
    uint32_t data_len = fdrain.mProto.checksum().size();

    for (auto i = 0u; i < data_len; ++i) {
      dst_params << eos::common::StringConversion::char_to_hex(
                   fdrain.mProto.checksum()[i]);
      ++i;
    }

    // Pad with zeros if necessary
    while (data_len < xs_len) {
      ++data_len;
      dst_params << '0';
    }
  }

  // Build the capability
  int caprc = 0;
  XrdOucEnv* output_cap = 0;
  XrdOucEnv input_cap(dst_params.str().c_str());
  eos::common::SymKey* symkey = eos::common::gSymKeyStore.GetCurrentKey();

  if ((caprc = gCapabilityEngine.Create(&input_cap, output_cap,
                                        symkey, gOFS->mCapabilityValidity))) {
    std::string err = "msg=\"unable to create dst capability, errno=";
    err += caprc;
    err += "\"";
    ReportError(err);
    return url_dst;
  }

  int cap_len = 0;
  std::ostringstream dst_cap;
  dst_cap << output_cap->Env(cap_len)
          << "&target.url=root://" << fs.mHostPort.c_str()
          << "//replicate:" << eos::common::FileId::Fid2Hex(mFileId);
  url_dst.SetProtocol("root");
  url_dst.SetHostName(fs.mHost.c_str());
  url_dst.SetPort(stoi(fs.mPort));
  url_dst.SetUserName("daemon");
  url_dst.SetParams(dst_cap.str());
  url_dst.SetPath(fdrain.mFullPath);
  delete output_cap;
  return url_dst;
}

//------------------------------------------------------------------------------
// Select destiantion file system for current transfer
//------------------------------------------------------------------------------
bool
DrainTransferJob::SelectDstFs(const FileDrainInfo& fdrain)
{
  if (mFsIdTarget) {
    return true;
  }

  unsigned int nfilesystems = 1;
  unsigned int ncollocatedfs = 0;
  std::vector<FileSystem::fsid_t> new_repl;
  eos::common::FileSystem::fs_snapshot source_snapshot;
  eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
  eos::common::FileSystem* source_fs = FsView::gFsView.mIdView[mFsIdSource];
  source_fs->SnapShotFileSystem(source_snapshot);
  FsGroup* group = FsView::gFsView.mGroupView[source_snapshot.mGroup];
  // Check other replicas for the file
  std::vector<std::string> fsid_geotags;
  std::vector<FileSystem::fsid_t> existing_repl;

  for (auto elem : fdrain.mProto.locations()) {
    existing_repl.push_back(elem);
  }

  if (!gGeoTreeEngine.getInfosFromFsIds(existing_repl, &fsid_geotags, 0, 0)) {
    eos_err("msg=\"fid=%llu failed to retrieve info for existing replicas\"",
            mFileId);
    return false;
  }

  bool res = gGeoTreeEngine.placeNewReplicasOneGroup(
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
               NULL, // excludeFS
               &fsid_geotags, // excludeGeoTags
               NULL);

  if (!res || new_repl.empty())  {
    eos_err("msg=\"fid=%llu could not place new replica\"", mFileId);
    return false;
  }

  std::ostringstream oss;

  for (auto elem : new_repl) {
    oss << " " << (unsigned long)(elem);
  }

  eos_static_debug("msg=\"drain placement retc=%d with fsids=%s", (int)res,
                   oss.str().c_str());
  // Return only one fs now
  mFsIdTarget = new_repl[0];
  return true;
}

EOSMGMNAMESPACE_END
