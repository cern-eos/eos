//------------------------------------------------------------------------------
//! @file FsckEntry.hh
//! @author Elvin Sindrilaru - CERN
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

#include "mgm/fsck/FsckEntry.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/FsView.hh"
#include "namespace/interface/IView.hh"
#include "namespace/interface/IFileMDSvc.hh"
#include "common/StringConversion.hh"
#include "common/LayoutId.hh"
#include "namespace/Prefetcher.hh"
#include "namespace/ns_quarkdb/persistency/MetadataFetcher.hh"

EOSMGMNAMESPACE_BEGIN

//----------------------------------------------------------------------------
//! Constructor
//----------------------------------------------------------------------------
FsckEntry::FsckEntry(eos::IFileMD::id_t fid,
                     eos::common::FileSystem::fsid_t fsid_err,
                     const std::string& expected_err):
  mFid(fid), mFsidErr(fsid_err),
  mReportedErr(ConvertToFsckErr(expected_err))
{
  mMapRepairOps = {
    {FsckErr::MgmXsDiff, {&FsckEntry::RepairMgmXsDiff}},
    {FsckErr::FstXsDiff, {&FsckEntry::RepairFstXsDiff}}
  };
}


//------------------------------------------------------------------------------
// Collect MGM file metadata information
//------------------------------------------------------------------------------
void
FsckEntry::CollectMgmInfo(qclient::QClient& qcl)
{
  mMgmFmd = eos::MetadataFetcher::getFileFromId(qcl, FileIdentifier(mFid)).get();
}

//------------------------------------------------------------------------------
// Collect FST file metadata information from all replicas
//------------------------------------------------------------------------------
void
FsckEntry::CollectAllFstInfo()
{
  for (const auto fsid : mMgmFmd.locations()) {
    CollectFstInfo(fsid);
  }
}

//------------------------------------------------------------------------------
// Method to repair an mgm checksum difference error
//------------------------------------------------------------------------------
bool
FsckEntry::RepairMgmXsDiff()
{
  using eos::common::StringConversion;
  using eos::common::LayoutId;

  // This only makes sense for replica layouts
  if (LayoutId::IsRain(mMgmFmd.layout_id())) {
    return true;
  }

  std::string mgm_xs_val =
    StringConversion::BinData2HexString(mMgmFmd.checksum().c_str(),
                                        SHA_DIGEST_LENGTH,
                                        LayoutId::GetChecksumLen(mMgmFmd.layout_id()));
  // Make sure the disk xs values match between all the replicas
  std::string xs_val;
  bool mgm_xs_match = false; // one of the disk xs matches the mgm one
  bool disk_xs_match = true; // flag to mark that all disk xs match

  for (auto it = mFstFileInfo.cbegin(); it != mFstFileInfo.cend(); ++it) {
    auto& finfo = it->second;

    if (xs_val.empty()) {
      xs_val = finfo->mFstFmd.diskchecksum();

      if (mgm_xs_val == xs_val) {
        mgm_xs_match = true;
        break;
      }
    } else {
      std::string current_xs_val = finfo->mFstFmd.diskchecksum();

      if (mgm_xs_val == current_xs_val) {
        mgm_xs_match = true;
        break;
      }

      if (xs_val != current_xs_val) {
        // There is a xs diff between two replicas, we can not fix this case
        disk_xs_match = false;
        break;
      }
    }
  }

  if (mgm_xs_match) {
    eos_warning("msg=\"mgm xs repair skip - found replica with matching xs\" "
                " fid=%08llx", mFid);
    return false;
  }

  if (disk_xs_match) {
    try {
      size_t out_sz;
      auto xs_binary = StringConversion::Hex2BinDataChar(xs_val, out_sz);
      eos::Buffer xs_buff;
      xs_buff.putData(xs_binary.get(), SHA_DIGEST_LENGTH);
      // Grab the file metadata object and update it
      eos::Prefetcher::prefetchFileMDAndWait(gOFS->eosView, mFid);
      eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex);
      auto fmd = gOFS->eosFileService->getFileMD(mFid);
      fmd->setChecksum(xs_buff);
      gOFS->eosView->updateFileStore(fmd.get());
    } catch (const eos::MDException& e) {
      eos_err("msg=\"mgm xs repair failed - no such filemd\" fid=%08llx", mFid);
      return false;
    }

    eos_info("msg=\"mgm xs repair successful\" fid=%08llx old_mgm_xs=\"%s\" "
             "new_mgm_xs=\"%s\"", mFid, mgm_xs_val.c_str(), xs_val.c_str());
  } else {
    eos_err("msg=\"mgm xs repair failed - no all disk xs match\" fid=%08llx",
            mFid);
  }

  return disk_xs_match;
}

//----------------------------------------------------------------------------
//! Method to repair an FST checksum difference error
//----------------------------------------------------------------------------
bool
FsckEntry::RepairFstXsDiff()
{
  return true;
}

//------------------------------------------------------------------------------
// Generate repair workflow for the current entry
//------------------------------------------------------------------------------
std::list<RepairFnT>
FsckEntry::GenerateRepairWokflow()
{
  auto it = mMapRepairOps.find(mReportedErr);

  if (it == mMapRepairOps.end()) {
    return {};
  }

  return it->second;
}

//------------------------------------------------------------------------------
// Collect FST file metadata information
//------------------------------------------------------------------------------
void
FsckEntry::CollectFstInfo(eos::common::FileSystem::fsid_t fsid)
{
  using eos::common::FileId;
  std::string host_port;
  std::string fst_local_path;
  {
    eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
    auto it_fs = FsView::gFsView.mIdView.find(fsid);

    if (it_fs != FsView::gFsView.mIdView.end()) {
      host_port = it_fs->second->GetString("hostport");
      fst_local_path = it_fs->second->GetPath();
    }
  }

  if (host_port.empty() || fst_local_path.empty()) {
    eos_err("msg=\"missing or misconfigured file system\" fsid=%lu", fsid);
    mFstFileInfo.emplace(fsid, std::make_unique<FstFileInfoT>("",
                         FstErr::NoContact));
    return;
  }

  std::ostringstream oss;
  oss << "root://" << host_port << "//dummy";
  std::string surl = oss.str();
  XrdCl::URL url(surl);

  if (!url.IsValid()) {
    eos_err("msg=\"invalid url\" url=\"%s\"", surl.c_str());
    mFstFileInfo.emplace(fsid, std::make_unique<FstFileInfoT>("",
                         FstErr::NoContact));
    return;
  }

  XrdOucString fpath_local;
  FileId::FidPrefix2FullPath(FileId::Fid2Hex(mFid).c_str(),
                             fst_local_path.c_str(),
                             fpath_local);
  // Check that the file exists on disk
  XrdCl::StatInfo* stat_info_raw {nullptr};
  std::unique_ptr<XrdCl::StatInfo> stat_info(stat_info_raw);
  uint16_t timeout = 10;
  XrdCl::FileSystem fs(url);
  XrdCl::XRootDStatus status = fs.Stat(fpath_local.c_str(), stat_info_raw,
                                       timeout);

  if (!status.IsOK()) {
    if (status.code == XrdCl::errOperationExpired) {
      mFstFileInfo.emplace(fsid, std::make_unique<FstFileInfoT>("",
                           FstErr::NoContact));
    } else {
      mFstFileInfo.emplace(fsid, std::make_unique<FstFileInfoT>("",
                           FstErr::NotOnDisk));
    }

    return;
  }

  // Collect file metadata stored on the FST about the current file
  auto ret_pair =  mFstFileInfo.emplace(fsid, std::make_unique<FstFileInfoT>
                                        (fpath_local.c_str(), FstErr::None));
  auto& finfo = ret_pair.first->second;
  finfo->mDiskSize = stat_info->GetSize();

  if (!GetFstFmd(finfo, fs, fsid)) {
    return;
  }
}

//------------------------------------------------------------------------------
// Get file metadata info stored at the FST
//------------------------------------------------------------------------------
bool
FsckEntry::GetFstFmd(std::unique_ptr<FstFileInfoT>& finfo,
                     XrdCl::FileSystem& fs,
                     eos::common::FileSystem::fsid_t fsid)
{
  XrdCl::Buffer* raw_response {nullptr};
  std::unique_ptr<XrdCl::Buffer> response(raw_response);
  // Create query command for file metadata
  std::ostringstream oss;
  oss << "/?fst.pcmd=getfmd&fst.getfmd.fsid=" << fsid
      << "&fst.getfmd.fid=" << std::hex << mFid;
  XrdCl::Buffer arg;
  arg.FromString(oss.str().c_str());
  uint16_t timeout = 10;
  XrdCl::XRootDStatus status = fs.Query(XrdCl::QueryCode::OpaqueFile, arg,
                                        raw_response, timeout);

  if (!status.IsOK()) {
    if (status.code == XrdCl::errOperationExpired) {
      eos_err("msg=\"timeout file metadata query\" fsid=%lu", fsid);
      finfo->mFstErr = FstErr::NoContact;
    } else {
      eos_err("msg=\"failed file metadata query\" fsid=%lu", fsid);
      finfo->mFstErr = FstErr::NoFmdInfo;
    }

    return false;
  }

  if ((response == nullptr) ||
      (strncmp(response->GetBuffer(), "ERROR", 5) == 0)) {
    finfo->mFstErr = FstErr::NoFmdInfo;
    return false;
  }

  // Parse in the file metadata info
  XrdOucEnv fmd_env(response->GetBuffer());

  if (!eos::fst::EnvToFstFmd(fmd_env, finfo->mFstFmd)) {
    eos_err("msg=\"failed parsing fmd env\" fsid=%lu", fsid);
    finfo->mFstErr = FstErr::NoFmdInfo;
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Convert string to FsckErr type
//------------------------------------------------------------------------------
FsckErr ConvertToFsckErr(const std::string& serr)
{
  if (serr == "m_cx_diff") {
    return FsckErr::MgmXsDiff;
  } else if (serr == "d_cx_diff") {
    return FsckErr::FstXsDiff;
  } else {
    return FsckErr::None;
  }
}

EOSMGMNAMESPACE_END
