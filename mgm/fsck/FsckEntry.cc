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
#include "mgm/fsck/Fsck.hh"
#include "mgm/ofs/XrdMgmOfs.hh"
#include "mgm/fsview/FsView.hh"
#include "mgm/stat/Stat.hh"
#include "mgm/proc/proc_fs.hh"
#include "namespace/interface/IView.hh"
#include "namespace/interface/IFileMDSvc.hh"
#include "common/StringConversion.hh"
#include "common/LayoutId.hh"
#include "namespace/Prefetcher.hh"
#include "namespace/ns_quarkdb/persistency/MetadataFetcher.hh"

using eos::common::StringConversion;
using eos::common::LayoutId;

EOSMGMNAMESPACE_BEGIN

//----------------------------------------------------------------------------
//! Constructor
//----------------------------------------------------------------------------
FsckEntry::FsckEntry(eos::IFileMD::id_t fid,
                     const std::set<eos::common::FileSystem::fsid_t>& fsid_err,
                     const std::string& expected_err, bool best_effort,
                     std::shared_ptr<qclient::QClient> qcl):
  mFid(fid), mFsidErr(fsid_err),
  mReportedErr(eos::common::ConvertToFsckErr(expected_err)),
  mBestEffort(best_effort), mRepairFactory(), mQcl(qcl)
{
  using namespace eos::common;
  mMapRepairOps = {
    {FsckErr::MgmXsDiff,  &FsckEntry::RepairMgmXsSzDiff},
    {FsckErr::MgmSzDiff,  &FsckEntry::RepairMgmXsSzDiff},
    {FsckErr::FstXsDiff,  &FsckEntry::RepairFstXsSzDiff},
    {FsckErr::FstSzDiff,  &FsckEntry::RepairFstXsSzDiff},
    {FsckErr::BlockxsErr, &FsckEntry::RepairFstXsSzDiff},
    {FsckErr::UnregRepl,  &FsckEntry::RepairInconsistencies},
    {FsckErr::DiffRepl,   &FsckEntry::RepairInconsistencies},
    {FsckErr::MissRepl,   &FsckEntry::RepairInconsistencies},
    {FsckErr::StripeErr,  &FsckEntry::RepairInconsistencies}
  };
  mRepairFactory = [](eos::common::FileId::fileid_t fid,
                      eos::common::FileSystem::fsid_t fsid_src,
                      eos::common::FileSystem::fsid_t fsid_trg,
                      std::set<eos::common::FileSystem::fsid_t> exclude_srcs,
                      std::set<eos::common::FileSystem::fsid_t> exclude_dsts,
                      bool drop_src, const std::string & app_tag,
  bool repair_excluded) {
    return std::make_shared<FsckRepairJob>(
             fid, fsid_src, fsid_trg, exclude_srcs, exclude_dsts, drop_src, app_tag,
             false, eos::common::VirtualIdentity::Root(), repair_excluded);
  };
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
FsckEntry::~FsckEntry()
{
  if (gOFS) {
    gOFS->mFidTracker.RemoveEntry(mFid);
  }
}

//------------------------------------------------------------------------------
// Collect MGM file metadata information
//------------------------------------------------------------------------------
bool
FsckEntry::CollectMgmInfo()
{
  if (mQcl == nullptr) {
    return false;
  }

  try {
    mMgmFmd = eos::MetadataFetcher::getFileFromId(*mQcl.get(),
              FileIdentifier(mFid)).get();
  } catch (const eos::MDException& e) {
    return false;
  }

  if (mMgmFmd.cont_id()) {
    // Double check that the parent exists, if not, this is a detached entry and
    // we need to clean it up and mark the parentId with 0 otherwise the fsck
    // mechanism gets confused.
    try {
      eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex);
      (void) gOFS->eosDirectoryService->getContainerMD(mMgmFmd.cont_id());
    } catch (const eos::MDException& e) {
      mMgmFmd.set_cont_id(0ull);
    }
  }

  return true;
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
// Collect FST file metadata information
//------------------------------------------------------------------------------
void
FsckEntry::CollectFstInfo(eos::common::FileSystem::fsid_t fsid)
{
  using eos::common::FileId;

  if ((fsid == 0ull) || (mFstFileInfo.find(fsid) != mFstFileInfo.end())) {
    return;
  }

  std::string host_port;
  std::string fst_local_path;
  {
    eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
    FileSystem* fs = FsView::gFsView.mIdView.lookupByID(fsid);

    if (fs) {
      host_port = fs->GetString("hostport");
      fst_local_path = fs->GetPath();
    }
  }

  if (host_port.empty() || fst_local_path.empty()) {
    eos_err("msg=\"missing or misconfigured file system\" fsid=%lu", fsid);
    mFstFileInfo.emplace(fsid, std::make_unique<FstFileInfoT>("",
                         FstErr::NotExistFs));
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

  std::string fpath_local = FileId::FidPrefix2FullPath
                            (FileId::Fid2Hex(mFid).c_str(), fst_local_path.c_str());
  // Check that the file exists on disk
  XrdCl::StatInfo* stat_info_raw {nullptr};
  std::unique_ptr<XrdCl::StatInfo> stat_info;
  uint16_t timeout = 10;
  XrdCl::FileSystem fs(url);
  XrdCl::XRootDStatus status = fs.Stat(fpath_local.c_str(), stat_info_raw,
                                       timeout);
  stat_info.reset(stat_info_raw);

  if (!status.IsOK()) {
    eos_err("msg=\"failed stat\" fxid=%08llx fsid=%lu local_path=%s "
            "xrd_code=%u xrd_errno=%u", mFid, fsid, fpath_local.c_str(),
            status.code, status.errNo);

    if (status.code == XrdCl::errOperationExpired) {
      mFstFileInfo.emplace(fsid, std::make_unique<FstFileInfoT>("",
                           FstErr::NoContact));
    } else {
      if (XProtocol::toErrno(status.errNo) == ENOENT) {
        mFstFileInfo.emplace(fsid, std::make_unique<FstFileInfoT>("",
                             FstErr::NotOnDisk));
      } else {
        mFstFileInfo.emplace(fsid, std::make_unique<FstFileInfoT>("",
                             FstErr::NoContact));
      }
    }

    return;
  }

  // Collect file metadata stored on the FST about the current file
  auto ret_pair =  mFstFileInfo.emplace(fsid, std::make_unique<FstFileInfoT>
                                        (fpath_local.c_str(), FstErr::None));
  auto& finfo = ret_pair.first->second;
  finfo->mDiskSize = stat_info->GetSize();
  (void) GetFstFmd(finfo, fs, fsid);
}

//------------------------------------------------------------------------------
// Repair entry in best-effort mode
//------------------------------------------------------------------------------
bool
FsckEntry::RepairBestEffort()
{
  // If not enabled then always fail
  if (!mBestEffort) {
    return false;
  }

  // Best-effort only works for replicas
  if (LayoutId::IsRain(mMgmFmd.layout_id())) {
    return false;
  }

  eos_info("msg=\"attempt best effort repair\" fxid=%08llx", mFid);
  // Find the best replica candidate that should be considered the reference
  eos::common::FileSystem::fsid_t ref_fsid = 0ul;
  uint64_t ref_sz = 0ull;
  std::string ref_xs;
  std::string mgm_xs_val =
    StringConversion::BinData2HexString(mMgmFmd.checksum().c_str(),
                                        SHA256_DIGEST_LENGTH,
                                        LayoutId::GetChecksumLen(mMgmFmd.layout_id()));

  for (auto it = mFstFileInfo.cbegin(); it != mFstFileInfo.cend(); ++it) {
    auto& finfo = it->second;

    if (finfo->mFstErr != FstErr::None) {
      continue;
    }

    if (finfo->mFstFmd.mProtoFmd.diskchecksum().empty()) {
      eos_static_info("msg=\"skip best-effort repair due to un-scanned "
                      "replica\" fxid=%08llx", mFid);
      return false;
    }

    // If there is replica that matches the MGM info then use as reference
    if ((finfo->mDiskSize == mMgmFmd.size()) &&
        (finfo->mFstFmd.mProtoFmd.diskchecksum() == mgm_xs_val)) {
      ref_fsid = it->first;
      ref_sz = finfo->mDiskSize;
      ref_xs = finfo->mFstFmd.mProtoFmd.diskchecksum();
      break;
    }

    // First available replica or the one with more data is the reference
    if ((ref_fsid == 0) || (ref_sz < finfo->mDiskSize)) {
      ref_fsid = it->first;
      ref_sz = finfo->mDiskSize;
      ref_xs = finfo->mFstFmd.mProtoFmd.diskchecksum();
    }
  }

  if (ref_fsid == 0) {
    eos_static_err("msg=\"no suitable replica for best-effort repair found\" "
                   "fxid=%08llx", mFid);
    return false;
  }

  size_t out_sz;
  auto xs_binary = StringConversion::Hex2BinDataChar(ref_xs, out_sz,
                   SHA256_DIGEST_LENGTH);

  if (xs_binary == nullptr) {
    eos_err("msg=\"best-effort repair failed due to disk checksum conversion "
            "error\" fxid=%08llx ref_xs=\"%s\"", mFid, ref_xs.c_str());
    return false;
  }

  eos::Buffer xs_buff;
  xs_buff.putData(xs_binary.get(), SHA256_DIGEST_LENGTH);

  // Issue a verifystripe command towards the reference replica
  if (gOFS) {
    XrdOucErrInfo lerr;
    auto root = eos::common::VirtualIdentity::Root();
    std::string options = "&mgm.verify.compute.checksum=1"
                          "&mgm.verify.commit.checksum=1&mgm.verify.commit.size=1";

    if (!gOFS->_verifystripe(mFid, lerr, root, ref_fsid, options)) {
      eos_err("msg=\"failed verify stripe command\" fxid=%08llx fsid=%lu",
              mFid, ref_fsid);
      return false;
    }

    // Wait until the MGM has received the update from the reference
    // replica but no more than 5 min.
    bool match = false;
    auto now = std::chrono::system_clock::now();
    auto ts_deadline = now + std::chrono::seconds(300);

    while (now <= ts_deadline) {
      try {
        auto fmd = gOFS->eosFileService->getFileMD(mFid);
        auto fmd_lock = eos::MDLocking::readLock(fmd.get());

        if ((fmd->getSize() == ref_sz) &&
            (strncmp(fmd->getChecksum().getDataPtr(),
                     xs_buff.getDataPtr(), xs_buff.getSize()) == 0)) {
          match = true;
          // Update also the MGM fmd object
          mMgmFmd.set_checksum(xs_buff.getDataPtr(), xs_buff.getSize());
          mMgmFmd.set_size(ref_sz);
          break;
        }
      } catch (const eos::MDException& e) {
        eos_debug("msg=\"best-effort repair successful, file removed in the "
                  "meantime\" fxid=%08llx", mFid);
        return true;
      }

      std::this_thread::sleep_for(std::chrono::seconds(10));
      now = std::chrono::system_clock::now();
    }

    if (!match) {
      eos_static_err("msg=\"best-effort repair failed as namespace info does "
                     "not match reference replica within 5min deadline\" "
                     "fxid=%08llx fsid=%lu", mFid, ref_fsid);
      return false;
    }
  } else {
    // For testing we just update the MGM fmd object
    mMgmFmd.set_checksum(xs_buff.getDataPtr(), xs_buff.getSize());
    mMgmFmd.set_size(ref_sz);
  }

  std::set<eos::common::FileSystem::fsid_t> bad_fsids;

  for (auto it = mFstFileInfo.cbegin(); it != mFstFileInfo.cend(); ++it) {
    if (it->first != ref_fsid) {
      bad_fsids.insert(it->first);
    }
  }

  // Attempt repair if we don't have enough good replicas
  size_t num_good_rep = 1;
  size_t num_nominal_rep = LayoutId::GetStripeNumber(mMgmFmd.layout_id()) + 1;
  bool all_repaired = true;

  for (const auto bad_fsid : bad_fsids) {
    if (num_good_rep >= num_nominal_rep) {
      break;
    }

    // Trigger an fsck repair job (much like a drain job) doing a TPC
    auto repair_job = mRepairFactory(mFid, bad_fsid, 0, bad_fsids,
                                     bad_fsids, true, "eos/fsck", false);
    repair_job->DoIt();

    if (repair_job->GetStatus() != FsckRepairJob::Status::OK) {
      eos_err("msg=\"best-effort repair failed\" fxid=%08llx bad_fsid=%lu",
              mFid, bad_fsid);
      all_repaired = false;
    } else {
      eos_info("msg=\"best-effort repair successful\" fxid=%08llx bad_fsid=%lu",
               mFid, bad_fsid);
      ++num_good_rep;
    }
  }

  return all_repaired;
}

//------------------------------------------------------------------------------
// Method to repair an mgm checksum difference error
//------------------------------------------------------------------------------
bool
FsckEntry::RepairMgmXsSzDiff()
{
  // This only makes sense for replica layouts
  if (LayoutId::IsRain(mMgmFmd.layout_id())) {
    return true;
  }

  std::string mgm_xs_val =
    StringConversion::BinData2HexString(mMgmFmd.checksum().c_str(),
                                        SHA256_DIGEST_LENGTH,
                                        LayoutId::GetChecksumLen(mMgmFmd.layout_id()));
  // Make sure the disk xs and size values match between all the replicas
  uint64_t sz_val {0ull};
  std::string xs_val;
  bool mgm_xs_sz_match = false; // one of the disk xs matches the mgm one
  bool disk_xs_sz_match = true; // flag to mark that all disk xs match
  // Mark if all replicas are not on disk - use case of 0-size files
  bool all_not_on_disk = true;

  for (auto it = mFstFileInfo.cbegin(); it != mFstFileInfo.cend(); ++it) {
    auto& finfo = it->second;

    if (finfo->mFstErr != FstErr::NotOnDisk) {
      all_not_on_disk = false;
    }

    if (finfo->mFstErr != FstErr::None) {
      eos_err("msg=\"unavailable replica info\" fxid=%08llx fsid=%lu",
              mFid, it->first);
      disk_xs_sz_match = false;
      continue;
    }

    if (finfo->mFstFmd.mProtoFmd.diskchecksum().empty()) {
      eos_info("msg=\"skip mgm xs/sz diff repair due to un-scanned replica\" "
               "fxid=%08llx", mFid);
      return false;
    }

    if (xs_val.empty() && (sz_val == 0ull)) {
      xs_val = finfo->mFstFmd.mProtoFmd.diskchecksum();
      sz_val = finfo->mFstFmd.mProtoFmd.size();

      if ((mgm_xs_val == xs_val) &&
          (mMgmFmd.size() == sz_val) &&
          (mMgmFmd.size() == finfo->mDiskSize)) {
        mgm_xs_sz_match = true;
        continue;
      }
    } else {
      uint64_t current_sz_val = finfo->mFstFmd.mProtoFmd.size();
      std::string current_xs_val = finfo->mFstFmd.mProtoFmd.diskchecksum();

      if ((mgm_xs_val == current_xs_val) &&
          (mMgmFmd.size() == current_sz_val) &&
          (mMgmFmd.size() == finfo->mDiskSize)) {
        mgm_xs_sz_match = true;
        continue;
      }

      if ((xs_val != current_xs_val) ||
          (sz_val != current_sz_val) ||
          (sz_val != finfo->mDiskSize)) {
        // There is a xs/size diff between two replicas, we can not fix
        disk_xs_sz_match = false;
        continue;
      }
    }
  }

  if (mgm_xs_sz_match) {
    std::set<eos::common::FileSystem::fsid_t> good_fsids;
    std::set<eos::common::FileSystem::fsid_t> bad_fsids;

    for (auto it = mFstFileInfo.cbegin(); it != mFstFileInfo.cend(); ++it) {
      auto& finfo = it->second;

      if ((mMgmFmd.size() != finfo->mFstFmd.mProtoFmd.size()) ||
          (mMgmFmd.size() != finfo->mDiskSize) ||
          (mgm_xs_val != finfo->mFstFmd.mProtoFmd.diskchecksum())) {
        if ((mMgmFmd.size() != finfo->mDiskSize) ||
            (mgm_xs_val != finfo->mFstFmd.mProtoFmd.diskchecksum())) {
          bad_fsids.insert(it->first);
        } else {
          // Trigger a resync of the FST info as it looks to be out of sync
          ResyncFstMd(false);
          return true;
        }
      } else {
        good_fsids.insert(it->first);
      }
    }

    if (good_fsids.empty()) {
      eos_err("msg=\"mgm xs/size repair failed, no correct replicas\" "
              "fxid=%08llx", mFid);
      return RepairBestEffort();
    }

    for (const auto bad_fsid : bad_fsids) {
      gOFS->DropReplica(mFid, bad_fsid);
    }

    bool all_repaired = true;
    // Attempt repair only if we don't have enough good replicas
    size_t num_nominal_rep = LayoutId::GetStripeNumber(mMgmFmd.layout_id()) + 1;

    if (good_fsids.size() < num_nominal_rep) {
      for (auto bad_fsid : bad_fsids) {
        // Trigger an fsck repair job (much like a drain job) doing a TPC
        auto repair_job = mRepairFactory(mFid, bad_fsid, 0, bad_fsids,
                                         bad_fsids, true, "fsck", false);
        repair_job->DoIt();

        if (repair_job->GetStatus() != FsckRepairJob::Status::OK) {
          eos_err("msg=\"mgm xs/size repair failed\" fxid=%08llx bad_fsid=%lu",
                  mFid, bad_fsid);
          all_repaired = false;
        } else {
          eos_info("msg=\"mgm xs/size repair replica successful\" "
                   "fxid=%08llx bad_fsid=%lu",  mFid, bad_fsid);
        }
      }
    }

    if (all_repaired) {
      eos_info("msg=\"mgm xs/size repair successful\" fxid=%08llx", mFid);
    } else {
      eos_warning("msg=\"mgm xs/size repair failed\" fxid=%08llx", mFid);
    }

    return all_repaired;
  }

  if (disk_xs_sz_match && sz_val) {
    size_t out_sz;
    auto xs_binary = StringConversion::Hex2BinDataChar(xs_val, out_sz,
                     SHA256_DIGEST_LENGTH);

    if (xs_binary == nullptr) {
      eos_err("msg=\"mgm xs/size repair failed due to disk checksum conversion "
              "error\" fxid=%08llx disk_xs=\"%s\"", mFid, xs_val.c_str());
      return false;
    }

    eos::Buffer xs_buff;
    xs_buff.putData(xs_binary.get(), SHA256_DIGEST_LENGTH);

    if (gOFS) {
      try {
        eos::Prefetcher::prefetchFileMDAndWait(gOFS->eosView, mFid);
        // Grab the file metadata object and update it
        eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex);
        auto fmd = gOFS->eosFileService->getFileMD(mFid);
        fmd->setChecksum(xs_buff);
        fmd->setSize(sz_val);
        gOFS->eosView->updateFileStore(fmd.get());
        // Update also the MGM fmd object
        mMgmFmd.set_checksum(xs_buff.getDataPtr(), xs_buff.getSize());
        mMgmFmd.set_size(sz_val);
      } catch (const eos::MDException& e) {
        eos_err("msg=\"mgm xs/size repair successful, file removed in the "
                "meantime\" fxid=%08llx", mFid);
        return true;
      }
    } else {
      // For testing we just update the MGM fmd object
      mMgmFmd.set_checksum(xs_buff.getDataPtr(), xs_buff.getSize());
      mMgmFmd.set_size(sz_val);
    }

    eos_info("msg=\"mgm xs/size repair successful\" fxid=%08llx old_mgm_xs=\"%s\" "
             "new_mgm_xs=\"%s\"", mFid, mgm_xs_val.c_str(), xs_val.c_str());
  } else {
    // Handle 0-size files with no replicas on disk which is legitimate
    if ((mMgmFmd.size() == 0) && all_not_on_disk) {
      eos_info("msg=\"repair successful for 0-size file with no replicas "
               "on disk\" fxid=%08llx", mFid);
      return true;
    }

    eos_err("msg=\"mgm xs/size repair failed, not all disk xs/size match\" "
            "fxid=%08llx", mFid);
    return RepairBestEffort();
  }

  return disk_xs_sz_match;
}

//----------------------------------------------------------------------------
// Method to repair an FST checksum and/or size difference error
//----------------------------------------------------------------------------
bool
FsckEntry::RepairFstXsSzDiff()
{
  std::set<eos::common::FileSystem::fsid_t> bad_fsids;
  std::set<eos::common::FileSystem::fsid_t> good_fsids;

  if (LayoutId::IsRain(mMgmFmd.layout_id())) {
    bad_fsids.insert(*mFsidErr.begin());
  } else { // for replica layouts
    std::string mgm_xs_val =
      StringConversion::BinData2HexString(mMgmFmd.checksum().c_str(),
                                          SHA256_DIGEST_LENGTH,
                                          LayoutId::GetChecksumLen(mMgmFmd.layout_id()));
    // Make sure at least one disk xs and size match the MGM ones
    uint64_t sz_val {0ull};
    std::string xs_val;

    for (auto it = mFstFileInfo.cbegin(); it != mFstFileInfo.cend(); ++it) {
      auto& finfo = it->second;

      if (finfo->mFstErr != FstErr::None) {
        eos_err("msg=\"unavailable replica info\" fxid=%08llx fsid=%lu",
                mFid, it->first);
        bad_fsids.insert(it->first);
        continue;
      }

      xs_val = finfo->mFstFmd.mProtoFmd.diskchecksum();
      sz_val = finfo->mFstFmd.mProtoFmd.disksize();
      eos_static_debug("mgm_sz=%llu mgm_xs=%s fst_sz_sz=%llu fst_sz_disk=%llu, "
                       "fst_xs=%s", mMgmFmd.size(), mgm_xs_val.c_str(),
                       finfo->mFstFmd.mProtoFmd.size(),
                       finfo->mFstFmd.mProtoFmd.disksize(),
                       finfo->mFstFmd.mProtoFmd.checksum().c_str());

      // The disksize/xs must also match the original reference size/xs
      if ((mgm_xs_val == xs_val) && (mMgmFmd.size() == sz_val) &&
          (finfo->mFstFmd.mProtoFmd.size() == sz_val) &&
          (finfo->mFstFmd.mProtoFmd.checksum() == xs_val)) {
        good_fsids.insert(finfo->mFstFmd.mProtoFmd.fsid());
      } else {
        // It could be that the diskchecksum for the replica was not yet
        // computed - this does not mean the replica is bad
        if (!finfo->mFstFmd.mProtoFmd.diskchecksum().empty()) {
          bad_fsids.insert(finfo->mFstFmd.mProtoFmd.fsid());
        }
      }
    }

    if (bad_fsids.empty()) {
      eos_warning("msg=\"fst xs/size repair skip - no bad replicas\" fxid=%08llx",
                  mFid);
      return true;
    }

    if (good_fsids.empty()) {
      eos_err("msg=\"fst xs/size repair failed - no good replicas\" fxid=%08llx",
              mFid);
      return RepairBestEffort();
    }
  }

  // Have more good stripes then layout requirements
  size_t num_nominal_rep = LayoutId::GetStripeNumber(mMgmFmd.layout_id()) + 1;

  if (good_fsids.size() >= num_nominal_rep) {
    if (LayoutId::IsRain(mMgmFmd.layout_id()) &&
        (good_fsids.size() > num_nominal_rep)) {
      eos_crit("msg=\"more stripes than RAIN layout\" fxid=%08llx", mFid);
      return false;
    }

    while (good_fsids.size() > num_nominal_rep) {
      bad_fsids.insert(*good_fsids.begin());
      good_fsids.erase(good_fsids.begin());
    }

    for (auto bad_fsid : bad_fsids) {
      // If we have enough stripes - just drop it
      gOFS->DropReplica(mFid, bad_fsid);
    }

    bad_fsids.clear();
  }

  bool all_repaired {true};

  for (auto bad_fsid : bad_fsids) {
    // Trigger an fsck repair job (much like a drain job) doing a TPC
    auto repair_job = mRepairFactory(mFid, bad_fsid, 0, bad_fsids,
                                     bad_fsids, true, "fsck", false);
    repair_job->DoIt();

    if (repair_job->GetStatus() != FsckRepairJob::Status::OK) {
      eos_err("msg=\"fst xs/size repair failed\" fxid=%08llx bad_fsid=%lu",
              mFid, bad_fsid);
      all_repaired = false;
    } else {
      eos_info("msg=\"fst xs/size repair successful\" fxid=%08llx bad_fsid=%lu",
               mFid, bad_fsid);
    }

    if (LayoutId::IsRain(mMgmFmd.layout_id())) {
      break;
    }
  }

  // Trigger an MGM resync on all the replicas so that the locations get
  // updated properly
  ResyncFstMd(true);
  return all_repaired;
}

//------------------------------------------------------------------------------
// Method to repair file inconsistencies
//------------------------------------------------------------------------------
bool
FsckEntry::RepairInconsistencies()
{
  if (LayoutId::IsRain(mMgmFmd.layout_id())) {
    return RepairRainInconsistencies();
  } else {
    return RepairReplicaInconsistencies();
  }
}

//------------------------------------------------------------------------------
// Method to repair RAIN file inconsistencies
//------------------------------------------------------------------------------
bool
FsckEntry::RepairRainInconsistencies()
{
  using namespace eos::common;

  if (mReportedErr == FsckErr::UnregRepl) {
    if (static_cast<unsigned long>(mMgmFmd.locations_size()) >=
        LayoutId::GetStripeNumber(mMgmFmd.layout_id()) + 1) {
      // If we have enough stripes and current error refers to a stripe which
      // is not in the list of locations then drop it
      bool found = false;

      for (const auto loc : mMgmFmd.locations()) {
        if (*mFsidErr.begin() == loc) {
          found = true;
          break;
        }
      }

      if (!found) {
        gOFS->DropReplica(mFid, *mFsidErr.begin());
      }

      return true;
    } else {
      // If not enough stripes then register it and trigger a check
      if (gOFS) {
        try {
          // Grab the file metadata object and update it
          eos::Prefetcher::prefetchFileMDAndWait(gOFS->eosView, mFid);
          eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex);
          auto fmd = gOFS->eosFileService->getFileMD(mFid);
          fmd->addLocation(*mFsidErr.begin());
          gOFS->eosView->updateFileStore(fmd.get());
        } catch (const eos::MDException& e) {
          eos_err("msg=\"unregistered repair successful, file removed "
                  "in the meantime\" fxid=%08llx", mFid);
          return true;
        }
      } else {
        // For testing just update the MGM fmd object
        mMgmFmd.mutable_locations()->Add(*mFsidErr.begin());
      }
    }
  }

  if (mMgmFmd.locations().empty()) {
    eos_err("msg=\"failed repair, no location available\" fxid=%08llx", mFid);
    return false;
  }

  // Trigger a fsck repair job to make sure all the remaining stripes are
  // recovered and new ones are created if need be. By default pick the
  // first stripe as "source" unless we have a better candidate
  bool drop_src_fsid = false;
  bool repair_excluded = false;
  eos::common::FileSystem::fsid_t src_fsid = mMgmFmd.locations(0);
  std::set<eos::common::FileSystem::fsid_t> bad_fsids;

  if (mReportedErr == FsckErr::MissRepl) {
    src_fsid = *mFsidErr.begin();
    drop_src_fsid = true;
    bool found = false;

    for (const auto loc : mMgmFmd.locations()) {
      if (src_fsid == loc) {
        found = true;
      }
    }

    // If reported missing stripe is not among the registred stripes and we
    // already have the nominal number of stripes then we consider this fixed
    if (!found) {
      size_t num_nominal_rep = LayoutId::GetStripeNumber(mMgmFmd.layout_id()) + 1;

      if (num_nominal_rep == mMgmFmd.locations().size()) {
        eos_info("msg=\"missing stripe repair successful\" fxid=%08llx "
                 "src_fsid=%lu", mFid, src_fsid);
        return true;
      }
    }
  } else if (mReportedErr == FsckErr::DiffRepl) {
    // For rep_diff_n errors the source file systems is not to be dropped
    // or skipped during the scheduling process as it's a valid stripe
    // useful when doing the transfer.
    src_fsid = 0;

    // Over-replication should never happend for RAIN files
    if (static_cast<unsigned long>(mMgmFmd.locations_size()) >
        LayoutId::GetStripeNumber(mMgmFmd.layout_id()) + 1) {
      eos_err("msg=\"RAIN file over-replicated, to be handled manually\" "
              "fxid=%08llu fsid_err=%lu", mFid, *mFsidErr.begin());
      return false;
    } else if (static_cast<unsigned long>(mMgmFmd.locations_size()) ==
               LayoutId::GetStripeNumber(mMgmFmd.layout_id()) + 1) {
      eos_info("msg=\"stripe inconsistency repair successful\" fxid=%08llx "
               "src_fsid=%lu", mFid, src_fsid);
      return true;
    }
  } else if (mReportedErr == FsckErr::StripeErr) {
    // File has too many corrupted stripes, we can't recover
    if (mFsidErr.find(0) != mFsidErr.end()) {
      eos_err("msg=\"RAIN file has too many corrupted stripes, unable to "
              "reconstruct\" fxid=%08llu", mFid);
      return false;
    }

    bad_fsids = mFsidErr;

    // If there is over replication, drop replicas until we have the right
    // number of stripes
    while ((mMgmFmd.locations_size() >
            LayoutId::GetStripeNumber(mMgmFmd.layout_id()) + 1) &&
           !bad_fsids.empty()) {
      const FileSystem::fsid_t drop_fsid = *bad_fsids.begin();
      bad_fsids.erase(drop_fsid);
      eos_info("msg=\"drop over-replicated stripe\" fxid=%08llx fsid=%lu",
               mFid, drop_fsid);
      (void)gOFS->DropReplica(mFid, drop_fsid);
      mFstFileInfo.erase(drop_fsid);
      auto* mutable_loc = mMgmFmd.mutable_locations();

      for (auto it = mutable_loc->begin(); it != mutable_loc->end(); ++it) {
        if (*it == drop_fsid) {
          mutable_loc->erase(it);
          break;
        }
      }
    }

    // If there is the nominal number of stripes and the bad fsids are not
    // among the attached fsids then these can be dropped
    if ((mMgmFmd.locations_size() ==
         LayoutId::GetStripeNumber(mMgmFmd.layout_id()) + 1) &&
        !bad_fsids.empty()) {
      std::set<eos::common::FileSystem::fsid_t> to_del;

      for (const auto& bfsid : bad_fsids) {
        bool found = false;

        for (const auto& loc : mMgmFmd.locations()) {
          if (bfsid == loc) {
            found = true;
            break;
          }
        }

        // We can drop this stripe which is not in the list of locations
        if (!found) {
          eos_info("msg=\"drop bad unregistered stripe\" fxid=%08llx fsid=%lu",
                   mFid, bfsid);
          to_del.insert(bfsid);
          (void)gOFS->DropReplica(mFid, bfsid);
          mFstFileInfo.erase(bfsid);
        }
      }

      // Remove all the stripes that have been dropped
      for (const auto& d_fsid : to_del) {
        bad_fsids.erase(d_fsid);
      }
    }

    if (bad_fsids.empty()) {
      ResyncFstMd(true);
      eos_info("msg=\"stripe inconsistency repair successful\" fxid=%08llx",
               mFid);
      return true;
    }

    src_fsid = *bad_fsids.begin();
    repair_excluded = true;
  }

  auto repair_job = mRepairFactory(mFid, src_fsid, 0, bad_fsids, bad_fsids,
                                   drop_src_fsid, "fsck", repair_excluded);
  repair_job->DoIt();

  if (repair_job->GetStatus() != FsckRepairJob::Status::OK) {
    eos_err("msg=\"stripe inconsistency repair failed\" fxid=%08llx "
            "src_fsid=%lu", mFid, src_fsid);
    return false;
  } else {
    eos_info("msg=\"stripe inconsistency repair successful\" fxid=%08llx "
             "src_fsid=%lu", mFid, src_fsid);
    return true;
  }
}

//------------------------------------------------------------------------------
// Method to repair replica file inconsistencies
//------------------------------------------------------------------------------
bool
FsckEntry::RepairReplicaInconsistencies()
{
  std::string mgm_xs_val =
    StringConversion::BinData2HexString(mMgmFmd.checksum().c_str(),
                                        SHA256_DIGEST_LENGTH,
                                        LayoutId::GetChecksumLen(mMgmFmd.layout_id()));
  std::set<eos::common::FileSystem::fsid_t> to_drop;
  std::set<eos::common::FileSystem::fsid_t> unreg_fsids;
  std::set<eos::common::FileSystem::fsid_t> repmiss_fsids;

  // Account for missing replicas from MGM's perspective
  for (const auto& fsid : mMgmFmd.locations()) {
    eos_info("fxid=%08llx fsid=%lu", mFid, fsid);
    auto it = mFstFileInfo.find(fsid);

    if ((it == mFstFileInfo.end()) ||
        (it->second->mFstErr == FstErr::NotOnDisk)) {
      eos_info("msg=\"mark as missing\" fxid=%08llx fsid=%lu", mFid, fsid);
      repmiss_fsids.insert(fsid);
    }
  }

  // Account for unregisterd replicas and other replicas to be dropped
  for (const auto& elem : mFstFileInfo) {
    bool found = false;

    for (const auto& loc : mMgmFmd.locations()) {
      if (elem.first == loc) {
        found = true;
        break;
      }
    }

    auto& finfo = elem.second;

    if (found) {
      if ((finfo->mFstErr == FstErr::NotOnDisk) ||
          (finfo->mFstErr == FstErr::NotExistFs)) {
        to_drop.insert(elem.first);
      }
    } else {
      // The file system id does not exist
      if (finfo->mFstErr == FstErr::NotExistFs) {
        to_drop.insert(elem.first);
      } else {
        // Make sure the FST size/xs match the MGM ones
        if ((finfo->mFstFmd.mProtoFmd.disksize() != mMgmFmd.size()) ||
            (finfo->mFstFmd.mProtoFmd.diskchecksum() != mgm_xs_val)) {
          to_drop.insert(elem.first);
        } else {
          unreg_fsids.insert(elem.first);
        }
      }
    }
  }

  // First drop any missing replicas from the MGM
  for (const auto& drop_fsid : repmiss_fsids) {
    // Update the local MGM fmd object
    auto mutable_loc = mMgmFmd.mutable_locations();

    for (auto it = mutable_loc->begin(); it != mutable_loc->end(); ++it) {
      if (*it == drop_fsid) {
        mutable_loc->erase(it);
        break;
      }
    }

    if (gOFS) {
      try { // Update the MGM file md object
        eos::Prefetcher::prefetchFileMDWithParentsAndWait(gOFS->eosView, mFid);
        eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex);
        auto fmd = gOFS->eosFileService->getFileMD(mFid);
        fmd->unlinkLocation(drop_fsid);
        fmd->removeLocation(drop_fsid);
        gOFS->eosView->updateFileStore(fmd.get());
        eos_info("msg=\"remove missing replica\" fxid=%08llx drop_fsid=%lu",
                 mFid, drop_fsid);
      } catch (const eos::MDException& e) {
        eos_err("msg=\"replica inconsistency repair successful, file removed "
                "in the meantime\" fxid=%08llx", mFid);
        return true;
      }
    }
  }

  // Then drop any other inconsistent replicas from both the MGM and the FST
  for (auto fsid : to_drop) {
    (void) gOFS->DropReplica(mFid, fsid);
    // Drop also from the local map of FST fmd info
    mFstFileInfo.erase(fsid);
    auto mutable_loc = mMgmFmd.mutable_locations();

    for (auto it = mutable_loc->begin(); it != mutable_loc->end(); ++it) {
      if (*it == fsid) {
        mutable_loc->erase(it);
        break;
      }
    }
  }

  to_drop.clear();
  bool to_delete = (mMgmFmd.cont_id() == 0ull);

  if (to_delete) {
    XrdOucErrInfo err;
    eos::common::VirtualIdentity vid = eos::common::VirtualIdentity::Root();
    XrdOucEnv env(SSTR("mgm.fid=" << eos::common::FileId::Fid2Hex(mFid)
                       << "&mgm.fsid=" << 0
                       << "&mgm.dropall=1").c_str());
    gOFS->Drop("", nullptr, env, err, vid, nullptr);
    eos_info("msg=\"deleted detached file md\" fxid=%08llx", mFid);
    return true;
  }

  // Decide if we need to attach or discard any replicas
  uint32_t num_expected_rep = LayoutId::GetStripeNumber(mMgmFmd.layout_id()) + 1;
  uint32_t num_actual_rep = mMgmFmd.locations().size();

  if (num_actual_rep >= num_expected_rep) { // over-replicated
    int over_replicated = num_actual_rep - num_expected_rep;
    // All the unregistered replicas can be dropped
    to_drop.insert(unreg_fsids.begin(), unreg_fsids.end());

    while ((over_replicated > 0) && !mMgmFmd.locations().empty()) {
      to_drop.insert(mMgmFmd.locations(0));
      mMgmFmd.mutable_locations()->erase(mMgmFmd.locations().begin());
      --over_replicated;
    }
  } else {
    if (num_actual_rep < num_expected_rep) { // under-replicated
      // While under-replicated and we still have unregistered replicas then
      // attach them
      while ((num_actual_rep < num_expected_rep) && !unreg_fsids.empty()) {
        eos::common::FileSystem::fsid_t new_fsid = *unreg_fsids.begin();
        unreg_fsids.erase(unreg_fsids.begin());
        mMgmFmd.add_locations(new_fsid);

        if (gOFS) {
          try {
            eos::Prefetcher::prefetchFileMDWithParentsAndWait(gOFS->eosView, mFid);
            eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex);
            auto fmd = gOFS->eosFileService->getFileMD(mFid);
            fmd->addLocation(new_fsid);
            gOFS->eosView->updateFileStore(fmd.get());
            eos_info("msg=\"attached unregistered replica\" fxid=%08llx "
                     "new_fsid=%lu", mFid, new_fsid);
          } catch (const eos::MDException& e) {
            eos_err("msg=\"unregistered replica repair successful, file "
                    " removed in the meantime\" fxid=%08llx", mFid);
            return true;
          }
        }

        ++num_actual_rep;
      }

      // Drop any remaining unregistered replicas
      to_drop.insert(unreg_fsids.begin(), unreg_fsids.end());

      // If still under-replicated then start creating new replicas
      while ((num_actual_rep < num_expected_rep) && mMgmFmd.locations_size()) {
        // Trigger a fsck repair job but without dropping the source, this is
        // similar to adjust replica
        eos::common::FileSystem::fsid_t good_fsid = mMgmFmd.locations(0);
        auto repair_job = mRepairFactory(mFid, good_fsid, 0, {}, to_drop,
                                         false, "fsck", false);
        repair_job->DoIt();

        if (repair_job->GetStatus() != FsckRepairJob::Status::OK) {
          eos_err("msg=\"replica inconsistency repair failed\" fxid=%08llx "
                  "src_fsid=%lu", mFid, good_fsid);
          return RepairBestEffort();
        } else {
          eos_info("msg=\"replica inconsistency repair successful\" fxid=%08llx "
                   "src_fsid=%lu", mFid, good_fsid);
        }

        ++num_actual_rep;
      }

      if ((num_actual_rep < num_expected_rep) && mMgmFmd.size()) {
        eos_err("msg=\"replica inconsistency repair failed\" fxid=%08llx", mFid);
        return false;
      }
    }
  }

  // Discard unregistered/bad replicas
  for (auto fsid : to_drop) {
    eos_info("msg=\"droping replica\" fxid=%08llx fsid=%lu", mFid, fsid);
    (void) gOFS->DropReplica(mFid, fsid);
    // Drop also from the local map of FST fmd info
    mFstFileInfo.erase(fsid);
  }

  ResyncFstMd(true);
  eos_info("msg=\"file replicas consistent\" fxid=%08llx", mFid);
  return true;
}

//----------------------------------------------------------------------------
// Resync local FST metadata with the MGM info. The refresh flag needs to
// be set whenever there is an FsckRepairJob done before.
//----------------------------------------------------------------------------
void
FsckEntry::ResyncFstMd(bool refresh_mgm_md)
{
  if (refresh_mgm_md) {
    CollectMgmInfo();
  }

  for (const auto& fsid : mMgmFmd.locations()) {
    if (gOFS) {
      (void) gOFS->QueryResync(mFid, fsid);
    }
  }
}

//------------------------------------------------------------------------------
// Repair entry
//------------------------------------------------------------------------------
bool
FsckEntry::Repair()
{
  using namespace eos::common;
  bool success = false;

  // If no MGM object then we are in testing mode
  if (gOFS) {
    gOFS->MgmStats.Add("FsckRepairStarted", 0, 0, 1);

    if (CollectMgmInfo() == false) {
      eos_err("msg=\"no repair action, file is orphan\" fxid=%08llx fsid=%lu "
              "err=%s", mFid, *mFsidErr.begin(), FsckErrToString(mReportedErr).c_str());
      success = true;
      NotifyOutcome(success);
      (void) gOFS->DropReplica(mFid, *mFsidErr.begin());
      // This could be a ghost fid entry still present in the file system map
      // and we need to also drop it from there
      std::string out, err;
      auto root_vid = eos::common::VirtualIdentity::Root();
      (void) proc_fs_dropghosts(*mFsidErr.begin(), {mFid}, root_vid, out, err);
      return success;
    }

    if (mMgmFmd.cont_id() == 0ull) {
      eos_info("msg=\"force remove detached file\" fxid=%08llx", mFid);
      std::string err_msg;

      if (!gOFS->RemoveDetached(mFid, false, true, err_msg)) {
        eos_err("msg=\"operation failed due to: %s\"", err_msg.c_str());
      }

      NotifyOutcome(true);
      return true;
    }

    CollectAllFstInfo();
    CollectFstInfo(*mFsidErr.begin());
  }

  if (mReportedErr != FsckErr::None) {
    auto it = mMapRepairOps.find(mReportedErr);

    if (it == mMapRepairOps.end()) {
      eos_err("msg=\"unknown type of error\" errr=%i", mReportedErr);
      NotifyOutcome(success);
      return success;
    }

    eos_static_info("msg=\"fsck repair\" fxid=%08llx err_type=%i fsid_err=%lu",
                    mFid, mReportedErr, *mFsidErr.begin());
    auto fn_with_obj = std::bind(it->second, this);
    success = fn_with_obj();
    NotifyOutcome(success);
    return success;
  }

  // If no explicit error given then try to repair all types of errors, we put
  // the ones with higher priority first
  std::list<RepairFnT> repair_ops {
    &FsckEntry::RepairMgmXsSzDiff,
    &FsckEntry::RepairFstXsSzDiff,
    &FsckEntry::RepairInconsistencies};

  for (const auto& op : repair_ops) {
    auto fn_with_obj = std::bind(op, this);

    if (!fn_with_obj()) {
      NotifyOutcome(success);
      return success;
    }
  }

  success = true;
  NotifyOutcome(success);
  return success;
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
  // Create query command for file metadata
  std::ostringstream oss;
  oss << "/?fst.pcmd=getfmd&fst.getfmd.fsid=" << fsid
      << "&fst.getfmd.fid=" << std::hex << mFid;
  XrdCl::Buffer arg;
  arg.FromString(oss.str().c_str());
  uint16_t timeout = 10;
  XrdCl::XRootDStatus status = fs.Query(XrdCl::QueryCode::OpaqueFile, arg,
                                        raw_response, timeout);
  std::unique_ptr<XrdCl::Buffer> response(raw_response);

  if (!status.IsOK()) {
    if (status.code == XrdCl::errOperationExpired) {
      eos_err("msg=\"timeout file metadata query\" fxid=%08llx fsid=%lu",
              mFid, fsid);
      finfo->mFstErr = FstErr::NoContact;
    } else {
      eos_err("msg=\"failed file metadata query\" fxid=08llx fsid=%lu",
              mFid, fsid);
      finfo->mFstErr = FstErr::NoFmdInfo;
    }

    return false;
  }

  if ((response == nullptr) ||
      (strncmp(response->GetBuffer(), "ERROR", 5) == 0)) {
    eos_err("msg=\"no local fst metadata present\" fxid=%08llx fsid=%lu",
            mFid, fsid);
    finfo->mFstErr = FstErr::NoFmdInfo;
    return false;
  }

  // Parse in the file metadata info
  XrdOucEnv fmd_env(response->GetBuffer());

  if (!eos::common::EnvToFstFmd(fmd_env, finfo->mFstFmd)) {
    eos_err("msg=\"failed parsing fmd env\" fsid=%lu", fsid);
    finfo->mFstErr = FstErr::NoFmdInfo;
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Update MGM stats and backend depending on the final outcome
//------------------------------------------------------------------------------
void
FsckEntry::NotifyOutcome(bool success) const
{
  if (gOFS) {
    // Update the MGM statistics and QDB backend in case of success
    if (success) {
      gOFS->MgmStats.Add("FsckRepairSuccessful", 0, 0, 1);
      const std::string sfsck_err = eos::common::FsckErrToString(mReportedErr);

      if (mReportedErr == eos::common::FsckErr::StripeErr) {
        for (auto fsid : mFsidErr) {
          gOFS->mFsckEngine->NotifyFixedErr(mFid, fsid, sfsck_err);
        }
      } else {
        // If error is not stripe error, only the first fsid has been fixed
        gOFS->mFsckEngine->NotifyFixedErr(mFid, *mFsidErr.begin(), sfsck_err);
      }

      // Such errors are reported by all the attached locations so when they
      // are fixed we need to update the fsck info for all of them
      if (mReportedErr == eos::common::FsckErr::DiffRepl) {
        for (const auto& loc : mMgmFmd.locations()) {
          gOFS->mFsckEngine->NotifyFixedErr(mFid, loc, sfsck_err);
        }
      }
    } else {
      gOFS->MgmStats.Add("FsckRepairFailed", 0, 0, 1);
    }
  }
}

EOSMGMNAMESPACE_END
