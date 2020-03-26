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
#include "mgm/Stat.hh"
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
                     eos::common::FileSystem::fsid_t fsid_err,
                     const std::string& expected_err,
                     std::shared_ptr<qclient::QClient> qcl):
  mFid(fid), mFsidErr(fsid_err),
  mReportedErr(ConvertToFsckErr(expected_err)), mRepairFactory(),
  mQcl(qcl)
{
  mMapRepairOps = {
    {FsckErr::MgmXsDiff,  &FsckEntry::RepairMgmXsSzDiff},
    {FsckErr::MgmSzDiff,  &FsckEntry::RepairMgmXsSzDiff},
    {FsckErr::FstXsDiff,  &FsckEntry::RepairFstXsSzDiff},
    {FsckErr::FstSzDiff,  &FsckEntry::RepairFstXsSzDiff},
    {FsckErr::BlockxsErr, &FsckEntry::RepairFstXsSzDiff},
    {FsckErr::UnregRepl,  &FsckEntry::RepairReplicaInconsistencies},
    {FsckErr::DiffRepl,   &FsckEntry::RepairReplicaInconsistencies},
    {FsckErr::MissRepl,   &FsckEntry::RepairReplicaInconsistencies}
  };
  mRepairFactory = [](eos::common::FileId::fileid_t fid,
                      eos::common::FileSystem::fsid_t fsid_src,
                      eos::common::FileSystem::fsid_t fsid_trg ,
                      std::set<eos::common::FileSystem::fsid_t> exclude_srcs,
                      std::set<eos::common::FileSystem::fsid_t> exclude_dsts,
  bool drop_src, const std::string & app_tag) {
    return std::make_shared<FsckRepairJob>(fid, fsid_src, fsid_trg,
                                           exclude_srcs, exclude_dsts,
                                           drop_src, app_tag);
  };
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
    } else {
      return;
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
    eos_err("msg=\"failed stat\" fxid=%08llx fsid=%lu local_path=%s", mFid,
            fsid, fpath_local.c_str());

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
  (void) GetFstFmd(finfo, fs, fsid);
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
                                        SHA_DIGEST_LENGTH,
                                        LayoutId::GetChecksumLen(mMgmFmd.layout_id()));
  // Make sure the disk xs and size values match between all the replicas
  uint64_t sz_val {0ull};
  std::string xs_val;
  bool mgm_xs_sz_match = false; // one of the disk xs matches the mgm one
  bool disk_xs_sz_match = true; // flag to mark that all disk xs match

  for (auto it = mFstFileInfo.cbegin(); it != mFstFileInfo.cend(); ++it) {
    auto& finfo = it->second;

    if (finfo->mFstErr != FstErr::None) {
      eos_err("msg=\"unavailable replica info\" fxid=%08llx fsid=%lu",
              mFid, it->first);
      disk_xs_sz_match = false;
      break;
    }

    if (xs_val.empty() && (sz_val == 0ull)) {
      xs_val = finfo->mFstFmd.mProtoFmd.diskchecksum();
      sz_val = finfo->mFstFmd.mProtoFmd.size();

      if ((mgm_xs_val == xs_val) && (mMgmFmd.size() == sz_val)) {
        mgm_xs_sz_match = true;
        break;
      }
    } else {
      uint64_t current_sz_val = finfo->mFstFmd.mProtoFmd.size();
      std::string current_xs_val = finfo->mFstFmd.mProtoFmd.diskchecksum();

      if ((mgm_xs_val == current_xs_val) && (mMgmFmd.size() == current_sz_val)) {
        mgm_xs_sz_match = true;
        break;
      }

      if ((xs_val != current_xs_val) || (sz_val != current_sz_val)) {
        // There is a xs/size diff between two replicas, we can not fix this case
        disk_xs_sz_match = false;
        break;
      }
    }
  }

  if (mgm_xs_sz_match) {
    eos_warning("msg=\"mgm xs/size repair skip - found replica with matching "
                "xs and size\" fxid=%08llx", mFid);
    // The local info stored on of the the FSTs is wrong, trigger a resync
    ResyncFstMd(false);
    return true;
  }

  if (disk_xs_sz_match && sz_val) {
    size_t out_sz;
    auto xs_binary = StringConversion::Hex2BinDataChar(xs_val, out_sz);

    if (xs_binary == nullptr) {
      eos_err("msg=\"mgm xs/size repair failed due to disk checksum conversion "
              "error\" fxid=%08llx disk_xs=\"%s\"", mFid, xs_val.c_str());
      return false;
    }

    eos::Buffer xs_buff;
    xs_buff.putData(xs_binary.get(), SHA_DIGEST_LENGTH);

    if (gOFS) {
      try {
        // Grab the file metadata object and update it
        eos::Prefetcher::prefetchFileMDAndWait(gOFS->eosView, mFid);
        eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex);
        auto fmd = gOFS->eosFileService->getFileMD(mFid);
        fmd->setChecksum(xs_buff);
        fmd->setSize(sz_val);
        gOFS->eosView->updateFileStore(fmd.get());
      } catch (const eos::MDException& e) {
        eos_err("msg=\"mgm xs/size repair failed - no such filemd\" fxid=%08llx", mFid);
        return false;
      }
    } else {
      // For testing we just update the MGM fmd object
      mMgmFmd.set_checksum(xs_buff.getDataPtr(), xs_buff.getSize());
      mMgmFmd.set_size(sz_val);
    }

    eos_info("msg=\"mgm xs/size repair successful\" fxid=%08llx old_mgm_xs=\"%s\" "
             "new_mgm_xs=\"%s\"", mFid, mgm_xs_val.c_str(), xs_val.c_str());
  } else {
    eos_err("msg=\"mgm xs/size repair failed - no all disk xs/size match\" "
            "fxid=%08llx", mFid);
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

  if (LayoutId::IsRain(mMgmFmd.layout_id())) {
    bad_fsids.insert(mFsidErr);
  } else { // for replica layouts
    std::string mgm_xs_val =
      StringConversion::BinData2HexString(mMgmFmd.checksum().c_str(),
                                          SHA_DIGEST_LENGTH,
                                          LayoutId::GetChecksumLen(mMgmFmd.layout_id()));
    // Make sure at least one disk xs and size match the MGM ones
    uint64_t sz_val {0ull};
    std::string xs_val;
    std::set<eos::common::FileSystem::fsid_t> good_fsids;

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
      eos_debug_lite("mgm_sz=%llu mgm_xs=%s fst_sz_sz=%llu fst_sz_disk=%llu, "
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
        bad_fsids.insert(finfo->mFstFmd.mProtoFmd.fsid());
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
      return false;
    }
  }

  bool all_repaired {true};

  for (auto bad_fsid : bad_fsids) {
    // Trigger an fsck repair job (much like a drain job) doing a TPC
    auto repair_job = mRepairFactory(mFid, bad_fsid, 0, {bad_fsids}, {},
                                     true, "fsck");
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
  // updated properly in the local DB of the FST
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
  if (mReportedErr == FsckErr::UnregRepl) {
    if (static_cast<unsigned long>(mMgmFmd.locations_size()) >=
        LayoutId::GetStripeNumber(mMgmFmd.layout_id() + 1)) {
      // If we have enough stripes - just drop it
      DropReplica(mFsidErr);
      return true;
    } else {
      // If not enough stripes then register it and trigger a check
      if (gOFS) {
        try {
          // Grab the file metadata object and update it
          eos::Prefetcher::prefetchFileMDAndWait(gOFS->eosView, mFid);
          eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex);
          auto fmd = gOFS->eosFileService->getFileMD(mFid);
          fmd->addLocation(mFsidErr);
          gOFS->eosView->updateFileStore(fmd.get());
        } catch (const eos::MDException& e) {
          eos_err("msg=\"unregistered stripe repair failed - no such filemd\" "
                  "fxid=%08llx", mFid);
          return false;
        }
      } else {
        // For testing just update the MGM fmd object
        mMgmFmd.mutable_locations()->Add(mFsidErr);
      }
    }
  } else  if (mReportedErr == FsckErr::DiffRepl) {
    // If "over-replicated" (this should hardly ever happen) then drop the excess
    // stripes and trigger a check (drain) with first stripe as source
    while (static_cast<unsigned long>(mMgmFmd.locations_size()) >
           LayoutId::GetStripeNumber(mMgmFmd.layout_id() + 1)) {
      eos::common::FileSystem::fsid_t drop_fsid = *mMgmFmd.locations().rbegin();
      mMgmFmd.mutable_locations()->RemoveLast();
      DropReplica(drop_fsid);
    }
  }

  // Trigger a fsck repair job to make sure all the remaining stripes are
  // recovered and and new ones are created if need be. By default pick the
  // first stripe as "source" unless we have a better candidate
  eos::common::FileSystem::fsid_t src_fsid = mMgmFmd.locations(0);

  if (mReportedErr == FsckErr::MissRepl) {
    src_fsid = mFsidErr;
  }

  auto repair_job = mRepairFactory(mFid, src_fsid, 0, {}, {}, true, "fsck");
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
                                        SHA_DIGEST_LENGTH,
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
      if (finfo->mFstErr == FstErr::NotOnDisk) {
        to_drop.insert(elem.first);
      }
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
        eos_err("msg=\"replica inconsistency repair failed, no file metadata\" "
                "fxid=%08llx", mFid);
        return false;
      }
    }
  }

  // Then drop any other inconsistent replicas from both the MGM and the FST
  for (auto fsid : to_drop) {
    (void) DropReplica(fsid);
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
            eos_err("msg=\"unregistered replica repair failed, no file metadata\" "
                    "fxid=%08llx", mFid);
            return false;
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
                                         false, "fsck");
        repair_job->DoIt();

        if (repair_job->GetStatus() != FsckRepairJob::Status::OK) {
          eos_err("msg=\"replica inconsistency repair failed\" fxid=%08llx "
                  "src_fsid=%lu", mFid, good_fsid);
          return false;
        } else {
          eos_info("msg=\"replica inconsistency repair successful\" fxid=%08llx "
                   "src_fsid=%lu", mFid, good_fsid);
        }

        ++num_actual_rep;
      }

      if (num_actual_rep < num_expected_rep) {
        eos_err("msg=\"replica inconsistency repair failed\" fxid=%08llx", mFid);
        return false;
      }
    }
  }

  // Discard unregistered/bad replicas
  for (auto fsid : to_drop) {
    eos_info("msg=\"droping replica\" fxid=%08llx fsid=%lu", mFid, fsid);
    (void) DropReplica(fsid);
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
      (void) gOFS->SendResync(mFid, fsid);
    }
  }
}

//------------------------------------------------------------------------------
// Drop replica form FST and also update the namespace view for the given
// file system id
//------------------------------------------------------------------------------
bool
FsckEntry::DropReplica(eos::common::FileSystem::fsid_t fsid) const
{
  bool retc = true;

  if (fsid == 0ull) {
    return retc;
  }

  eos_info("msg=\"drop (unregistered) replica\" fxid=%08llx fsid=%lu",
           mFid, fsid);

  // Send external deletion to the FST
  if (gOFS && !gOFS->DeleteExternal(fsid, mFid)) {
    eos_err("msg=\"failed to send unlink to FST\" fxid=%08llx fsid=%lu",
            mFid, fsid);
    retc = false;
  }

  // Drop from the namespace, we don't need the path as root can drop by fid
  XrdOucErrInfo err;
  eos::common::VirtualIdentity vid = eos::common::VirtualIdentity::Root();

  if (gOFS && gOFS->_dropstripe("", mFid, err, vid, fsid, true)) {
    eos_err("msg=\"failed to drop replicas from ns\" fxid=%08llx fsid=%lu",
            mFid, fsid);
  }

  return retc;
}

//------------------------------------------------------------------------------
// Repair entry
//------------------------------------------------------------------------------
bool
FsckEntry::Repair()
{
  bool success = false;

  // If no MGM object then we are in testing mode
  if (gOFS) {
    gOFS->MgmStats.Add("FsckRepairStarted", 0, 0, 1);

    if (CollectMgmInfo() == false) {
      eos_err("msg=\"no repair action, file is orphan\" fxid=%08llx fsid=%lu",
              mFid, mFsidErr);
      UpdateMgmStats(success);
      (void) DropReplica(mFsidErr);
      // This could be a ghost fid entry still present in the file system map
      // and we need to also drop it from there
      std::string out, err;
      auto root_vid = eos::common::VirtualIdentity::Root();
      (void) proc_fs_dropghosts(mFsidErr, {mFid}, root_vid, out, err);
      return success;
    }

    if (mMgmFmd.cont_id() == 0ull) {
      eos_info("msg=\"no repair action, file is being deleted\" fxid=%08llx",
               mFid);
      UpdateMgmStats(true);
      return true;
    }

    CollectAllFstInfo();
    CollectFstInfo(mFsidErr);
  }

  if (mReportedErr != FsckErr::None) {
    auto it = mMapRepairOps.find(mReportedErr);

    if (it == mMapRepairOps.end()) {
      eos_err("msg=\"unknown type of error\" errr=%i", mReportedErr);
      UpdateMgmStats(success);
      return success;
    }

    auto fn_with_obj = std::bind(it->second, this);
    success = fn_with_obj();
    UpdateMgmStats(success);
    return success;
  }

  // If no explicit error given then try to repair all types of errors, we put
  // the ones with higher priority first
  std::list<RepairFnT> repair_ops {
    &FsckEntry::RepairMgmXsSzDiff,
    &FsckEntry::RepairFstXsSzDiff,
    &FsckEntry::RepairReplicaInconsistencies};

  for (const auto& op : repair_ops) {
    auto fn_with_obj = std::bind(op, this);

    if (!fn_with_obj()) {
      UpdateMgmStats(success);
      return success;
    }
  }

  success = true;
  UpdateMgmStats(success);
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
// Convert string to FsckErr type
//------------------------------------------------------------------------------
FsckErr ConvertToFsckErr(const std::string& serr)
{
  if (serr == "m_cx_diff") {
    return FsckErr::MgmXsDiff;
  } else if (serr == "m_mem_sz_diff") {
    return FsckErr::MgmSzDiff;
  } else if (serr == "d_cx_diff") {
    return FsckErr::FstXsDiff;
  } else if (serr == "d_mem_sz_diff") {
    return FsckErr::FstSzDiff;
  } else if (serr == "unreg_n") {
    return FsckErr::UnregRepl;
  } else if (serr == "rep_diff_n") {
    return FsckErr::DiffRepl;
  } else if (serr == "rep_missing_n") {
    return FsckErr::MissRepl;
  } else if (serr == "blockxs_err") {
    return FsckErr::BlockxsErr;
  } else {
    return FsckErr::None;
  }
}

//------------------------------------------------------------------------------
// Update MGM stats depending on the final outcome
//------------------------------------------------------------------------------
void
FsckEntry::UpdateMgmStats(bool success) const
{
  if (gOFS) {
    if (success) {
      gOFS->MgmStats.Add("FsckRepairSuccessful", 0, 0, 1);
    } else {
      gOFS->MgmStats.Add("FsckRepairFailed", 0, 0, 1);
    }
  }
}

EOSMGMNAMESPACE_END
