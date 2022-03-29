#include "mgm/GroupDrainer.hh"
#include "mgm/groupbalancer/StdDrainerEngine.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Master.hh"
#include "mgm/groupbalancer/GroupsInfoFetcher.hh"
#include "mgm/GroupBalancer.hh"
#include "common/utils/ContainerUtils.hh"
#include "namespace/interface/IView.hh"
#include "namespace/interface/IFsView.hh"
#include "mgm/FsView.hh"
#include "common/FileSystem.hh"

namespace eos::mgm {

using group_balancer::eosGroupsInfoFetcher;
using group_balancer::GroupStatus;

GroupDrainer::GroupDrainer(std::string_view spacename) : mSpaceName(spacename),
                                                         mEngine(std::make_unique<group_balancer::StdDrainerEngine>()),
                                                         numTx(10000),
                                                         mThreshold(0.0)
{
  mThread.reset(&GroupDrainer::GroupDrain, this);
}

GroupDrainer::~GroupDrainer()
{
  mThread.join();
}

void
GroupDrainer::GroupDrain(ThreadAssistant& assistant) noexcept
{
  eosGroupsInfoFetcher fetcher(mSpaceName,
                               [](GroupStatus s) {
                                 return s == GroupStatus::DRAIN || s == GroupStatus::ON;
                               });
  std::map<std::string,string, std::less<>> config_map = {{"threshold",std::to_string(mThreshold)}};
  bool refresh_groups = true;
  while (!assistant.terminationRequested()) {
    if (!gOFS->mMaster->IsMaster()) {
      assistant.wait_for(std::chrono::seconds(60));
      eos_debug("%s", "msg=\"GroupDrainer disabled for slave\"");
      continue;
    }

    pruneTransfers();

    if (isTransfersFull()) {
      // We are currently full, wait for a few seconds before pruning & trying
      // again
      eos_info("msg=\"transfer queue full, pausing before trying again\"");
      assistant.wait_for(std::chrono::seconds(10));
      continue;
    }

    if (isUpdateNeeded(mLastUpdated, refresh_groups)) {
      mEngine->configure(config_map);
      mEngine->populateGroupsInfo(fetcher.fetch());
      refresh_groups = false;
    }

    if (!mEngine->canPick()) {
      eos_info("msg=\"Cannot pick, Empty source or target groups, check status "
               "if this is not expected\", %s",
               mEngine->get_status_str(false, true).c_str());
      assistant.wait_for(std::chrono::seconds(60));
      continue;
    }

    prepareTransfers();
  }
}

bool
GroupDrainer::isUpdateNeeded(std::chrono::time_point<std::chrono::steady_clock>& tp,
                             bool force)
{
  using namespace std::chrono_literals;
  auto now = chrono::steady_clock::now();
  if (force) {
    tp = now;
    return true;
  }
  auto elapsed = chrono::duration_cast<chrono::seconds>(now - mLastUpdated);
  if (elapsed > mCacheExpiryTime) {
    tp = now;
    return true;
  }
  return false;
}

// Prune all transfers which are done by Converter, since the converter will
// pop entries off FidTracker once done, this should give us an idea of our
// queued transfers being actually realized
void
GroupDrainer::pruneTransfers()
{
  auto prune_count = eos::common::erase_if(mTransfers, [](const auto& p) {
    return !gOFS->mFidTracker.HasEntry(p);
  });

  eos_info("msg=\"pruned %ul transfers, transfers in flight=%ul\"",
           prune_count, mTransfers.size());
}

void
GroupDrainer::prepareTransfers()
{
  uint64_t allowed_tx = numTx - mTransfers.size();
  try {
    for (uint64_t i = 0; i < allowed_tx; ++i) {
      prepareTransfer(i);
    }
  } catch (std::exception& ec) {
    eos_crit("msg=\"Got an exception while creating transfers=%s\"", ec.what());
  }

}

void
GroupDrainer::prepareTransfer(uint64_t index)
{
  auto [grp_drain_from, grp_drain_to] = mEngine->pickGroupsforTransfer(index);

  if (grp_drain_from.empty() || grp_drain_to.empty()) {
    eos_static_info("msg=\"engine gave us empty groups skipping\"");
    return;
  }

  auto fsids = mDrainFsMap.find(grp_drain_from);
  if (fsids == mDrainFsMap.end() || isUpdateNeeded(mDrainMapLastUpdated, mRefreshFSMap)) {
    std::tie(fsids, std::ignore) = mDrainFsMap.insert_or_assign(grp_drain_from,
                                                                FsidsinGroup(grp_drain_from));
    mRefreshFSMap = false;
  }

  auto fsid = eos::common::pickIndexRR(fsids->second, index);
  auto fids = mCacheFileList.find(fsid);
  if (fids == mCacheFileList.end() || fids->second.empty()) {
    bool status;
    std::tie(status, fids) = populateFids(fsid);
    if (!status) {
      eos_info("\"Refreshing FS drain statuses\"")
      mRefreshFSMap = true;
      return;
    }
  }

  // Cross check that we do have a valid iterator anyway!
  if (fids != mCacheFileList.end()) {
    if (fids->second.size() > 0) {
      scheduleTransfer(fids->second.back(), grp_drain_from, grp_drain_to);
      fids->second.pop_back();
    }
  } else {
    eos_info("\"msg=couldn't find files in fsid=%d\"", fsid);
  }
}

//! TODO: implement the following!!!!
void
GroupDrainer::scheduleTransfer(eos::common::FileId::fileid_t fid,
                               const string& src_grp, const string& tgt_grp)
{
  if (src_grp.empty() || tgt_grp.empty()) {
    eos_err("msg=\"Got empty transfer groups!\"");
  }

  uint64_t filesz;
  auto conv_tag = GroupBalancer::getFileProcTransferNameAndSize(fid, tgt_grp,
                                                                &filesz);
  conv_tag += "^groupdrainer^";
  conv_tag.erase(0, gOFS->MgmProcConversionPath.length()+1);
  if (gOFS->mConverterDriver->ScheduleJob(fid, conv_tag)) {
    eos_static_info("msg=\"group drainer scheduled job file=\"%s\" "
                    "src_grp=\"%s\" dst_grp=\"%s\"", conv_tag.c_str(),
                    src_grp.c_str(), tgt_grp.c_str());
    mTransfers.emplace(fid);
  } else {
    // TODO have a routine to handle this!
    mFailedTransfers.emplace(fid, std::move(conv_tag));
  }
}

std::pair<bool, GroupDrainer::cache_fid_map_t::iterator>
GroupDrainer::populateFids(eos::common::FileSystem::fsid_t fsid)
{
  //TODO: mark FSes in RO after threshold percent drain
  auto total_files = gOFS->eosFsView->getNumFilesOnFs(fsid);
  if (total_files == 0) {
    ApplyDrainedStatus(fsid);
    mCacheFileList.erase(fsid);

    return {false, mCacheFileList.end()};
  }
  std::vector<eos::common::FileId::fileid_t> local_fids;
  uint32_t ctr = 0;
  for (auto it_fid = gOFS->eosFsView->getStreamingFileList(fsid);
       ctr++ < FID_CACHE_LIST_SZ && it_fid && it_fid->valid();
       it_fid->next())
  {
    local_fids.emplace_back(it_fid->getElement());
  }
  auto [it, _] = mCacheFileList.insert_or_assign(fsid, std::move(local_fids));
  return {true, it};
}

void
GroupDrainer::ApplyDrainedStatus(unsigned int fsid)
{
  eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
  auto fs = FsView::gFsView.mIdView.lookupByID(fsid);

  if (fs) {
    auto status = eos::common::DrainStatus::kDrained;
    eos::common::FileSystemUpdateBatch batch;
    batch.setDrainStatusLocal(status);
    batch.setLongLongLocal("local.drain.bytesleft", 0);
    batch.setLongLongLocal("local.drain.timeleft", 0);
    batch.setLongLongLocal("local.drain.failed", 0);
    batch.setLongLongLocal("local.drain.files", 0);

    if (!gOFS->Shutdown) {
      // If drain done and the system is not shutting down then set the
      // file system to "empty" state
      batch.setLongLongLocal("local.drain.progress", 100);
      batch.setLongLongLocal("local.drain.failed", 0);
      batch.setStringDurable("configstatus", "empty");
      FsView::gFsView.StoreFsConfig(fs);
    }

    fs->applyBatch(batch);
  }
}

std::vector<eos::common::FileSystem::fsid_t>
FsidsinGroup(const string& groupname)
{
  std::vector<eos::common::FileSystem::fsid_t> result;
  eos::common::RWMutexReadLock rlock(FsView::gFsView.ViewMutex);
  auto group_it = FsView::gFsView.mGroupView.find(groupname);
  if (group_it == FsView::gFsView.mGroupView.end()) {
    eos_static_err("msg=\"group not found: \" %s", groupname.c_str());
    return {};
  }

  for (auto fs_it=group_it->second->begin();
       fs_it != group_it->second->end();
       ++fs_it) {
    auto target = FsView::gFsView.mIdView.lookupByID(*fs_it);
    if (target &&
        target->GetActiveStatus() == eos::common::ActiveStatus::kOnline &&
        target->GetDrainStatus() == eos::common::DrainStatus::kNoDrain) {
      result.emplace_back(*fs_it);
    }
  }
  return result;
}
} // eos::mgm
