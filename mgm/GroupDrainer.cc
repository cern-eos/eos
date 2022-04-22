#include "mgm/GroupDrainer.hh"
#include "mgm/convert/ConversionInfo.hh"
#include "mgm/convert/ConverterDriver.hh"
#include "mgm/groupbalancer/StdDrainerEngine.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Master.hh"
#include "mgm/groupbalancer/GroupsInfoFetcher.hh"
#include "mgm/GroupBalancer.hh"
#include "common/utils/ContainerUtils.hh"
#include "common/StringUtils.hh"
#include "namespace/interface/IView.hh"
#include "namespace/interface/IFsView.hh"
#include "mgm/FsView.hh"
#include "common/FileSystem.hh"
#include "mgm/utils/FileSystemStatusUtils.hh"

namespace eos::mgm {

using group_balancer::eosGroupsInfoFetcher;
using group_balancer::GroupStatus;

GroupDrainer::GroupDrainer(std::string_view spacename) : mSpaceName(spacename),
                                                         mEngine(std::make_unique<group_balancer::StdDrainerEngine>()),
                                                         numTx(10000)
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
  mRefreshGroups = true;
  bool config_status = false;
  eos::common::observer_tag_t observer_tag = {0};
  while (!assistant.terminationRequested()) {
    if (!gOFS->mMaster->IsMaster()) {
      assistant.wait_for(std::chrono::seconds(60));
      eos_debug("%s", "msg=\"GroupDrainer disabled for slave\"");
      continue;
    }

    bool expected_reconfiguration = true;
    if (mDoConfigUpdate.compare_exchange_strong(expected_reconfiguration, false,
                                                std::memory_order_acq_rel)) {
      config_status = Configure(mSpaceName);
    }

    if (!gOFS->mConverterDriver || !config_status) {
      // wait for a few seconds before trying to see for reconfiguration in order
      // to not simply always check the atomic in an inf loop
      assistant.wait_for(std::chrono::seconds(30));
      continue;
    }

    if (!observer_tag) {
      // Safe to access gOFS->mConverterDriver as config_status would've failed
      // before this!
      if (auto mgr = gOFS->mConverterDriver->getObserverMgr()) {
        observer_tag = mgr->addObserver([this](
            ConverterDriver::JobStatusT status,
            std::string tag) {
          auto info = ConversionInfo::parseConversionString(tag);
          if (!info) {
            eos_crit("Unable to parse conversion info from tag=%s",
                     tag.c_str());
            return;
          }

          switch (status) {
          case ConverterDriver::JobStatusT::DONE:
            this->dropTransferEntry(info->mFid);
            eos_info("msg=\"Dropping completed entry\" fid=%lu tag=%s",
                     info->mFid, tag.c_str());
            break;
          case ConverterDriver::JobStatusT::FAILED:
            eos_info("msg=\"Tracking failed transfer\" fid=%lu tag=%s",
                     info->mFid, tag.c_str());
            this->addFailedTransferEntry(info->mFid, std::move(tag));
            break;
          default:
            eos_debug("Handler not applied");
          }
        });
      } else {
        // We're reaching here as the converter is still initializing, wait a few seconds
        eos_info("%s",
                 "msg=\"Couldn't register observers on Converter, trying again after 30s\"");
        assistant.wait_for(std::chrono::seconds(30));
        continue;
      }
    }

    pruneTransfers();

    if (isTransfersFull()) {
      // We are currently full, wait for a few seconds before pruning & trying
      // again
      eos_info("msg=\"transfer queue full, pausing before trying again\"");
      assistant.wait_for(std::chrono::seconds(10));
      continue;
    }



    if (isUpdateNeeded(mLastUpdated, mRefreshGroups || config_status)) {
      mEngine->configure(mDrainerEngineConf);
      mEngine->populateGroupsInfo(fetcher.fetch());
      mRefreshGroups = false;
      config_status = false;
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
  size_t prune_count {0};
  {
    std::lock_guard lg(mTransfersMtx);
    prune_count = eos::common::erase_if(mTransfers, [](const auto& p) {
      return !gOFS->mFidTracker.HasEntry(p);
    });
  }


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
      if (mRefreshGroups) {
        return;
      }
    }
  } catch (std::exception& ec) {
    // Very unlikely to reach here, since we already x-check that we don't supply
    // empty containers to RR picker, but in the rare case, just force a refresh of
    // our cached groups info
    eos_crit("msg=\"Got an exception while creating transfers=%s\"", ec.what());
    mRefreshGroups = true;
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
                                                                fsutils::FsidsinGroup(grp_drain_from));
    if (fsids->second.empty()) {
      // We reach here when all the FSes in the group are either offline or empty!
      // force a refresh of Groups Info for the next cycle, in that case the new
      // Groups Info will have 0 capacity groups and the engine will find that it
      // has no more targets to pick effectively stopping any further processing
      // other than the Groups Refresh every few minutes to check any new drain states
      eos_static_info("msg=\"Encountered group with no online FS\" group_name=%s",
                      grp_drain_from.c_str());
      mRefreshGroups = true;
      return;
    }
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
    eos_info("msg=\"group drainer scheduled job file=\"%s\" "
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
    fsutils::ApplyDrainedStatus(fsid);
    mCacheFileList.erase(fsid);

    return {false, mCacheFileList.end()};
  }

  std::vector<eos::common::FileId::fileid_t> local_fids;
  std::vector<eos::common::FileId::fileid_t> failed_fids;
  uint32_t ctr = 0;

  {
    std::scoped_lock slock(mTransfersMtx, mFailedTransfersMtx);
    for (auto it_fid = gOFS->eosFsView->getStreamingFileList(fsid);
         it_fid && it_fid->valid() && ctr < FID_CACHE_LIST_SZ;
         it_fid->next()) {
      auto fid = it_fid->getElement();
      if (mFailedTransfers.count(fid)) {
        failed_fids.emplace_back(fid);
      } else if (!mTransfers.count(fid)) {
        local_fids.emplace_back(fid);
        ++ctr;
      }
    }
  }

  if (local_fids.empty() && !failed_fids.empty()) {
    handleRetries(fsid, std::move(failed_fids));
  }
  auto [it, _] = mCacheFileList.insert_or_assign(fsid, std::move(local_fids));
  return {true, it};
}

bool
GroupDrainer::Configure(const string& spaceName)
{
  using eos::common::StringToNumeric;
  eos::common::RWMutexReadLock vlock(FsView::gFsView.ViewMutex);
  FsSpace *space = nullptr;
  if (auto kv = FsView::gFsView.mSpaceView.find(spaceName);
      kv != FsView::gFsView.mSpaceView.end()) {
    space = kv->second;
  }

  if (space == nullptr) {
    eos_err("msg=\"No such space found\" space=%s", spaceName.c_str());
    return false;
  }

  bool is_enabled = space->GetConfigMember("groupdrainer") == "on";
  bool is_conv_enabled = space->GetConfigMember("converter") == "on";

  if (!is_enabled || !is_conv_enabled) {
    eos_info("msg=\"group drainer or converter not enabled\""
             " space=%s drainer_status=%d converter_status=%d",
             mSpaceName.c_str(), is_enabled, is_conv_enabled);
    return false;
  }
  eos::common::StringToNumeric(
      space->GetConfigMember("groupdrainer.ntx"), numTx, DEFAULT_NUM_TX);

  eos::common::StringToNumeric(
      space->GetConfigMember("groupdrainer.retry_interval"), mRetryInterval,
      DEFAULT_RETRY_INTERVAL);

  uint64_t cache_expiry_time;
  bool status = eos::common::StringToNumeric(
      space->GetConfigMember("groupdrainer.group_refresh_interval"),
      cache_expiry_time, DEFAULT_CACHE_EXPIRY_TIME);

  if (status) {
    mCacheExpiryTime = std::chrono::seconds(cache_expiry_time);
  }
  auto threshold_str = space->GetConfigMember("groupbalancer.threshold");
  if (!threshold_str.empty()) {
    mDrainerEngineConf.insert_or_assign("threshold", std::move(threshold_str));
  }

  return true;
}

void
GroupDrainer::handleRetries(eos::common::FileSystem::fsid_t fsid,
                            std::vector<eos::common::FileId::fileid_t>&& fids)
{
  auto tracker = mFsidRetryCtr[fsid];
  if (tracker.count > MAX_RETRIES) {
    fsutils::ApplyFailedDrainStatus(fsid, fids.size());
    mCacheFileList.erase(fsid);
    return;
  }

  if (tracker.need_update(mRetryInterval)) {
    eos_info("msg=\"Retrying failed transfers for\" fsid=%lu, count=%lu",
             fsid, fids.size());
    mCacheFileList.insert_or_assign(fsid, std::move(fids));
    mFsidRetryCtr[fsid].update();
  }
}

std::string
GroupDrainer::getStatus() const
{
  auto tx_sz = mTransfers.size();
  auto failed_tx_sz = mFailedTransfers.size();

  std::stringstream ss;
  ss << "Transfers in Queue     : " << tx_sz << "\n";
  ss << "Transfers Failed       : " << failed_tx_sz << "\n";
  ss << mEngine->get_status_str();
  return ss.str();
}

} // eos::mgm
