#include "mgm/GroupDrainer.hh"
#include "mgm/convert/ConversionInfo.hh"
#include "mgm/convert/ConverterEngine.hh"
#include "mgm/groupbalancer/StdDrainerEngine.hh"
#include "mgm/groupbalancer/ConverterUtils.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/groupbalancer/GroupsInfoFetcher.hh"
#include "common/utils/ContainerUtils.hh"
#include "common/StringUtils.hh"
#include "common/table_formatter/TableFormatterBase.hh"
#include "namespace/interface/IFsView.hh"
#include "mgm/FsView.hh"
#include "common/FileSystem.hh"
#include "mgm/utils/FileSystemStatusUtils.hh"
#include "common/utils/BackOffInvoker.hh"

namespace eos::mgm
{

using group_balancer::eosGroupsInfoFetcher;
using group_balancer::GroupStatus;
using group_balancer::getFileProcTransferNameAndSize;

static const std::string format_s = "-s";
static const std::string format_l = "l";
static const std::string format_f = "f";

static constexpr auto GROUPDRAINER_THREADNAME = "GroupDrainer";

GroupDrainer::GroupDrainer(std::string_view spacename):
  mMaxTransfers(DEFAULT_NUM_TX),
  mSpaceName(spacename),
  mEngine(std::make_unique<group_balancer::StdDrainerEngine>())
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
  ThreadAssistant::setSelfThreadName(GROUPDRAINER_THREADNAME);
  eosGroupsInfoFetcher fetcher(mSpaceName,
  [](GroupStatus s) {
    return s == GroupStatus::DRAIN || s == GroupStatus::ON;
  });
  mRefreshGroups = true;
  bool config_status = false;
  eos::common::observer_tag_t observer_tag {};
  eos_info("%s", "msg=\"starting group drainer thread\"");
  eos::common::BackOffInvoker backoff_logger;

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
      mRefreshGroups = config_status;
    }

    if (!gOFS->mConverterEngine || !config_status) {
      // wait for a few seconds before trying to see for reconfiguration in order
      // to not simply always check the atomic in an inf loop
      backoff_logger.invoke([this, &config_status]() {
        eos_info("msg=\"Invalid GroupDrainer Configuration or Converter "
                 "not enabled, sleeping!\" config_status=%d, space=%s",
                 config_status, mSpaceName.c_str());
      });
      assistant.wait_for(std::chrono::seconds(30));
      continue;
    }

    if (!observer_tag) {
      // Safe to access gOFS->mConverterEngine as config_status would've failed
      // before this!
      if (auto mgr = gOFS->mConverterEngine->getObserverMgr()) {
        observer_tag = mgr->addObserver([this](
                                          ConverterEngine::JobStatusT status,
        std::string tag) {
          auto info = ConversionInfo::parseConversionString(tag);

          if (!info) {
            eos_crit("Unable to parse conversion info from tag=%s",
                     tag.c_str());
            return;
          }

          switch (status) {
          case ConverterEngine::JobStatusT::DONE:
            this->dropTransferEntry(info->mFid);
            eos_info("msg=\"Dropping completed entry\" fid=%lu tag=%s",
                     info->mFid, tag.c_str());
            break;

          case ConverterEngine::JobStatusT::FAILED:
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

    if (isTransfersFull()) {
      // We are currently full, wait for a few seconds before pruning & trying
      // again
      eos_info("msg=\"transfer queue full, pausing before trying again\"");
      assistant.wait_for(std::chrono::seconds(2));
      continue;
    }

    if (isUpdateNeeded(mLastUpdated, mRefreshGroups)) {
      mEngine->configure(mDrainerEngineConf);
      mEngine->populateGroupsInfo(fetcher.fetch());
      pruneTransfers();
    }

    if (!mEngine->canPick()) {
      eos_info("msg=\"Cannot pick, Empty source or target groups, check status "
               "if this is not expected\", %s",
               mEngine->get_status_str(false, true).c_str());
      assistant.wait_for(std::chrono::seconds(60));
      continue;
    }

    prepareTransfers();

    if (mPauseExecution) {
      eos_info("%s", "msg=\"Pausing Execution for 30s!\"");
      assistant.wait_for(std::chrono::seconds(30));
    }
  }
}

bool
GroupDrainer::isUpdateNeeded(std::chrono::time_point<std::chrono::steady_clock>&
                             tp,
                             bool& force)
{
  auto now = std::chrono::steady_clock::now();

  if (force) {
    tp = now;
    force = false;
    return true;
  }

  auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - tp);

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
    prune_count = eos::common::erase_if(mTransfers, [](const auto & p) {
      return !gOFS->mFidTracker.HasEntry(p);
    });
  }

  if (prune_count > 0)
    eos_info("msg=\"pruned %lu transfers, transfers in flight=%lu\"",
             prune_count, mTransfers.size());
}

void
GroupDrainer::prepareTransfers()
{
  uint64_t allowed_tx = getAllowedTransfers();

  try {
    for (uint64_t i = 0; i < allowed_tx; ++i) {
      prepareTransfer(mRRSeed++);

      if (mRefreshGroups || mPauseExecution) {
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
    // will not be likely reached, as Engine->canPick shouldn't reply earlier
    eos_static_info("msg=\"engine gave us empty groups skipping\"");
    return;
  }

  eos_debug("msg=\"Doing transfer \" index=%d", index);
  // No need for lock here as if there is a write it is serial here and sync., the UI
  // call from another thread doesn't do modifications! Also we expect the failure
  // conditions to only happen during the initial phases when we haven't yet filled the
  // various drain Fs Maps or during periodic intervals when we run out files to
  // transfer!
  auto fsids = mDrainFsMap.find(grp_drain_from);

  if (fsids == mDrainFsMap.end() ||
      isUpdateNeeded(mDrainMapLastUpdated, mRefreshFSMap) ||
      fsids->second.empty()) {
    {
      std::scoped_lock slock(mDrainFsMapMtx);
      std::tie(fsids, std::ignore) = mDrainFsMap.insert_or_assign(grp_drain_from,
                                     fsutils::FsidsinGroup(grp_drain_from));
      mPauseExecution = isDrainFSMapEmpty(mDrainFsMap);
    }
    mDrainMapLastUpdated = std::chrono::steady_clock::now();

    // We enter the following conditional if the group concerned is having empty FSes
    // check if we reach a drain complete state!
    if (fsids->second.empty()) {
      // We reach here when all the FSes in the group are either offline or empty!
      // force a refresh of Groups Info for the next cycle, in that case the new
      // Groups Info will have 0 capacity groups and the engine will find that it
      // has no more targets to pick effectively stopping any further processing
      // other than the Groups Refresh every few minutes to check any new drain states
      eos_debug("msg=\"Encountered group with no online FS\" group=%s",
                grp_drain_from.c_str());
      mRefreshGroups = setDrainCompleteStatus(grp_drain_from,
                                              checkGroupDrainStatus(grp_drain_from));

      if (mRefreshGroups) {
        eos_info("msg=\"Group completed drain!\" group=%s",
                 grp_drain_from.c_str());
      }

      return;
    }
  }

  auto fsid = eos::common::pickIndexRR(fsids->second,
                                       mGroupFSSeed[grp_drain_from]++);
  auto fids = mCacheFileList.find(fsid);

  if (fids == mCacheFileList.end() || fids->second.empty()) {
    bool status;
    std::tie(status, fids) = populateFids(fsid);

    if (!status) {
      eos_debug("%s", "\"Refreshing FS drain statuses\"");
      mRefreshFSMap = true;
      return;
    }
  }

  // Cross check that we do have a valid iterator anyway!
  if (fids != mCacheFileList.end()) {
    if (fids->second.size() > 0) {
      scheduleTransfer(fids->second.back(), grp_drain_from, grp_drain_to, fsid);
      fids->second.pop_back();
    } else {
      eos_debug("%s", "Got a valid iter but empty files!");
    }
  } else {
    eos_debug("\"msg=couldn't find files in fsid=%d\"", fsid);
  }
}

void
GroupDrainer::scheduleTransfer(eos::common::FileId::fileid_t fid,
                               const std::string& src_grp,
                               const std::string& tgt_grp,
                               eos::common::FileSystem::fsid_t src_fsid)
{
  if (src_grp.empty() || tgt_grp.empty()) {
    eos_err("%s", "msg=\"Got empty transfer groups!\"");
    return;
  }

  // Cross-check that the file wasn't scheduled before we attempt to check FS
  // and possibly redo a transfer
  if (trackedTransferEntry(fid)) {
    eos_info("msg=\"Skipping scheduling of Tracked Transfer\" fid=%08llx", fid);
    return;
  }

  uint64_t filesz;
  auto conv_tag = getFileProcTransferNameAndSize(fid, tgt_grp, &filesz,
                  group_balancer::NullFilter);

  if (conv_tag.empty()) {
    eos_err("msg=\"Possibly failed proc file found\" fid=%08llx", fid);
    return;
  }

  conv_tag += "^groupdrainer^";
  conv_tag.erase(0, gOFS->MgmProcConversionPath.length() + 1);

  if (gOFS->mConverterEngine->ScheduleJob(fid, conv_tag)) {
    eos_info("msg=\"group drainer scheduled job file=\"%s\" "
             "src_grp=\"%s\" dst_grp=\"%s\"", conv_tag.c_str(),
             src_grp.c_str(), tgt_grp.c_str());
    addTransferEntry(fid);
    mDrainProgressTracker.increment(src_fsid);
  } else {
    addFailedTransferEntry(fid, std::move(conv_tag));
  }
}

std::pair<bool, GroupDrainer::cache_fid_map_t::iterator>
GroupDrainer::populateFids(eos::common::FileSystem::fsid_t fsid)
{
  eos_debug("msg=\"populating FIDS from\" fsid=%d", fsid);
  //TODO: mark FSes in RO after threshold percent drain
  auto total_files = gOFS->eosFsView->getNumFilesOnFs(fsid);

  if (total_files == 0) {
    fsutils::ApplyDrainedStatus(fsid);
    mCacheFileList.erase(fsid);
    return {false, mCacheFileList.end()};
  }

  mDrainProgressTracker.setTotalFiles(fsid, total_files);

  //Check if the FS is in the Retrytracker, skip these FSes,
  //TODO: We could skip getNumFilesOnFs altogether every loop if we have
  //RetryTracker entry and only check once every minute or so for the FSID
  if (auto kv = mFsidRetryCtr.find(fsid);
      kv != mFsidRetryCtr.end()) {
    if (!kv->second.need_update(mRetryInterval)) {
      eos_debug("msg=\"skipping retries as retry_interval hasn't passed\", "
                " fsid=%d", fsid);
      return {false, mCacheFileList.end()};
    }
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
      } else if (!mTransfers.count(fid) && !mTrackedTransfers.count(fid)) {
        local_fids.emplace_back(fid);
        ++ctr;
      }
    }
  }

  if (local_fids.empty() && !failed_fids.empty()) {
    eos_debug("msg=\"Handling Retries for\" fsid=%lu", fsid);
    return handleRetries(fsid, std::move(failed_fids));
  }

  auto [it, _] = mCacheFileList.insert_or_assign(fsid, std::move(local_fids));
  return {true, it};
}

bool
GroupDrainer::Configure(const std::string& spaceName)
{
  using eos::common::StringToNumeric;
  eos::common::RWMutexReadLock vlock(FsView::gFsView.ViewMutex);
  FsSpace* space = nullptr;

  if (auto kv = FsView::gFsView.mSpaceView.find(spaceName);
      kv != FsView::gFsView.mSpaceView.end()) {
    space = kv->second;
  }

  if (space == nullptr) {
    eos_err("msg=\"no such space found\" space=%s", spaceName.c_str());
    return false;
  }

  bool is_enabled = space->GetConfigMember("groupdrainer") == "on";
  bool is_conv_enabled = gOFS->mConverterEngine->IsRunning();

  if (!is_enabled || !is_conv_enabled) {
    eos_info("msg=\"group drainer or converter not enabled\""
             " space=%s drainer_status=%d converter_status=%d",
             mSpaceName.c_str(), is_enabled, is_conv_enabled);
    return false;
  }

  {
    std::scoped_lock slock(mTransfersMtx);
    eos::common::StringToNumeric(
      space->GetConfigMember("groupdrainer.ntx"), mMaxTransfers,
      DEFAULT_NUM_TX);
  }

  eos::common::StringToNumeric(
    space->GetConfigMember("groupdrainer.ntx"), mMaxTransfers,
    DEFAULT_NUM_TX);
  eos::common::StringToNumeric(
    space->GetConfigMember("groupdrainer.retry_interval"), mRetryInterval,
    DEFAULT_RETRY_INTERVAL);
  eos::common::StringToNumeric(
    space->GetConfigMember("groupdrainer.retry_count"), mRetryCount,
    MAX_RETRIES);
  uint64_t cache_expiry_time;
  bool status = eos::common::StringToNumeric(
                  space->GetConfigMember("groupdrainer.group_refresh_interval"),
                  cache_expiry_time, DEFAULT_CACHE_EXPIRY_TIME);

  if (status) {
    mCacheExpiryTime = std::chrono::seconds(cache_expiry_time);
  }

  auto threshold_str = space->GetConfigMember("groupdrainer.threshold");

  if (!threshold_str.empty()) {
    mDrainerEngineConf.insert_or_assign("threshold", std::move(threshold_str));
  }

  return true;
}

std::pair<bool, GroupDrainer::cache_fid_map_t::iterator>
GroupDrainer::handleRetries(eos::common::FileSystem::fsid_t fsid,
                            std::vector<eos::common::FileId::fileid_t>&& fids)
{
  auto tracker = mFsidRetryCtr[fsid];

  if (tracker.count > mRetryCount) {
    fsutils::ApplyFailedDrainStatus(fsid, fids.size());
    mCacheFileList.erase(fsid);
    return {false, mCacheFileList.end()};
  }

  if (tracker.need_update(mRetryInterval)) {
    mFsidRetryCtr[fsid].update();
    eos_info("msg=\"Retrying failed transfers for\" fsid=%lu, count=%lu retry_count=%d",
             fsid, fids.size(), tracker.count);
    auto [it, _] = mCacheFileList.insert_or_assign(fsid, std::move(fids));
    return {true, it};
  }

  eos_debug("%s", "Nothing to do here, returning empty!");
  return {true, mCacheFileList.end()};
}

GroupStatus
GroupDrainer::checkGroupDrainStatus(const fsutils::fs_status_map_t& fs_map)
{
  uint16_t total_fs = 0, failed_fs = 0, drained_fs = 0;

  for (const auto& kv : fs_map) {
    if (kv.second.active_status == eos::common::ActiveStatus::kOffline) {
      return GroupStatus::OFF;
    }

    ++total_fs;

    switch (kv.second.drain_status) {
    case eos::common::DrainStatus::kDrainFailed:
      ++failed_fs;
      break;

    case eos::common::DrainStatus::kDrained:
      ++drained_fs;
      break;

    case eos::common::DrainStatus::kNoDrain:
      [[fallthrough]];

    default:
      // We've reached here because the fs is in one of the
      // regular draining states and not one from GroupDrainer, this means
      // the FS is either actually draining or in a state we don't recognize
      return GroupStatus::ON;
    }
  }

  if (failed_fs + drained_fs != total_fs) {
    // Unlikely to reach!
    eos_static_crit("msg=\"some FSes in unrecognized state\" total_fs=%d, "
                    "failed_fs=%d drained_fs=%d", total_fs, failed_fs,
                    drained_fs);
    return GroupStatus::ON;
  }

  if (failed_fs > 0) {
    return GroupStatus::DRAINFAILED;
  }

  return GroupStatus::DRAINCOMPLETE;
}


GroupStatus
GroupDrainer::checkGroupDrainStatus(const std::string& groupname)
{
  auto fs_map = fsutils::GetGroupFsStatus(groupname);
  return checkGroupDrainStatus(fs_map);
}

bool
GroupDrainer::setDrainCompleteStatus(const std::string& groupname,
                                     GroupStatus s)
{
  if (!isValidDrainCompleteStatus(s)) {
    return false;
  }

  eos::common::RWMutexWriteLock lock(FsView::gFsView.ViewMutex);
  auto group_it = FsView::gFsView.mGroupView.find(groupname);

  if (group_it == FsView::gFsView.mGroupView.end()) {
    return false;
  }

  return group_it->second->SetConfigMember("status",
         group_balancer::GroupStatusToStr(s));
}

static TableRow
generate_progress_row(eos::common::FileSystem::fsid_t fsid,
                      float drain_percent, uint64_t file_ctr,
                      uint64_t total_files)
{
  TableRow row;
  row.emplace_back(fsid, format_l);
  row.emplace_back((double)drain_percent, format_f);
  row.emplace_back((long long)file_ctr, format_l);
  row.emplace_back((long long)total_files, format_l);
  return row;
}

std::string
GroupDrainer::getStatus(StatusFormat status_fmt) const
{
  // TODO: Expose more counters in monitoring!
  if (status_fmt == StatusFormat::MONITORING) {
    return mEngine->get_status_str(false, true);
  }

  std::stringstream ss;
  {
    std::scoped_lock sl(mTransfersMtx);
    ss << "Max allowed Transfers  : " << mMaxTransfers << "\n"
       << "Transfers in Queue     : " << mTransfers.size() << "\n"
       << "Total Transfers        : " << mTrackedTransfers.size() << "\n";
  }
  auto failed_tx_sz = mFailedTransfers.size();
  ss << "Transfers Failed       : " << failed_tx_sz << "\n";
  ss << mEngine->get_status_str();

  if (mDrainFsMap.empty()) {
    return ss.str();
  }

  if (status_fmt == StatusFormat::DETAIL) {
    std::scoped_lock sl(mDrainFsMapMtx);

    if (isDrainFSMapEmpty(mDrainFsMap)) {
      return ss.str();
    }

    for (const auto& kv : mDrainFsMap) {
      ss << "Group: " << kv.first << "\n";
      TableFormatterBase table_fs_status(true);
      TableData table_data;
      table_fs_status.SetHeader({
        {"fsid", 10, format_l},
        {"Drain Progress", 10, format_f},
        {"Total Transfers", 10, format_l},
        {"Total files", 10, format_l}
      });

      for (const auto& fsid : kv.second) {
        table_data.emplace_back(generate_progress_row(fsid,
                                mDrainProgressTracker.getDrainStatus(fsid),
                                mDrainProgressTracker.getFileCounter(fsid),
                                mDrainProgressTracker.getTotalFiles(fsid)));
      }

      table_fs_status.AddRows(table_data);
      ss << table_fs_status.GenerateTable() << "\n";
    }
  }

  return ss.str();
}

void
GroupDrainer::resetFailedTransfers()
{
  std::scoped_lock sl(mFailedTransfersMtx, mTransfersMtx);
  mFailedTransfers.clear();
  mTrackedTransfers.clear();
}

void
GroupDrainer::resetCaches()
{
  {
    std::scoped_lock sl(mFailedTransfersMtx, mTransfersMtx);
    mFailedTransfers.clear();
    mTransfers.clear();
    mTrackedTransfers.clear();
  }
  // force a refresh of the global groups map info
  mDoConfigUpdate.store(true, std::memory_order_relaxed);
  // TODO: Have a functionality to clear cached filelists as well!
}

bool
GroupDrainer::isDrainFSMapEmpty(const drain_fs_map_t& drainFsMap)
{
  return std::all_of(drainFsMap.begin(), drainFsMap.end(),
  [](const auto & p) {
    return p.second.empty();
  });
}

} // eos::mgm
