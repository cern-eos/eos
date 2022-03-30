//------------------------------------------------------------------------------
// File: GroupBalancer.cc
// Author: Joaquim Rocha - CERN
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

#include "mgm/GroupBalancer.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/FsView.hh"
#include "mgm/Master.hh"
#include "namespace/interface/IFsView.hh"
#include "namespace/interface/IView.hh"
#include "namespace/Prefetcher.hh"
#include "common/StringConversion.hh"
#include "common/FileId.hh"
#include "common/LayoutId.hh"
#include "XrdSys/XrdSysError.hh"
#include "XrdOuc/XrdOucTrace.hh"
#include "Xrd/XrdScheduler.hh"
#include <random>
#include <cmath>
#include "mgm/groupbalancer/BalancerEngineFactory.hh"
#include "mgm/groupbalancer/BalancerEngineUtils.hh"
#include "mgm/groupbalancer/GroupsInfoFetcher.hh"

extern XrdSysError gMgmOfsEroute;
extern XrdOucTrace gMgmOfsTrace;

#define CACHE_LIFE_TIME 60 // seconds

EOSMGMNAMESPACE_BEGIN

using group_balancer::BalancerEngineT;
using group_balancer::group_size_map;
using group_balancer::eosGroupsInfoFetcher;


//-------------------------------------------------------------------------------
// GroupBalancer constructor
//-------------------------------------------------------------------------------
GroupBalancer::GroupBalancer(const char* spacename)
  : mSpaceName(spacename), mLastCheck(0)
{
  mEngine.reset(group_balancer::make_balancer_engine(BalancerEngineT::stddev));
  mThread.reset(&GroupBalancer::GroupBalance, this);
}

//------------------------------------------------------------------------------
// Stop group balancing thread
//------------------------------------------------------------------------------
void
GroupBalancer::Stop()
{
  mThread.join();
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
GroupBalancer::~GroupBalancer()
{
  Stop();
  mEngine->clear();
}

//------------------------------------------------------------------------------
// Produces a file conversion path to be placed in the proc directory taking
// into account the given group and also returns its size
//------------------------------------------------------------------------------
std::string
GroupBalancer::getFileProcTransferNameAndSize(eos::common::FileId::fileid_t fid,
    FsGroup* group, uint64_t* size)

{
  char fileName[1024];
  std::shared_ptr<eos::IFileMD> fmd;
  eos::common::LayoutId::layoutid_t layoutid = 0;
  eos::common::FileId::fileid_t fileid = 0;
  {
    eos::Prefetcher::prefetchFileMDAndWait(gOFS->eosView, fid);
    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);

    try {
      fmd = gOFS->eosFileService->getFileMD(fid);
      layoutid = fmd->getLayoutId();
      fileid = fmd->getId();

      if (fmd->getContainerId() == 0) {
        return std::string("");
      }

      if (size) {
        *size = fmd->getSize();
      }

      XrdOucString fileURI = gOFS->eosView->getUri(fmd.get()).c_str();

      if (fileURI.beginswith(gOFS->MgmProcPath.c_str())) {
        // don't touch files in any ../proc/ directory
        return std::string("");
      }

      eos_static_debug("msg=\"found file for transfering\" file=\"%s\"",
                       fileURI.c_str());
    } catch (eos::MDException& e) {
      eos_static_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),
                       e.getMessage().str().c_str());
      return std::string("");
    }
  }
  snprintf(fileName, 1024, "%s/%016llx:%s#%08lx",
           gOFS->MgmProcConversionPath.c_str(),
           fileid, group->mName.c_str(), (unsigned long) layoutid);
  return std::string(fileName);
}

//------------------------------------------------------------------------------
// For each entry in mTransfers, checks if the files' paths exist, if they
// don't, they are deleted from the mTransfers
//------------------------------------------------------------------------------
void
GroupBalancer::UpdateTransferList()
{
  for (auto it = mTransfers.begin(); it != mTransfers.end();) {
    if (gOFS->mConverterDriver) {
      if (!gOFS->mFidTracker.HasEntry(it->first)) {
        mTransfers.erase(it++);
      } else {
        ++it;
      }
    } else {
      struct stat buf;
      XrdOucErrInfo error;
      eos::common::VirtualIdentity rootvid = eos::common::VirtualIdentity::Root();
      const std::string& fileName = (*it).second;

      if (gOFS->_stat(fileName.c_str(), &buf, error, rootvid, "")) {
        mTransfers.erase(it++);
      } else {
        ++it;
      }
    }
  }

  eos_static_info("scheduledtransfers=%d", mTransfers.size());
}


//------------------------------------------------------------------------------
// Creates the conversion file in proc for the file ID, from the given
// sourceGroup, to the targetGroup (and updates the cache structures)
//------------------------------------------------------------------------------
void
GroupBalancer::scheduleTransfer(const FileInfo& file_info,
                                FsGroup* sourceGroup, FsGroup* targetGroup)
{
  if (sourceGroup == nullptr || targetGroup == nullptr) {
    return;
  }

  auto mGroupSizes = mEngine->get_group_sizes();

  if ((mGroupSizes.count(sourceGroup->mName) == 0) ||
      (mGroupSizes.count(targetGroup->mName) == 0)) {
    eos_static_err("msg=\"no src/trg group in map\" src_group=%s trg_group=%s",
                   sourceGroup->mName.c_str(), targetGroup->mName.c_str());
    return;
  }

  // Use new converter if available
  if (gOFS->mConverterDriver) {
    // Push conversion job to QuarkDB
    // Since the proc file name is generated by
    // getFileProcTransferNameAndSize it doesn't contain (!) so we can append without checking
    std::string conv_tag = file_info.filename;
    conv_tag += "^groupbalancer^";
    conv_tag.erase(0, gOFS->MgmProcConversionPath.length() + 1);

    if (gOFS->mConverterDriver->ScheduleJob(file_info.fid, conv_tag)) {
      eos_static_info("msg=\"grp_balance scheduled job\" file=\"%s\" "
                      "src_grp=\"%s\" dst_grp=\"%s\"", conv_tag.c_str(),
                      sourceGroup->mName.c_str(), targetGroup->mName.c_str());
    } else {
      eos_static_err("msg=\"grp_balance could not schedule job\" "
                     "file=\"%s\" src_grp=\"%s\" dst_grp=\"%s\"",
                     conv_tag.c_str(), sourceGroup->mName.c_str(),
                     targetGroup->mName.c_str());
    }
  } else { // use old converter
    eos::common::VirtualIdentity rootvid = eos::common::VirtualIdentity::Root();
    XrdOucErrInfo mError;

    if (!gOFS->_touch(file_info.filename.c_str(), mError, rootvid, 0)) {
      eos_static_info("scheduledfile=%s src_group=%s trg_group=%s",
                      file_info.filename.c_str(), sourceGroup->mName.c_str(),
                      targetGroup->mName.c_str());
    } else {
      eos_static_err("msg=\"failed to schedule transfer\" schedulingfile=\"%s\"",
                     file_info.filename.c_str());
      return;
    }
  }

  mTransfers[file_info.fid] = file_info.filename;
}

//------------------------------------------------------------------------------
// Chooses a random file ID from a random filesystem in the given group
//------------------------------------------------------------------------------
eos::common::FileId::fileid_t
GroupBalancer::chooseFidFromGroup(FsGroup* group)
{
  if (group == nullptr) {
    return {};
  }

  int rndIndex;
  bool found = false;
  uint64_t fsid_size = 0ull;
  eos::common::FileSystem::fsid_t fsid = 0;
  eos::common::RWMutexReadLock vlock(FsView::gFsView.ViewMutex);
  eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
  // TODO(gbitzes): Add prefetching, make more efficient.
  std::vector<int> validFsIndexes(group->size());

  for (size_t i = 0; i < group->size(); i++) {
    validFsIndexes[i] = (int) i;
  }

  eos::mgm::BaseView::const_iterator fs_it;

  while (validFsIndexes.size() > 0) {
    fs_it = group->begin();
    rndIndex = group_balancer::getRandom(validFsIndexes.size() - 1);
    std::advance(fs_it, validFsIndexes[rndIndex]);
    fsid = *fs_it;
    // Accept only active file systems
    FileSystem* target = FsView::gFsView.mIdView.lookupByID(fsid);

    if (target && target->GetActiveStatus() == eos::common::ActiveStatus::kOnline) {
      fsid_size = gOFS->eosFsView->getNumFilesOnFs(fsid);

      if (fsid_size) {
        found = true;
        break;
      }
    }

    validFsIndexes.erase(validFsIndexes.begin() + rndIndex);
  }

  // Check if we have any files to transfer
  if (!found) {
    return {};
  }

  int attempts = 10;

  while (attempts-- > 0) {
    eos::IFileMD::id_t randomPick;

    if (gOFS->eosFsView->getApproximatelyRandomFileInFs(fsid, randomPick) &&
        mTransfers.count(randomPick) == 0) {
      return randomPick;
    }
  }

  return {};
}

GroupBalancer::FileInfo
GroupBalancer::chooseFileFromGroup(FsGroup* from_group, FsGroup* to_group,
                                   int attempts)
{
  if (from_group == nullptr || to_group == nullptr) {
    return {};
  }

  if (from_group->size() == 0) {
    return {};
  }

  uint64_t filesize;

  while (attempts-- > 0) {
    auto fid = chooseFidFromGroup(from_group);

    if (!fid) {
      continue;
    }

    auto filename = getFileProcTransferNameAndSize(fid, to_group, &filesize);

    if (filename.empty() ||
        (mCfg.mMinFileSize > filesize) ||
        (mCfg.mMaxFileSize < filesize)) {
      continue;
    }

    // We've a hit!
    return {fid, std::move(filename), filesize};
  }

  return {};
}

//------------------------------------------------------------------------------
// Print size
//------------------------------------------------------------------------------
static void
printSizes(const group_size_map& group_sizes)
{
  for (const auto& it : group_sizes)
    eos_static_debug("group=%s average=%.02f", it.first.c_str(),
                     (double)it.second.filled() * 100.0);
}

//------------------------------------------------------------------------------
// Picks two groups (source and target) randomly and schedule a file ID
// to be transferred
//------------------------------------------------------------------------------
void
GroupBalancer::prepareTransfer()
{
  FsGroup* fromGroup, *toGroup;
  auto&& [over_it, under_it] = mEngine->pickGroupsforTransfer();

  if (over_it.empty() || under_it.empty()) {
    eos_static_info("msg=\"engine gave us empty groups skipping\" "
                    "engine_status=%s",
                    mEngine->get_status_str(false, true).c_str());
    return;
  }

  {
    eos::common::RWMutexReadLock rlock(FsView::gFsView.ViewMutex);
    auto from_group_it = FsView::gFsView.mGroupView.find(over_it);
    auto to_group_it = FsView::gFsView.mGroupView.find(under_it);

    if (from_group_it == FsView::gFsView.mGroupView.end() ||
        to_group_it == FsView::gFsView.mGroupView.end()) {
      return;
    }

    fromGroup = from_group_it->second;
    toGroup = to_group_it->second;
  }

  auto file_info = chooseFileFromGroup(fromGroup, toGroup, mCfg.file_attempts);

  if (!file_info) {
    eos_static_info("msg=\"failed to choose any fid to schedule\" "
                    "failedgroup=%s", fromGroup->mName.c_str());
    return;
  }

  scheduleTransfer(file_info, fromGroup, toGroup);
}

//------------------------------------------------------------------------------
// Check if the sizes cache should be updated (based on the time passed since
// they were last updated)
//------------------------------------------------------------------------------
bool
GroupBalancer::cacheExpired()
{
  time_t currentTime = time(NULL);

  if (difftime(currentTime, mLastCheck) > CACHE_LIFE_TIME) {
    mLastCheck = currentTime;
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Schedule a pre-defined number of transfers
//------------------------------------------------------------------------------
void
GroupBalancer::prepareTransfers(int nrTransfers)
{
  int allowedTransfers = nrTransfers - mTransfers.size();

  for (int i = 0; i < allowedTransfers; i++) {
    prepareTransfer();
  }

  if (allowedTransfers > 0) {
    printSizes(mEngine->get_group_sizes());
  }
}

std::string
GroupBalancer::Status(bool detail, bool monitoring) const
{
  eos::common::RWMutexReadLock lock(mEngineMtx);
  return mEngine->get_status_str(detail, monitoring);
}

bool
GroupBalancer::is_valid_engine(std::string_view engine_name)
{
  return engine_name == "std" || engine_name == "minmax";
}

//------------------------------------------------------------------------------
// Appply configuration stored at the space level
//------------------------------------------------------------------------------
bool
GroupBalancer::Configure(FsSpace* const space, GroupBalancer::Config& cfg)
{
  cfg.is_enabled = space->GetConfigMember("groupbalancer") == "on";
  cfg.is_conv_enabled = space->GetConfigMember("converter") == "on";

  if (!cfg.is_enabled || !cfg.is_conv_enabled) {
    eos_static_info("msg=\"group balancer or converter not enabled\""
                    " space=%s balancer_status=%d converter_status=%d",
                    mSpaceName.c_str(), cfg.is_enabled, cfg.is_conv_enabled);
    return false;
  }

  cfg.num_tx = atoi(space->GetConfigMember("groupbalancer.ntx").c_str());
  cfg.mMinFileSize = common::StringConversion::GetSizeFromString(
                       space->GetConfigMember("groupbalancer.min_file_size"));
  cfg.mMaxFileSize = common::StringConversion::GetSizeFromString(
                       space->GetConfigMember("groupbalancer.max_file_size"));

  if (!cfg.mMaxFileSize) {
    eos_static_debug("%s", "msg=\"invalid Max File Size, using default\"");
    cfg.mMaxFileSize = GROUPBALANCER_MAX_FILE_SIZE;
  }

  cfg.engine_type = group_balancer::get_engine_type(
                      space->GetConfigMember("groupbalancer.engine"));
  cfg.file_attempts = atoi(
                        space->GetConfigMember("groupbalancer.file_attempts").c_str());

  if (!cfg.file_attempts) {
    eos_static_debug("%s", "msg=\"invalid File Attempts Count, using default\"");
    cfg.file_attempts = GROUPBALANCER_FILE_ATTEMPTS;
  }

  auto min_threshold_str = space->GetConfigMember("groupbalancer.min_threshold");
  auto max_threshold_str = space->GetConfigMember("groupbalancer.max_threshold");

  if (!group_balancer::is_valid_threshold(min_threshold_str, max_threshold_str)) {
    if (cfg.engine_type == BalancerEngineT::minmax) {
      eos_static_err("msg=\"invalid min/max balancer threshold configuration\""
                     " space=%s", mSpaceName.c_str());
      return false;
    }

    // This is a temporary stop gap until we force min/max threshold to be set
    // and remove this param. For std. balancer in case there isn't an explicit
    // min/max, let's set to configured threshold
    auto threshold_str = space->GetConfigMember("groupbalancer.threshold");

    if (!group_balancer::is_valid_threshold(threshold_str)) {
      eos_static_err("msg=\"invalid std balancer threshold configuration\""
                     " space=%s", mSpaceName.c_str());
      return false;
    }

    min_threshold_str = threshold_str;
    max_threshold_str = threshold_str;
  }

  mEngineConf.insert_or_assign("min_threshold", std::move(min_threshold_str));
  mEngineConf.insert_or_assign("max_threshold", std::move(max_threshold_str));
  return true;
}


//------------------------------------------------------------------------------
// Eternal loop trying to run conversion jobs
//------------------------------------------------------------------------------
void
GroupBalancer::GroupBalance(ThreadAssistant& assistant) noexcept
{
  uint64_t timeout_ns = 100 * 1e6; // 100 ms
  gOFS->WaitUntilNamespaceIsBooted();
  eos_static_info("%s", "msg=\"starting group balancer thread\"");
  eosGroupsInfoFetcher fetcher(mSpaceName);
  group_balancer::BalancerEngineT prev_engine_type {BalancerEngineT::stddev};
  bool engine_reconfigured = false;
  bool config_status = true;

  // Loop forever until cancelled
  while (!assistant.terminationRequested()) {
    bool expected_reconfiguration = true;
    assistant.wait_for(std::chrono::seconds(10));

    if (!gOFS->mMaster->IsMaster()) {
      assistant.wait_for(std::chrono::seconds(10));
      eos_static_debug("%s", "msg=\"group balancer disabled for slave\"");
      continue;
    }

    // Try to read lock the mutex
    while (!FsView::gFsView.ViewMutex.TimedRdLock(timeout_ns)) {
      if (assistant.terminationRequested()) {
        return;
      }
    }

    if (!FsView::gFsView.mSpaceGroupView.count(mSpaceName.c_str())) {
      FsView::gFsView.ViewMutex.UnLockRead();
      eos_static_warning("msg=\"no groups to balance\" space=\"%s\"",
                         mSpaceName.c_str());
      break;
    }

    // Update tracker for scheduled fid balance jobs
    gOFS->mFidTracker.DoCleanup(TrackerType::Balance);
    FsSpace* space = FsView::gFsView.mSpaceView[mSpaceName.c_str()];

    if (mDoConfigUpdate.compare_exchange_strong(expected_reconfiguration, false,
        std::memory_order_acq_rel)) {
      config_status = Configure(space, mCfg);
    }

    FsView::gFsView.ViewMutex.UnLockRead();

    if (!config_status) {
      continue;
    }

    if (prev_engine_type != mCfg.engine_type) {
      mEngine.reset(group_balancer::make_balancer_engine(mCfg.engine_type));
      engine_reconfigured = true;
      prev_engine_type = mCfg.engine_type;
    }

    mEngine->configure(mEngineConf);
    UpdateTransferList();

    if ((int) mTransfers.size() >= mCfg.num_tx) {
      continue;
    }

    eos_static_debug("msg=\"group balancer enabled\" ntx=%d ", mCfg.num_tx);

    if (cacheExpired() || engine_reconfigured) {
      {
        eos::common::RWMutexWriteLock lock(mEngineMtx);
        mEngine->populateGroupsInfo(fetcher.fetch());
      }
      printSizes(mEngine->get_group_sizes());

      if (engine_reconfigured) {
        eos_static_info("msg=\"group balancer engine reconfigured\"");
        engine_reconfigured = false;
      }
    }

    prepareTransfers(mCfg.num_tx);
  }
}

EOSMGMNAMESPACE_END
