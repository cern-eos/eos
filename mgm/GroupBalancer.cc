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

extern XrdSysError gMgmOfsEroute;
extern XrdOucTrace gMgmOfsTrace;

#define CACHE_LIFE_TIME 60 // seconds

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// GroupSize constructor (capacity must be > 0)
//------------------------------------------------------------------------------
GroupSize::GroupSize(uint64_t usedBytes, uint64_t capacity)
  : mSize(usedBytes), mCapacity(capacity)
{
  assert(capacity > 0);
}

//------------------------------------------------------------------------------
// Subtracts the given size from this group and adds it to the given toGroup
//------------------------------------------------------------------------------
void
GroupSize::swapFile(GroupSize* toGroup, uint64_t size)
{
  toGroup->mSize += size;
  mSize -= size;
}

//-------------------------------------------------------------------------------
// GroupBalancer constructor
//-------------------------------------------------------------------------------
GroupBalancer::GroupBalancer(const char* spacename)
  : mSpaceName(spacename), mAvgUsedSize(0), mLastCheck(0)
{
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
  clearCachedSizes();
}

//------------------------------------------------------------------------------
// Gets a random int between 0 and a given maximum
//------------------------------------------------------------------------------
int
GroupBalancer::getRandom(int max)
{
  return (int) round(max * random() / (double) RAND_MAX);
}

//------------------------------------------------------------------------------
// Recalculates the sizes average from the mGroupSizes
//------------------------------------------------------------------------------
void
GroupBalancer::recalculateAvg()
{
  mAvgUsedSize = 0;

  for (auto size_it = mGroupSizes.cbegin(); size_it != mGroupSizes.cend();
       ++size_it) {
    mAvgUsedSize += (*size_it).second->filled();
  }

  mAvgUsedSize /= (double) mGroupSizes.size();
  eos_static_debug("New average calculated: %.02f %%", mAvgUsedSize * 100.0);
}

//------------------------------------------------------------------------------
// Deletes all the GrouSize objects stored in mGroupSizes and empties it
//------------------------------------------------------------------------------
void
GroupBalancer::clearCachedSizes()
{
  for (auto it = mGroupSizes.begin(); it != mGroupSizes.end(); ++it) {
    delete(*it).second;
  }

  mGroupSizes.clear();
  mGroupsOverAvg.clear();
  mGroupsUnderAvg.clear();
}

//------------------------------------------------------------------------------
// Places group in mGroupsOverAvg or mGroupsUnderAvg in case they're greater
// than or less than the current mAvgUsedSize, respectively.
//------------------------------------------------------------------------------
void
GroupBalancer::updateGroupAvgCache(const std::string& group_name)

{
  auto kv = mGroupSizes.find(group_name);
  if (kv == mGroupSizes.end()) {
    return;
  }

  GroupSize* groupSize = kv->second;
  double diffWithAvg = ((double) groupSize->filled()
                        - ((double) mAvgUsedSize));

  // set erase only erases if found, so this is safe without key checking
  mGroupsOverAvg.erase(group_name);
  mGroupsUnderAvg.erase(group_name);

  eos_static_debug("diff=%.02f threshold=%.02f", diffWithAvg, cfg.mThreshold);

  // Group is mThreshold over or under the average used size
  if (abs(diffWithAvg) > cfg.mThreshold) {
    if (diffWithAvg > 0) {
      mGroupsOverAvg.emplace(group_name);
    } else {
      mGroupsUnderAvg.emplace(group_name);
    }
  }
}

//------------------------------------------------------------------------------
// Fills mGroupsOverAvg and mGroupsUnderAvg with the objects in mGroupSizes,
// in case they're greater than or less than the current mAvgUsedSize,
// respectively
//------------------------------------------------------------------------------
void
GroupBalancer::fillGroupsByAvg()

{
  mGroupsOverAvg.clear();
  mGroupsUnderAvg.clear();

  if (mGroupSizes.size() == 0) {
    return;
  }

  for (auto size_it = mGroupSizes.cbegin(); size_it != mGroupSizes.cend();
       ++size_it) {
    const std::string& name = (*size_it).first;
    updateGroupAvgCache(name);
  }
}

//------------------------------------------------------------------------------
// Fills mGroupSizes, calculates the mAvgUsedSize and fills mGroupsUnderAvg and
// mGroupsOverAvg
//------------------------------------------------------------------------------
void
GroupBalancer::populateGroupsInfo()
{
  const char* spaceName = mSpaceName.c_str();
  eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
  mAvgUsedSize = 0;
  clearCachedSizes();

  if (FsView::gFsView.mSpaceGroupView.count(spaceName) == 0) {
    eos_static_err("No such space %s", spaceName);
    return;
  }

  auto set_fsgrp = FsView::gFsView.mSpaceGroupView[spaceName];

  for (auto it = set_fsgrp.cbegin(); it != set_fsgrp.cend(); it++) {
    if ((*it)->GetConfigMember("status") != "on") {
      continue;
    }

    uint64_t size = (*it)->AverageDouble("stat.statfs.usedbytes", false);
    uint64_t capacity = (*it)->AverageDouble("stat.statfs.capacity", false);

    if (capacity == 0) {
      continue;
    }

    mGroupSizes[(*it)->mName] = new GroupSize(size, capacity);
    mAvgUsedSize += mGroupSizes[(*it)->mName]->filled();
  }

  if (mGroupSizes.size() == 0) {
    mAvgUsedSize = 0;
    eos_static_debug("No groups to be balanced!");
    return;
  }

  mAvgUsedSize /= ((double) mGroupSizes.size());
  eos_static_debug("New average calculated: %.02f %%", mAvgUsedSize * 100.0);
  fillGroupsByAvg();
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
    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__,
                                      __FILE__);

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

      eos_static_debug("found file for transfering file=%s",
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
GroupBalancer::scheduleTransfer(eos::common::FileId::fileid_t fid,
                                FsGroup* sourceGroup, FsGroup* targetGroup)
{
  if ((mGroupSizes.count(sourceGroup->mName) == 0) ||
      (mGroupSizes.count(targetGroup->mName) == 0)) {
    eos_static_err("msg=\"no src/trg group in map\" src_group=%s trg_group=%s",
                   sourceGroup->mName.c_str(), targetGroup->mName.c_str());
    return;
  }

  eos::common::VirtualIdentity rootvid = eos::common::VirtualIdentity::Root();
  XrdOucErrInfo mError;
  uint64_t size = -1;
  std::string fileName = getFileProcTransferNameAndSize(fid, targetGroup, &size);

  if (fileName == "") {
    return;
  }

  // Use new converter if available
  if (gOFS->mConverterDriver) {
    // Push conversion job to QuarkDB
    std::string conv_tag = fileName;
    conv_tag.erase(0, gOFS->MgmProcConversionPath.length() + 1);

    if (gOFS->mConverterDriver->ScheduleJob(fid, conv_tag)) {
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
    if (!gOFS->_touch(fileName.c_str(), mError, rootvid, 0)) {
      eos_static_info("scheduledfile=%s src_group=%s trg_group=%s",
                      fileName.c_str(), sourceGroup->mName.c_str(),
                      targetGroup->mName.c_str());
    } else {
      eos_static_err("msg=\"failed to schedule transfer\" schedulingfile=\"%s\"",
                     fileName.c_str());
      return;
    }
  }

  mTransfers[fid] = fileName.c_str();
  mGroupSizes[sourceGroup->mName]->swapFile(mGroupSizes[targetGroup->mName],
      size);
  updateGroupAvgCache(sourceGroup->mName);
  updateGroupAvgCache(targetGroup->mName);
}

//------------------------------------------------------------------------------
// Creates the conversion file in proc for the file ID, from the given
// sourceGroup, to the targetGroup (and updates the cache structures)
//------------------------------------------------------------------------------
void
GroupBalancer::scheduleTransfer(const FileInfo& file_info,
                                FsGroup* sourceGroup, FsGroup* targetGroup)
{
  if ((mGroupSizes.count(sourceGroup->mName) == 0) ||
      (mGroupSizes.count(targetGroup->mName) == 0)) {
    eos_static_err("msg=\"no src/trg group in map\" src_group=%s trg_group=%s",
                   sourceGroup->mName.c_str(), targetGroup->mName.c_str());
    return;
  }

  // Use new converter if available
  if (gOFS->mConverterDriver) {
    // Push conversion job to QuarkDB
    std::string conv_tag = file_info.filename;
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
  mGroupSizes[sourceGroup->mName]->swapFile(mGroupSizes[targetGroup->mName],
      file_info.filesize);
  updateGroupAvgCache(sourceGroup->mName);
  updateGroupAvgCache(targetGroup->mName);
}

//------------------------------------------------------------------------------
// Chooses a random file ID from a random filesystem in the given group
//------------------------------------------------------------------------------
eos::common::FileId::fileid_t
GroupBalancer::chooseFidFromGroup(FsGroup* group)
{
  int rndIndex;
  bool found = false;
  uint64_t fsid_size = 0ull;
  eos::common::FileSystem::fsid_t fsid = 0;
  eos::common::RWMutexReadLock vlock(FsView::gFsView.ViewMutex);
  eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex, __FUNCTION__, __LINE__,
                                    __FILE__);
  // TODO(gbitzes): Add prefetching, make more efficient.
  std::vector<int> validFsIndexes(group->size());

  for (size_t i = 0; i < group->size(); i++) {
    validFsIndexes[i] = (int) i;
  }

  eos::mgm::BaseView::const_iterator fs_it;

  while (validFsIndexes.size() > 0) {
    fs_it = group->begin();
    rndIndex = getRandom(validFsIndexes.size() - 1);
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
GroupBalancer::chooseFileFromGroup(FsGroup *group, int attempts)
{
  uint64_t filesize;

  while (attempts-- > 0) {
    auto fid = chooseFidFromGroup(group);

    if (!fid)
    {
      continue;
    }

    auto filename = getFileProcTransferNameAndSize(fid, group, &filesize);

    if (filename.empty() ||
        (cfg.mMinFileSize > filesize) ||
        (cfg.mMaxFileSize < filesize)) {
      continue;
    }

    // We've a hit!
    return {fid,std::move(filename), filesize};
  }

  return {};
}

//------------------------------------------------------------------------------
// Print size
//------------------------------------------------------------------------------
static void
printSizes(const std::map<std::string, GroupSize*>* sizes)
{
  std::map<std::string, GroupSize*>::const_iterator it;

  for (it = sizes->cbegin(); it != sizes->cend(); it++)
    eos_static_debug("group=%s average=%.02f", (*it).first.c_str(),
                     (double)(*it).second->filled() * 100.0);
}

//------------------------------------------------------------------------------
// Picks two groups (source and target) randomly and schedule a file ID
// to be transferred
//------------------------------------------------------------------------------
void
GroupBalancer::prepareTransfer()
{
  FsGroup* fromGroup, *toGroup;

  if (mGroupsUnderAvg.size() == 0 || mGroupsOverAvg.size() == 0) {
    if (mGroupsOverAvg.size() == 0) {
      eos_static_debug("No groups over the average!");
    }

    if (mGroupsUnderAvg.size() == 0) {
      eos_static_debug("No groups under the average!");
    }

    recalculateAvg();
    return;
  }

  auto over_it = mGroupsOverAvg.begin();
  auto under_it = mGroupsUnderAvg.begin();
  int rndIndex = getRandom(mGroupsOverAvg.size() - 1);
  std::advance(over_it, rndIndex);
  rndIndex = getRandom(mGroupsUnderAvg.size() - 1);
  std::advance(under_it, rndIndex);
  fromGroup = FsView::gFsView.mGroupView[*over_it];
  toGroup = FsView::gFsView.mGroupView[*under_it];

  if (fromGroup->size() == 0) {
    return;
  }

  auto file_info = chooseFileFromGroup(fromGroup);

  if (!file_info) {
    eos_static_info("Couldn't choose any FID to schedule: failedgroup=%s",
                    fromGroup->mName.c_str());
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
    printSizes(&mGroupSizes);
  }
}

void
GroupBalancer::Configure(FsSpace* const space, GroupBalancer::Config& cfg)
{
  cfg.is_enabled = space->GetConfigMember("groupbalancer") == "on";
  cfg.is_conv_enabled = space->GetConfigMember("converter") == "on";
  cfg.num_tx = atoi(space->GetConfigMember("groupbalancer.ntx").c_str());
  cfg.mThreshold = atof(space->GetConfigMember("groupbalancer.threshold").c_str()) /
    100.0;
  cfg.mMinFileSize = common::StringConversion::GetSizeFromString(space->GetConfigMember("groupbalancer.min_file_size"));
  cfg.mMaxFileSize = common::StringConversion::GetSizeFromString(space->GetConfigMember("groupbalancer.max_file_size"));
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

  // Loop forever until cancelled
  while (!assistant.terminationRequested()) {
    assistant.wait_for(std::chrono::seconds(10));

    if (!gOFS->mMaster->IsMaster()) {
      assistant.wait_for(std::chrono::seconds(10));
      eos_static_debug("%s", "msg=\"group balancer disabled for slave mode\"");
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
    GroupBalancer::Configure(space, cfg);

    if (!cfg.is_enabled || !cfg.is_conv_enabled) {
      FsView::gFsView.ViewMutex.UnLockRead();
      eos_static_debug("msg=\"group balancer or converter not enabled\" space=\"%s\""
                       " balancer status=%d converter status=%d",
                       mSpaceName.c_str(), cfg.is_enabled, cfg.is_conv_enabled);
      continue;
    }

    FsView::gFsView.ViewMutex.UnLockRead();
    eos_static_info("msg=\"group balancer enabled\" ntx=%d ", cfg.num_tx);
    UpdateTransferList();

    if ((int) mTransfers.size() >= cfg.num_tx) {
      continue;
    }

    if (cacheExpired()) {
      populateGroupsInfo();
      printSizes(&mGroupSizes);
    } else {
      recalculateAvg();
    }

    prepareTransfers(cfg.num_tx);
  }
}

EOSMGMNAMESPACE_END
