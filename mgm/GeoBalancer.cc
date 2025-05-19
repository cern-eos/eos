// ----------------------------------------------------------------------
// File: GeoBalancer.cc
// Author: Joaquim Rocha - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2013 CERN/Switzerland                                  *
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

#include "mgm/GeoBalancer.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/FsView.hh"
#include "mgm/convert/ConverterDriver.hh"
#include "namespace/interface/IFsView.hh"
#include "namespace/interface/IView.hh"
#include "namespace/Prefetcher.hh"
#include "common/StringConversion.hh"
#include "common/FileId.hh"
#include "common/LayoutId.hh"
#include "common/utils/RandUtils.hh"
#include <XrdSys/XrdSysError.hh>
#include <XrdOuc/XrdOucTrace.hh>
#include <Xrd/XrdScheduler.hh>
#include <random>
#include <cmath>

extern XrdSysError gMgmOfsEroute;
extern XrdOucTrace gMgmOfsTrace;

#define CACHE_LIFE_TIME 300 // seconds

/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
GeoBalancer::GeoBalancer(const char* spacename)
  : mThreshold(.5),
    mAvgUsedSize(0)
    /*----------------------------------------------------------------------------*/
    /**
     * @brief Constructor by space name
     *
     * @param spacename name of the associated space
     */
    /*----------------------------------------------------------------------------*/
{
  mSpaceName = spacename;
  mLastCheck = 0;
  mThread.reset(&GeoBalancer::GeoBalance, this);
}

/*----------------------------------------------------------------------------*/
void
GeoBalancer::Stop()
/*----------------------------------------------------------------------------*/
/**
 * @brief thread stop function
 */
/*----------------------------------------------------------------------------*/
{
  mThread.join();
}

/*----------------------------------------------------------------------------*/
GeoBalancer::~GeoBalancer()
/*----------------------------------------------------------------------------*/
/**
 * @brief Destructor
 */
/*----------------------------------------------------------------------------*/
{
  Stop();
  clearCachedSizes();
}

/*----------------------------------------------------------------------------*/
GeotagSize::GeotagSize(uint64_t usedBytes, uint64_t capacity)
  : mSize(usedBytes),
    mCapacity(capacity)
    /*----------------------------------------------------------------------------*/
    /**
     * @brief GeotagSize constructor (capacity must be > 0)
     */
    /*----------------------------------------------------------------------------*/
{
  assert(capacity > 0);
}


/*----------------------------------------------------------------------------*/
void
GeoBalancer::clearCachedSizes()
/*----------------------------------------------------------------------------*/
/**
 * @brief Clears the cache structures
 */
/*----------------------------------------------------------------------------*/
{
  mGeotagFs.clear();
  mFsGeotag.clear();
  std::map<std::string, GeotagSize*>::iterator it;

  for (it = mGeotagSizes.begin(); it != mGeotagSizes.end(); it++) {
    delete(*it).second;
  }

  mGeotagSizes.clear();
}

/*----------------------------------------------------------------------------*/
void
GeoBalancer::fillGeotagsByAvg()
/*----------------------------------------------------------------------------*/
/**
 * @brief Fills mGeotagsOverAvg with the objects in mGeotagSizes, in case
 *        they're greater than the current mAvgUsedSize
 */
/*----------------------------------------------------------------------------*/
{
  mGeotagsOverAvg.clear();
  std::map<std::string, GeotagSize*>::const_iterator it;

  for (it = mGeotagSizes.cbegin(); it != mGeotagSizes.cend(); it++) {
    double geotagAvg = (*it).second->filled();

    if (geotagAvg - mAvgUsedSize > mThreshold) {
      mGeotagsOverAvg.push_back((*it).first);
    }
  }
}

static void
printSizes(const std::map<std::string, GeotagSize*>* sizes)
{
  std::map<std::string, GeotagSize*>::const_iterator it;

  for (it = sizes->cbegin(); it != sizes->cend(); it++)
    eos_static_info("geotag=%s average=%.02f", (*it).first.c_str(),
                    (double)(*it).second->filled() * 100.0);
}

/*----------------------------------------------------------------------------*/
void
GeoBalancer::populateGeotagsInfo()
/*----------------------------------------------------------------------------*/
/**
 * @brief Fills mGeotagSizes, calculates the mAvgUsedSize and fills
 *        mGeotagsOverAvg
 */
/*----------------------------------------------------------------------------*/
{
  clearCachedSizes();
  const char* spaceName = mSpaceName.c_str();
  eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
  const FsSpace* spaceView = FsView::gFsView.mSpaceView[spaceName];

  if (spaceView->size() == 0) {
    eos_static_info("msg=\"no filesystems in space\" space=%s", spaceName);
    return;
  }

  for (auto it = spaceView->cbegin(); it != spaceView->cend(); it++) {
    FileSystem* fs = FsView::gFsView.mIdView.lookupByID(*it);

    if (!fs || (fs->GetActiveStatus() != eos::common::ActiveStatus::kOnline)) {
      continue;
    }

    eos::common::FileSystem::fs_snapshot_t snapshot;
    fs->SnapShotFileSystem(snapshot, false);

    if (snapshot.mStatus != eos::common::BootStatus::kBooted ||
        snapshot.mConfigStatus < eos::common::ConfigStatus::kRO ||
        snapshot.mGeoTag.empty()) {
      continue;
    }

    mGeotagFs[snapshot.mGeoTag].push_back(*it);
    mFsGeotag[*it] = snapshot.mGeoTag;
    uint64_t capacity = snapshot.mDiskCapacity;
    uint64_t usedBytes = (uint64_t)(capacity - snapshot.mDiskFreeBytes);

    if (mGeotagSizes.count(snapshot.mGeoTag) == 0) {
      mGeotagSizes[snapshot.mGeoTag] = new GeotagSize(usedBytes, capacity);
    } else {
      uint64_t currentUsedBytes = mGeotagSizes[snapshot.mGeoTag]->usedBytes();
      uint64_t currentCapacity = mGeotagSizes[snapshot.mGeoTag]->capacity();
      mGeotagSizes[snapshot.mGeoTag]->setUsedBytes(currentUsedBytes + usedBytes);
      mGeotagSizes[snapshot.mGeoTag]->setCapacity(currentCapacity + capacity);
    }
  }

  mAvgUsedSize = 0;
  std::map<std::string, std::vector<eos::common::FileSystem::fsid_t>>::const_iterator
      git;

  for (git = mGeotagFs.cbegin(); git != mGeotagFs.cend(); git++) {
    const std::string geotag = (*git).first;
    const std::vector<eos::common::FileSystem::fsid_t> fsVector = (*git).second;
    mAvgUsedSize += mGeotagSizes[geotag]->filled();
  }

  mAvgUsedSize /= ((double) mGeotagSizes.size());
  eos_static_info("msg=\"geo_balancer update average fill\" average=%.02f %%",
                  mAvgUsedSize * 100.0);
  fillGeotagsByAvg();
}

/*----------------------------------------------------------------------------*/
/**
 * @brief Checks if a file is spread in more than one location
 * @param fmd the file metadata object
 * @return whether the file is in more than one location or not
 */
/*----------------------------------------------------------------------------*/
bool
GeoBalancer::fileIsInDifferentLocations(const eos::IFileMD* fmd)
{
  const std::string* geotag = 0;
  eos::IFileMD::LocationVector::const_iterator lociter;
  eos::IFileMD::LocationVector loc_vect = fmd->getLocations();

  for (lociter = loc_vect.begin(); lociter != loc_vect.end(); ++lociter) {
    // ignore filesystem id 0
    if (!(*lociter)) {
      eos_static_err("msg=\"fsid 0 found\" fxid=%08llx", fmd->getId());
      continue;
    }

    // Ignore EOS_TAPE_FSID
    if (EOS_TAPE_FSID == *lociter) {
      eos_static_debug("msg=\"skip tape fsid\" fxid=%08llx", fmd->getId());
      continue;
    }

    if (geotag == 0) {
      geotag = &mFsGeotag[*lociter];
    } else if (geotag->compare(mFsGeotag[*lociter]) != 0) {
      return true;
    }
  }

  return false;
}

/*----------------------------------------------------------------------------*/
/**
 * @brief Produces a file conversion path to be placed in the proc directory
 *        and also returns its size
 * @param fid the file ID
 * @param size return address for the size of the file
 * @return the file path
 */
/*----------------------------------------------------------------------------*/
std::string
GeoBalancer::getFileProcTransferNameAndSize(eos::common::FileId::fileid_t fid,
    uint64_t* size)
{
  char fileName[1024];
  std::string file_uri;
  std::shared_ptr<eos::IFileMD> fmd;
  eos::common::LayoutId::layoutid_t layoutid = 0;
  {
    eos::Prefetcher::prefetchFileMDWithParentsAndWait(gOFS->eosView, fid);

    try {
      fmd = gOFS->eosFileService->getFileMD(fid);
      // Don't lock the file before getting its URI
      file_uri = gOFS->eosView->getUri(fmd.get()).c_str();
      // Now we can lock the file
      eos::MDLocking::FileReadLock fmdLock(fmd.get());
      layoutid = fmd->getLayoutId();

      if ((fmd->getContainerId() == 0) ||
          (fmd->getNumLocation() == 0) ||
          (fmd->getSize() == 0)) {
        return std::string("");
      }

      if (fileIsInDifferentLocations(fmd.get())) {
        eos_static_debug("msg=\"file is already in more than one location\" "
                         "name=%s fxid=%08llx", fmd->getName().c_str(), fid);
        return std::string("");
      }

      if (size) {
        *size = fmd->getSize();
      }

      // Don't touch files in any ../proc/ directory
      if (file_uri.rfind(gOFS->MgmProcPath.c_str(), 0) == 0) {
        return std::string("");
      }
    } catch (eos::MDException& e) {
      eos_static_debug("msg=\"exception\" ec=%d emsg=\"%s\"", e.getErrno(),
                       e.getMessage().str().c_str());
      return std::string("");
    }
  }
  eos_static_debug("msg=\"found file to geobalance\" path=%s",
                   file_uri.c_str());
  snprintf(fileName, 1024, "%s/%016llx:%s#%08lx",
           gOFS->MgmProcConversionPath.c_str(), fid, mSpaceName.c_str(),
           (unsigned long) layoutid);
  return std::string(fileName);
}

//------------------------------------------------------------------------------
// Update the list of ongoing transfers
//------------------------------------------------------------------------------
void
GeoBalancer::updateTransferList()
{
  // Update tracker for scheduled jobs if using new converter
  gOFS->mFidTracker.DoCleanup(TrackerType::Convert);

  for (auto it = mTransfers.begin(); it != mTransfers.end();) {
    if (!gOFS->mFidTracker.HasEntry(it->first)) {
      mTransfers.erase(it++);
    } else {
      ++it;
    }
  }

  eos_static_info("msg=\"geo_balancer update transfers\" scheduled_transfers=%d",
                  mTransfers.size());
}

//------------------------------------------------------------------------------
// Creates the conversion file in proc for the file ID, from the given
// fromGeotag (updates the cache structures).
//
// @note: All this works based on the assumption that kScattered is the default
// placement policy.
//------------------------------------------------------------------------------
bool
GeoBalancer::scheduleTransfer(eos::common::FileId::fileid_t fid,
                              const std::string& fromGeotag)
{
  uint64_t size = 0;
  std::string file_path = getFileProcTransferNameAndSize(fid, &size);

  if (file_path == "") {
    return false;
  }

  std::string conv_tag = file_path;
  conv_tag += "^geobalancer^";
  conv_tag.erase(0, gOFS->MgmProcConversionPath.length() + 1);

  if (gOFS->mConverterDriver->ScheduleJob(fid, conv_tag)) {
    eos_static_info("msg=\"geo_balancer scheduled job\" file=\"%s\" "
                    "from_geotag=\"%s\"", conv_tag.c_str(),
                    fromGeotag.c_str());
  } else {
    eos_static_err("msg=\"geo_balancer failed to schedule job\" "
                   "file=\"%s\" from_geotag=\"%s\"", conv_tag.c_str(),
                   fromGeotag.c_str());
    return false;
  }

  mTransfers[fid] = file_path.c_str();
  uint64_t usedBytes = mGeotagSizes[fromGeotag]->usedBytes();
  mGeotagSizes[fromGeotag]->setUsedBytes(usedBytes - size);
  fillGeotagsByAvg();
  return true;
}

/*----------------------------------------------------------------------------*/
eos::common::FileId::fileid_t
GeoBalancer::chooseFidFromGeotag(const std::string& geotag)
/*----------------------------------------------------------------------------*/
/**
 * @brief Chooses a random file ID from a random filesystem in the given geotag
 * @param geotag the location's name from which the file id will be chosen
 * @return the chosen file ID
 */
/*----------------------------------------------------------------------------*/
{
  int rndIndex;
  bool found = false;
  uint64_t fsid_size = 0ull;
  eos::common::FileSystem::fsid_t fsid = 0;
  eos::common::RWMutexReadLock vlock(FsView::gFsView.ViewMutex);
  std::vector<eos::common::FileSystem::fsid_t>& validFs = mGeotagFs[geotag];
  // TODO(gbitzes): Add prefetching here.

  while (validFs.size() > 0) {
    rndIndex = eos::common::getRandom(0ul, validFs.size() - 1);
    fsid = validFs[rndIndex];
    fsid_size = gOFS->eosFsView->getNumFilesOnFs(fsid);

    if (fsid_size) {
      found = true;
      break;
    }

    validFs.erase(validFs.begin() + rndIndex);
  }

  if (validFs.size() == 0) {
    mGeotagFs.erase(geotag);
    mGeotagSizes.erase(geotag);
    fillGeotagsByAvg();
  }

  if (!found) {
    return -1;
  }

  int attempts = 10;

  while (attempts-- > 0) {
    eos::IFileMD::id_t randomPick;

    if (gOFS->eosFsView->getApproximatelyRandomFileInFs(fsid, randomPick) &&
        mTransfers.count(randomPick) == 0) {
      return randomPick;
    }
  }

  return -1;
}

/*----------------------------------------------------------------------------*/
void
GeoBalancer::prepareTransfer()
/*----------------------------------------------------------------------------*/
/**
 * @brief Picks a geotag randomly and schedule a file ID to be transferred
 */
/*----------------------------------------------------------------------------*/
{
  if (mGeotagsOverAvg.size() == 0) {
    eos_static_debug("%s", "msg=\"no geotags above average\"");
    return;
  }

  int attempts = 10;

  while (attempts-- > 0) {
    int rndIndex = eos::common::getRandom(0ul, mGeotagsOverAvg.size() - 1);
    std::vector<std::string>::const_iterator over_it = mGeotagsOverAvg.cbegin();
    std::advance(over_it, rndIndex);
    // TODO: this loop should be improved not to request the file list too
    // many times in a tight loop
    eos::common::FileId::fileid_t fid = chooseFidFromGeotag(*over_it);

    if ((int) fid == -1) {
      eos_static_debug("msg=\"no fid found to schedule\" failed_geotag=%s",
                       (*over_it).c_str());
      continue;
    }

    if (scheduleTransfer(fid, *over_it)) {
      break;
    }
  }
}

/*----------------------------------------------------------------------------*/
bool
GeoBalancer::cacheExpired()
/*----------------------------------------------------------------------------*/
/**
 * @brief Check if the sizes cache should be updated (based on the time passed
 *        since they were last updated)
 * @return whether the cache expired or not
 */
/*----------------------------------------------------------------------------*/
{
  time_t currentTime = time(NULL);

  if (difftime(currentTime, mLastCheck) > CACHE_LIFE_TIME) {
    mLastCheck = currentTime;
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
//Schedule a pre-defined number of transfers
//------------------------------------------------------------------------------
void
GeoBalancer::prepareTransfers(int nrTransfers)
{
  int allowedTransfers = nrTransfers - mTransfers.size();

  for (int i = 0; i < allowedTransfers; i++) {
    prepareTransfer();
  }

  if (allowedTransfers > 0) {
    printSizes(&mGeotagSizes);
  }
}

//------------------------------------------------------------------------------
//! @brief eternal loop trying to run conversion jobs
//------------------------------------------------------------------------------
void
GeoBalancer::GeoBalance(ThreadAssistant& assistant) noexcept
{
  ThreadAssistant::setSelfThreadName("GeoBalancer");
  eos::common::VirtualIdentity rootvid = eos::common::VirtualIdentity::Root();
  gOFS->WaitUntilNamespaceIsBooted(assistant);
  assistant.wait_for(std::chrono::seconds(10));

  // Loop forever until cancelled
  while (!assistant.terminationRequested()) {
    bool is_enabled = true;
    int nrTransfers = 0;
    FsSpace* space {nullptr};
    decltype(FsView::gFsView.mSpaceView.begin()) it_space;

    if (!gOFS->mMaster->IsMaster()) {
      eos_static_debug("%s", "msg=\"geo balancer is disabled for slave\"");
      goto wait;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    FsView::gFsView.ViewMutex.LockRead();

    if (!FsView::gFsView.mSpaceGroupView.count(mSpaceName.c_str())) {
      FsView::gFsView.ViewMutex.UnLockRead();
      eos_static_warning("msg=\"no space to geo balance\" space=\"%s\"",
                         mSpaceName.c_str());
      break;
    }

    it_space = FsView::gFsView.mSpaceView.find(mSpaceName.c_str());

    if (it_space == FsView::gFsView.mSpaceView.end()) {
      eos_static_err("msg=\"geo_balancer terminating, no such space\" space=%s",
                     mSpaceName.c_str());
      break;
    }

    space = it_space->second;

    if (space->GetConfigMember("converter") != "on") {
      eos_static_debug("msg=\"geo balancer disabled since it needs the "
                       "converter enabled to work and it's not\" space=%s",
                       mSpaceName.c_str());
      FsView::gFsView.ViewMutex.UnLockRead();
      goto wait;
    }

    // Extract the current settings if conversion enabled and how many
    // conversion jobs should run
    is_enabled = space->GetConfigMember("geobalancer") == "on";
    nrTransfers = atoi(space->GetConfigMember("geobalancer.ntx").c_str());
    mThreshold = atof(space->GetConfigMember("geobalancer.threshold").c_str());
    mThreshold /= 100.0;
    FsView::gFsView.ViewMutex.UnLockRead();

    if (is_enabled) {
      eos_static_info("msg=\"geo balancer is enabled\" ntx=%d ", nrTransfers);
      updateTransferList();

      if ((int) mTransfers.size() >= nrTransfers) {
        goto wait;
      }

      if (cacheExpired()) {
        populateGeotagsInfo();
        printSizes(&mGeotagSizes);
      }

      prepareTransfers(nrTransfers);
    } else {
      eos_static_debug("%s", "msg=\"geo balancer is disabled\"");
    }

wait:
    // Let some time pass or wait for a notification
    assistant.wait_for(std::chrono::seconds(10));

    if (assistant.terminationRequested()) {
      return;
    }
  }
}

EOSMGMNAMESPACE_END
