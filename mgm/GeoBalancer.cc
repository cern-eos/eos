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

/*----------------------------------------------------------------------------*/
#include "mgm/GeoBalancer.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/XrdMgmOfsDirectory.hh"
#include "mgm/FsView.hh"
#include "common/StringConversion.hh"
#include "common/FileId.hh"
#include "common/LayoutId.hh"
/*----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysTimer.hh"
#include "XrdSys/XrdSysError.hh"
#include "XrdOuc/XrdOucTrace.hh"
#include "Xrd/XrdScheduler.hh"
/*----------------------------------------------------------------------------*/
#include <random>
#include <cmath>
/*----------------------------------------------------------------------------*/
extern XrdSysError gMgmOfsEroute;
extern XrdOucTrace gMgmOfsTrace;

#define CACHE_LIFE_TIME 300 // seconds

/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
GeoBalancer::GeoBalancer (const char* spacename)
: mThreshold (.5),
mAvgUsedSize (0)
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

  XrdSysThread::Run(&mThread,
                    GeoBalancer::StaticGeoBalancer,
                    static_cast<void *> (this),
                    XRDSYSTHREAD_HOLD,
                    "GeoBalancer Thread");
}

/*----------------------------------------------------------------------------*/
void
GeoBalancer::Stop ()
/*----------------------------------------------------------------------------*/
/**
 * @brief thread stop function
 */
/*----------------------------------------------------------------------------*/
{
  XrdSysThread::Cancel(mThread);
}

/*----------------------------------------------------------------------------*/
GeoBalancer::~GeoBalancer ()
/*----------------------------------------------------------------------------*/
/**
 * @brief Destructor
 */
/*----------------------------------------------------------------------------*/
{
  Stop();
  if (!gOFS->Shutdown)
  {
    XrdSysThread::Join(mThread, NULL);
  }
  clearCachedSizes();
}

/*----------------------------------------------------------------------------*/
void*
GeoBalancer::StaticGeoBalancer (void* arg)
/*----------------------------------------------------------------------------*/
/**
 * @brief Static thread startup function calling Convert
 */
/*----------------------------------------------------------------------------*/
{

  return reinterpret_cast<GeoBalancer*> (arg)->GeoBalance();
}

/*----------------------------------------------------------------------------*/
GeotagSize::GeotagSize (uint64_t usedBytes, uint64_t capacity)
: mSize (usedBytes),
  mCapacity (capacity)
/*----------------------------------------------------------------------------*/
/**
 * @brief GeotagSize constructor (capacity must be > 0)
 */
/*----------------------------------------------------------------------------*/
{
  assert(capacity > 0);
}

/*----------------------------------------------------------------------------*/
int
GeoBalancer::getRandom (int max)
/*----------------------------------------------------------------------------*/
/**
 * @brief Gets a random int between 0 and a given maximum
 * @param max the upper bound of the range within which the int will be
 *        generated
 */
/*----------------------------------------------------------------------------*/
{
  return (int) round(max * random() / (double) RAND_MAX);
}

/*----------------------------------------------------------------------------*/
void
GeoBalancer::clearCachedSizes ()
/*----------------------------------------------------------------------------*/
/**
 * @brief Clears the cache structures
 */
/*----------------------------------------------------------------------------*/
{
  mGeotagFs.clear();
  mFsGeotag.clear();

  std::map<std::string, GeotagSize*>::iterator it;
  for (it = mGeotagSizes.begin(); it != mGeotagSizes.end(); it++)
    delete (*it).second;
  mGeotagSizes.clear();
}

/*----------------------------------------------------------------------------*/
void
GeoBalancer::fillGeotagsByAvg ()
/*----------------------------------------------------------------------------*/
/**
 * @brief Fills mGeotagsOverAvg with the objects in mGeotagSizes, in case
 *        they're greater than the current mAvgUsedSize
 */
/*----------------------------------------------------------------------------*/
{
  mGeotagsOverAvg.clear();

  std::map<std::string, GeotagSize *>::const_iterator it;
  for (it = mGeotagSizes.cbegin(); it != mGeotagSizes.cend(); it++)
  {
    double geotagAvg = (*it).second->filled();
    if (geotagAvg - mAvgUsedSize > mThreshold)
      mGeotagsOverAvg.push_back((*it).first);
  }
}

static void
printSizes (const std::map<std::string, GeotagSize*> *sizes)
{
  std::map<std::string, GeotagSize *>::const_iterator it;

  for (it = sizes->cbegin(); it != sizes->cend(); it++)
    eos_static_info("geotag=%s average=%.02f", (*it).first.c_str(),
                    (double) (*it).second->filled() * 100.0);
}

/*----------------------------------------------------------------------------*/
void
GeoBalancer::populateGeotagsInfo ()
/*----------------------------------------------------------------------------*/
/**
 * @brief Fills mGeotagSizes, calculates the mAvgUsedSize and fills
 *        mGeotagsOverAvg
 */
/*----------------------------------------------------------------------------*/
{
  clearCachedSizes();

  const char *spaceName = mSpaceName.c_str();
  eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
  const FsSpace *spaceView = FsView::gFsView.mSpaceView[spaceName];

  if (spaceView->size() == 0)
  {
    eos_static_info ("No filesystems in space=%s", spaceName);
    return;
  }

  //std::set<eos::common::FileSystem::fsid_t>::const_iterator it;
  eos::mgm::BaseView::const_iterator it;

  for (it = spaceView->cbegin(); it != spaceView->cend(); it++)
  {
    FileSystem *fs = FsView::gFsView.mIdView[*it];

    if (fs->GetActiveStatus() != eos::common::FileSystem::kOnline)
      continue;

    eos::common::FileSystem::fs_snapshot_t snapshot;
    fs->SnapShotFileSystem(snapshot, false);

    if (snapshot.mStatus != eos::common::FileSystem::kBooted ||
        snapshot.mConfigStatus < eos::common::FileSystem::kRO)
      continue;

    mGeotagFs[snapshot.mGeoTag].push_back(*it);
    mFsGeotag[*it] = snapshot.mGeoTag;

    uint64_t capacity = snapshot.mDiskCapacity;
    uint64_t usedBytes = (uint64_t) (capacity - snapshot.mDiskFreeBytes);
    if (mGeotagSizes.count(snapshot.mGeoTag) == 0)
    {
      mGeotagSizes[snapshot.mGeoTag] = new GeotagSize(usedBytes, capacity);
    }
    else
    {
      uint64_t currentUsedBytes = mGeotagSizes[snapshot.mGeoTag]->usedBytes();
      uint64_t currentCapacity = mGeotagSizes[snapshot.mGeoTag]->capacity();
      mGeotagSizes[snapshot.mGeoTag]->setUsedBytes(currentUsedBytes + usedBytes);
      mGeotagSizes[snapshot.mGeoTag]->setCapacity(currentCapacity + capacity);
    }
  }

  mAvgUsedSize = 0;

  std::map<std::string, std::vector<eos::common::FileSystem::fsid_t>>::const_iterator git;
  for (git = mGeotagFs.cbegin(); git != mGeotagFs.cend(); git++)
  {
    const std::string geotag = (*git).first;
    const std::vector<eos::common::FileSystem::fsid_t> fsVector = (*git).second;
    mAvgUsedSize += mGeotagSizes[geotag]->filled();
  }

  mAvgUsedSize /= ((double) mGeotagSizes.size());

  eos_static_info("New average calculated: average=%.02f %%", mAvgUsedSize * 100.0);

  fillGeotagsByAvg();
}

/*----------------------------------------------------------------------------*/
bool
GeoBalancer::fileIsInDifferentLocations(const eos::FileMD *fmd)
/*----------------------------------------------------------------------------*/
/**
 * @brief Checks if a file is spread in more than one location
 * @param fmd the file metadata object
 * @return whether the file is in more than one location or not
 */
/*----------------------------------------------------------------------------*/
{
  eos::FileMD::LocationVector::const_iterator lociter;
  const std::string *geotag = 0;
  for (lociter = fmd->locationsBegin(); lociter != fmd->locationsEnd(); ++lociter)
  {
    // ignore filesystem id 0
    if (!(*lociter))
    {
      eos_static_err("fsid 0 found fid=%lld", fmd->getId());
      continue;
    }

    if (geotag == 0)
      geotag = &mFsGeotag[*lociter];
    else if (geotag->compare(mFsGeotag[*lociter]) != 0)
      return true;
  }

  return false;
}

/*----------------------------------------------------------------------------*/
std::string
GeoBalancer::getFileProcTransferNameAndSize (eos::common::FileId::fileid_t fid,
                                             uint64_t *size)
/*----------------------------------------------------------------------------*/
/**
 * @brief Produces a file conversion path to be placed in the proc directory
 *        and also returns its size
 * @param fid the file ID
 * @param size return address for the size of the file
 * @return the file path
 */
/*----------------------------------------------------------------------------*/
{
  char fileName[1024];
  eos::FileMD* fmd = 0;
  eos::common::LayoutId::layoutid_t layoutid = 0;
  eos::common::FileId::fileid_t fileid = 0;

  {
    eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
    try
    {
      fmd = gOFS->eosFileService->getFileMD(fid);
      layoutid = fmd->getLayoutId();
      fileid = fmd->getId();

      if (fmd->getContainerId() == 0)
        return std::string("");
    
      if (fmd->getSize() == 0)
        return std::string("");
       
      if (fmd->getNumLocation() == 0)
        return std::string("");
      
      if (fileIsInDifferentLocations(fmd))
      {
        eos_static_debug("filename=%s fid=%d is already in more than "
                         "one location", fmd->getName().c_str(), fileid);
        return std::string("");
      }

      if (size)
        *size = fmd->getSize();
    }
    catch (eos::MDException &e)
    {
      eos_static_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),
                       e.getMessage().str().c_str());
      return std::string("");
    }

    XrdOucString fileURI = gOFS->eosView->getUri(fmd).c_str();
    if (fileURI.beginswith(gOFS->MgmProcPath.c_str()))
    {
      // don't touch files in any ../proc/ directory
      return std::string("");
    }
    
    eos_static_debug("found file for transfering file=%s",
                     fileURI.c_str());
  }

  snprintf(fileName,
           1024,
           "%s/%016llx:%s#%08lx",
           gOFS->MgmProcConversionPath.c_str(),
           fileid,
           mSpaceName.c_str(),
           (unsigned long) layoutid);

  return std::string(fileName);
}

/*----------------------------------------------------------------------------*/
void
GeoBalancer::updateTransferList ()
/*----------------------------------------------------------------------------*/
/**
 * @brief For each entry in mTransfers, checks if the files' paths exist, if
 *        they don't, they are deleted from the mTransfers
 */
/*----------------------------------------------------------------------------*/
{
  std::map<eos::common::FileId::fileid_t, std::string>::iterator it;
  for (it = mTransfers.begin(); it != mTransfers.end(); it++)
  {
    eos::common::Mapping::VirtualIdentity rootvid;
    eos::common::Mapping::Root(rootvid);
    XrdOucErrInfo error;
    const std::string &fileName = (*it).second;
    struct stat buf;

    if (gOFS->_stat(fileName.c_str(), &buf, error, rootvid, ""))
      mTransfers.erase(it);
  }

  eos_static_info("scheduledtransfers=%d", mTransfers.size());
}

/*----------------------------------------------------------------------------*/
bool
GeoBalancer::scheduleTransfer (eos::common::FileId::fileid_t fid,
                               const std::string &fromGeotag)
/*----------------------------------------------------------------------------*/
/**
 * @brief Creates the conversion file in proc for the file ID, from the given
 *        fromGeotag (updates the cache structures)
 * @param fid the id of the file to be transferred
 * @param fromGeotag the geotag of the location where the file is located
 * @return whether the transfer file was successfully created or not
 */
/*----------------------------------------------------------------------------*/
{
  eos::common::Mapping::VirtualIdentity rootvid;
  eos::common::Mapping::Root(rootvid);
  XrdOucErrInfo mError;
  uint64_t size = 0;
  std::string fileName = getFileProcTransferNameAndSize(fid, &size);

  if (fileName == "")
    return false;

  if (!gOFS->_touch(fileName.c_str(), mError, rootvid, 0))
  {
    eos_static_info("scheduledfile=%s", fileName.c_str());
  }
  else
  {
    eos_static_err("msg=\"failed to schedule transfer\" schedulingfile=\"%s\"",
                   fileName.c_str());
  }

  mTransfers[fid] = fileName.c_str();

  uint64_t usedBytes = mGeotagSizes[fromGeotag]->usedBytes();
  mGeotagSizes[fromGeotag]->setUsedBytes(usedBytes - size);

  fillGeotagsByAvg();

  return true;
}

/*----------------------------------------------------------------------------*/
eos::common::FileId::fileid_t
GeoBalancer::chooseFidFromGeotag (const std::string &geotag)
/*----------------------------------------------------------------------------*/
/**
 * @brief Chooses a random file ID from a random filesystem in the given geotag
 * @param geotag the location's name from which the file id will be chosen
 * @return the chosen file ID
 */
/*----------------------------------------------------------------------------*/
{
  int rndIndex;
  eos::common::RWMutexReadLock vlock(FsView::gFsView.ViewMutex);
  eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
  const eos::FileSystemView::FileList *filelist = 0;
  std::vector<eos::common::FileSystem::fsid_t> &validFs = mGeotagFs[geotag];

  eos::common::FileSystem::fsid_t fsid;
  while (validFs.size() > 0)
  {
    rndIndex = getRandom(validFs.size() - 1);
    fsid = validFs[rndIndex];

    try
    {
      filelist = &gOFS->eosFsView->getFileList(fsid);
    }
    catch (eos::MDException &e)
    {
    }

    if (filelist && filelist->size() > 0)
      break;

    validFs.erase(validFs.begin() + rndIndex);
  }

  if (validFs.size() == 0)
  {
    mGeotagFs.erase(geotag);
    mGeotagSizes.erase(geotag);
    fillGeotagsByAvg();
  }

  if (!filelist || filelist->size() == 0)
    return -1;

  int attempts = 10;
  eos::FileSystemView::FileIterator fid_it;

  while (attempts-- > 0)
  {
    rndIndex = getRandom(filelist->size() - 1);
    fid_it = filelist->begin();
    std::advance(fid_it, rndIndex);

    if (mTransfers.count(*fid_it) == 0)
      return *fid_it;
  }

  return -1;
}

/*----------------------------------------------------------------------------*/
void
GeoBalancer::prepareTransfer ()
/*----------------------------------------------------------------------------*/
/**
 * @brief Picks a geotag randomly and schedule a file ID to be transferred
 */
/*----------------------------------------------------------------------------*/
{
  if (mGeotagsOverAvg.size() == 0)
  {
    eos_static_debug("No geotags over the average!");
    return;
  }

  int attempts = 10;
  while (attempts-- > 0)
  {
    int rndIndex = getRandom(mGeotagsOverAvg.size() - 1);
    std::vector<std::string>::const_iterator over_it = mGeotagsOverAvg.cbegin();
    std::advance(over_it, rndIndex);

    eos::common::FileId::fileid_t fid = chooseFidFromGeotag(*over_it);
    if ((int) fid == -1)
    {
      eos_static_debug("Couldn't choose any FID to schedule: failedgeotag=%s",
                       (*over_it).c_str());
      continue;
    }

    if (scheduleTransfer(fid, *over_it))
      break;
  }
}

/*----------------------------------------------------------------------------*/
bool
GeoBalancer::cacheExpired ()
/*----------------------------------------------------------------------------*/
/**
 * @brief Check if the sizes cache should be updated (based on the time passed
 *        since they were last updated)
 * @return whether the cache expired or not
 */
/*----------------------------------------------------------------------------*/
{
  time_t currentTime = time(NULL);

  if (difftime(currentTime, mLastCheck) > CACHE_LIFE_TIME)
  {
    mLastCheck = currentTime;
    return true;
  }

  return false;
}

/*----------------------------------------------------------------------------*/
void
GeoBalancer::prepareTransfers (int nrTransfers)
/*--------------------------------------------------------------------------*/
/**
 * @brief Schedule a pre-defined number of transfers
 */
/*--------------------------------------------------------------------------*/
{
  int allowedTransfers = nrTransfers - mTransfers.size();
  for (int i = 0; i < allowedTransfers; i++)
    prepareTransfer();

  if (allowedTransfers > 0)
    printSizes(&mGeotagSizes);
}

/*----------------------------------------------------------------------------*/
void*
GeoBalancer::GeoBalance ()
/*----------------------------------------------------------------------------*/
/**
 * @brief eternal loop trying to run conversion jobs
 */
/*----------------------------------------------------------------------------*/
{
  eos::common::Mapping::VirtualIdentity rootvid;
  eos::common::Mapping::Root(rootvid);
  XrdOucErrInfo error;
  XrdSysThread::SetCancelOn();
  // ---------------------------------------------------------------------------
  // wait that the namespace is initialized
  // ---------------------------------------------------------------------------
  bool go = false;
  do
  {
    XrdSysThread::SetCancelOff();
    {
      XrdSysMutexHelper(gOFS->InitializationMutex);
      if (gOFS->Initialized == gOFS->kBooted)
      {
        go = true;
      }
    }
    XrdSysThread::SetCancelOn();
    XrdSysTimer sleeper;
    sleeper.Wait(1000);
  }
  while (!go);

  XrdSysTimer sleeper;
  sleeper.Snooze(10);
  // ---------------------------------------------------------------------------
  // loop forever until cancelled
  // ---------------------------------------------------------------------------
  while (1)
  {
    bool isSpaceGeoBalancer = true;
    bool isMaster = true;
    int nrTransfers = 0;

    XrdSysThread::SetCancelOff();
    {
      // -----------------------------------------------------------------------
      // extract the current settings if conversion enabled and how many
      // conversion jobs should run
      // -----------------------------------------------------------------------
      eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);

      FsSpace* space = FsView::gFsView.mSpaceView[mSpaceName.c_str()];

      if (space->GetConfigMember("converter") != "on")
      {
        eos_static_debug("Converter is off for! It needs to be on "
                         "for the geotag balancer to work. space=%s",
                         mSpaceName.c_str());
        goto wait;
      }

      isSpaceGeoBalancer = space->GetConfigMember("geobalancer") == "on";

      nrTransfers = atoi(space->GetConfigMember("geobalancer.ntx").c_str());
      mThreshold =
        atof(space->GetConfigMember("geobalancer.threshold").c_str());
      mThreshold /= 100.0;
    }

    isMaster = gOFS->MgmMaster.IsMaster();

    if (isMaster && isSpaceGeoBalancer)
      eos_static_info("geobalancer is enabled ntx=%d ", nrTransfers);
    else
    {
      if (isMaster)
        eos_static_debug("geotag balancer is disabled");
      else
        eos_static_debug("geotag balancer is in slave mode");
    }

    if (isMaster && isSpaceGeoBalancer)
    {
      updateTransferList();
      if ((int) mTransfers.size() >= nrTransfers)
        goto wait;

      if (cacheExpired())
      {
        populateGeotagsInfo();
        printSizes(&mGeotagSizes);
      }

      prepareTransfers(nrTransfers);
    }

wait:
    XrdSysThread::SetCancelOn();
    // -------------------------------------------------------------------------
    // Let some time pass or wait for a notification
    // -------------------------------------------------------------------------
    XrdSysTimer sleeper;
    sleeper.Wait(10000);


    XrdSysThread::CancelPoint();
  }
  return 0;
}

EOSMGMNAMESPACE_END
