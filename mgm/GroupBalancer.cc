// ----------------------------------------------------------------------
// File: GroupBalancer.cc
// Author: Joaquim Rocha - CERN
// ----------------------------------------------------------------------

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

/*----------------------------------------------------------------------------*/
#include "mgm/GroupBalancer.hh"
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

#define CACHE_LIFE_TIME 60 // seconds

/*----------------------------------------------------------------------------*/
EOSMGMNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
GroupBalancer::GroupBalancer (const char* spacename)
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
                    GroupBalancer::StaticGroupBalancer,
                    static_cast<void *> (this),
                    XRDSYSTHREAD_HOLD,
                    "GroupBalancer Thread");
}

/*----------------------------------------------------------------------------*/
void
GroupBalancer::Stop ()
/*----------------------------------------------------------------------------*/
/**
 * @brief thread stop function
 */
/*----------------------------------------------------------------------------*/
{
  XrdSysThread::Cancel(mThread);
}

/*----------------------------------------------------------------------------*/
GroupBalancer::~GroupBalancer ()
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
GroupBalancer::StaticGroupBalancer (void* arg)
/*----------------------------------------------------------------------------*/
/**
 * @brief Static thread startup function calling Convert
 */
/*----------------------------------------------------------------------------*/
{

  return reinterpret_cast<GroupBalancer*> (arg)->GroupBalance();
}

/*----------------------------------------------------------------------------*/
GroupSize::GroupSize (uint64_t usedBytes, uint64_t capacity)
: mSize (usedBytes),
mCapacity (capacity)
/*----------------------------------------------------------------------------*/
/**
 * @brief GroupSize constructor (capacity must be > 0)
 */
/*----------------------------------------------------------------------------*/
{
  assert(capacity > 0);
}

/*----------------------------------------------------------------------------*/
void
GroupSize::swapFile (GroupSize *toGroup, uint64_t size)
/*----------------------------------------------------------------------------*/
/**
 * @brief Subtracts the given size from this group and adds it to the given
 *        toGroup
 * @param toGroup the group where to add the size
 * @param size the file size that should be swapped
 */
/*----------------------------------------------------------------------------*/
{
  toGroup->mSize += size;
  mSize -= size;
}

/*----------------------------------------------------------------------------*/
int
GroupBalancer::getRandom (int max)
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
GroupBalancer::recalculateAvg ()
/*----------------------------------------------------------------------------*/
/**
 * @brief Recalculates the sizes average from the mGroupSizes
 */
/*----------------------------------------------------------------------------*/
{
  mAvgUsedSize = 0;
  std::map<std::string, GroupSize *>::const_iterator size_it;
  for (size_it = mGroupSizes.cbegin(); size_it != mGroupSizes.cend(); size_it++)
  {
    mAvgUsedSize += (*size_it).second->filled();
  }

  mAvgUsedSize /= (double) mGroupSizes.size();

  eos_static_debug("New average calculated: %.02f %%", mAvgUsedSize * 100.0);
}

/*----------------------------------------------------------------------------*/
void
GroupBalancer::clearCachedSizes ()
/*----------------------------------------------------------------------------*/
/**
 * @brief Deletes all the GrouSize objects stored in mGroupSizes and empties it
 */
/*----------------------------------------------------------------------------*/
{
  std::map<std::string, GroupSize*>::iterator it;
  for (it = mGroupSizes.begin(); it != mGroupSizes.end(); it++)
    delete (*it).second;
  mGroupSizes.clear();
}

/*----------------------------------------------------------------------------*/
void
GroupBalancer::updateGroupAvgCache (FsGroup *group)
/*----------------------------------------------------------------------------*/
/**
 * @brief Places group in mGroupsOverAvg or mGroupsUnderAvg in case they're
 *        greater than or less than the current mAvgUsedSize, respectively
 */
/*----------------------------------------------------------------------------*/
{
  if (mGroupSizes.count(group->mName) == 0)
    return;

  const std::string &name = group->mName;
  GroupSize *groupSize = mGroupSizes[name];
  double diffWithAvg = ((double) groupSize->filled()
                                - ((double) mAvgUsedSize));

  if (mGroupsOverAvg.count(name))
    mGroupsOverAvg.erase(name);
  else if (mGroupsUnderAvg.count(name))
    mGroupsUnderAvg.erase(name);

  eos_static_debug("diff=%.02f threshold=%.02f", diffWithAvg, mThreshold);
  // Group is mThreshold over or under the average used size
  if (abs(diffWithAvg) > mThreshold)
  {
    if (diffWithAvg > 0)
      mGroupsOverAvg[name] = group;
    else
      mGroupsUnderAvg[name] = group;
  }
}

/*----------------------------------------------------------------------------*/
void
GroupBalancer::fillGroupsByAvg ()
/*----------------------------------------------------------------------------*/
/**
 * @brief Fills mGroupsOverAvg and mGroupsUnderAvg with the objects in
 *        mGroupSizes, in case they're greater than or less than the current
 *        mAvgUsedSize, respectively
 */
/*----------------------------------------------------------------------------*/
{
  mGroupsOverAvg.clear();
  mGroupsUnderAvg.clear();

  if (mGroupSizes.size() == 0)
    return;

  std::map<std::string, GroupSize *>::const_iterator size_it;
  for (size_it = mGroupSizes.cbegin(); size_it != mGroupSizes.cend(); size_it++)
  {
    const std::string &name = (*size_it).first;
    FsGroup *group = FsView::gFsView.mGroupView[name];
    updateGroupAvgCache(group);
  }
}

/*----------------------------------------------------------------------------*/
void
GroupBalancer::populateGroupsInfo ()
/*----------------------------------------------------------------------------*/
/**
 * @brief Fills mGroupSizes, calculates the mAvgUsedSize and fills
 *        mGroupsUnderAvg and mGroupsOverAvg
 */
/*----------------------------------------------------------------------------*/
{
  const char *spaceName = mSpaceName.c_str();
  eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);
  std::set<FsGroup*>::const_iterator it;

  mAvgUsedSize = 0;
  clearCachedSizes();

  for (it = FsView::gFsView.mSpaceGroupView[spaceName].cbegin();
    it != FsView::gFsView.mSpaceGroupView[spaceName].cend();
    it++)
  {
    if ((*it)->GetConfigMember("status") != "on")
      continue;

    uint64_t size = (*it)->AverageDouble("stat.statfs.usedbytes");
    uint64_t capacity = (*it)->AverageDouble("stat.statfs.capacity");

    if (capacity == 0)
      continue;

    mGroupSizes[(*it)->mName] = new GroupSize(size, capacity);
    mAvgUsedSize += mGroupSizes[(*it)->mName]->filled();
  }

  if (mGroupSizes.size() == 0)
  {
    mAvgUsedSize = 0;
    eos_static_debug("No groups to be balanced!");
    return;
  }

  mAvgUsedSize /= ((double) mGroupSizes.size());

  eos_static_debug("New average calculated: %.02f %%", mAvgUsedSize * 100.0);

  fillGroupsByAvg();
}

/*----------------------------------------------------------------------------*/
std::string
GroupBalancer::getFileProcTransferNameAndSize (eos::common::FileId::fileid_t fid,
                                               FsGroup *group,
                                               uint64_t *size)
/*----------------------------------------------------------------------------*/
/**
 * @brief Produces a file conversion path to be placed in the proc directory
 *        taking into account the given group and also returns its size
 * @param fid the file ID
 * @param group the group to which the file will be transferred
 * @param size return address for the size of the file
 * 
 * @return name of the proc transfer file
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
           group->mName.c_str(),
           (unsigned long) layoutid);

  return std::string(fileName);
}

/*----------------------------------------------------------------------------*/
void
GroupBalancer::updateTransferList ()
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
void
GroupBalancer::scheduleTransfer (eos::common::FileId::fileid_t fid,
                                 FsGroup *sourceGroup,
                                 FsGroup *targetGroup)
/*----------------------------------------------------------------------------*/
/**
 * @brief Creates the conversion file in proc for the file ID, from the given
 *        sourceGroup, to the targetGroup (and updates the cache structures)
 * @param fid the id of the file to be transferred
 * @param sourceGroup the group where the file is currently located
 * @param targetGroup the group to which the file is will be transferred
 */
/*----------------------------------------------------------------------------*/
{
  eos::common::Mapping::VirtualIdentity rootvid;
  eos::common::Mapping::Root(rootvid);
  XrdOucErrInfo mError;
  uint64_t size = -1;
  std::string fileName = getFileProcTransferNameAndSize(fid, targetGroup, &size);

  if (fileName == "")
    return;

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
  mGroupSizes[sourceGroup->mName]->swapFile(mGroupSizes[targetGroup->mName],
                                            size);

  updateGroupAvgCache(sourceGroup);
  updateGroupAvgCache(targetGroup);
}

/*----------------------------------------------------------------------------*/
eos::common::FileId::fileid_t
GroupBalancer::chooseFidFromGroup (FsGroup *group)
/*----------------------------------------------------------------------------*/
/**
 * @brief Chooses a random file ID from a random filesystem in the given group
 * @param group the group from which the file id will be chosen
 * @return the chosen file ID
 */
/*----------------------------------------------------------------------------*/
{
  int rndIndex;
  eos::common::RWMutexReadLock vlock(FsView::gFsView.ViewMutex);
  eos::common::RWMutexReadLock lock(gOFS->eosViewRWMutex);
  const eos::FileSystemView::FileList *filelist = 0;
  std::vector<int> validFsIndexes(group->size());
  for (size_t i = 0; i < group->size(); i++)
    validFsIndexes[i] = (int) i;

  eos::mgm::BaseView::const_iterator fs_it;

  while (validFsIndexes.size() > 0)
  {
    fs_it = group->begin();
    rndIndex = getRandom(validFsIndexes.size() - 1);
    std::advance(fs_it, validFsIndexes[rndIndex]);

    // accept only active file systems
    if (FsView::gFsView.mIdView[*fs_it]->GetActiveStatus() ==
        eos::common::FileSystem::kOnline)
    {
      try
      {
        filelist = &gOFS->eosFsView->getFileList(*fs_it);
      }
      catch (eos::MDException &e)
      {
      }

      if (filelist && filelist->size() > 0)
        break;
    }

    validFsIndexes.erase(validFsIndexes.begin() + rndIndex);
  }

  // CHECK IF THIS IS POSSIBLE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
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

static void
printSizes (const std::map<std::string, GroupSize *> *sizes)
{
  std::map<std::string, GroupSize *>::const_iterator it;

  for (it = sizes->cbegin(); it != sizes->cend(); it++)
    eos_static_info("group=%s average=%.02f", (*it).first.c_str(),
                    (double) (*it).second->filled() * 100.0);
}

/*----------------------------------------------------------------------------*/
void
GroupBalancer::prepareTransfer ()
/*----------------------------------------------------------------------------*/
/**
 * @brief Picks two groups (source and target) randomly and schedule a file ID
 *        to be transferred
 */
/*----------------------------------------------------------------------------*/
{
  FsGroup *fromGroup, *toGroup;
  std::map<std::string, FsGroup *>::iterator over_it, under_it;
  eos::mgm::BaseView::const_iterator fsid_it;

  if (mGroupsUnderAvg.size() == 0 || mGroupsOverAvg.size() == 0)
  {
    if (mGroupsOverAvg.size() == 0)
      eos_static_debug("No groups over the average!");

    if (mGroupsUnderAvg.size() == 0)
      eos_static_debug("No groups under the average!");

    recalculateAvg();
    return;
  }

  over_it = mGroupsOverAvg.begin();
  under_it = mGroupsUnderAvg.begin();

  int rndIndex = getRandom(mGroupsOverAvg.size() - 1);
  std::advance(over_it, rndIndex);

  rndIndex = getRandom(mGroupsUnderAvg.size() - 1);
  std::advance(under_it, rndIndex);

  fromGroup = (*over_it).second;
  toGroup = (*under_it).second;

  if (fromGroup->size() == 0)
    return;

  eos::common::FileId::fileid_t fid = chooseFidFromGroup(fromGroup);
  if ((int) fid == -1)
  {
    eos_static_info("Couldn't choose any FID to schedule: failedgroup=%s",
                    fromGroup->mName.c_str());
    return;
  }

  scheduleTransfer(fid, fromGroup, toGroup);
}

/*----------------------------------------------------------------------------*/
bool
GroupBalancer::cacheExpired ()
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
GroupBalancer::prepareTransfers (int nrTransfers)
{
  /*--------------------------------------------------------------------------*/
  /**
   * @brief Schedule a pre-defined number of transfers
   */
  /*--------------------------------------------------------------------------*/
  int allowedTransfers = nrTransfers - mTransfers.size();
  for (int i = 0; i < allowedTransfers; i++)
    prepareTransfer();

  if (allowedTransfers > 0)
    printSizes(&mGroupSizes);
}

/*----------------------------------------------------------------------------*/
void*
GroupBalancer::GroupBalance ()
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
    bool isSpaceGroupBalancer = true;
    bool isMaster = true;
    int nrTransfers = 0;

    XrdSysThread::SetCancelOff();
    {
      // -----------------------------------------------------------------------
      // extract the current settings if conversion enabled and how many
      // conversion jobs should run
      // -----------------------------------------------------------------------
      eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);

      std::set<FsGroup*>::const_iterator git;
      if (!FsView::gFsView.mSpaceGroupView.count(mSpaceName.c_str()))
        break;

      FsSpace* space = FsView::gFsView.mSpaceView[mSpaceName.c_str()];

      if (space->GetConfigMember("converter") != "on")
      {
        eos_static_debug("Converter is off for! It needs to be on "
                         "for the group balancer to work. space=%s",
                         mSpaceName.c_str());
        goto wait;
      }

      isSpaceGroupBalancer = space->GetConfigMember("groupbalancer") == "on";

      nrTransfers = atoi(space->GetConfigMember("groupbalancer.ntx").c_str());
      mThreshold =
        atof(space->GetConfigMember("groupbalancer.threshold").c_str());
      mThreshold /= 100.0;
    }

    isMaster = gOFS->MgmMaster.IsMaster();

    if (isMaster && isSpaceGroupBalancer)
      eos_static_info("groupbalancer is enabled ntx=%d ", nrTransfers);
    else
    {
      if (isMaster)
        eos_static_debug("group balancer is disabled");
      else
        eos_static_debug("group balancer is in slave mode");
    }

    if (isMaster && isSpaceGroupBalancer)
    {
      updateTransferList();
      if ((int) mTransfers.size() >= nrTransfers)
        goto wait;

      if (cacheExpired())
      {
        populateGroupsInfo();
        printSizes(&mGroupSizes);
      }
      else
        recalculateAvg();

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
