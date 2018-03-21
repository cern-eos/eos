//------------------------------------------------------------------------------
// @file BalancerGroup.cc
// @author Andrea Manzi - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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

#include "mgm/balancer/BalancerGroup.hh"
#include "mgm/balancer/BalancerJob.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/GeoTreeEngine.hh"
#include "mgm/Master.hh"
#include "namespace/interface/IFsView.hh"
#include <sstream>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
BalancerGroup::~BalancerGroup()
{

  if ( mThread.joinable()) {
    mThread.join();
  }
  SetInitialCounters();
}

//------------------------------------------------------------------------------
// Set initial balancer counters and status
//------------------------------------------------------------------------------
void
BalancerGroup::SetInitialCounters()
{
  //@todo: put hear what is now done in the Balancer to broadast the FSTs(if needed)
  /*if (FsView::gFsView.mIdView.count(mFsId)) {
    fs = FsView::gFsView.mIdView[mFsId];

    if (fs) {
      fs->OpenTransaction();
      fs->SetLongLong("stat.drainbytesleft", 0);
      fs->SetLongLong("stat.drainfiles", 0);
      fs->SetLongLong("stat.timeleft", 0);
      fs->SetLongLong("stat.drainprogress", 0);
      fs->SetLongLong("stat.drainretry", 0);
      fs->SetDrainStatus(eos::common::FileSystem::kNoDrain);
      fs->CloseTransaction();
    }
  }
  
  
  FsView::gFsView.StoreFsConfig(fs);
 */
}


//------------------------------------------------------------------------------
// Get space defined balance variables
//------------------------------------------------------------------------------
void
BalancerGroup::GetSpaceConfiguration()
{
  //@todo: check if we need to get some soace configuration here
  /*
  if (FsView::gFsView.mSpaceView.count(mSpace)) {
    auto space = FsView::gFsView.mSpaceView[mSpace];

    if (space->GetConfigMember("drainer.retries") != "") {
      mMaxRetries = std::stoi(space->GetConfigMember("drainer.retries"));
      eos_static_debug("setting retries to:%u", mMaxRetries);
    }

    if (space->GetConfigMember("drainer.fs.ntx") != "") {
      maxParallelJobs =  std::stoi(space->GetConfigMember("drainer.fs.ntx"));
      eos_static_debug("setting paralleljobs to:%u", maxParallelJobs);
    }
  }*/
}


//------------------------------------------------------------------------------
// Method doing the balancing supervision
//------------------------------------------------------------------------------
void*
BalancerGroup::Balance()
{
  //@todo: implements the balancing activity by selecting the Source and target FSs and the source file to move
  //@todo: it should select a configurable portion of file to move ( e.g. 100 ) from the source FS and then iterate again to the next FS
  while(true)
  {
    if (!mBalanceStop) {
      eos_info("starting balancing group=%s", mGroup.c_str());
      sleep(100000000);

    } else {
      eos_info("stopping balancing group=%s",mGroup.c_str());

    }

  }
  return 0;
}

//------------------------------------------------------------------------------
// Select source file system (using the GeoTreeEngine)
//------------------------------------------------------------------------------
eos::common::FileSystem::fsid_t
BalancerGroup::SelectSourceFS()
{
  eos::common::RWMutexReadLock vlock(FsView::gFsView.ViewMutex); 
  FsGroup* group = FsView::gFsView.mGroupView[mGroup];

  if (!group) {
    eos_err("group=%s is not in group view", mGroup.c_str());
    return 0;
  }

    
  FsGroup::const_iterator group_iterator;

  eos::common::FileSystem* source_fs = 0;
  eos::common::FileSystem::fsid_t source_fsid = 0;

  double avgDiskFilled = group->AverageDouble("stat.statfs.filled", false);
  double maxDiskFilled = 0;

  for (group_iterator = group->begin(); group_iterator != group->end();  group_iterator++) {
    source_fs = FsView::gFsView.mIdView[*group_iterator];

    if (!source_fs) {
        continue;
    }

    eos::common::FileSystem::fs_snapshot snapshot;
    source_fs->SnapShotFileSystem(snapshot);

    //checks
    //not empty
    if ((snapshot.mDiskFilled > snapshot.mNominalFilled) &&
       //booted
       (snapshot.mStatus == eos::common::FileSystem::kBooted) &&
       // this filesystem is  readable
       (snapshot.mConfigStatus > eos::common::FileSystem::kRO) &&
       //no errors
       (snapshot.mErrCode == 0) &&
       //active 
       (source_fs->GetActiveStatus(snapshot) != eos::common::FileSystem::kOffline) &&
       //drain status
       (snapshot.mDrainStatus == eos::common::FileSystem::kNoDrain)) {

       if ((snapshot.mDiskFilled > avgDiskFilled) && (snapshot.mDiskFilled > maxDiskFilled)) {
            maxDiskFilled = snapshot.mDiskFilled;
            source_fsid =*group_iterator;
          }
       }  
    }    

  return source_fsid;
}

//------------------------------------------------------------------------------
// Select target file system (using the GeoTreeEngine)
//------------------------------------------------------------------------------
eos::common::FileSystem::fsid_t
BalancerGroup::SelectTargetFS(eos::common::FileId::fileid_t fileId, eos::common::FileSystem::fsid_t sourceFS)
{

  eos::common::RWMutexReadLock(FsView::gFsView.ViewMutex);
  std::vector<FileSystem::fsid_t>* newReplicas = new
  std::vector<FileSystem::fsid_t>();
  std::vector<FileSystem::fsid_t> existingReplicas;
  eos::common::FileSystem::fs_snapshot source_snapshot;
  eos::common::FileSystem* source_fs = 0;
  std::shared_ptr<eos::IFileMD> fmd = nullptr;
  {
    eos::common::RWMutexReadLock nsLock(gOFS->eosViewRWMutex);
    fmd =  gOFS->eosFileService->getFileMD(fileId);
    existingReplicas = static_cast<std::vector<FileSystem::fsid_t>>
                     (fmd->getLocations());
  }
  unsigned int nfilesystems = 1;
  unsigned int ncollocatedfs = 0;
  source_fs = FsView::gFsView.mIdView[sourceFS];
  source_fs->SnapShotFileSystem(source_snapshot);
  FsGroup* group = FsView::gFsView.mGroupView[source_snapshot.mGroup];
  //check other replicas for the file
  std::vector<std::string> fsidsgeotags;

  if (!gGeoTreeEngine.getInfosFromFsIds(existingReplicas,
                                        &fsidsgeotags,
                                        0, 0)) {
    eos_notice("could not retrieve info for all avoid fsids");
    delete newReplicas;
    return 0;
  }

  auto repl = existingReplicas.begin();

  while (repl != existingReplicas.end()) {
    eos_static_debug("existing replicas: %d", *repl);
    repl++;
  }

  auto geo = fsidsgeotags.begin();

  while (geo != fsidsgeotags.end()) {
    eos_static_debug("geotags: %s", (*geo).c_str());
    geo++;
  }
  bool res = gGeoTreeEngine.placeNewReplicasOneGroup(
               group, nfilesystems,
               newReplicas,
               (ino64_t) fmd->getId(),
               NULL, //entrypoints
               NULL, //firewall
               GeoTreeEngine::balancing,
               &existingReplicas,
               &fsidsgeotags,
               fmd->getSize(),
               "",//start from geotag
               "",//client geo tag
               ncollocatedfs,
               NULL, //excludeFS
               &fsidsgeotags, //excludeGeoTags
               NULL);

  if (res) {
    std::ostringstream oss;

    for (auto it = newReplicas->begin(); it != newReplicas->end(); ++it) {
      oss << " " << (unsigned long)(*it);
    }

    eos_static_debug("GeoTree Balancing Placement returned %d with fs id's -> %s",
                     (int)res, oss.str().c_str());
    //return only one FS now
    eos::common::FileSystem::fsid_t targetFS = *newReplicas->begin();
    delete newReplicas;
    return targetFS;
  } else {
    eos_notice("could not place the replica");
    delete newReplicas;
    return 0;
  }

  
}

std::set<BalancerJob::FileBalanceInfo>
BalancerGroup::SelectFilesToBalance(eos::common::FileSystem::fsid_t sourceFS)
{ 
  std::set<eos::mgm::BalancerJob::FileBalanceInfo> info;
  return info;
}
EOSMGMNAMESPACE_END
