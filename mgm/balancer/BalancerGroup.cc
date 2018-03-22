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
#include <future>

EOSMGMNAMESPACE_BEGIN

using namespace std::chrono;

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
  //@todo: do we need it?

}


//------------------------------------------------------------------------------
// Get space defined balance variables
//------------------------------------------------------------------------------
void
BalancerGroup::GetSpaceConfiguration()
{
  if (FsView::gFsView.mSpaceView.count(mSpace)) {
    auto space = FsView::gFsView.mSpaceView[mSpace];

    if (space->GetConfigMember("balancer.node.rate") != "") {
      maxParallelJobs =  std::stoi(space->GetConfigMember("balancer.node.rate"));
      eos_static_debug("setting paralleljobs to:%u", maxParallelJobs);
    }
  }
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

      GetSpaceConfiguration();

      eos::common::FileSystem::fsid_t sourceFS = SelectSourceFS();

      eos_info("selected FS=%d", sourceFS);
  
      std::set<eos::common::FileId::fileid_t> filesToBalance = SelectFilesToBalance(sourceFS);

      if (filesToBalance.size() == 0) {
        continue;
      }
      if (CollectBalanceJobs(sourceFS, filesToBalance) == 0) {
        continue;
      }

      do { // Loop to balance the files
        auto it_job = mJobsPending.begin();

        while ((mJobsRunning.size() <= maxParallelJobs) &&
             (it_job != mJobsPending.end())) {
          std::this_thread::sleep_for(milliseconds(200));
          auto job = *it_job;
          mJobsRunning.emplace(*it_job, mThreadPool.PushTask<void>(
                               [job] {job->Start();}));
          it_job = mJobsPending.erase(it_job);
        }

        for (auto it = mJobsRunning.begin(); it !=  mJobsRunning.end();) {
          if (it->first->GetStatus() == BalancerJob::OK) {
            it->second.get();
            it = mJobsRunning.erase(it);
          } else if (it->first->GetStatus() == BalancerJob::Failed) {
            it->second.get();
            mJobsFailed.push_back(it->first);
            it = mJobsRunning.erase(it);
          } else {
            ++it;
          }
        }

        if (mJobsRunning.size() > maxParallelJobs) {
          std::this_thread::sleep_for(seconds(1));
        }
      } while (mJobsPending.size() != 0);
      //wait for the original file to be dropped
      sleep(70); 
    } else {
       sleep(1);
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
// Collect and prepare all the Balancing jobs
//------------------------------------------------------------------------------
uint64_t
BalancerGroup::CollectBalanceJobs(eos::common::FileSystem::fsid_t sourceFS, 
                        const std::set<eos::common::FileId::fileid_t>& inputFiles)
{
  for (auto info: inputFiles) {
    mJobsPending.emplace_back(new BalancerJob(info, sourceFS, 0));
  }
  return inputFiles.size();
}

std::set<eos::common::FileId::fileid_t>
BalancerGroup::SelectFilesToBalance(eos::common::FileSystem::fsid_t sourceFS)
{ 
  //here we may also check if the files can be balanced out using the GeoTreEngine
  unsigned int nfiles=0;
  std::set<eos::common::FileId::fileid_t> info;
  eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex);
  auto it_fid = gOFS->eosFsView->getFileList(sourceFS);
  while((nfiles < filesToBalance) && (it_fid && it_fid->valid())) {
      it_fid->next();
      info.emplace(it_fid->getElement()); 
      nfiles++;
  }
  return info;
}
EOSMGMNAMESPACE_END
