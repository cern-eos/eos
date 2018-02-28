//------------------------------------------------------------------------------
// @file BalanceGroup.cc
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

#include "mgm/balancer/BalanceGroup.hh"
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
BalanceGroup::~BalanceGroup()
{
  eos_notice("waiting for join ...");

  if (mThread) {
    XrdSysThread::Cancel(mThread);

    if (!gOFS->Shutdown) {
      XrdSysThread::Join(mThread, NULL);
    }

    mThread = 0;
  }

  SetInitialCounters();
  eos_notice("Stopping Balancing group=%u", mGroup);
}

//------------------------------------------------------------------------------
// Set initial balancer counters and status
//------------------------------------------------------------------------------
void
BalanceGroup::SetInitialCounters()
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
// Static thread startup function
//------------------------------------------------------------------------------
void*
BalanceGroup::StaticThreadProc(void* arg)
{
  return reinterpret_cast<BalanceGroup*>(arg)->Drain();
}

//------------------------------------------------------------------------------
// Get space defined balance variables
//------------------------------------------------------------------------------
void
BalanceGroup::GetSpaceConfiguration()
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
// Method doing the drain supervision
//------------------------------------------------------------------------------
void*
BalanceGroup::Balance()
{
  //@todo: implements the balancing activity by selecting the Source and target FSs and the source file to move
  return 0;
}


//------------------------------------------------------------------------------
// Stop Balancing  the attached group
//------------------------------------------------------------------------------
void
BalanceGroup::BalanceStop()
{
  mBalanceStop = true;
}

//------------------------------------------------------------------------------
// Select source file system (using the GeoTreeEngine?)
//------------------------------------------------------------------------------
eos::common::FileSystem::fsid_t
BalancerGroup::SelectSourceFS()
{ 
  //@todo: this should return the FS which has highest fill ratio
  //@todo: we should check if the FS is not under drain

  return NULL;
}

//------------------------------------------------------------------------------
// Select target file system (using the GeoTreeEngine?)
//------------------------------------------------------------------------------
eos::common::FileSystem::fsid_t
BalancerGroup::SelectTargetFS()
{

 //@todo: this should return the FS which has the lowest fill ratio
 
 return NULL;
}

BalancerJob::FileBalanceInfo
SelectFileToBalance(eos::common::FileSystem::fsid_t sourceFS);
{
 //@todo select the best file to move
 return NULL;
}
EOSMGMNAMESPACE_END
