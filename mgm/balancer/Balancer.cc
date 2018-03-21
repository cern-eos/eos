// ----------------------------------------------------------------------
// File: Balancer.cc
// Author: Andreas-Joachim Peters - CERN
// Author: Andrea Manzi - CERN
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

#include "mgm/balancer/Balancer.hh"
#include "mgm/FsView.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Master.hh"
#include "common/StringConversion.hh"
#include "XrdSys/XrdSysTimer.hh"


EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
Balancer::Balancer(const char* space_name):
  mSpaceName(space_name)
{
  XrdSysThread::Run(&mThread, Balancer::StaticBalance, static_cast<void*>(this),
                    XRDSYSTHREAD_HOLD, "Balancer Thread");
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
Balancer::~Balancer()
{
  Stop();

  if (!gOFS->Shutdown) {
    XrdSysThread::Join(mThread, NULL);
  }
}

//------------------------------------------------------------------------------
// Method use to stop balancing thread
//------------------------------------------------------------------------------
void
Balancer::Stop()
{
  XrdSysThread::Cancel(mThread);
}

//------------------------------------------------------------------------------
// Static function used to start thread
//------------------------------------------------------------------------------
void*
Balancer::StaticBalance(void* arg)
{
  return reinterpret_cast<Balancer*>(arg)->Balance();
}

//------------------------------------------------------------------------------
// Balancer implementation
//------------------------------------------------------------------------------
void*
Balancer::Balance(void)
{
  XrdSysThread::SetCancelOn();
  // Wait that the namespace is initialized
  bool go = false;

  do {
    XrdSysThread::SetCancelOff();
    {
      XrdSysMutexHelper(gOFS->InitializationMutex);

      if (gOFS->Initialized == gOFS->kBooted) {
        go = true;
      }
    }
    XrdSysThread::SetCancelOn();
    XrdSysTimer sleeper;
    sleeper.Wait(1000);
  } while (!go);

  // Loop forever until cancelled
  while (true) {
    bool IsSpaceBalancing = true;
    bool IsMaster = true;
    double SpaceDifferenceThreshold = 0;
    std::string SpaceNodeTransfers = "";
    std::string SpaceNodeTransferRate = "";
    std::string SpaceNodeThreshold = "";
    uint64_t timeout_ms = 100;

    // Try to read lock the mutex
    while (FsView::gFsView.ViewMutex.TimedRdLock(timeout_ms)) {
      XrdSysThread::CancelPoint();
    }

    XrdSysThread::SetCancelOff();

    if (!FsView::gFsView.mSpaceGroupView.count(mSpaceName.c_str())) {
      FsView::gFsView.ViewMutex.UnLockRead();
      break;
    }

    if (FsView::gFsView.mSpaceView[mSpaceName.c_str()]->GetConfigMember("balancer")
        == "on") {
      IsSpaceBalancing = true;
    } else {
      IsSpaceBalancing = false;
    }

    if (gOFS->MgmMaster.GetServiceDelay()) {
      eos_static_debug("msg=\"force balancing off due to slave-master transition\"");
      IsSpaceBalancing = false;
    }

    IsMaster = gOFS->MgmMaster.IsMaster();
    SpaceNodeThreshold = FsView::gFsView.mSpaceView[mSpaceName.c_str()]->\
                         GetConfigMember("balancer.threshold");
    SpaceDifferenceThreshold = strtod(SpaceNodeThreshold.c_str(), 0);
    SpaceNodeTransfers = FsView::gFsView.mSpaceView[mSpaceName.c_str()]->\
                         GetConfigMember("balancer.node.ntx");
    SpaceNodeTransferRate = FsView::gFsView.mSpaceView[mSpaceName.c_str()]->\
                            GetConfigMember("balancer.node.rate");

    if (IsMaster && IsSpaceBalancing) {
      size_t total_files; // number of files currently in transfer
      auto set_fsgrps = FsView::gFsView.mSpaceGroupView[mSpaceName.c_str()];

      // Loop over all groups
      for (auto grp : set_fsgrps) {
        total_files = 0;

        for (auto fs :*grp) {
          if (FsView::gFsView.mIdView.count(fs)) {
            eos::common::FileSystem* fsptr = FsView::gFsView.mIdView[fs];
            total_files += fsptr->GetLongLong("stat.balancer.running");
          }

          // Set transfer running by group
          char srunning[256];
          snprintf(srunning, sizeof(srunning) - 1, "%lu", (unsigned long) total_files);
          std::string brunning = srunning;

          if (grp->GetConfigMember("stat.balancing.running") != brunning) {
            grp->SetConfigMember("stat.balancing.running",
                                    brunning, false, "", true);
          }
        }

        double dev = 0;
        double avg = 0;

        std::string group = std::string(grp->GetMember("name").c_str());

        //if the standard MaxStandardDevitaion is > of the configured threshould we start the balancing
        if ((dev = grp->MaxAbsDeviation("stat.statfs.filled", false)) >
            SpaceDifferenceThreshold) {
          avg = grp->AverageDouble("stat.statfs.filled", false);
          
          grp->SetConfigMember("stat.balancing", "balancing", false,
                                    "", true);

          if ( grp->mBalancerGroup == nullptr) {
            grp->mBalancerGroup = std::unique_ptr<BalancerGroup>(new BalancerGroup(group,mSpaceName));
            eos_static_info("creating new BalancerGroup for group=%s", group.c_str());
          }
          if (!grp->mBalancerGroup->isBalancerGroupRunning()) {
            grp->mBalancerGroup->BalancerGroupStart();
          }
        } else {
         if (grp->mBalancerGroup != nullptr && grp->mBalancerGroup->isBalancerGroupRunning()) {
           grp->mBalancerGroup->BalancerGroupStop();
           eos_static_info("stopping BalancerGroup for group=%s", group.c_str());
         }
           
         if (grp->GetConfigMember("stat.balancing") != "idle") {
           grp->SetConfigMember("stat.balancing", "idle",
                                          false, "", true);
         }
        }

        XrdOucString sizestring1;
        XrdOucString sizestring2;
        eos_static_info("space=%-10s group=%-20s deviation=%-10s threshold=%-10s",
                         mSpaceName.c_str(), grp->GetMember("name").c_str(),
                         eos::common::StringConversion::GetReadableSizeString(sizestring1,
                             (unsigned long long) dev, "B"),
                         eos::common::StringConversion::GetReadableSizeString(sizestring2,
                             (unsigned long long) SpaceDifferenceThreshold, "B"));
      }
    } else {
      if (FsView::gFsView.mSpaceGroupView.count(mSpaceName.c_str())) {
        auto set_fsgrps = FsView::gFsView.mSpaceGroupView[mSpaceName.c_str()];

        for (auto grp : set_fsgrps) {
          if (grp->GetConfigMember("stat.balancing.running") != "0") {
            grp->SetConfigMember("stat.balancing.running", "0", false,
                                    "", true);
          }
          if (grp->GetConfigMember("stat.balancing") != "idle") {
            grp->SetConfigMember("stat.balancing", "idle", false,
                                    "", true);
          }
        }
      }
    }

    FsView::gFsView.ViewMutex.UnLockRead();
    XrdSysThread::SetCancelOn();
    XrdSysTimer sleeper;

    // Wait a while ...
    for (size_t i = 0; i < 10; ++i) {
      sleeper.Snooze(1);
      XrdSysThread::CancelPoint();
    }
  }

  return 0;
}

EOSMGMNAMESPACE_END
