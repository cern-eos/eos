// ----------------------------------------------------------------------
// File: Balancer.cc
// Author: Andreas-Joachim Peters - CERN
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

#include "mgm/Balancer.hh"
#include "mgm/FsView.hh"
#include "mgm/XrdMgmOfs.hh"
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
    XrdSysThread::SetCancelOff();
    {
      eos::common::RWMutexReadLock lock(FsView::gFsView.ViewMutex);

      if (!FsView::gFsView.mSpaceGroupView.count(mSpaceName.c_str())) {
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
        for (auto git = set_fsgrps.begin(); git != set_fsgrps.end(); ++git) {
          // Need to make sure, nobody is drainig here, otherwise we can get
          // a scheduling interference between drain and balancing!
          bool has_drainjob = false;
          total_files = 0;

          for (auto it = (*git)->begin(); it != (*git)->end(); ++it) {
            if (FsView::gFsView.mIdView.count(*it)) {
              eos::common::FileSystem* fs = FsView::gFsView.mIdView[*it];
              total_files += fs->GetLongLong("stat.balancer.running");
              eos::common::FileSystem::fsstatus_t configstatus = fs->GetConfigStatus();

              if (((configstatus == eos::common::FileSystem::kDrain)
                   || (configstatus == eos::common::FileSystem::kDrainDead))) {
                has_drainjob = true;
              }
            }

            // Set transfer running by group
            char srunning[256];
            snprintf(srunning, sizeof(srunning) - 1, "%lu", (unsigned long) total_files);
            std::string brunning = srunning;

            if ((*git)->GetConfigMember("stat.balancing.running") != brunning) {
              (*git)->SetConfigMember("stat.balancing.running",
                                      brunning, false, "", true);
            }
          }

          double dev = 0;
          double avg = 0;
          double fsdev = 0;

          if ((dev = (*git)->MaxAbsDeviation("stat.statfs.filled", false)) >
              SpaceDifferenceThreshold) {
            avg = (*git)->AverageDouble("stat.statfs.filled", false);

            if (has_drainjob) {
              (*git)->SetConfigMember("stat.balancing", "drainwait", false,
                                      "", true);
            } else {
              (*git)->SetConfigMember("stat.balancing", "balancing", false,
                                      "", true);
            }

            for (auto fsit = (*git)->begin(); fsit != (*git)->end(); ++fsit) {
              FileSystem* fs = FsView::gFsView.mIdView[*fsit];

              if (fs) {
                FsNode* node = FsView::gFsView.mNodeView[fs->GetQueue()];

                if (node) {
                  // Broadcast the rate & stream configuration if changed
                  if (node->GetConfigMember("stat.balance.ntx") !=
                      SpaceNodeTransfers) {
                    node->SetConfigMember("stat.balance.ntx", SpaceNodeTransfers,
                                          false, "", true);
                  }

                  if (node->GetConfigMember("stat.balance.rate") !=
                      SpaceNodeTransferRate) {
                    node->SetConfigMember("stat.balance.rate", SpaceNodeTransferRate,
                                          false, "", true);
                  }

                  if (node->GetConfigMember("stat.balance.threshold") !=
                      SpaceNodeThreshold) {
                    node->SetConfigMember("stat.balance.threshold", SpaceNodeThreshold,
                                          false, "", true);
                  }
                }

                // Broadcast the avg. value to all filesystems
                fsdev = fs->GetDouble("stat.nominal.filled");

                // If the value changes significantly, broadcast it
                if (fabs(fsdev - avg) > 0.1) {
                  if (!has_drainjob) {
                    fs->SetDouble("stat.nominal.filled", avg, true);
                  }
                }

                // Disable balancing on this filesystem if draining is running
                // in the group
                if (has_drainjob && fsdev) {
                  fs->SetDouble("stat.nominal.filled", 0.0, true);
                }
              }
            }
          } else {
            for (auto fsit = (*git)->begin(); fsit != (*git)->end(); ++fsit) {
              FileSystem* fs = FsView::gFsView.mIdView[*fsit];

              if (fs) {
                std::string isset = fs->GetString("stat.nominal.filled");
                fsdev = fs->GetDouble("stat.nominal.filled");

                if ((fsdev > 0) || (!isset.length())) {
                  // 0.0 indicates, that we are perfectly filled
                  // (or the balancing is disabled)
                  if (fsdev) {
                    fs->SetDouble("stat.nominal.filled", 0.0, true);
                  }

                  if ((*git)->GetConfigMember("stat.balancing") != "idle")
                    (*git)->SetConfigMember("stat.balancing", "idle",
                                            false, "", true);
                }
              }
            }
          }

          XrdOucString sizestring1;
          XrdOucString sizestring2;
          eos_static_debug("space=%-10s group=%-20s deviation=%-10s threshold=%-10s",
                           mSpaceName.c_str(), (*git)->GetMember("name").c_str(),
                           eos::common::StringConversion::GetReadableSizeString(sizestring1,
                               (unsigned long long) dev, "B"),
                           eos::common::StringConversion::GetReadableSizeString(sizestring2,
                               (unsigned long long) SpaceDifferenceThreshold, "B"));
        }
      } else {
        if (FsView::gFsView.mSpaceGroupView.count(mSpaceName.c_str())) {
          auto set_fsgrps = FsView::gFsView.mSpaceGroupView[mSpaceName.c_str()];

          for (auto git = set_fsgrps.begin(); git != set_fsgrps.end(); ++git) {
            if ((*git)->GetConfigMember("stat.balancing.running") != "0") {
              (*git)->SetConfigMember("stat.balancing.running", "0", false,
                                      "", true);
            }

            for (auto fsit = (*git)->begin(); fsit != (*git)->end(); ++fsit) {
              FileSystem* fs = FsView::gFsView.mIdView[*fsit];

              if (fs) {
                std::string isset = fs->GetString("stat.nominal.filled");
                double fsdev = fs->GetDouble("stat.nominal.filled");

                if ((fsdev > 0) || (!isset.length())) {
                  // 0.0 indicates, that we are perfectly filled
                  // (or the balancing is disabled)
                  if (fsdev) {
                    fs->SetDouble("stat.nominal.filled", 0.0, true);
                  }
                }
              }
            }

            if ((*git)->GetConfigMember("stat.balancing") != "idle") {
              (*git)->SetConfigMember("stat.balancing", "idle", false,
                                      "", true);
            }
          }
        }
      }
    }
    XrdSysThread::SetCancelOn();

    // Wait a while ...
    for (size_t i = 0; i < 10; ++i) {
      XrdSysTimer sleeper;
      sleeper.Snooze(1);
      XrdSysThread::CancelPoint();
    }
  }

  return 0;
}

EOSMGMNAMESPACE_END
