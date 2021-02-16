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

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
Balancer::Balancer(const char* space_name):
  mSpaceName(space_name)
{
  mThread.reset(&Balancer::Balance, this);
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
Balancer::~Balancer()
{
  Stop();
}

//------------------------------------------------------------------------------
// Method use to stop balancing thread
//------------------------------------------------------------------------------
void
Balancer::Stop()
{
  mThread.join();
}

//------------------------------------------------------------------------------
// Balancer implementation
//------------------------------------------------------------------------------
void
Balancer::Balance(ThreadAssistant& assistant) noexcept
{
  gOFS->WaitUntilNamespaceIsBooted(assistant);
  assistant.wait_for(std::chrono::seconds(10));
  eos_static_info("%s", "msg=\"starting balancer thread\"");

  // Loop forever until cancelled
  while (!assistant.terminationRequested()) {
    bool IsSpaceBalancing = true;
    double SpaceDifferenceThreshold = 0;
    std::string SpaceNodeTransfers = "";
    std::string SpaceNodeTransferRate = "";
    std::string SpaceNodeThreshold = "";
    uint64_t timeout_ns = 100 * 1e6; // 100ms

    // Try to read lock the mutex
    while (!FsView::gFsView.ViewMutex.TimedRdLock(timeout_ns)) {
      if (assistant.terminationRequested()) {
        return;
      }
    }

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

    if (gOFS->mMaster->GetServiceDelay()) {
      eos_static_debug("msg=\"force balancing off due to slave-master transition\"");
      IsSpaceBalancing = false;
    }

    SpaceNodeThreshold = FsView::gFsView.mSpaceView[mSpaceName.c_str()]->\
                         GetConfigMember("balancer.threshold");
    SpaceDifferenceThreshold = strtod(SpaceNodeThreshold.c_str(), 0);
    SpaceNodeTransfers = FsView::gFsView.mSpaceView[mSpaceName.c_str()]->\
                         GetConfigMember("balancer.node.ntx");
    SpaceNodeTransferRate = FsView::gFsView.mSpaceView[mSpaceName.c_str()]->\
                            GetConfigMember("balancer.node.rate");

    if (gOFS->mMaster->IsMaster() && IsSpaceBalancing) {
      size_t total_files; // number of files currently in transfer
      auto set_fsgrps = FsView::gFsView.mSpaceGroupView[mSpaceName.c_str()];

      // Loop over all groups
      for (auto git = set_fsgrps.begin(); git != set_fsgrps.end(); ++git) {
        total_files = 0;

        for (auto it = (*git)->begin(); it != (*git)->end(); ++it) {
          eos::common::FileSystem* fs = FsView::gFsView.mIdView.lookupByID(*it);

          if (fs) {
            total_files += fs->GetLongLong("stat.balancer.running");
          }

          // Set transfer running by group
          char srunning[256];
          snprintf(srunning, sizeof(srunning) - 1, "%lu", (unsigned long) total_files);
          std::string brunning = srunning;

          if ((*git)->GetConfigMember("stat.balancing.running") != brunning) {
            (*git)->SetConfigMember("stat.balancing.running", brunning, true);
          }
        }

        double dev = 0;
        double avg = 0;
        double fsdev = 0;

        if ((dev = (*git)->MaxAbsDeviation("stat.statfs.filled", false)) >
            SpaceDifferenceThreshold) {
          avg = (*git)->AverageDouble("stat.statfs.filled", false);
          (*git)->SetConfigMember("stat.balancing", "balancing", true);

          for (auto fsit = (*git)->begin(); fsit != (*git)->end(); ++fsit) {
            FileSystem* fs = FsView::gFsView.mIdView.lookupByID(*fsit);

            if (fs) {
              if (FsView::gFsView.mNodeView.count(fs->GetQueue())) {
                FsNode* node = FsView::gFsView.mNodeView[fs->GetQueue()];

                if (node) {
                  // Broadcast the rate & stream configuration if changed
                  if (node->GetConfigMember("stat.balance.ntx") !=
                      SpaceNodeTransfers) {
                    node->SetConfigMember("stat.balance.ntx", SpaceNodeTransfers, true);
                  }

                  if (node->GetConfigMember("stat.balance.rate") !=
                      SpaceNodeTransferRate) {
                    node->SetConfigMember("stat.balance.rate", SpaceNodeTransferRate, true);
                  }

                  if (node->GetConfigMember("stat.balance.threshold") !=
                      SpaceNodeThreshold) {
                    node->SetConfigMember("stat.balance.threshold", SpaceNodeThreshold, true);
                  }
                }
              }

              // Broadcast the avg. value to all filesystems
              fsdev = fs->GetDouble("stat.nominal.filled");

              // If the value changes significantly, broadcast it
              if (fabs(fsdev - avg) > 0.1) {
                fs->SetDouble("stat.nominal.filled", avg);
              }
            }
          }
        } else {
          for (auto fsit = (*git)->begin(); fsit != (*git)->end(); ++fsit) {
            FileSystem* fs = FsView::gFsView.mIdView.lookupByID(*fsit);

            if (fs) {
              std::string isset = fs->GetString("stat.nominal.filled");
              fsdev = fs->GetDouble("stat.nominal.filled");
              fsdev = fabs(fsdev);

              if ((fsdev > 0) || (!isset.length())) {
                // 0.0 indicates, that we are perfectly filled
                // (or the balancing is disabled)
                if (fsdev) {
                  fs->SetDouble("stat.nominal.filled", 0.0);
                }

                if ((*git)->GetConfigMember("stat.balancing") != "idle") {
                  (*git)->SetConfigMember("stat.balancing", "idle", true);
                }
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
            (*git)->SetConfigMember("stat.balancing.running", "0", true);
          }

          for (auto fsit = (*git)->begin(); fsit != (*git)->end(); ++fsit) {
            FileSystem* fs = FsView::gFsView.mIdView.lookupByID(*fsit);

            if (fs) {
              std::string isset = fs->GetString("stat.nominal.filled");
              double fsdev = fs->GetDouble("stat.nominal.filled");

              if ((fsdev > 0) || (!isset.length())) {
                // 0.0 indicates, that we are perfectly filled
                // (or the balancing is disabled)
                if (fsdev) {
                  fs->SetDouble("stat.nominal.filled", 0.0);
                }
              }
            }
          }

          if ((*git)->GetConfigMember("stat.balancing") != "idle") {
            (*git)->SetConfigMember("stat.balancing", "idle", true);
          }
        }
      }
    }

    FsView::gFsView.ViewMutex.UnLockRead();
    // Wait a while ...
    assistant.wait_for(std::chrono::seconds(10));
  }
}

EOSMGMNAMESPACE_END
