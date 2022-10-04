//------------------------------------------------------------------------------
//! @file FsBalancer.cc
//! @author Elvin Sindrilaru <esindril@cern.ch>
//-----------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2021 CERN/Switzerland                                  *
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

#include "mgm/balancer/FsBalancer.hh"
#include "mgm/FsView.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/drain/DrainTransferJob.hh"
#include "namespace/interface/IFsView.hh"
#include "namespace/Prefetcher.hh"

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Update balancer config based on the info registered at the space
//------------------------------------------------------------------------------
void
FsBalancer::ConfigUpdate()
{
  if (!mDoConfigUpdate) {
    return;
  }

  eos_static_info("msg=\"fs balancer configuration update\" space=%s",
                  mSpaceName.c_str());
  mDoConfigUpdate = false;
  // Collect all the relevant info from the parent space
  eos::common::RWMutexReadLock fs_rd_lock(FsView::gFsView.ViewMutex);
  auto it_space = FsView::gFsView.mSpaceView.find(mSpaceName);

  // Space no longer exist, just disable the balancer
  if (it_space == FsView::gFsView.mSpaceView.end()) {
    mIsEnabled = false;
    return;
  }

  auto* space = it_space->second;

  // Check if balancer is enabled
  if (space->GetConfigMember("balancer") != "on") {
    mIsEnabled = false;
    return;
  }

  mIsEnabled = true;
  // Update other balancer related parameters
  std::string svalue = space->GetConfigMember("balancer.threshold");

  if (svalue.empty()) {
    eos_static_err("msg=\"balancer threshold missing, use default value\" value=%f",
                   mThreshold);
  } else {
    try {
      mThreshold = std::stod(svalue);
    } catch (...) {
      eos_static_err("msg=\"balancer threshold invalid format\" input=\"%s\"",
                     svalue.c_str());
    }
  }

  svalue = space->GetConfigMember("balancer.node.ntx");

  if (svalue.empty()) {
    eos_static_err("msg=\"balancer node tx missing, use default value\" value=%f",
                   mTxNumPerNode);
  } else {
    try {
      mTxNumPerNode = std::stoul(svalue);
    } catch (...) {
      eos_static_err("msg=\"balancer node tx invalid format\" input=\"%s\"",
                     svalue.c_str());
    }
  }

  svalue = space->GetConfigMember("balancer.node.rate");

  if (svalue.empty()) {
    eos_static_err("msg=\"balancer node rate missing, use default value\" value=%f",
                   mTxRatePerNode);
  } else {
    try {
      mTxRatePerNode = std::stoul(svalue);
    } catch (...) {
      eos_static_err("msg=\"balancer node rate invalid format\" input=\"%s\"",
                     svalue.c_str());
    }
  }

  svalue = space->GetConfigMember("balancer.max-queue-size");

  if (!svalue.empty()) {
    try {
      unsigned int max_queued_jobs = std::stoul(svalue);

      if ((max_queued_jobs > 10) && (max_queued_jobs < 10000)) {
        mMaxQueuedJobs = max_queued_jobs;
      } else {
        eos_static_err("msg=\"balancer max-queue-size invalid value\" "
                       "input=\"%s\"", svalue.c_str());
      }
    } catch (...) {
      eos_static_err("msg=\"balancer max-queue-size invalid format\" "
                     "input=\"%s\"", svalue.c_str());
    }
  }

  svalue = space->GetConfigMember("balancer.max-thread-pool-size");

  if (!svalue.empty()) {
    try {
      unsigned int max_thread_pool_size = std::stoul(svalue);

      if ((max_thread_pool_size > 2) && (max_thread_pool_size < 10000)) {
        if (mMaxThreadPoolSize != max_thread_pool_size) {
          mMaxThreadPoolSize = max_thread_pool_size;
          mThreadPool.SetMaxThreads(mMaxThreadPoolSize);
        }
      } else {
        eos_static_err("msg=\"balancer max-thread-pool-size invalid value\" "
                       "input=\"%s\"", svalue.c_str());
      }
    } catch (...) {
      eos_static_err("msg=\"balancer max-thread-pool-size invalid format\" "
                     "input=\"%s\"", svalue.c_str());
    }
  }

  return;
}

//------------------------------------------------------------------------------
// Loop handling balancing jobs
//------------------------------------------------------------------------------
void
FsBalancer::Balance(ThreadAssistant& assistant) noexcept
{
  static constexpr std::chrono::seconds enable_refresh_delay {10};
  static constexpr std::chrono::seconds no_transfers_delay {30};
  static constexpr std::chrono::seconds no_slots_delay {10};

  if (gOFS) {
    gOFS->WaitUntilNamespaceIsBooted(assistant);
  }

  eos_static_info("msg=\"started file system balancer thread\" space=%s",
                  mSpaceName.c_str());
  VectBalanceFs vect_tx;

  while (!assistant.terminationRequested()) {
    eos_static_info("%s", "msg=\"loop\"");
    ConfigUpdate();

    if (!mIsEnabled) {
      eos_static_info("msg=\"balancer disabled\" wait=%is\"",
                      enable_refresh_delay.count());
      assistant.wait_for(enable_refresh_delay);
      continue;
    }

    if (mBalanceStats.NeedsUpdate()) {
      eos_static_info("msg=\"update balancer stats\" threshold=%0.2f",
                      mThreshold);
      mBalanceStats.Update(&FsView::gFsView, mThreshold);
      vect_tx = mBalanceStats.GetTxEndpoints();
    }

    if (vect_tx.empty()) {
      eos_static_info("msg=\"no groups to balance\" wait=%is\"",
                      no_transfers_delay.count());
      assistant.wait_for(no_transfers_delay);
      continue;
    }

    bool no_slots = true;
    // Circular iterator over all the groups that need to be balanced with a
    // random starting point inside the vector
    auto it_start = GetRandomIter(vect_tx);
    auto it_current = it_start;

    do {
      const auto& src_fses = it_current->first;

      for (const auto& src : src_fses) {
        if (assistant.terminationRequested()) {
          break;
        }

        if (!mBalanceStats.HasTxSlot(src.mNodeInfo, mTxNumPerNode)) {
          eos_static_info("msg=\"exhaused transfers slots\" node=%s tx=%lu",
                          src.mNodeInfo.c_str(), mTxNumPerNode);
          continue;
        }

        while ((mThreadPool.GetQueueSize() > mMaxQueuedJobs) &&
               !assistant.terminationRequested()) {
          assistant.wait_for(std::chrono::seconds(1));
        }

        no_slots = false;
        FsBalanceInfo dst;
        const std::string src_node = src.mNodeInfo;
        const auto fid = GetFileToBalance(src, it_current->second, dst);
        const std::string dst_node = dst.mNodeInfo;

        if (fid == 0ull) {
          continue;
        }

        // Found file and destination file system where to balance it
        eos_static_info("msg=\"balance job\" fxid=%08llx src_fsid=%lu "
                        "dst_fsid=%lu", fid, src.mFsId, dst.mFsId);
        std::shared_ptr<DrainTransferJob> job {
          new DrainTransferJob(fid, src.mFsId, dst.mFsId, {}, {},
          true, "balance", true)};
        mThreadPool.PushTask<void>([ = ]() {
          job->UpdateMgmStats();
          job->DoIt();
          job->UpdateMgmStats();
          FreeTxSlot(fid, src_node, dst_node);
        });
        ++it_current;

        if (it_current == vect_tx.end()) {
          it_current = vect_tx.begin();
        }
      }
    } while ((it_current != it_start) && !assistant.terminationRequested());

    if (no_slots) {
      assistant.wait_for(std::chrono::seconds(no_slots_delay));
    }
  }

  while (mThreadPool.GetQueueSize() && mRunningJobs) {
    eos_static_info("msg=\"wait for balance jobs to finish\" queue_size=%lu",
                    mThreadPool.GetQueueSize());
    std::this_thread::sleep_for(std::chrono::seconds(5));
  }

  eos_static_info("msg=\"stopped file system balancer thread\" space=%s",
                  mSpaceName.c_str());
}

//------------------------------------------------------------------------------
// Get file identifier to balance from the given source file system
//------------------------------------------------------------------------------
const eos::IFileMD::id_t
FsBalancer::GetFileToBalance(const FsBalanceInfo& src,
                             const std::set<FsBalanceInfo>& set_dsts,
                             FsBalanceInfo& dst)
{
  int attempts = 10;
  const eos::common::FileSystem::fsid_t src_fsid = src.mFsId;
  eos::IFileMD::id_t random_fid {0ull};
  eos::common::FileSystem::fsid_t dst_fsid {0ul};

  while (attempts-- > 0) {
    if (gOFS->eosFsView->getApproximatelyRandomFileInFs(src_fsid, random_fid)) {
      if (!gOFS->mFidTracker.AddEntry(random_fid, TrackerType::Balance)) {
        eos_static_debug("msg=\"skip busy file identifier\" fxid=%08llx",
                         random_fid);
        continue;
      }

      std::set<uint32_t> avoid_fsids;
      eos::Prefetcher::prefetchFileMDAndWait(gOFS->eosView, random_fid);

      try {
        eos::common::RWMutexReadLock ns_rd_lock(gOFS->eosViewRWMutex);
        std::shared_ptr<eos::IFileMD> fmd = gOFS->eosFileService->getFileMD(random_fid);

        if (fmd) {
          auto loc = fmd->getLocations();
          avoid_fsids.insert(loc.cbegin(), loc.cend());
          loc = fmd->getUnlinkedLocations();
          avoid_fsids.insert(loc.cbegin(), loc.cend());
        }
      } catch (eos::MDException& e) {
        eos_static_err("msg=\"failed to find file\" fxid=%08llx", random_fid);
        gOFS->mFidTracker.RemoveEntry(random_fid);
        continue;
      }

      if (avoid_fsids.empty()) {
        gOFS->mFidTracker.RemoveEntry(random_fid);
        continue;
      }

      // Search for a suitable destination file system
      if (random_fid % 2 == 0) {
        for (auto it = set_dsts.cbegin(); it != set_dsts.cend(); ++it) {
          if ((avoid_fsids.find(it->mFsId) == avoid_fsids.end()) &&
              mBalanceStats.HasTxSlot(it->mNodeInfo, mTxNumPerNode)) {
            dst = *it;
            dst_fsid = dst.mFsId;
            break;
          }
        }
      } else {
        for (auto it = set_dsts.rbegin(); it != set_dsts.rend(); ++it) {
          if ((avoid_fsids.find(it->mFsId) == avoid_fsids.end()) &&
              mBalanceStats.HasTxSlot(it->mNodeInfo, mTxNumPerNode)) {
            dst = *it;
            dst_fsid = dst.mFsId;
            break;
          }
        }
      }

      if (dst_fsid == 0ul) {
        random_fid = 0ull;
        gOFS->mFidTracker.RemoveEntry(random_fid);
      } else {
        break;
      }
    }
  }

  if (random_fid) {
    mBalanceStats.TakeTxSlot(src.mNodeInfo, dst.mNodeInfo);
    ++mRunningJobs;
  }

  return random_fid;
}

//----------------------------------------------------------------------------
// Account for finished transfer by freeing up the slot and un-tracking the
// file identifier
//----------------------------------------------------------------------------
void
FsBalancer::FreeTxSlot(const eos::IFileMD::id_t& fid,
                       const std::string& src_node,
                       const std::string& dst_node)
{
  mBalanceStats.FreeTxSlot(src_node, dst_node);
  gOFS->mFidTracker.RemoveEntry(fid);
  --mRunningJobs;
}

EOSMGMNAMESPACE_END
