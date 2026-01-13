//------------------------------------------------------------------------------
// File: GroupDrainer.hh
// Author: Abhishek Lekshmanan - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2022 CERN/Switzerland                                  *
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

#pragma once
#include <string_view>
#include "common/AssistedThread.hh"
#include "common/Logging.hh"
#include "common/FileId.hh"
#include "common/FileSystem.hh"
#include "mgm/fsview/FsView.hh"
#include <vector>
#include <unordered_set>
#include "mgm/groupbalancer/BalancerEngineTypes.hh"
#include "mgm/groupdrainer/RetryTracker.hh"
#include "mgm/groupdrainer/DrainProgressTracker.hh"
#include "mgm/utils/FileSystemStatusUtils.hh"

namespace eos::mgm
{

namespace group_balancer
{
class BalancerEngine;
} // namespace group_balancer

constexpr uint32_t FID_CACHE_LIST_SZ = 1000;
constexpr uint32_t DEFAULT_NUM_TX = 1000;
constexpr uint64_t DEFAULT_CACHE_EXPIRY_TIME = 300;
constexpr uint16_t MAX_RETRIES = 5;


using mgm::group_balancer::GroupStatus;

class GroupDrainer: public eos::common::LogId
{
public:
  using cache_fid_map_t = std::map<eos::common::FileSystem::fsid_t,
        std::vector<eos::common::FileId::fileid_t>>;
  using drain_fs_map_t = std::map<std::string,
        std::vector<common::FileSystem::fsid_t>>;

  GroupDrainer(std::string_view spacename);
  ~GroupDrainer();
  void GroupDrain(ThreadAssistant& assistant) noexcept;

  /*!
   * Deteremine if an update is needed depending on the time point supplied
   * We check against the set value of mCacheExpiryTime which is configured
   * via groupdrainer.
   * @param tp  The time point of last update, this will be modified to current
   * time if an update happens
   * @param force Force an update. This param will be reset on update
   * @return
   */
  bool isUpdateNeeded(std::chrono::time_point<std::chrono::steady_clock>& tp,
                      bool& force);
  void pruneTransfers();
  bool isTransfersFull() const
  {
    std::scoped_lock slock(mTransfersMtx);
    return mTransfers.size() >= mMaxTransfers;
  }

  uint64_t getAllowedTransfers() const
  {
    std::scoped_lock slock(mTransfersMtx);

    // unlikely, we always call this after checking isTransfersFull()
    if (mMaxTransfers <= mTransfers.size()) {
      return 0;
    }

    return mMaxTransfers - mTransfers.size();
  }

  void prepareTransfers();
  void prepareTransfer(uint64_t index);
  void scheduleTransfer(eos::common::FileId::fileid_t fid,
                        const std::string& src_grp, const std::string& tgt_grp,
                        eos::common::FileSystem::fsid_t src_fsid);

  std::pair<bool, cache_fid_map_t::iterator>
  populateFids(eos::common::FileSystem::fsid_t fsid);

  void reconfigure()
  {
    mDoConfigUpdate.store(true, std::memory_order_release);
  }

  bool Configure(const std::string& spaceName);

  void addTransferEntry(eos::common::FileId::fileid_t fid)
  {
    std::scoped_lock slock(mTransfersMtx);
    mTransfers.emplace(fid);
    mTrackedTransfers.emplace(fid);
  }

  void dropTransferEntry(eos::common::FileId::fileid_t fid)
  {
    {
      std::scoped_lock slock(mTransfersMtx);
      mTransfers.erase(fid);
    }
    {
      std::scoped_lock slock(mFailedTransfersMtx);
      mFailedTransfers.erase(fid);
    }
  }

  void addFailedTransferEntry(eos::common::FileId::fileid_t fid,
                              std::string&& entry)
  {
    {
      std::scoped_lock slock(mFailedTransfersMtx);
      mFailedTransfers.emplace(fid, std::move(entry));
    }
    {
      std::scoped_lock slock(mTransfersMtx);
      mTransfers.erase(fid);
    }
  }

  // Returns if a transfer is tracked already by GroupDrainer, we are NOT
  // supposed to schedule if (trackedTransferEntry(fid)) returns true, as it means
  // we have already scheduled this transfer before.
  // We allow failed transfers to be rescheduled again.
  bool trackedTransferEntry(eos::common::FileId::fileid_t fid)
  {
    std::scoped_lock slock(mFailedTransfersMtx, mTransfersMtx);

    if (mFailedTransfers.count(fid)) {
      // Allow scheduling of failed transfers
      return false;
    }

    // return if we have a tracked transfer entry
    return mTrackedTransfers.find(fid) != mTrackedTransfers.end();
  }

  std::pair<bool, GroupDrainer::cache_fid_map_t::iterator>
  handleRetries(eos::common::FileSystem::fsid_t fsid,
                std::vector<eos::common::FileId::fileid_t>&& fids);

  enum class StatusFormat {
    NONE,
    DETAIL,
    MONITORING
  };

  std::string getStatus(StatusFormat status_fmt = StatusFormat::NONE) const;

  void resetFailedTransfers();
  void resetCaches();

  static GroupStatus
  checkGroupDrainStatus(const fsutils::fs_status_map_t& fs_map);

  //! Check the drain statuses of all FSes in a group and map this to
  //! a groupstatus. This might move a GroupDrainStatus in the future, but given
  //! that we don't have a separate GroupDrain info, we just map it back to
  //! a group status. This function is to be used to check the statuses of all FSes in a group.
  //! Do not use this to actually just check the status of a group
  //! from FSView!
  //! \param groupname the group whose status to check
  //! \return DrainFailed if one of the FSes is having a DrainFailed status
  //!         DrainComplete if all of the FSes have completed draining
  //!         Offline      if any of the FSes are offline
  //!         Online       any other status
  static GroupStatus checkGroupDrainStatus(const std::string& groupname);

  static bool isValidDrainCompleteStatus(GroupStatus s)
  {
    return s == GroupStatus::DRAINCOMPLETE ||
           s == GroupStatus::DRAINFAILED;
  }

  static bool setDrainCompleteStatus(const std::string& groupname,
                                     GroupStatus s);

  static bool isDrainFSMapEmpty(const drain_fs_map_t& drainFsMap);
private:
  bool mRefreshFSMap {true};
  bool mRefreshGroups {true};
  bool mPauseExecution {false};
  std::atomic<bool> mDoConfigUpdate {true};
  uint16_t mRetryCount; // < Max retries for failed transfers
  uint16_t mRRSeed {0};
  uint32_t mMaxTransfers; // < Max no of transactions to keep in flight
  uint64_t mRetryInterval; // < Retry Interval for failed transfers
  std::chrono::time_point<std::chrono::steady_clock> mLastUpdated;
  std::chrono::time_point<std::chrono::steady_clock> mDrainMapLastUpdated;
  std::chrono::seconds mCacheExpiryTime {300};

  std::string mSpaceName;
  AssistedThread mThread;
  std::unique_ptr<group_balancer::BalancerEngine> mEngine;

  group_balancer::engine_conf_t
  mDrainerEngineConf; ///< string k-v map of engine conf
  //! map tracking scheduled transfers, will be cleared periodically
  //! TODO: use a flat_map structure here, we are usually size capped to ~10K
  mutable std::mutex mTransfersMtx;
  mutable std::mutex mFailedTransfersMtx;
  std::unordered_set<eos::common::FileId::fileid_t> mTransfers;
  std::unordered_map<eos::common::FileId::fileid_t, std::string> mFailedTransfers;

  // TODO future: use a bloom filter here if we find heavy mem. usage
  // The only use case is to check if a file is not a member of a set, so a
  // perfect use case as we don't care about false +ve memberships
  std::unordered_set<eos::common::FileId::fileid_t> mTrackedTransfers;

  //! map holding a seed for RR picker for every Group for the FS
  std::map<std::string, uint16_t> mGroupFSSeed;

  //! a map holding the current list of FSes in the draining groups
  //! this is unlikely to have more than a single digit number of keys..maybe a
  //! a vector of pairs might be ok?
  mutable std::mutex
  mDrainFsMapMtx; ///< This mutex is only to sync. b/w UI thread
  // and the internal GroupDrainer Threads, there is no need for locking for
  // reads within GroupDrainer!

  drain_fs_map_t mDrainFsMap;
  std::map<common::FileSystem::fsid_t, RetryTracker> mFsidRetryCtr;
  std::set<common::FileSystem::fsid_t> mFailedFsids;
  cache_fid_map_t mCacheFileList;
  DrainProgressTracker mDrainProgressTracker;
};

} // namespace eos::mgm
