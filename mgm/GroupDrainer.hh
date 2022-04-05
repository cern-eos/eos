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
#include <vector>
#include <unordered_set>

namespace eos::mgm {

namespace group_balancer {
class BalancerEngine;
} // group_balancer

std::vector<eos::common::FileSystem::fsid_t>
FsidsinGroup(const string& groupname);

constexpr uint32_t FID_CACHE_LIST_SZ=1000;

class GroupDrainer: public eos::common::LogId {
public:
  using cache_fid_map_t = std::map<eos::common::FileSystem::fsid_t,
                                   std::vector<eos::common::FileId::fileid_t>>;
  GroupDrainer(std::string_view spacename);
  ~GroupDrainer();
  void GroupDrain(ThreadAssistant& assistant) noexcept;

  bool isUpdateNeeded(std::chrono::time_point<std::chrono::steady_clock>& tp,
                      bool force=false);
  void pruneTransfers();
  bool isTransfersFull() const {
    return mTransfers.size() > numTx;
  }

  void prepareTransfers();
  void prepareTransfer(uint64_t index);
  void scheduleTransfer(eos::common::FileId::fileid_t fid,
                        const string& src_grp, const string& tgt_grp);

  std::pair<bool, cache_fid_map_t::iterator>
  populateFids(eos::common::FileSystem::fsid_t fsid);

private:
  std::chrono::time_point<std::chrono::steady_clock> mLastUpdated;
  std::chrono::time_point<std::chrono::steady_clock> mDrainMapLastUpdated;
  std::chrono::seconds mCacheExpiryTime {300};
  bool mRefreshFSMap {true};
  bool mRefreshGroups {true};
  std::string mSpaceName;
  AssistedThread mThread;
  std::unique_ptr<group_balancer::BalancerEngine> mEngine;
  uint32_t numTx; // < Max no of transactions to keep in flight
  double mThreshold;
  //! map tracking scheduled transfers, will be cleared periodically
  //! TODO: use a flat_map structure here, we are usually size capped to ~10K
  std::unordered_set<eos::common::FileId::fileid_t> mTransfers;
  std::unordered_map<eos::common::FileId::fileid_t, std::string> mFailedTransfers;

  //! a map holding the current list of FSes in the draining groups
  //! this is unlikely to have more than a single digit number of keys..maybe a
  //! a vector of pairs might be ok?
  std::map<std::string, std::vector<common::FileSystem::fsid_t>> mDrainFsMap;
  cache_fid_map_t mCacheFileList;
  void ApplyDrainedStatus(unsigned int fsid);
};


} // namespace eos::mgm