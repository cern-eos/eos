//------------------------------------------------------------------------------
//! @file FsBalancerStats.cc
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

#include "mgm/balancer/FsBalancerStats.hh"

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Update statistics with information from the group and file systems stats
//------------------------------------------------------------------------------
void
FsBalancerStats::UpdateInfo(eos::mgm::FsView* fs_view, double threshold)
{
  std::set<std::string> grp_to_remove;
  std::set<std::string> grp_to_update;
  auto grp_dev = fs_view->GetUnbalancedGroups(mSpaceName, threshold);

  for (const auto& elem : grp_dev) {
    // Check if group needs to be added or updated
    auto it = mGrpToMaxDev.find(elem.first);

    if (it != mGrpToMaxDev.end()) {
      // Check against the cached value
      const double grp_dev = it->second.first;
      const auto last_upd_ts = it->second.second;
      eos_static_info("msg=\"compare group max abs deviation\" group=%s "
                      "current=%0.2f new=%0.2f last_update_ts=%llu",
                      elem.first.c_str(), grp_dev, elem.second,
                      std::chrono::duration_cast<std::chrono::milliseconds>
                      (last_upd_ts.time_since_epoch()).count());
      bool do_update = false;

      // Trigger update due to group max dev changes
      if (std::abs(grp_dev - std::floor(elem.second)) >= sGrpDevUpdThreshold) {
        do_update = true;
      }

      // Trigger time based update
      if (!do_update &&
          (std::chrono::duration_cast<std::chrono::minutes>
           (std::chrono::steady_clock::now() - last_upd_ts).count() >=
           sGrpUpdTimeThreshold)) {
        do_update = true;
      }

      if (do_update) {
        grp_to_update.insert(elem.first);
      }
    } else {
      grp_to_update.insert(elem.first);
    }
  }

  // Check if there are groups that need to be removed
  for (const auto& elem : mGrpToMaxDev) {
    if (grp_dev.find(elem.first) == grp_dev.end()) {
      grp_to_remove.insert(elem.first);
    }
  }

  for (const auto& grp : grp_to_remove) {
    mGrpToMaxDev.erase(grp);
    mGrpToPrioritySets.erase(grp);
  }

  if (grp_to_update.empty()) {
    return;
  }

  for (const auto& grp : grp_to_update) {
    mGrpToMaxDev[grp] = std::make_pair(grp_dev[grp],
                                       std::chrono::steady_clock::now());
    mGrpToPrioritySets[grp] =
      fs_view->GetFsToBalance(grp, FsPrioritySets::sThreshold);
  }

  return;
}

//------------------------------------------------------------------------------
// Decide if an update of the data structures is needed
//------------------------------------------------------------------------------
bool
FsBalancerStats::NeedsUpdate(std::chrono::seconds upd_interval)
{
  using namespace std::chrono;

  // Trigger update if interval elapsed
  if (duration_cast<seconds>(system_clock::now() - mLastTs).count()
      >= upd_interval.count()) {
    mLastTs = system_clock::now();
    return true;
  }

  return false;
}

//----------------------------------------------------------------------------
// Get vector of balance source and destination file systems to be used for
// doing transfers, one entry per group to be balanced
//----------------------------------------------------------------------------
VectBalanceFs
FsBalancerStats::GetTxEndpoints()
{
  VectBalanceFs ret;

  for (auto& elem : mGrpToPrioritySets) {
    std::set<FsBalanceInfo>& dst_fses = elem.second.mPrioLow;

    if (dst_fses.empty()) {
      dst_fses = elem.second.mLow;

      if (dst_fses.empty()) {
        continue;
      }
    }

    std::set<FsBalanceInfo>& src_fses = elem.second.mPrioHigh;

    if (src_fses.empty()) {
      src_fses = elem.second.mHigh;

      if (src_fses.empty()) {
        continue;
      }
    }

    ret.emplace_back(std::make_pair(src_fses, dst_fses));
  }

  return ret;
}

//------------------------------------------------------------------------------
// Check if node still has avilable transfer slots
//------------------------------------------------------------------------------
bool
FsBalancerStats::HasTxSlot(const std::string& node_id,
                           unsigned int tx_per_node) const
{
  std::unique_lock<std::mutex> scope_lock(mMutex);
  auto it = mNodeNumTx.find(node_id);

  if (it == mNodeNumTx.end()) {
    return true;
  }

  if (it->second < tx_per_node) {
    return true;
  }

  return false;
}


//------------------------------------------------------------------------------
// Account for new transfer slot
//----------------------------------------------------------------------------
void
FsBalancerStats::TakeTxSlot(const std::string& src_node,
                            const std::string& dst_node)
{
  std::unique_lock<std::mutex> scope_lock(mMutex);
  ++mNodeNumTx[src_node];
  ++mNodeNumTx[dst_node];
}

//----------------------------------------------------------------------------
//! Account for finished transfer by freeing up a slot
//----------------------------------------------------------------------------
void
FsBalancerStats::FreeTxSlot(const std::string& src_node,
                            const std::string& dst_node)
{
  std::unique_lock<std::mutex> scope_lock(mMutex);
  auto it = mNodeNumTx.find(src_node);

  if (it != mNodeNumTx.end() && it->second) {
    --it->second;
  }

  it = mNodeNumTx.find(dst_node);

  if (it != mNodeNumTx.end() && it->second) {
    --it->second;
  }
}

EOSMGMNAMESPACE_END
