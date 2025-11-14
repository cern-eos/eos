//------------------------------------------------------------------------------
// File: RecyclePolicy.cc
// Author: Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/****************************(********************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2025 CERN/Switzerland                                  *
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

#include "mgm/recycle/RecyclePolicy.hh"
#include "mgm/recycle/Recycle.hh"
#include "mgm/Quota.hh"
#include "mgm/XrdMgmOfs.hh"
#include <XrdOuc/XrdOucErrInfo.hh>

EOSMGMNAMESPACE_BEGIN

//----------------------------------------------------------------------------
// Refresh recycle policy if needed
//----------------------------------------------------------------------------
void RecyclePolicy::Refresh(const std::string& path)
{
  // Check if recycle directory metadata modified in the meantime
  eos::IContainerMD::ctime_t new_ctime;

  try {
    auto cmd = gOFS->eosView->getContainer(path, false);
    auto cmd_rlock = eos::MDLocking::readLock(cmd.get());
    cmd->getCTime(new_ctime);
  } catch (eos::MDException& e) {
    errno = e.getErrno();
    eos_debug("msg=\"exception\" ec=%d emsg=\"%s\"\n", e.getErrno(),
              e.getMessage().str().c_str());
    mEnforced = false;
    return;
  }

  if ((mRecycleDirCtime.tv_sec == new_ctime.tv_sec) &&
      (mRecycleDirCtime.tv_nsec == new_ctime.tv_nsec)) {
    // No need for a refresh
    return;
  }

  mRecycleDirCtime = new_ctime;
  // Do a refresh
  XrdOucErrInfo err_obj;
  eos::IContainerMD::XAttrMap attr_map;
  eos::common::VirtualIdentity root_vid = eos::common::VirtualIdentity::Root();

  if (gOFS->_attr_ls(path.c_str(), err_obj, root_vid, "", attr_map)) {
    eos_static_err("msg=\"unable to get attributes for recycle\" path=\"%s\"",
                   path.c_str());
    mEnforced = false;
    return;
  }

  // Get the space keep ratio
  if (auto it = attr_map.find(Recycle::gRecyclingKeepRatio);
      it != attr_map.end()) {
    try {
      mSpaceKeepRatio = std::stod(it->second);
    } catch (...) {
      mSpaceKeepRatio = 0.0;
      eos_static_err("msg=\"recycle keep ratio conversion to double failed\""
                     " val=\"%s\"", it->second.c_str());
    }
  } else {
    mSpaceKeepRatio = 0.0;
  }

  // Get the time keep value
  if (auto it = attr_map.find(Recycle::gRecyclingTimeAttribute);
      it != attr_map.end()) {
    try {
      mKeepTimeSec = std::stoull(it->second);
    } catch (...) {
      mKeepTimeSec = 0ull;
      eos_static_err("msg=\"recycle keep time conversion to ull failed\""
                     " val=\"%s\"", it->second.c_str());
    }
  } else {
    mKeepTimeSec = 0ull;
  }

  // Get the collect interval
  if (auto it = attr_map.find(Recycle::gRecyclingCollectInterval);
      it != attr_map.end()) {
    try {
      mCollectInterval = std::chrono::seconds(std::stoull(it->second));
    } catch (...) {
      // No changes to the default collect interval
      eos_static_err("msg=\"recycle collect interval conversion failed\" "
                     "val=\"%s\"", it->second.c_str());
    }
  }

  // Get the remove interval
  if (auto it = attr_map.find(Recycle::gRecyclingRemoveInterval);
      it != attr_map.end()) {
    try {
      mRemoveInterval = std::chrono::seconds(std::stoull(it->second));
    } catch (...) {
      // No changes to the default remove interval
      eos_static_err("msg=\"recycle remove interval conversion failed\" "
                     "val=\"%s\"", it->second.c_str());
    }
  }

  // Get the dry-run mode
  if (auto it = attr_map.find(Recycle::gRecyclingDryRunAttribute);
      it != attr_map.end()) {
    if (it->second == "yes") {
      mDryRun = true;
    } else {
      mDryRun = false;
    }
  }

  if (mKeepTimeSec || mSpaceKeepRatio) {
    mEnforced = true;
  }

  eos_static_info("msg=\"recycle config refresh\" %s", Dump(" ").c_str());
}

//----------------------------------------------------------------------------
// Dump current active recycle policy
//----------------------------------------------------------------------------
std::string
RecyclePolicy::Dump(const std::string& delim) const
{
  std::ostringstream oss;
  oss << "enforced=" << (mEnforced ? "on" : "off") << delim
      << "dry_run=" << (mDryRun ? "yes" : "no") << delim
      << "keep_time_sec=" << mKeepTimeSec << delim
      << "space_keep_ratio=" << mSpaceKeepRatio << delim
      << "low_space_watermark=" << mLowSpaceWatermark << delim
      << "low_inode_watermark=" << mLowInodeWatermark << delim
      << "collect_interval_sec=" << mCollectInterval.load().count() << delim
      << "remove_interval_sec=" << mRemoveInterval.load().count() << delim;
  return oss.str();
}

//----------------------------------------------------------------------------
// Get quota statistics for the recycle bin
//----------------------------------------------------------------------------
std::map<int, unsigned long long>
RecyclePolicy::GetQuotaStats()
{
  return Quota::GetGroupStatistics(Recycle::gRecyclingPrefix,
                                   Quota::gProjectId);
}

//------------------------------------------------------------------------------
// Refresh watermark values based on the configured quota
//------------------------------------------------------------------------------
void
RecyclePolicy::RefreshWatermarks()
{
  auto map_quotas = GetQuotaStats();

  if (!map_quotas.empty()) {
    unsigned long long usedbytes = map_quotas[SpaceQuota::kGroupLogicalBytesIs];
    unsigned long long maxbytes = map_quotas[SpaceQuota::kGroupLogicalBytesTarget];
    unsigned long long usedfiles = map_quotas[SpaceQuota::kGroupFilesIs];
    unsigned long long maxfiles = map_quotas[SpaceQuota::kGroupFilesTarget];

    if ((mSpaceKeepRatio > (1.0 * usedbytes / (maxbytes ? maxbytes : 999999999))) &&
        (mSpaceKeepRatio > (1.0 * usedfiles / (maxfiles ? maxfiles : 999999999)))) {
      eos_static_debug("msg=\"skip recycle watermark update - ratio still low\" "
                       "space-ratio=%.02f inode-ratio=%.02f ratio=%.02f",
                       1.0 * usedbytes / (maxbytes ? maxbytes : 999999999),
                       1.0 * usedfiles / (maxfiles ? maxfiles : 999999999),
                       mSpaceKeepRatio);
      return;
    } else {
      // Make local copy to avoid modifying the original space ratio
      double space_ratio = mSpaceKeepRatio;

      if (space_ratio - 0.1 > 0) {
        space_ratio -= 0.1;
      }

      mLowInodeWatermark = (maxfiles * space_ratio);
      mLowSpaceWatermark = (maxbytes * space_ratio);
      eos_static_info("msg=\"cleaning by ratio policy\" low-inodes-mark=%lld "
                      "low-space-mark=%lld ratio=%.02f", mLowInodeWatermark,
                      mLowSpaceWatermark, mSpaceKeepRatio);
    }
  } else {
    mLowInodeWatermark = 0ull;
    mLowSpaceWatermark = 0ull;
  }
}

//------------------------------------------------------------------------------
// Check based on the quota information if we are within the watermark
//------------------------------------------------------------------------------
bool
RecyclePolicy::IsWithinLimits()
{
  if (mSpaceKeepRatio) {
    auto map_quotas = GetQuotaStats();

    if (!map_quotas.empty()) {
      unsigned long long usedbytes = map_quotas[SpaceQuota::kGroupLogicalBytesIs];
      unsigned long long usedfiles = map_quotas[SpaceQuota::kGroupFilesIs];
      eos_static_debug("volume=%lld volume_low_wm=%lld "
                       "inodes=%lld inodes_low_wm=%lld",
                       usedbytes, mLowSpaceWatermark,
                       usedfiles, mLowInodeWatermark);

      if ((mLowInodeWatermark && (mLowInodeWatermark > usedfiles)) ||
          (mLowSpaceWatermark && (mLowSpaceWatermark > usedbytes))) {
        return true;
      }
    }
  }

  eos_static_debug("%s", "msg=\"do cleanup, space ratio not configured or "
                   "above watermark limits\"");
  return false;
}

EOSMGMNAMESPACE_END
