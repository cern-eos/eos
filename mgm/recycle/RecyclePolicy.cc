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
  auto it = attr_map.find(Recycle::gRecyclingKeepRatio);

  if (it != attr_map.end()) {
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
  it = attr_map.find(Recycle::gRecyclingTimeAttribute);

  if (it != attr_map.end()) {
    try {
      mKeepTime = std::stoull(it->second);
    } catch (...) {
      mKeepTime = 0ull;
      eos_static_err("msg=\"recycle keep time conversion to ull failed\""
                     " val=\"%s\"", it->second.c_str());
    }
  } else {
    mKeepTime = 0ull;
  }

  if (mKeepTime || mSpaceKeepRatio) {
    mEnforced = true;
  }
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
      eos_static_debug("msg=\"skipping recycle clean-up - ratio still low\" "
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
                      "low-space-mark=%lld ratio=%.02f",
                      mLowInodeWatermark.load(), mLowSpaceWatermark.load(),
                      mSpaceKeepRatio);
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
    if (mLowInodeWatermark && mLowSpaceWatermark) {
      auto map_quotas = GetQuotaStats();

      if (!map_quotas.empty()) {
        unsigned long long usedbytes = map_quotas[SpaceQuota::kGroupLogicalBytesIs];
        unsigned long long usedfiles = map_quotas[SpaceQuota::kGroupFilesIs];
        eos_static_debug("volume=%lld volume_low_wm=%lld "
                         "inodes=%lld inodes_low_wn=%lld",
                         usedfiles, mLowInodeWatermark.load(),
                         usedbytes, mLowSpaceWatermark.load());

        if ((mLowInodeWatermark < usedfiles) ||
            (mLowSpaceWatermark < usedbytes)) {
          return false;
        }
      }
    }
  }

  eos_static_debug("%s",
                   "msg=\"skip recycle clean-up, space ratio not configured "
                   " or below watermark limits\"");
  return true;
}

EOSMGMNAMESPACE_END
