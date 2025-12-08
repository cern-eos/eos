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
#include "mgm/FsView.hh"
#include "common/StringTokenizer.hh"
#include <XrdOuc/XrdOucErrInfo.hh>

EOSMGMNAMESPACE_BEGIN

//----------------------------------------------------------------------------
// Apply the recycle configuration stored in the configuration engine
//----------------------------------------------------------------------------
void RecyclePolicy::ApplyConfig(eos::mgm::FsView* fsview)
{
  using eos::common::StringTokenizer;
  std::string config = fsview->GetGlobalConfig("recycle");
  std::map<std::string, std::string> kv_map;
  auto pairs = StringTokenizer::split<std::list<std::string>>(config, ' ');

  for (const auto& pair : pairs) {
    auto kv = StringTokenizer::split<std::vector<std::string>>(pair, '=');

    if (kv.empty()) {
      eos_err("msg=\"unknown recycle config data\" data=\"%s\"", config.c_str());
      continue;
    }

    if (kv.size() == 1) {
      kv.emplace_back("");
    }

    kv_map.emplace(kv[0], kv[1]);
  }

  std::string msg;

  for (const auto& [key, value] : kv_map) {
    if (!Config(key, value, msg)) {
      eos_err("msg=\"failed to apply recycle config\" key=\"%s\" value=\"%s\" "
              "error=\"%s\"", key.c_str(), value.c_str(), msg.c_str());
    }
  }
}

//----------------------------------------------------------------------------
// Store the current running recycle configuration in the config engine
//----------------------------------------------------------------------------
bool RecyclePolicy::StoreConfig()
{
  std::ostringstream oss;
  oss << sKeepTimeKey << "=" << mKeepTimeSec.load() << " "
      << sRatioKey << "=" << mSpaceKeepRatio.load() << " "
      << sCollectKey << "=" << mCollectInterval.load().count() << " "
      << sRemoveKey << "=" << mRemoveInterval.load().count() << " "
      << sDryRunKey << "=" << (mDryRun.load() ? "yes" : "no") << " "
      << sEnforceKey << "=" << (mEnforced.load() ? "on" : "off");
  return FsView::gFsView.SetGlobalConfig("recycle", oss.str());
}

//----------------------------------------------------------------------------
// Apply configuration options to the recycle mechanism
//----------------------------------------------------------------------------
bool RecyclePolicy::Config(const std::string& key, const std::string& value,
                           std::string& msg)
{
  if (value.empty()) {
    return true;
  }

  if (key == sKeepTimeKey) {
    try {
      mKeepTimeSec = std::stoull(value);
    } catch (...) {
      msg = "error: recycle keep time conversion to ull failed";
      return false;
    }
  } else if (key == sRatioKey) {
    try {
      mSpaceKeepRatio = std::stod(value);
    } catch (...) {
      msg = "error: recycle keep ratio conversion to double failed";
      return false;
    }
  } else if (key == sCollectKey) {
    try {
      mCollectInterval = std::chrono::seconds(std::stoull(value));
    } catch (...) {
      msg = "error: recycle collect interval conversion failed";
      return false;
    }
  } else if (key == sRemoveKey) {
    try {
      mRemoveInterval = std::chrono::seconds(std::stoull(value));
    } catch (...) {
      msg = "error: recycle remove interval conversion failed";
      return false;
    }
  } else if (key == sDryRunKey) {
    mDryRun = (value == "yes");
  } else if (key == sEnforceKey) {
    if (value == "on") {
      mEnforced = true;
    } else if (value == "off") {
      mEnforced = false;
    } else {
      msg = "error: unknown value for recycle-enforce - expected on|off";
      return false;
    }
  } else {
    // Ignore unknown keys
    return true;
  }

  eos_static_info("msg=\"recycle config updated\" %s", Dump(" ").c_str());
  return StoreConfig();
}

//----------------------------------------------------------------------------
// Dump current active recycle policy
//----------------------------------------------------------------------------
std::string
RecyclePolicy::Dump(const std::string& delim) const
{
  std::ostringstream oss;
  oss << "enforced=" << (mEnforced.load() ? "on" : "off") << delim
      << "dry_run=" << (mDryRun.load() ? "yes" : "no") << delim
      << "keep_time_sec=" << mKeepTimeSec.load() << delim
      << "space_keep_ratio=" << mSpaceKeepRatio.load() << delim
      << "low_space_watermark=" << mLowSpaceWatermark.load() << delim
      << "low_inode_watermark=" << mLowInodeWatermark.load() << delim
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

    if ((mSpaceKeepRatio.load() > (1.0 * usedbytes / (maxbytes ? maxbytes :
                                   999999999))) &&
        (mSpaceKeepRatio.load() > (1.0 * usedfiles / (maxfiles ? maxfiles :
                                   999999999)))) {
      eos_static_debug("msg=\"skip recycle watermark update - ratio still low\" "
                       "space-ratio=%.02f inode-ratio=%.02f ratio=%.02f",
                       1.0 * usedbytes / (maxbytes ? maxbytes : 999999999),
                       1.0 * usedfiles / (maxfiles ? maxfiles : 999999999),
                       mSpaceKeepRatio.load());
      return;
    } else {
      // Make local copy to avoid modifying the original space ratio
      double space_ratio = mSpaceKeepRatio.load();

      if (space_ratio - 0.1 > 0) {
        space_ratio -= 0.1;
      }

      mLowInodeWatermark = (maxfiles * space_ratio);
      mLowSpaceWatermark = (maxbytes * space_ratio);
      eos_static_info("msg=\"cleaning by ratio policy\" low-inodes-mark=%lld "
                      "low-space-mark=%lld ratio=%.02f", mLowInodeWatermark.load(),
                      mLowSpaceWatermark.load(), mSpaceKeepRatio.load());
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
  if (mSpaceKeepRatio.load()) {
    auto map_quotas = GetQuotaStats();

    if (!map_quotas.empty()) {
      unsigned long long usedbytes = map_quotas[SpaceQuota::kGroupLogicalBytesIs];
      unsigned long long usedfiles = map_quotas[SpaceQuota::kGroupFilesIs];
      eos_static_debug("volume=%lld volume_low_wm=%lld "
                       "inodes=%lld inodes_low_wm=%lld",
                       usedbytes, mLowSpaceWatermark.load(),
                       usedfiles, mLowInodeWatermark.load());

      if ((mLowInodeWatermark.load() && (mLowInodeWatermark.load() > usedfiles)) ||
          (mLowSpaceWatermark.load() && (mLowSpaceWatermark.load() > usedbytes))) {
        return true;
      }
    }
  }

  eos_static_debug("%s", "msg=\"do cleanup, space ratio not configured or "
                   "above watermark limits\"");
  return false;
}

EOSMGMNAMESPACE_END
