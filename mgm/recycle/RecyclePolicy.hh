//------------------------------------------------------------------------------
// File: RecyclePolicy.hh
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

#pragma once
#include "mgm/Namespace.hh"
#include "common/Logging.hh"
#include "common/Mapping.hh"
#include "namespace/interface/IView.hh"
#include <atomic>

EOSMGMNAMESPACE_BEGIN

//! Forward declarations
class FsView;

//------------------------------------------------------------------------------
//! Class RecyclePolicy
//------------------------------------------------------------------------------
class RecyclePolicy: public eos::common::LogId
{
public:
  friend class Recycle;

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  RecyclePolicy() = default;

  //----------------------------------------------------------------------------
  //! Apply the recycle configuration stored in the configuration engine
  //!
  //! @param fsview file system view info
  //----------------------------------------------------------------------------
  void ApplyConfig(eos::mgm::FsView* fsview);

  //----------------------------------------------------------------------------
  //! Store the current running recycle configuration in the config engine
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  virtual bool StoreConfig();

  //----------------------------------------------------------------------------
  //! Apply configuration options to the recycle mechanism
  //!
  //! @param key configuration key
  //! @param value configuration value
  //! @param msg output message/error
  //!
  //! @return true if successful, otherwise false
  //----------------------------------------------------------------------------
  bool Config(const std::string& key, const std::string& value, std::string& msg);

  //----------------------------------------------------------------------------
  //! Refresh watermark values based on the configured quota
  //----------------------------------------------------------------------------
  void RefreshWatermarks();

  //----------------------------------------------------------------------------
  //! Check based on the quota information if we are within the watermark
  //! limits. If no space keep ratio set then we consider this condition false
  //! so that time based cleanup can still continue.
  //!
  //! @return true if within watermark limits, false otherwise
  //----------------------------------------------------------------------------
  bool IsWithinLimits();

  static constexpr auto sKeepTimeKey = "recycle-keep-time";
  static constexpr auto sRatioKey = "recycle-ratio";
  static constexpr auto sCollectKey = "recycle-collect-time";
  static constexpr auto sRemoveKey = "recycle-remove-time";
  static constexpr auto sDryRunKey = "recycle-dry-run";
  static constexpr auto sEnforceKey = "recycle-enforce";
  static constexpr auto sEnableKey = "recycle-enable";
#ifdef IN_TEST_HARNESS
public:
#endif
  //! Mark that recycle is enabled
  std::atomic<bool> mEnabled {true};
  //! Mark if recycle is enforced instance wide
  std::atomic<bool> mEnforced {false};
  std::atomic<uint64_t> mKeepTimeSec {0};
  std::atomic<double> mSpaceKeepRatio {0.0};
  //! Flag if we are in dry-run mode or not
  std::atomic<bool> mDryRun {false};
  //! How often the collection of entries is happening, default 1 day
  std::atomic<std::chrono::seconds> mCollectInterval =
    std::chrono::seconds(24 * 3600);
  //! How often the removal of entries is happening, default 1 hour
  std::atomic<std::chrono::seconds> mRemoveInterval =
    std::chrono::seconds(3600);

  std::atomic<unsigned long long> mLowSpaceWatermark {0ull};
  std::atomic<unsigned long long> mLowInodeWatermark {0ull};

  //----------------------------------------------------------------------------
  //! Get quota statistics for the recycle bin
  //!
  //! return map storing the quota information or empty if not quota
  //----------------------------------------------------------------------------
  virtual std::map<int, unsigned long long> GetQuotaStats();

  //----------------------------------------------------------------------------
  //! Dump current active recycle policy
  //!
  //! @param delim delimiter between output entries
  //----------------------------------------------------------------------------
  std::string Dump(const std::string& delim = "\n") const;
};

EOSMGMNAMESPACE_END
