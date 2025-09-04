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

//------------------------------------------------------------------------------
//! Class RecyclePolicy
//------------------------------------------------------------------------------
class RecyclePolicy: public eos::common::LogId
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  RecyclePolicy() = default;

  //----------------------------------------------------------------------------
  //! Refresh policy if needed
  //!
  //! @param path path to recycle bin
  //----------------------------------------------------------------------------
  virtual void Refresh(const std::string& path);

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

public:
  bool mEnforced {false};
  time_t mKeepTime {0};
  double mSpaceKeepRatio {0.0};
  eos::IContainerMD::ctime_t mRecycleDirCtime {0, 0};
  std::atomic<unsigned long long> mLowSpaceWatermark {0ull};
  std::atomic<unsigned long long> mLowInodeWatermark {0ull};

  //----------------------------------------------------------------------------
  //! Get quota statistics for the recycle bin
  //!
  //! return map storing the quota information or empty if not quota
  //----------------------------------------------------------------------------
  virtual std::map<int, unsigned long long> GetQuotaStats();
};

EOSMGMNAMESPACE_END

