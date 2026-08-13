//------------------------------------------------------------------------------
// File: ScanRate.cc
// Author: Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2026 CERN/Switzerland                                  *
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

#include "fst/utils/ScanRate.hh"
#include "fst/Load.hh"
#include <algorithm>
#include <thread>

EOSFSTNAMESPACE_BEGIN

namespace utils
{

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
ScanRateLimiter::ScanRateLimiter(int rate, Load* fst_load, const std::string& dir_path,
                                 int max_rate)
    : mRate(SanitizeRate(rate))
    , mMaxRate(SanitizeRate(max_rate))
    , mFstLoad(fst_load)
    , mDirPath(dir_path)
    , mDeadline(std::chrono::steady_clock::now())
{
}

//------------------------------------------------------------------------------
// Throttle the calling thread and adjust the rate
//------------------------------------------------------------------------------
void
ScanRateLimiter::Throttle(off_t offset)
{
  using namespace std::chrono;
  // The first call establishes the baseline, otherwise the whole prefix given
  // by the initial offset would be priced at the current rate
  const off_t delta = mHasOffset ? offset - mOffset : 0;
  mOffset = offset;
  mHasOffset = true;

  if (mRate && (delta > 0)) {
    // Time budget for the data handed over since the previous call. The
    // computation is done in 64 bit since (rate * 1024 * 1024) overflows an
    // int for rates above 2 GB/s.
    const microseconds budget(((uint64_t)delta * 1000000ull) /
                              ((uint64_t)mRate * 1024ull * 1024ull));
    const auto now_ts = steady_clock::now();

    // Never carry over credit from the past, otherwise a slow chunk would be
    // followed by an unthrottled burst
    if (mDeadline < now_ts) {
      mDeadline = now_ts;
    }

    mDeadline += budget;
    std::this_thread::sleep_until(mDeadline);
  }

  AdjustRate();
}

//------------------------------------------------------------------------------
// Adjust the current rate depending on the IO load of the mountpoint
//------------------------------------------------------------------------------
void
ScanRateLimiter::AdjustRate()
{
  if (!mRate || !mFstLoad || mDirPath.empty()) {
    return;
  }

  const double load = mFstLoad->GetDiskRate(mDirPath.c_str(), "millisIO") / 1000.0;

  if (load > 0.7) {
    // Adjust the rate which is in MB/s but no lower then sMinRate
    if (mRate > sMinRate) {
      mRate = std::max(sMinRate, (int)(0.9 * mRate));
    }
  } else if (mMaxRate) {
    mRate = mMaxRate;
  }
}

} // namespace utils

EOSFSTNAMESPACE_END
