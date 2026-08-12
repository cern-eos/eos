//------------------------------------------------------------------------------
// File: ScanRate.hh
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

#pragma once
#include "fst/Namespace.hh"
#include <chrono>
#include <string>
#include <sys/types.h>

EOSFSTNAMESPACE_BEGIN

class Load;

namespace utils
{

//------------------------------------------------------------------------------
//! Class ScanRateLimiter - throttles a sequential scan to a given rate in MB/s
//! and adjusts this rate depending on the IO load of the mountpoint.
//!
//! The limiter holds a schedule deadline which is advanced only by the amount
//! of data handed over since the previous call. Therefore a rate change never
//! re-prices the data which was already scanned and the time spent inside one
//! Throttle call is bounded by (chunk size / current rate). One limiter object
//! must be used per scan i.e. it is not thread-safe.
//------------------------------------------------------------------------------
class ScanRateLimiter {
public:
  //! Lowest rate in MB/s that the load based adjustment can drop to
  static constexpr int sMinRate = 5;

  //----------------------------------------------------------------------------
  //! Constructor
  //!
  //! @param rate initial scan rate in MB/s, if 0 or negative then the rate
  //!        limiting is disabled
  //! @param fst_load load object, if null then no rate adjustment is done
  //! @param dir_path path of the mountpoint being scanned
  //! @param max_rate maximum allowed scan rate in MB/s, restored whenever the
  //!        disk load drops below the threshold, 0 disables the restore
  //----------------------------------------------------------------------------
  ScanRateLimiter(int rate, Load* fst_load = nullptr, const std::string& dir_path = "",
                  int max_rate = 0);

  //----------------------------------------------------------------------------
  //! Throttle the calling thread so that the data handed over since the
  //! previous call does not exceed the current rate and adjust this rate
  //! depending on the IO load of the mountpoint.
  //!
  //! @param offset cumulative number of bytes processed since the scan started
  //----------------------------------------------------------------------------
  void Throttle(off_t offset);

  //----------------------------------------------------------------------------
  //! Get the current scan rate in MB/s
  //----------------------------------------------------------------------------
  inline int
  GetRate() const
  {
    return mRate;
  }

private:
  //----------------------------------------------------------------------------
  //! Adjust the current rate depending on the IO load of the mountpoint
  //----------------------------------------------------------------------------
  void AdjustRate();

  //----------------------------------------------------------------------------
  //! Reject rate values that can not be honoured - a negative rate used to
  //! produce a negative sleep interval which, converted to an unsigned type,
  //! blocked the scanner thread for years
  //----------------------------------------------------------------------------
  static inline int
  SanitizeRate(int rate)
  {
    return (rate > 0) ? rate : 0;
  }

  int mRate;            ///< Current scan rate in MB/s, 0 means no rate limiting
  int mMaxRate;         ///< Maximum allowed scan rate in MB/s
  Load* mFstLoad;       ///< Object for providing load information
  std::string mDirPath; ///< Mountpoint being scanned
  off_t mOffset{0};     ///< Offset handed over by the previous call
  //! Time by which the data handed over so far should have been scanned
  std::chrono::steady_clock::time_point mDeadline;
};

} // namespace utils

EOSFSTNAMESPACE_END
