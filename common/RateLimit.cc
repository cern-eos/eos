//------------------------------------------------------------------------------
//! @file RateLimit.cc
//! @author Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#include "common/RateLimit.hh"
#include <chrono>
#include <iostream>
#include <stdexcept>

using namespace std::chrono;

EOSCOMMONNAMESPACE_BEGIN

//----------------------------------------------------------------------------
//! Get delay for the current request
//!
//! @return delay in microseconds for the current requestor
//----------------------------------------------------------------------------
microseconds
RequestRateLimit::GetDelay()
{
  // If no rate limit enforced then no delay
  if (mRate == 0ull) {
    return microseconds(0);
  }

  std::lock_guard<std::mutex> lock(mMutex);
  // Compute expire timestamp which is one second back in time
  uint64_t now_us = duration_cast<microseconds>
                    (mClock.GetTime().time_since_epoch()).count();
  // Round to the beginning of the interval
  now_us = (now_us / mRateIntervalUs) * mRateIntervalUs;
  uint64_t expire_us = now_us - 1000000;

  // Try to expire old entries if any present
  if (!mSetReqTimestamps.empty()) {
    auto it = mSetReqTimestamps.lower_bound(expire_us);

    if (it != mSetReqTimestamps.end()) {
      // If current entry is after the expire timestamp we have to go one
      // back unless this is the beginning
      if ((it != mSetReqTimestamps.begin()) && (*it > expire_us)) {
        --it;
      }
    }

    if (it == mSetReqTimestamps.begin()) {
      if (*it <= expire_us) {
        (void) mSetReqTimestamps.erase(it);
      }
    } else {
      // Delete all the entries before this iterator i.e. expired ones
      for (auto it_del = mSetReqTimestamps.begin(); it_del != it; /*no inc*/) {
        it_del = mSetReqTimestamps.erase(it_del);
      }
    }
  }

  uint64_t new_entry_us {now_us};
  uint64_t num_requests = mSetReqTimestamps.size();
  bool has_delay = false;

  if (num_requests >= mRate) {
    has_delay =  true;
    // We need to wait for the first entry in the set to expire
    new_entry_us = *mSetReqTimestamps.begin() + 1000000;
    auto it = mSetReqTimestamps.lower_bound(new_entry_us);

    // If that slot is taken already then move on to the next one
    while (it != mSetReqTimestamps.end()) {
      new_entry_us += mRateIntervalUs;
      it = mSetReqTimestamps.lower_bound(new_entry_us);
    }
  } else {
    // If the current slot is taken then try the next one
    while (mSetReqTimestamps.find(new_entry_us) != mSetReqTimestamps.end()) {
      new_entry_us += mRateIntervalUs;
    }
  }

  mSetReqTimestamps.insert(new_entry_us);
  uint64_t delay {0ull};

  if (has_delay) {
    delay = new_entry_us - now_us;
  }

  // This is for testing purposes only
  if (mLastTimestampUs < new_entry_us) {
    mLastTimestampUs = new_entry_us;
  }

  return microseconds(delay);
}

EOSCOMMONNAMESPACE_END
